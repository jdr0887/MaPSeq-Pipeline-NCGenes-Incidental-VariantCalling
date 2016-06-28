package edu.unc.mapseq.commons.ncgenes.incidental.variantcalling;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.module.sequencing.gatk.GATKUnifiedGenotyper;
import edu.unc.mapseq.module.sequencing.picard.PicardAddOrReplaceReadGroups;
import edu.unc.mapseq.workflow.SystemType;
import edu.unc.mapseq.workflow.core.WorkflowUtil;
import edu.unc.mapseq.workflow.sequencing.IRODSBean;

public class RegisterToIRODSRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RegisterToIRODSRunnable.class);

    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    private String version;

    private String incidental;

    private Long sampleId;

    private SystemType system;

    public RegisterToIRODSRunnable(MaPSeqDAOBeanService maPSeqDAOBeanService, String version, String incidental, Long sampleId,
            SystemType system) {
        super();
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
        this.version = version;
        this.incidental = incidental;
        this.sampleId = sampleId;
        this.system = system;
    }

    @Override
    public void run() {

        Set<Sample> sampleSet = new HashSet<Sample>();
        SampleDAO sampleDAO = maPSeqDAOBeanService.getSampleDAO();

        if (sampleId != null) {
            try {
                sampleSet.add(sampleDAO.findById(sampleId));
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
                return;
            }
        }

        Workflow ncgenesWorkflow = null;
        try {
            ncgenesWorkflow = maPSeqDAOBeanService.getWorkflowDAO().findByName("NCGenesBaseline").get(0);
        } catch (MaPSeqDAOException e1) {
            e1.printStackTrace();
        }

        for (Sample sample : sampleSet) {

            if ("Undetermined".equals(sample.getBarcode())) {
                continue;
            }

            File outputDirectory = new File(sample.getOutputDirectory(), "NCGenesBaseline");
            File tmpDir = new File(outputDirectory, "tmp");
            if (!tmpDir.exists()) {
                tmpDir.mkdirs();
            }

            if (version == null & incidental == null) {
                logger.warn("Both version and incidental id were null...returning empty irods post-run registration dag");
                return;
            }

            Set<FileData> fileDataSet = sample.getFileDatas();

            File bamFile = WorkflowUtil.findFileByJobAndMimeTypeAndWorkflowId(maPSeqDAOBeanService, fileDataSet,
                    PicardAddOrReplaceReadGroups.class, MimeType.APPLICATION_BAM, ncgenesWorkflow.getId());

            if (bamFile == null) {
                File ncgenesDirectory = new File(sample.getOutputDirectory(), "NCGenesBaseline");
                for (File file : ncgenesDirectory.listFiles()) {
                    if (file.getName().endsWith(".fixed-rg.bam")) {
                        bamFile = file;
                        break;
                    }
                }
            }

            if (bamFile == null) {
                logger.error("bam file to process was not found");
                return;
            }

            // assumption: a dash is used as a delimiter between a participantId
            // and the external code
            int idx = sample.getName().lastIndexOf("-");
            String participantId = idx != -1 ? sample.getName().substring(0, idx) : sample.getName();

            String irodsDirectory = String.format("/MedGenZone/sequence_data/ncgenes/%s", participantId);

            CommandOutput commandOutput = null;

            List<CommandInput> commandInputList = new ArrayList<CommandInput>();

            CommandInput commandInput = new CommandInput();
            commandInput.setExitImmediately(Boolean.FALSE);
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("$IRODS_HOME/imkdir -p %s%n", irodsDirectory));
            sb.append(String.format("$IRODS_HOME/imeta add -C %s Project NCGENES%n", irodsDirectory));
            sb.append(String.format("$IRODS_HOME/imeta add -C %s ParticipantID %s NCGENES%n", irodsDirectory, participantId));
            commandInput.setCommand(sb.toString());
            commandInput.setWorkDir(tmpDir);
            commandInputList.add(commandInput);

            // set recal out file
            String gatkTableRecalibrationOut = bamFile.getName().replace(".bam", ".deduped.realign.fixmate.recal.bam");

            List<IRODSBean> files2RegisterToIRODS = new LinkedList<IRODSBean>();

            List<ImmutablePair<String, String>> attributeList = Arrays.asList(
                    new ImmutablePair<String, String>("ParticipantId", participantId),
                    new ImmutablePair<String, String>("MaPSeqWorkflowVersion", version),
                    new ImmutablePair<String, String>("MaPSeqWorkflowName", "NCGenesIncidentalVariantCalling"),
                    new ImmutablePair<String, String>("MaPSeqStudyName", sample.getStudy().getName()),
                    new ImmutablePair<String, String>("MaPSeqSampleId", sample.getId().toString()),
                    new ImmutablePair<String, String>("MaPSeqSystem", system.getValue()),
                    new ImmutablePair<String, String>("MaPSeqFlowcellId", sample.getFlowcell().getId().toString()),
                    new ImmutablePair<String, String>("IncidentalID", incidental),
                    new ImmutablePair<String, String>("IncidentalVersion", version));

            File filterVariant1Output = new File(outputDirectory, gatkTableRecalibrationOut.replace(".bam", ".vcf"));
            File incidentalVcf = new File(outputDirectory,
                    filterVariant1Output.getName().replace(".vcf", String.format(".incidental-%s.v-%s.vcf", incidental, version)));

            if (!incidentalVcf.exists()) {
                outputDirectory = new File(sample.getOutputDirectory(), "NCGenesIncidentalVariantCalling");
                incidentalVcf = new File(outputDirectory,
                        filterVariant1Output.getName().replace(".vcf", String.format(".incidental-%s.v-%s.vcf", incidental, version)));
            }

            List<ImmutablePair<String, String>> attributeListWithJob = new ArrayList<>(attributeList);
            attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKUnifiedGenotyper.class.getSimpleName()));
            attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_VCF.toString()));
            files2RegisterToIRODS.add(new IRODSBean(incidentalVcf, attributeListWithJob));

            for (IRODSBean bean : files2RegisterToIRODS) {

                File f = bean.getFile();
                if (!f.exists()) {
                    logger.warn("file to register doesn't exist: {}", f.getAbsolutePath());
                    continue;
                }

                commandInput = new CommandInput();
                commandInput.setExitImmediately(Boolean.FALSE);

                StringBuilder registerCommandSB = new StringBuilder();
                String registrationCommand = String.format("$IRODS_HOME/ireg -f %s %s/%s", bean.getFile().getAbsolutePath(), irodsDirectory,
                        bean.getFile().getName());
                String deRegistrationCommand = String.format("$IRODS_HOME/irm -U %s/%s", irodsDirectory, bean.getFile().getName());
                registerCommandSB.append(registrationCommand).append("\n");
                registerCommandSB.append(String.format("if [ $? != 0 ]; then %s; %s; fi%n", deRegistrationCommand, registrationCommand));
                commandInput.setCommand(registerCommandSB.toString());
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                commandInput = new CommandInput();
                commandInput.setExitImmediately(Boolean.FALSE);
                sb = new StringBuilder();
                for (ImmutablePair<String, String> attribute : bean.getAttributes()) {
                    sb.append(String.format("$IRODS_HOME/imeta add -d %s/%s %s %s NCGenes%n", irodsDirectory, bean.getFile().getName(),
                            attribute.getLeft(), attribute.getRight()));
                }
                commandInput.setCommand(sb.toString());
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

            }

            File mapseqrc = new File(System.getProperty("user.home"), ".mapseqrc");
            Executor executor = BashExecutor.getInstance();

            for (CommandInput ci : commandInputList) {
                try {
                    logger.info("ci.getCommand(): {}", ci.getCommand());
                    commandOutput = executor.execute(ci, mapseqrc);
                    logger.info("commandOutput.getExitCode(): {}", commandOutput.getExitCode());
                    logger.debug("commandOutput.getStdout(): {}", commandOutput.getStdout());
                } catch (ExecutorException e) {
                    if (commandOutput != null) {
                        logger.warn("commandOutput.getStderr(): {}", commandOutput.getStderr());
                    }
                }
            }

        }

    }

    public MaPSeqDAOBeanService getMaPSeqDAOBeanService() {
        return maPSeqDAOBeanService;
    }

    public void setMaPSeqDAOBeanService(MaPSeqDAOBeanService maPSeqDAOBeanService) {
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getIncidental() {
        return incidental;
    }

    public void setIncidental(String incidental) {
        this.incidental = incidental;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

}
