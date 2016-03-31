package edu.unc.mapseq.commons.ncgenes.incidental.variantcalling;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.config.RunModeType;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.module.sequencing.picard.PicardAddOrReplaceReadGroups;
import edu.unc.mapseq.workflow.impl.IRODSBean;
import edu.unc.mapseq.workflow.impl.SampleWorkflowUtil;

public class RegisterToIRODSRunnable implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(RegisterToIRODSRunnable.class);

    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    private RunModeType runMode;

    private String version;

    private String incidental;

    private Long sampleId;

    public RegisterToIRODSRunnable() {
        super();
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

            File bamFile = SampleWorkflowUtil.findFileByJobAndMimeTypeAndWorkflowId(maPSeqDAOBeanService, fileDataSet,
                    PicardAddOrReplaceReadGroups.class, MimeType.APPLICATION_BAM, ncgenesWorkflow.getId());

            if (bamFile == null) {
                File ncgenesDirectory = new File(sample.getOutputDirectory(), "NCGenes");
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

            String irodsHome = System.getenv("NCGENES_IRODS_HOME");
            if (StringUtils.isEmpty(irodsHome)) {
                logger.error("irodsHome is not set");
                return;
            }

            String ncgenesIRODSDirectory;

            switch (runMode) {
                case DEV:
                case STAGING:
                    ncgenesIRODSDirectory = String.format("/MedGenZone/home/medgenuser/sequence_data/%s/ncgenes/%s",
                            runMode.toString().toLowerCase(), participantId);
                    break;
                case PROD:
                default:
                    ncgenesIRODSDirectory = String.format("/MedGenZone/home/medgenuser/sequence_data/ncgenes/%s", participantId);
                    break;
            }

            CommandOutput commandOutput = null;

            List<CommandInput> commandInputList = new ArrayList<CommandInput>();

            CommandInput commandInput = new CommandInput();
            commandInput.setExitImmediately(Boolean.FALSE);
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%s/bin/imkdir -p %s%n", irodsHome, ncgenesIRODSDirectory));
            sb.append(String.format("%s/bin/imeta add -C %s Project NCGENES%n", irodsHome, ncgenesIRODSDirectory));
            sb.append(String.format("%s/bin/imeta add -C %s ParticipantID %s NCGENES%n", irodsHome, ncgenesIRODSDirectory, participantId));
            commandInput.setCommand(sb.toString());
            commandInput.setWorkDir(tmpDir);
            commandInputList.add(commandInput);

            // set recal out file
            String gatkTableRecalibrationOut = bamFile.getName().replace(".bam", ".deduped.realign.fixmate.recal.bam");

            List<IRODSBean> files2RegisterToIRODS = new LinkedList<IRODSBean>();
            File filterVariant1Output = new File(outputDirectory, gatkTableRecalibrationOut.replace(".bam", ".vcf"));
            File gatkApplyRecalibrationOut = new File(outputDirectory,
                    filterVariant1Output.getName().replace(".vcf", String.format(".incidental-%s.v-%s.vcf", incidental, version)));

            if (!gatkApplyRecalibrationOut.exists()) {
                outputDirectory = new File(sample.getOutputDirectory(), "NCGenesIncidentalVariantCalling");
                gatkApplyRecalibrationOut = new File(outputDirectory,
                        filterVariant1Output.getName().replace(".vcf", String.format(".incidental-%s.v-%s.vcf", incidental, version)));
            }

            files2RegisterToIRODS.add(new IRODSBean(gatkApplyRecalibrationOut, "IncidentalVcf", version, incidental, runMode));

            for (IRODSBean bean : files2RegisterToIRODS) {

                File f = bean.getFile();
                if (!f.exists()) {
                    logger.warn("file to register doesn't exist: {}", f.getAbsolutePath());
                    continue;
                }

                commandInput = new CommandInput();
                commandInput.setExitImmediately(Boolean.FALSE);

                StringBuilder registerCommandSB = new StringBuilder();
                String registrationCommand = String.format("%s/bin/ireg -f %s %s/%s", irodsHome, bean.getFile().getAbsolutePath(),
                        ncgenesIRODSDirectory, bean.getFile().getName());
                String deRegistrationCommand = String.format("%s/bin/irm -U %s/%s", irodsHome, ncgenesIRODSDirectory,
                        bean.getFile().getName());
                registerCommandSB.append(registrationCommand).append("\n");
                registerCommandSB.append(String.format("if [ $? != 0 ]; then %s; %s; fi%n", deRegistrationCommand, registrationCommand));
                commandInput.setCommand(registerCommandSB.toString());
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                commandInput = new CommandInput();
                commandInput.setExitImmediately(Boolean.FALSE);
                sb = new StringBuilder();
                sb.append(String.format("%s/bin/imeta add -d %s/%s ParticipantID %s NCGENES%n", irodsHome, ncgenesIRODSDirectory,
                        f.getName(), participantId));
                sb.append(String.format("%s/bin/imeta add -d %s/%s FileType %s NCGENES%n", irodsHome, ncgenesIRODSDirectory, f.getName(),
                        bean.getType()));
                sb.append(String.format("%s/bin/imeta add -d %s/%s System %s NCGENES%n", irodsHome, ncgenesIRODSDirectory, f.getName(),
                        StringUtils.capitalize(bean.getRunMode().toString().toLowerCase())));
                commandInput.setCommand(sb.toString());
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                if (StringUtils.isNotEmpty(incidental)) {
                    commandInput = new CommandInput();
                    commandInput.setCommand(String.format("%s/bin/imeta add -d %s/%s IncidentalID %s NCGENES", irodsHome,
                            ncgenesIRODSDirectory, bean.getFile().getName(), incidental));
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);
                }

                if (StringUtils.isNotEmpty(version)) {
                    commandInput = new CommandInput();
                    commandInput.setCommand(String.format("%s/bin/imeta add -d %s/%s IncidentalVersion %s NCGENES", irodsHome,
                            ncgenesIRODSDirectory, bean.getFile().getName(), version));
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);
                }

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

    public RunModeType getRunMode() {
        return runMode;
    }

    public void setRunMode(RunModeType runMode) {
        this.runMode = runMode;
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
