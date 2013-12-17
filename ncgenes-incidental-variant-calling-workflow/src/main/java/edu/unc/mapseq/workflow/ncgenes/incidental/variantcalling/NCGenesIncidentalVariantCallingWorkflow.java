package edu.unc.mapseq.workflow.ncgenes.incidental.variantcalling;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.config.RunModeType;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.EntityAttribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.module.gatk.GATKDownsamplingType;
import edu.unc.mapseq.module.gatk.GATKPhoneHomeType;
import edu.unc.mapseq.module.gatk.GATKUnifiedGenotyperCLI;
import edu.unc.mapseq.module.picard.PicardAddOrReplaceReadGroups;
import edu.unc.mapseq.workflow.AbstractWorkflow;
import edu.unc.mapseq.workflow.IRODSBean;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.WorkflowJobFactory;
import edu.unc.mapseq.workflow.WorkflowUtil;

public class NCGenesIncidentalVariantCallingWorkflow extends AbstractWorkflow {

    private final Logger logger = LoggerFactory.getLogger(NCGenesIncidentalVariantCallingWorkflow.class);

    public NCGenesIncidentalVariantCallingWorkflow() {
        super();
    }

    @Override
    public String getName() {
        return NCGenesIncidentalVariantCallingWorkflow.class.getSimpleName().replace("Workflow", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle bundle = ResourceBundle
                .getBundle("edu/unc/mapseq/workflow/ncgenes/incidental/variantcalling/workflow");
        String version = bundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.info("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;
        // will come over from active mq JSON request
        String version = null;
        String dx = null;

        Set<HTSFSample> htsfSampleSet = getAggregateHTSFSampleSet();
        logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String knownVCF = getWorkflowBeanService().getAttributes().get("knownVCF");
        String referenceSequence = getWorkflowBeanService().getAttributes().get("referenceSequence");

        Workflow ncgenesWorkflow = null;
        try {
            ncgenesWorkflow = getWorkflowBeanService().getMaPSeqDAOBean().getWorkflowDAO().findByName("NCGenes");
        } catch (MaPSeqDAOException e1) {
            e1.printStackTrace();
        }

        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            Set<EntityAttribute> attributeSet = htsfSample.getAttributes();
            Iterator<EntityAttribute> attributeIter = attributeSet.iterator();
            while (attributeIter.hasNext()) {
                EntityAttribute attribute = attributeIter.next();
                if ("version".equals(attribute.getName())) {
                    version = attribute.getValue();
                }
                if ("dx".equals(attribute.getName())) {
                    dx = attribute.getValue();
                }
            }

            SequencerRun sequencerRun = htsfSample.getSequencerRun();
            File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample,
                    getName().replace("IncidentalVariantCalling", ""), getVersion());

            String format = "/proj/renci/sequence_analysis/annotation/abeast/NCGenes/Incidental/incidental_%2$s_1.interval_list";

            File intervalListByDXAndVersionFile = new File(String.format(format, version, dx));

            Set<FileData> fileDataSet = htsfSample.getFileDatas();

            File bamFile = null;

            List<File> potentialBAMFileList = WorkflowUtil.lookupFileByJobAndMimeTypeAndWorkflowId(fileDataSet,
                    getWorkflowBeanService().getMaPSeqDAOBean(), PicardAddOrReplaceReadGroups.class,
                    MimeType.APPLICATION_BAM, ncgenesWorkflow.getId());

            // assume that only one PicardAddOrReplaceReadGroups job exists
            if (potentialBAMFileList.size() > 0) {
                bamFile = potentialBAMFileList.get(0);
            }

            if (bamFile == null) {
                logger.error("bam file to process was not found");
                throw new WorkflowException("bam file to process was not found");
            }

            try {

                // create graph content here
                // output variable matches
                File gatkTableRecalibrationOut = new File(bamFile.getParentFile(), bamFile.getName().replace(".bam",
                        ".deduped.realign.fixmate.recal.bam"));

                // new job
                CondorJob gatkUnifiedGenotyperJob = WorkflowJobFactory.createJob(++count,
                        GATKUnifiedGenotyperCLI.class, getWorkflowPlan(), htsfSample);
                gatkUnifiedGenotyperJob.setSiteName(siteName);
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.PHONEHOME,
                        GATKPhoneHomeType.NO_ET.toString());
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLINGTYPE,
                        GATKDownsamplingType.NONE.toString());
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.REFERENCESEQUENCE, referenceSequence);
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.DBSNP, knownVCF);
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.STANDCALLCONF, "30");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.STANDEMITCONF, "0");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.GENOTYPELIKELIHOODSMODEL, "BOTH");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.INPUTFILE,
                        gatkTableRecalibrationOut.getAbsolutePath());
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.NUMTHREADS, "4");
                gatkUnifiedGenotyperJob.setNumberOfProcessors(4);
                File gatkUnifiedGenotyperOut = new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(
                        ".bam", ".incidental.vcf"));
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.OUT,
                        gatkUnifiedGenotyperOut.getAbsolutePath());
                // this is a different list that is being called
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.INTERVALS,
                        intervalListByDXAndVersionFile.getAbsolutePath());
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.OUTPUTMODE, "EMIT_ALL_SITES");
                File gatkUnifiedGenotyperMetrics = new File(outputDirectory, gatkTableRecalibrationOut.getName()
                        .replace(".bam", ".incidental.metrics"));
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.METRICS, gatkUnifiedGenotyperMetrics);
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLETOCOVERAGE, "250");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "AlleleBalance");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "DepthOfCoverage");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "HomopolymerRun");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "MappingQualityZero");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "QualByDepth");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "RMSMappingQuality");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "HaplotypeScore");
                graph.addVertex(gatkUnifiedGenotyperJob);

            } catch (Exception e) {
                throw new WorkflowException(e);
            }

        }

        return graph;
    }

    @Override
    public void postRun() throws WorkflowException {

        Set<HTSFSample> htsfSampleSet = getAggregateHTSFSampleSet();

        RunModeType runMode = getWorkflowBeanService().getMaPSeqConfigurationService().getRunMode();

        Workflow ncgenesWorkflow = null;
        try {
            ncgenesWorkflow = getWorkflowBeanService().getMaPSeqDAOBean().getWorkflowDAO().findByName("NCGenes");
        } catch (MaPSeqDAOException e1) {
            e1.printStackTrace();
        }

        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            File outputDirectory = new File(htsfSample.getOutputDirectory());
            File tmpDir = new File(outputDirectory, "tmp");
            if (!tmpDir.exists()) {
                tmpDir.mkdirs();
            }

            Set<FileData> fileDataSet = htsfSample.getFileDatas();
            File bamFile = null;

            List<File> potentialBAMFileList = WorkflowUtil.lookupFileByJobAndMimeTypeAndWorkflowId(fileDataSet,
                    getWorkflowBeanService().getMaPSeqDAOBean(), PicardAddOrReplaceReadGroups.class,
                    MimeType.APPLICATION_BAM, ncgenesWorkflow.getId());

            // assume that only one PicardAddOrReplaceReadGroups job exists
            if (potentialBAMFileList.size() > 0) {
                bamFile = potentialBAMFileList.get(0);
            }

            if (bamFile == null) {
                logger.error("bam file to process was not found");
                throw new WorkflowException("bam file to process was not found");
            }

            // assumption: a dash is used as a delimiter between a participantId
            // and the external code
            int idx = htsfSample.getName().lastIndexOf("-");
            String participantId = idx != -1 ? htsfSample.getName().substring(0, idx) : htsfSample.getName();

            String irodsHome = System.getenv("NCGENES_IRODS_HOME");
            if (StringUtils.isEmpty(irodsHome)) {
                logger.error("irodsHome is not set");
                return;
            }

            String ncgenesIRODSDirectory;

            switch (runMode) {
                case DEV:
                case STAGING:
                    ncgenesIRODSDirectory = String.format("/genomicsDataGridZone/sequence_data/%s/ncgenes/%s", runMode
                            .toString().toLowerCase(), participantId);
                    break;
                case PROD:
                default:
                    ncgenesIRODSDirectory = String.format("/genomicsDataGridZone/sequence_data/ncgenes/%s",
                            participantId);
                    break;
            }

            CommandOutput commandOutput = null;

            List<CommandInput> commandInputList = new ArrayList<CommandInput>();
            CommandInput commandInput = new CommandInput();
            commandInput.setCommand(String.format("%s/bin/imkdir -p %s", irodsHome, ncgenesIRODSDirectory));
            commandInput.setWorkDir(tmpDir);
            commandInputList.add(commandInput);

            commandInput = new CommandInput();
            commandInput.setCommand(String.format("%s/bin/imeta add -C %s Project NCGENES", irodsHome,
                    ncgenesIRODSDirectory));
            commandInput.setWorkDir(tmpDir);
            commandInputList.add(commandInput);

            commandInput = new CommandInput();
            commandInput.setCommand(String.format("%s/bin/imeta add -C %s ParticipantID %s NCGENES", irodsHome,
                    ncgenesIRODSDirectory, participantId));
            commandInput.setWorkDir(tmpDir);
            commandInputList.add(commandInput);

            // set recal out file
            String gatkTableRecalibrationOut = bamFile.getName().replace(".bam", ".deduped.realign.fixmate.recal.bam");

            List<IRODSBean> files2RegisterToIRODS = new ArrayList<IRODSBean>();
            File filterVariant1Output = new File(outputDirectory, gatkTableRecalibrationOut.replace(".bam",
                    ".variant.vcf"));
            File gatkApplyRecalibrationOut = new File(outputDirectory, filterVariant1Output.getName().replace(".vcf",
                    ".incidental.vcf"));
            files2RegisterToIRODS.add(new IRODSBean(gatkApplyRecalibrationOut, "IncidentalVcf", null, null, runMode));

            for (IRODSBean bean : files2RegisterToIRODS) {

                commandInput = new CommandInput();
                commandInput.setExitImmediately(Boolean.FALSE);

                StringBuilder registerCommandSB = new StringBuilder();
                String registrationCommand = String.format("%s/bin/ireg -f %s %s/%s", irodsHome, bean.getFile()
                        .getAbsolutePath(), ncgenesIRODSDirectory, bean.getFile().getName());
                String deRegistrationCommand = String.format("%s/bin/irm -U %s/%s", irodsHome, ncgenesIRODSDirectory,
                        bean.getFile().getName());
                registerCommandSB.append(registrationCommand).append("\n");
                registerCommandSB.append(String.format("if [ $? != 0 ]; then %s; %s; fi%n", deRegistrationCommand,
                        registrationCommand));
                commandInput.setCommand(registerCommandSB.toString());
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                commandInput = new CommandInput();
                commandInput.setCommand(String.format("%s/bin/imeta add -d %s/%s ParticipantID %s NCGENES", irodsHome,
                        ncgenesIRODSDirectory, bean.getFile().getName(), participantId));
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                commandInput = new CommandInput();
                commandInput.setCommand(String.format("%s/bin/imeta add -d %s/%s FileType %s NCGENES", irodsHome,
                        ncgenesIRODSDirectory, bean.getFile().getName(), bean.getType()));
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                if (StringUtils.isNotEmpty(bean.getDx())) {
                    commandInput = new CommandInput();
                    commandInput.setCommand(String.format("%s/bin/imeta add -d %s/%s DxID %s NCGENES", irodsHome,
                            ncgenesIRODSDirectory, bean.getFile().getName(), bean.getDx()));
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);
                }

                if (StringUtils.isNotEmpty(bean.getVersion())) {
                    commandInput = new CommandInput();
                    commandInput.setCommand(String.format("%s/bin/imeta add -d %s/%s DxVersion %s NCGENES", irodsHome,
                            ncgenesIRODSDirectory, bean.getFile().getName(), bean.getVersion()));
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);
                }
            }

            File mapseqrc = new File(System.getProperty("user.home"), ".mapseqrc");
            Executor executor = BashExecutor.getInstance();

            for (CommandInput ci : commandInputList) {
                try {
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
}
