package edu.unc.mapseq.workflow.ncgenes.incidental.variantcalling;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
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
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.module.gatk.GATKDownsamplingType;
import edu.unc.mapseq.module.gatk.GATKPhoneHomeType;
import edu.unc.mapseq.module.gatk.GATKUnifiedGenotyperCLI;
import edu.unc.mapseq.module.picard.PicardAddOrReplaceReadGroups;
import edu.unc.mapseq.pipeline.AbstractPipeline;
import edu.unc.mapseq.pipeline.IRODSBean;
import edu.unc.mapseq.pipeline.PipelineException;
import edu.unc.mapseq.pipeline.PipelineJobFactory;
import edu.unc.mapseq.pipeline.PipelineUtil;
//import edu.unc.mapseq.workflow.WorkflowUtil;

public class NCGenesIncidentalVariantCallingPipeline extends AbstractPipeline {

    private final Logger logger = LoggerFactory.getLogger(NCGenesIncidentalVariantCallingPipeline.class);

    public NCGenesIncidentalVariantCallingPipeline() {
        super();
    }

    @Override
    public String getName() {
        return NCGenesIncidentalVariantCallingPipeline.class.getSimpleName().replace("Pipeline", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle bundle = ResourceBundle.getBundle("edu/unc/mapseq/workflow/ncgenes/incidental/variantcalling/workflow");
        String version = bundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws PipelineException {
        logger.info("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;
        String version = null;
        String dx = null;

        if (getWorkflowPlan().getSequencerRun() == null && getWorkflowPlan().getHTSFSamples() == null) {
            logger.error("Don't have either sequencerRun and htsfSample");
            throw new PipelineException("Don't have either sequencerRun and htsfSample");
        }

        Set<HTSFSample> htsfSampleSet = new HashSet<HTSFSample>();

        if (getWorkflowPlan().getSequencerRun() != null) {
            logger.info("sequencerRun: {}", getWorkflowPlan().getSequencerRun().toString());
            try {
                htsfSampleSet.addAll(getPipelineBeanService().getMaPSeqDAOBean().getHTSFSampleDAO()
                        .findBySequencerRunId(getWorkflowPlan().getSequencerRun().getId()));
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }
        }

        if (getWorkflowPlan().getHTSFSamples() != null) {
            htsfSampleSet.addAll(getWorkflowPlan().getHTSFSamples());
        }

        String knownVCF = getPipelineBeanService().getAttributes().get("knownVCF");
        String referenceSequence = getPipelineBeanService().getAttributes().get("referenceSequence");
        String unifiedGenotyperIntervalList = getPipelineBeanService().getAttributes().get(
                "unifiedGenotyperIntervalList");
        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            SequencerRun sequencerRun = htsfSample.getSequencerRun();
            File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample, getName(), getVersion());

            String format = "/proj/renci/sequence_analysis/annotation/abeast/NCGenes/Incidental/incidental_%2$s_%1$s.interval_list";
            
            File intervalListByDXAndVersionFile = new File(String.format(format, version, dx));
            Integer laneIndex = htsfSample.getLaneIndex();
            //logger.debug("laneIndex = {}", laneIndex);
            Set<FileData> fileDataSet = htsfSample.getFileDatas();

            File bamFile = null;
            
            Workflow ncgenesWorkflow = null;
            try {
                ncgenesWorkflow = getPipelineBeanService().getMaPSeqDAOBean().getWorkflowDAO().findByName("NCGenes");
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
            }

            List<File> potentialBAMFileList = PipelineUtil.lookupFileByJobAndMimeTypeAndWorkflowId(fileDataSet,
                    getPipelineBeanService().getMaPSeqDAOBean(), PicardAddOrReplaceReadGroups.class,
                    MimeType.APPLICATION_BAM, ncgenesWorkflow.getId());

            // assume that only one PicardAddOrReplaceReadGroups job exists
            if (potentialBAMFileList.size() > 0) {
                bamFile = potentialBAMFileList.get(0);
            }

            if (bamFile == null) {
                logger.error("bam file to process was not found");
                throw new PipelineException("bam file to process was not found");
            }
            List<File> readPairList = PipelineUtil.getReadPairList(htsfSample.getFileDatas(), sequencerRun.getName(),
                    htsfSample.getLaneIndex());
            logger.info("fileList = {}", readPairList.size());

            // assumption: a dash is used as a delimiter between a participantId
            // and the external code
            int idx = htsfSample.getName().lastIndexOf("-");
            String participantId = idx != -1 ? htsfSample.getName().substring(0, idx) : htsfSample.getName();

            if (readPairList.size() != 2) {
                throw new PipelineException("ReadPairList is not 2");
            }

            File r1FastqFile = readPairList.get(0);
            String r1FastqRootName = PipelineUtil.getRootFastqName(r1FastqFile.getName());

            File r2FastqFile = readPairList.get(1);
            String r2FastqRootName = PipelineUtil.getRootFastqName(r2FastqFile.getName());

            String fastqLaneRootName = StringUtils.removeEnd(r2FastqRootName, "_R2");

            try {

                // create graph content here
                // output variable matches 
                // new job
                String gatkTableRecalibrationOut = bamFile.getName().replace(".bam", ".deduped.realign.fixmate.recal.bam");
                CondorJob gatkUnifiedGenotyperJob = PipelineJobFactory.createJob(++count,
                        GATKUnifiedGenotyperCLI.class, getWorkflowPlan(), htsfSample);
                gatkUnifiedGenotyperJob.setInitialDirectory(outputDirectory);
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.PHONEHOME,
                        GATKPhoneHomeType.NO_ET.toString());
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLINGTYPE,
                        GATKDownsamplingType.NONE.toString());
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.REFERENCESEQUENCE, referenceSequence);
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.DBSNP, knownVCF);
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.STANDCALLCONF, "30");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.STANDEMITCONF, "0");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.GENOTYPELIKELIHOODSMODEL, "BOTH");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.INPUTFILE, gatkTableRecalibrationOut);
                gatkUnifiedGenotyperJob.addTransferInput(gatkTableRecalibrationOut);
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.NUMTHREADS, "4");
                gatkUnifiedGenotyperJob.setNumberOfProcessors(4);
                String gatkUnifiedGenotyperOut = gatkTableRecalibrationOut.replace(".bam", ".vcf");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.OUT, gatkUnifiedGenotyperOut);
                gatkUnifiedGenotyperJob.addTransferOutput(gatkUnifiedGenotyperOut);
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.INTERVALS, unifiedGenotyperIntervalList); // a different list needs to be called in its new workflow
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.OUTPUTMODE, "EMIT_ALL_SITES"); // this is perfect
                String gatkUnifiedGenotyperMetrics = gatkTableRecalibrationOut.replace(".bam", ".metrics");
                gatkUnifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.METRICS, gatkUnifiedGenotyperMetrics);
                gatkUnifiedGenotyperJob.addTransferOutput(gatkUnifiedGenotyperMetrics);
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
                throw new PipelineException(e);
            }

        }

        return graph;
    }

    @Override
    public void postRun() throws PipelineException {

        Set<HTSFSample> htsfSampleSet = new HashSet<HTSFSample>();

        if (getWorkflowPlan().getSequencerRun() != null) {
            logger.info("sequencerRun: {}", getWorkflowPlan().getSequencerRun().toString());
            try {
                htsfSampleSet.addAll(getPipelineBeanService().getMaPSeqDAOBean().getHTSFSampleDAO()
                        .findBySequencerRunId(getWorkflowPlan().getSequencerRun().getId()));
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }
        }

        if (getWorkflowPlan().getHTSFSamples() != null) {
            logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());
            htsfSampleSet.addAll(getWorkflowPlan().getHTSFSamples());
        }

        RunModeType runMode = getPipelineBeanService().getMaPSeqConfigurationService().getRunMode();

        Workflow ncgenesWorkflow = null;
        try {
            ncgenesWorkflow = getPipelineBeanService().getMaPSeqDAOBean().getWorkflowDAO().findByName("NCGenes");
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

            List<File> potentialBAMFileList = PipelineUtil.lookupFileByJobAndMimeTypeAndWorkflowId(fileDataSet,
                    getPipelineBeanService().getMaPSeqDAOBean(), PicardAddOrReplaceReadGroups.class,
                    MimeType.APPLICATION_BAM, ncgenesWorkflow.getId());

            // assume that only one PicardAddOrReplaceReadGroups job exists
            if (potentialBAMFileList.size() > 0) {
                bamFile = potentialBAMFileList.get(0);
            }

            if (bamFile == null) {
                logger.error("bam file to process was not found");
                throw new PipelineException("bam file to process was not found");
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
                    ".incidental.recalibrated.filtered.vcf"));
            files2RegisterToIRODS.add(new IRODSBean(gatkApplyRecalibrationOut, "RecalibratedVcf", null, null, runMode));

            File filterVariant2Output = new File(outputDirectory, filterVariant1Output.getName().replace(".vcf",
                    ".incidental.ic_snps.vcf"));
            files2RegisterToIRODS.add(new IRODSBean(filterVariant2Output, "IcSnpsVcf", null, null, runMode));
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
