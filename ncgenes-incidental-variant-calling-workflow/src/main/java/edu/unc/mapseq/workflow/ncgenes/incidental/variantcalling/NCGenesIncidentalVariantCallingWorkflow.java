package edu.unc.mapseq.workflow.ncgenes.incidental.variantcalling;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
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
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.config.RunModeType;
import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.gatk.GATKDownsamplingType;
import edu.unc.mapseq.module.gatk.GATKPhoneHomeType;
import edu.unc.mapseq.module.gatk.GATKUnifiedGenotyperCLI;
import edu.unc.mapseq.module.picard.PicardAddOrReplaceReadGroups;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.WorkflowUtil;
import edu.unc.mapseq.workflow.impl.AbstractSampleWorkflow;
import edu.unc.mapseq.workflow.impl.IRODSBean;
import edu.unc.mapseq.workflow.impl.WorkflowJobFactory;

public class NCGenesIncidentalVariantCallingWorkflow extends AbstractSampleWorkflow {

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
        String incidental = null;

        Set<Sample> sampleSet = getAggregatedSamples();
        logger.info("sampleSet.size(): {}", sampleSet.size());

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String knownVCF = getWorkflowBeanService().getAttributes().get("knownVCF");
        String referenceSequence = getWorkflowBeanService().getAttributes().get("referenceSequence");

        MaPSeqDAOBean daoBean = getWorkflowBeanService().getMaPSeqDAOBean();
        WorkflowDAO workflowDAO = daoBean.getWorkflowDAO();

        Workflow ncgenesWorkflow = null;
        try {
            ncgenesWorkflow = workflowDAO.findByName("NCGenes").get(0);
        } catch (MaPSeqDAOException e1) {
            e1.printStackTrace();
        }

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();
        WorkflowRun workflowRun = attempt.getWorkflowRun();

        for (Sample sample : sampleSet) {

            if ("Undetermined".equals(sample.getBarcode())) {
                continue;
            }

            logger.debug(sample.toString());

            Set<Attribute> attributeSet = workflowRun.getAttributes();
            if (attributeSet != null && !attributeSet.isEmpty()) {
                Iterator<Attribute> attributeIter = attributeSet.iterator();
                while (attributeIter.hasNext()) {
                    Attribute attribute = attributeIter.next();
                    if ("version".equals(attribute.getName())) {
                        version = attribute.getValue();
                    }
                    if ("incidental".equals(attribute.getName())) {
                        incidental = attribute.getValue();
                    }
                }
            }

            File outputDirectory = new File(sample.getOutputDirectory());

            String format = "/proj/renci/sequence_analysis/annotation/abeast/NCGenes/Incidental/incidental_%s_11.interval_list";

            File intervalListByIncidentalAndVersionFile = new File(String.format(format, incidental));

            Set<FileData> fileDataSet = sample.getFileDatas();

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
                CondorJobBuilder builder = WorkflowJobFactory
                        .createJob(++count, GATKUnifiedGenotyperCLI.class, attempt, sample).siteName(siteName)
                        .numberOfProcessors(4);
                File gatkUnifiedGenotyperOut = new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(
                        ".bam", String.format(".incidental-%s.v-%s.vcf", incidental, version)));
                File gatkUnifiedGenotyperMetrics = new File(outputDirectory, gatkTableRecalibrationOut.getName()
                        .replace(".bam", String.format(".incidental-%s.v-%s.metrics", incidental, version)));
                builder.addArgument(GATKUnifiedGenotyperCLI.PHONEHOME, GATKPhoneHomeType.NO_ET.toString())
                        .addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLINGTYPE, GATKDownsamplingType.NONE.toString())
                        .addArgument(GATKUnifiedGenotyperCLI.REFERENCESEQUENCE, referenceSequence)
                        .addArgument(GATKUnifiedGenotyperCLI.DBSNP, knownVCF)
                        .addArgument(GATKUnifiedGenotyperCLI.STANDCALLCONF, "30")
                        .addArgument(GATKUnifiedGenotyperCLI.STANDEMITCONF, "0")
                        .addArgument(GATKUnifiedGenotyperCLI.GENOTYPELIKELIHOODSMODEL, "BOTH")
                        .addArgument(GATKUnifiedGenotyperCLI.INPUTFILE, gatkTableRecalibrationOut.getAbsolutePath())
                        .addArgument(GATKUnifiedGenotyperCLI.NUMTHREADS, "4")
                        .addArgument(GATKUnifiedGenotyperCLI.OUT, gatkUnifiedGenotyperOut.getAbsolutePath())
                        .addArgument(GATKUnifiedGenotyperCLI.INTERVALS,
                                intervalListByIncidentalAndVersionFile.getAbsolutePath())
                        .addArgument(GATKUnifiedGenotyperCLI.OUTPUTMODE, "EMIT_ALL_SITES")
                        .addArgument(GATKUnifiedGenotyperCLI.METRICS, gatkUnifiedGenotyperMetrics.getAbsolutePath())
                        .addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLETOCOVERAGE, "250")
                        .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "AlleleBalance")
                        .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "DepthOfCoverage")
                        .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "HomopolymerRun")
                        .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "MappingQualityZero")
                        .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "QualByDepth")
                        .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "RMSMappingQuality")
                        .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "HaplotypeScore");
                CondorJob gatkUnifiedGenotyperJob = builder.build();
                logger.info(gatkUnifiedGenotyperJob.toString());
                graph.addVertex(gatkUnifiedGenotyperJob);

            } catch (Exception e) {
                throw new WorkflowException(e);
            }

        }

        return graph;
    }

    @Override
    public void postRun() throws WorkflowException {

        Set<Sample> sampleSet = getAggregatedSamples();

        RunModeType runMode = getWorkflowBeanService().getMaPSeqConfigurationService().getRunMode();

        String version = null;
        String incidental = null;

        Workflow ncgenesWorkflow = null;
        try {
            ncgenesWorkflow = getWorkflowBeanService().getMaPSeqDAOBean().getWorkflowDAO().findByName("NCGenes").get(0);
        } catch (MaPSeqDAOException e1) {
            e1.printStackTrace();
        }

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();
        WorkflowRun workflowRun = attempt.getWorkflowRun();

        for (Sample sample : sampleSet) {

            if ("Undetermined".equals(sample.getBarcode())) {
                continue;
            }

            File outputDirectory = new File(sample.getOutputDirectory());
            File tmpDir = new File(outputDirectory, "tmp");
            if (!tmpDir.exists()) {
                tmpDir.mkdirs();
            }

            Set<Attribute> attributeSet = workflowRun.getAttributes();
            if (attributeSet != null && !attributeSet.isEmpty()) {
                Iterator<Attribute> attributeIter = attributeSet.iterator();
                while (attributeIter.hasNext()) {
                    Attribute attribute = attributeIter.next();
                    if ("version".equals(attribute.getName())) {
                        version = attribute.getValue();
                    }
                    if ("incidental".equals(attribute.getName())) {
                        incidental = attribute.getValue();
                    }
                }
            }

            if (version == null & incidental == null) {
                logger.warn("Both version and incidental id were null...returning empty irods post-run registration dag");
                return;
            }

            Set<FileData> fileDataSet = sample.getFileDatas();
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

            List<IRODSBean> files2RegisterToIRODS = new LinkedList<IRODSBean>();
            File filterVariant1Output = new File(outputDirectory, gatkTableRecalibrationOut.replace(".bam", ".vcf"));
            File gatkApplyRecalibrationOut = new File(outputDirectory, filterVariant1Output.getName().replace(".vcf",
                    ".incidental-" + incidental + ".v-" + version + ".vcf"));

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

                if (StringUtils.isNotEmpty(incidental)) {
                    commandInput = new CommandInput();
                    commandInput.setCommand(String.format("%s/bin/imeta add -d %s/%s IncidentalID %s NCGENES",
                            irodsHome, ncgenesIRODSDirectory, bean.getFile().getName(), incidental));
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);
                }

                if (StringUtils.isNotEmpty(version)) {
                    commandInput = new CommandInput();
                    commandInput.setCommand(String.format("%s/bin/imeta add -d %s/%s IncidentalVersion %s NCGENES",
                            irodsHome, ncgenesIRODSDirectory, bean.getFile().getName(), version));
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);
                }

                commandInput = new CommandInput();
                commandInput.setCommand(String.format("%s/bin/imeta add -d %s/%s System %s NCGENES", irodsHome,
                        ncgenesIRODSDirectory, bean.getFile().getName(),
                        StringUtils.capitalize(bean.getRunMode().toString().toLowerCase())));
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
}
