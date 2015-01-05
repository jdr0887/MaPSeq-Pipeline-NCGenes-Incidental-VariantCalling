package edu.unc.mapseq.workflow.ncgenes.incidental.variantcalling;

import java.io.File;
import java.util.Iterator;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncgenes.incidental.variantcalling.RegisterToIRODSRunnable;
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
import edu.unc.mapseq.workflow.impl.AbstractSampleWorkflow;
import edu.unc.mapseq.workflow.impl.WorkflowJobFactory;
import edu.unc.mapseq.workflow.impl.WorkflowUtil;

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

            File outputDirectory = new File(sample.getOutputDirectory(), getName());
            File tmpDirectory = new File(outputDirectory, "tmp");
            tmpDirectory.mkdirs();

            String format = "/proj/renci/sequence_analysis/annotation/abeast/NCGenes/Incidental/incidental_%s_11.interval_list";

            File intervalListByIncidentalAndVersionFile = new File(String.format(format, incidental));

            Set<FileData> fileDataSet = sample.getFileDatas();

            File bamFile = WorkflowUtil.findFileByJobAndMimeTypeAndWorkflowId(getWorkflowBeanService()
                    .getMaPSeqDAOBean(), fileDataSet, PicardAddOrReplaceReadGroups.class, MimeType.APPLICATION_BAM,
                    ncgenesWorkflow.getId());

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
                        .createJob(++count, GATKUnifiedGenotyperCLI.class, attempt.getId(), sample.getId())
                        .siteName(siteName).numberOfProcessors(4);
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

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();
        WorkflowRun workflowRun = attempt.getWorkflowRun();

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

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        for (Sample sample : sampleSet) {

            if ("Undetermined".equals(sample.getBarcode())) {
                continue;
            }

            RegisterToIRODSRunnable runnable = new RegisterToIRODSRunnable();
            runnable.setMapseqDAOBean(getWorkflowBeanService().getMaPSeqDAOBean());
            runnable.setRunMode(runMode);
            runnable.setSampleId(sample.getId());
            runnable.setIncidental(incidental);
            runnable.setVersion(version);
            executorService.submit(runnable);

        }

        executorService.shutdown();

    }

}
