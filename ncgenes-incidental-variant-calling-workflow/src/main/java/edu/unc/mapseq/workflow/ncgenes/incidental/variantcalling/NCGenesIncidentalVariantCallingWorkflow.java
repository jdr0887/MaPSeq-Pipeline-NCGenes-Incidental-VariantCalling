package edu.unc.mapseq.workflow.ncgenes.incidental.variantcalling;

import java.io.File;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncgenes.incidental.variantcalling.RegisterToIRODSRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.sequencing.gatk.GATKDownsamplingType;
import edu.unc.mapseq.module.sequencing.gatk.GATKPhoneHomeType;
import edu.unc.mapseq.module.sequencing.gatk.GATKUnifiedGenotyperCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardAddOrReplaceReadGroups;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.core.WorkflowUtil;
import edu.unc.mapseq.workflow.sequencing.AbstractSequencingWorkflow;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowJobFactory;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class NCGenesIncidentalVariantCallingWorkflow extends AbstractSequencingWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(NCGenesIncidentalVariantCallingWorkflow.class);

    public NCGenesIncidentalVariantCallingWorkflow() {
        super();
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.info("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        int count = 0;
        // will come over from active mq JSON request
        String version = null;
        String incidental = null;

        Set<Sample> sampleSet = SequencingWorkflowUtil.getAggregatedSamples(getWorkflowBeanService().getMaPSeqDAOBeanService(),
                getWorkflowRunAttempt());
        logger.info("sampleSet.size(): {}", sampleSet.size());

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String knownVCF = getWorkflowBeanService().getAttributes().get("knownVCF");
        String referenceSequence = getWorkflowBeanService().getAttributes().get("referenceSequence");

        MaPSeqDAOBeanService daoBean = getWorkflowBeanService().getMaPSeqDAOBeanService();
        WorkflowDAO workflowDAO = daoBean.getWorkflowDAO();

        Workflow ncgenesWorkflow = null;
        try {
            ncgenesWorkflow = workflowDAO.findByName("NCGenesBaseline").get(0);
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

            File outputDirectory = SequencingWorkflowUtil.createOutputDirectory(sample,
                    getWorkflowRunAttempt().getWorkflowRun().getWorkflow());
            File tmpDirectory = new File(outputDirectory, "tmp");
            tmpDirectory.mkdirs();

            Set<FileData> fileDataSet = sample.getFileDatas();

            File bamFile = WorkflowUtil.findFileByJobAndMimeTypeAndWorkflowId(getWorkflowBeanService().getMaPSeqDAOBeanService(),
                    fileDataSet, PicardAddOrReplaceReadGroups.class, MimeType.APPLICATION_BAM, ncgenesWorkflow.getId());

            if (bamFile == null) {
                File ncgenesDirectory = SequencingWorkflowUtil.createOutputDirectory(sample, ncgenesWorkflow);
                for (File file : ncgenesDirectory.listFiles()) {
                    if (file.getName().endsWith(".fixed-rg.bam")) {
                        bamFile = file;
                        break;
                    }
                }
            }

            if (bamFile == null) {
                logger.error("bam file to process was not found");
                throw new WorkflowException("bam file to process was not found");
            }

            try {

                // create graph content here
                // output variable matches
                File gatkTableRecalibrationOut = new File(bamFile.getParentFile(),
                        bamFile.getName().replace(".bam", ".deduped.realign.fixmate.recal.bam"));

                // new job
                CondorJobBuilder builder = SequencingWorkflowJobFactory
                        .createJob(++count, GATKUnifiedGenotyperCLI.class, attempt.getId(), sample.getId()).siteName(siteName)
                        .numberOfProcessors(4);
                File gatkUnifiedGenotyperOut = new File(outputDirectory,
                        gatkTableRecalibrationOut.getName().replace(".bam", String.format(".incidental-%s.v-%s.vcf", incidental, version)));
                File gatkUnifiedGenotyperMetrics = new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(".bam",
                        String.format(".incidental-%s.v-%s.metrics", incidental, version)));
                builder.addArgument(GATKUnifiedGenotyperCLI.PHONEHOME, GATKPhoneHomeType.NO_ET.toString())
                        .addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLINGTYPE, GATKDownsamplingType.NONE.toString())
                        .addArgument(GATKUnifiedGenotyperCLI.REFERENCESEQUENCE, referenceSequence)
                        .addArgument(GATKUnifiedGenotyperCLI.DBSNP, knownVCF).addArgument(GATKUnifiedGenotyperCLI.STANDCALLCONF, "30")
                        .addArgument(GATKUnifiedGenotyperCLI.STANDEMITCONF, "0")
                        .addArgument(GATKUnifiedGenotyperCLI.GENOTYPELIKELIHOODSMODEL, "BOTH")
                        .addArgument(GATKUnifiedGenotyperCLI.INPUTFILE, gatkTableRecalibrationOut.getAbsolutePath())
                        .addArgument(GATKUnifiedGenotyperCLI.NUMTHREADS, "4")
                        .addArgument(GATKUnifiedGenotyperCLI.OUT, gatkUnifiedGenotyperOut.getAbsolutePath())
                        .addArgument(GATKUnifiedGenotyperCLI.INTERVALS,
                                String.format(
                                        "$NCGENESINCIDENTALVARIANTCALLING_RESOURCES_DIRECTORY/annotation/abeast/NCGenes/Incidental/incidental_%s_11.interval_list",
                                        incidental))
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
        logger.info("ENTERING postRun()");
        try {
            ExecutorService es = Executors.newSingleThreadExecutor();
            RegisterToIRODSRunnable runnable = new RegisterToIRODSRunnable(getWorkflowBeanService().getMaPSeqDAOBeanService(),
                    getWorkflowRunAttempt());
            es.submit(runnable);
            es.shutdown();
            es.awaitTermination(1L, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
