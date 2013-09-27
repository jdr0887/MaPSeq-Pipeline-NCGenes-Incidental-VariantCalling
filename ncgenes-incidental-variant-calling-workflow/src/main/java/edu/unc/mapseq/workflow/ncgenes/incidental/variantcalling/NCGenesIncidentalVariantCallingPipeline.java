package edu.unc.mapseq.workflow.ncgenes.incidental.variantcalling;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.pipeline.AbstractPipeline;
import edu.unc.mapseq.pipeline.PipelineException;
import edu.unc.mapseq.pipeline.PipelineUtil;

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

        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            SequencerRun sequencerRun = htsfSample.getSequencerRun();
            File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample, getName());

            Integer laneIndex = htsfSample.getLaneIndex();
            logger.debug("laneIndex = {}", laneIndex);
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

            } catch (Exception e) {
                throw new PipelineException(e);
            }

        }

        return graph;
    }

}
