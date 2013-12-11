package edu.unc.mapseq.workflow.ncgenes.incidental.variantcalling;

import java.io.File;
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

import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.workflow.AbstractWorkflow;
import edu.unc.mapseq.workflow.WorkflowException;
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

        Set<HTSFSample> htsfSampleSet = getAggregateHTSFSampleSet();
        logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());

        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            SequencerRun sequencerRun = htsfSample.getSequencerRun();
            File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample, getName(), getVersion());

            Integer laneIndex = htsfSample.getLaneIndex();
            logger.debug("laneIndex = {}", laneIndex);
            List<File> readPairList = WorkflowUtil.getReadPairList(htsfSample.getFileDatas(), sequencerRun.getName(),
                    htsfSample.getLaneIndex());
            logger.info("fileList = {}", readPairList.size());

            // assumption: a dash is used as a delimiter between a participantId
            // and the external code
            int idx = htsfSample.getName().lastIndexOf("-");
            String participantId = idx != -1 ? htsfSample.getName().substring(0, idx) : htsfSample.getName();

            if (readPairList.size() != 2) {
                throw new WorkflowException("ReadPairList is not 2");
            }

            File r1FastqFile = readPairList.get(0);
            String r1FastqRootName = WorkflowUtil.getRootFastqName(r1FastqFile.getName());

            File r2FastqFile = readPairList.get(1);
            String r2FastqRootName = WorkflowUtil.getRootFastqName(r2FastqFile.getName());

            String fastqLaneRootName = StringUtils.removeEnd(r2FastqRootName, "_R2");

            try {

                // create graph content here

            } catch (Exception e) {
                throw new WorkflowException(e);
            }

        }

        return graph;
    }

}
