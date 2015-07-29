package edu.unc.mapseq.workflow;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.jgrapht.DirectedGraph;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.ext.CondorDOTExporter;

import edu.unc.mapseq.module.core.CatCLI;
import edu.unc.mapseq.module.core.EchoCLI;
import edu.unc.mapseq.workflow.impl.WorkflowJobFactory;

public class WorkflowTest {

    @Test
    public void createDot() {

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;

        try {
            // new job
            CondorJob helloJob = WorkflowJobFactory.createJob(++count, EchoCLI.class, null).build();
            graph.addVertex(helloJob);

            // new job
            CondorJob worldJob = WorkflowJobFactory.createJob(++count, EchoCLI.class, null).build();
            graph.addVertex(worldJob);

            // new job
            CondorJob catJob = WorkflowJobFactory.createJob(++count, CatCLI.class, null).build();
            graph.addVertex(catJob);
            graph.addEdge(helloJob, catJob);
            graph.addEdge(worldJob, catJob);
        } catch (WorkflowException e1) {
            e1.printStackTrace();
        }

        VertexNameProvider<CondorJob> vnpId = new VertexNameProvider<CondorJob>() {
            @Override
            public String getVertexName(CondorJob job) {
                return job.getName();
            }
        };

        VertexNameProvider<CondorJob> vnpLabel = new VertexNameProvider<CondorJob>() {
            @Override
            public String getVertexName(CondorJob job) {
                return job.getName();
            }
        };

        CondorDOTExporter<CondorJob, CondorJobEdge> dotExporter = new CondorDOTExporter<CondorJob, CondorJobEdge>(
                vnpId, vnpLabel, null, null, null, null);
        File srcSiteResourcesImagesDir = new File("../src/site/resources/images");
        if (!srcSiteResourcesImagesDir.exists()) {
            srcSiteResourcesImagesDir.mkdirs();
        }
        File dotFile = new File(srcSiteResourcesImagesDir, "workflow.dag.dot");
        try {
            FileWriter fw = new FileWriter(dotFile);
            dotExporter.export(fw, graph);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
