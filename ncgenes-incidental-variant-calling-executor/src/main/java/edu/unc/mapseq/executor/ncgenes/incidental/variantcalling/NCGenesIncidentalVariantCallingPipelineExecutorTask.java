package edu.unc.mapseq.executor.ncgenes.incidental.variantcalling;

import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowPlanDAO;
import edu.unc.mapseq.dao.WorkflowRunDAO;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowPlan;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.pipeline.PipelineBeanService;
import edu.unc.mapseq.pipeline.PipelineExecutor;
import edu.unc.mapseq.pipeline.PipelineTPE;
import edu.unc.mapseq.workflow.ncgenes.incidental.variantcalling.NCGenesIncidentalVariantCallingPipeline;

public class NCGenesIncidentalVariantCallingPipelineExecutorTask extends TimerTask {

    private final Logger logger = LoggerFactory.getLogger(NCGenesIncidentalVariantCallingPipelineExecutorTask.class);

    private final PipelineTPE threadPoolExecutor = new PipelineTPE();

    private PipelineBeanService pipelineBeanService;

    public NCGenesIncidentalVariantCallingPipelineExecutorTask() {
        super();
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        threadPoolExecutor.setCorePoolSize(pipelineBeanService.getCorePoolSize());
        threadPoolExecutor.setMaximumPoolSize(pipelineBeanService.getMaxPoolSize());

        logger.info(String.format("CorePoolSize: %d, MaxPoolSize: %d", threadPoolExecutor.getCorePoolSize(),
                threadPoolExecutor.getMaximumPoolSize()));

        logger.info(String.format("ActiveCount: %d, TaskCount: %d, CompletedTaskCount: %d",
                threadPoolExecutor.getActiveCount(), threadPoolExecutor.getTaskCount(),
                threadPoolExecutor.getCompletedTaskCount()));

        WorkflowDAO workflowDAO = getPipelineBeanService().getMaPSeqDAOBean().getWorkflowDAO();
        WorkflowRunDAO workflowRunDAO = getPipelineBeanService().getMaPSeqDAOBean().getWorkflowRunDAO();
        WorkflowPlanDAO workflowPlanDAO = getPipelineBeanService().getMaPSeqDAOBean().getWorkflowPlanDAO();

        try {
            Workflow workflow = workflowDAO.findByName("NCGenesIncidentalVariantCalling");
            List<WorkflowPlan> workflowPlanList = workflowPlanDAO.findEnqueued(workflow.getId());

            if (workflowPlanList != null && workflowPlanList.size() > 0) {

                logger.info("dequeuing {} WorkflowPlans", workflowPlanList.size());
                for (WorkflowPlan workflowPlan : workflowPlanList) {

                    NCGenesIncidentalVariantCallingPipeline pipeline = new NCGenesIncidentalVariantCallingPipeline();
                    pipeline.setPipelineBeanService(pipelineBeanService);

                    WorkflowRun workflowRun = workflowPlan.getWorkflowRun();
                    workflowRun.setVersion(pipeline.getVersion());
                    workflowRun.setDequeuedDate(new Date());
                    workflowRunDAO.save(workflowRun);

                    pipeline.setWorkflowPlan(workflowPlan);

                    threadPoolExecutor.submit(new PipelineExecutor(pipeline));

                }

            }

        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

    public PipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(PipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

}
