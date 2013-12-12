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
import edu.unc.mapseq.workflow.WorkflowBeanService;
import edu.unc.mapseq.workflow.WorkflowExecutor;
import edu.unc.mapseq.workflow.WorkflowTPE;
import edu.unc.mapseq.workflow.ncgenes.incidental.variantcalling.NCGenesIncidentalVariantCallingWorkflow;

public class NCGenesIncidentalVariantCallingWorkflowExecutorTask extends TimerTask {

    private final Logger logger = LoggerFactory.getLogger(NCGenesIncidentalVariantCallingWorkflowExecutorTask.class);

    private final WorkflowTPE threadPoolExecutor = new WorkflowTPE();

    private WorkflowBeanService workflowBeanService;

    public NCGenesIncidentalVariantCallingWorkflowExecutorTask() {
        super();
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        threadPoolExecutor.setCorePoolSize(workflowBeanService.getCorePoolSize());
        threadPoolExecutor.setMaximumPoolSize(workflowBeanService.getMaxPoolSize());

        logger.info(String.format("CorePoolSize: %d, MaxPoolSize: %d", threadPoolExecutor.getCorePoolSize(),
                threadPoolExecutor.getMaximumPoolSize()));

        logger.info(String.format("ActiveCount: %d, TaskCount: %d, CompletedTaskCount: %d",
                threadPoolExecutor.getActiveCount(), threadPoolExecutor.getTaskCount(),
                threadPoolExecutor.getCompletedTaskCount()));

        WorkflowDAO workflowDAO = getWorkflowBeanService().getMaPSeqDAOBean().getWorkflowDAO();
        WorkflowRunDAO workflowRunDAO = getWorkflowBeanService().getMaPSeqDAOBean().getWorkflowRunDAO();
        WorkflowPlanDAO workflowPlanDAO = getWorkflowBeanService().getMaPSeqDAOBean().getWorkflowPlanDAO();

        try {
            Workflow workflow = workflowDAO.findByName("NCGenesIncidentalVariantCalling");
            List<WorkflowPlan> workflowPlanList = workflowPlanDAO.findEnqueued(workflow.getId());

            if (workflowPlanList != null && workflowPlanList.size() > 0) {

                logger.info("dequeuing {} WorkflowPlans", workflowPlanList.size());
                for (WorkflowPlan workflowPlan : workflowPlanList) {

                    NCGenesIncidentalVariantCallingWorkflow ncGenesIncidentalVariantCallingWorkflow = new NCGenesIncidentalVariantCallingWorkflow();

                    WorkflowRun workflowRun = workflowPlan.getWorkflowRun();
                    workflowRun.setVersion(ncGenesIncidentalVariantCallingWorkflow.getVersion());
                    workflowRun.setDequeuedDate(new Date());
                    workflowRunDAO.save(workflowRun);

                    ncGenesIncidentalVariantCallingWorkflow.setWorkflowBeanService(workflowBeanService);
                    ncGenesIncidentalVariantCallingWorkflow.setWorkflowPlan(workflowPlan);
                    threadPoolExecutor.submit(new WorkflowExecutor(ncGenesIncidentalVariantCallingWorkflow));

                }

            }

        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

    public WorkflowBeanService getWorkflowBeanService() {
        return workflowBeanService;
    }

    public void setWorkflowBeanService(WorkflowBeanService workflowBeanService) {
        this.workflowBeanService = workflowBeanService;
    }

}
