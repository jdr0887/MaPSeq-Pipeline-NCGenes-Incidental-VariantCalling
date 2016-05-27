package edu.unc.mapseq.executor.ncgenes.incidental.variantcalling;

import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowRunAttemptDAO;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.workflow.WorkflowBeanService;
import edu.unc.mapseq.workflow.WorkflowExecutor;
import edu.unc.mapseq.workflow.WorkflowTPE;
import edu.unc.mapseq.workflow.ncgenes.incidental.variantcalling.NCGenesIncidentalVariantCallingWorkflow;

public class NCGenesIncidentalVariantCallingWorkflowExecutorTask extends TimerTask {

    private static final Logger logger = LoggerFactory.getLogger(NCGenesIncidentalVariantCallingWorkflowExecutorTask.class);

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

        logger.info(String.format("ActiveCount: %d, TaskCount: %d, CompletedTaskCount: %d", threadPoolExecutor.getActiveCount(),
                threadPoolExecutor.getTaskCount(), threadPoolExecutor.getCompletedTaskCount()));

        WorkflowDAO workflowDAO = getWorkflowBeanService().getMaPSeqDAOBeanService().getWorkflowDAO();
        WorkflowRunAttemptDAO workflowRunAttemptDAO = this.workflowBeanService.getMaPSeqDAOBeanService().getWorkflowRunAttemptDAO();

        try {
            List<Workflow> workflowList = workflowDAO.findByName("NCGenesIncidentalVariantCalling");
            if (CollectionUtils.isEmpty(workflowList)) {
                logger.error("No Workflow Found: {}", "NCGenesIncidentalVariantCalling");
                return;
            }
            Workflow workflow = workflowList.get(0);
            List<WorkflowRunAttempt> attempts = workflowRunAttemptDAO.findEnqueued(workflow.getId());
            if (CollectionUtils.isNotEmpty(attempts)) {
                logger.info("dequeuing {} WorkflowRunAttempt", attempts.size());
                for (WorkflowRunAttempt attempt : attempts) {

                    NCGenesIncidentalVariantCallingWorkflow ncGenesIncidentalVariantCallingWorkflow = new NCGenesIncidentalVariantCallingWorkflow();
                    attempt.setVersion(ncGenesIncidentalVariantCallingWorkflow.getVersion());
                    attempt.setDequeued(new Date());
                    workflowRunAttemptDAO.save(attempt);

                    ncGenesIncidentalVariantCallingWorkflow.setWorkflowBeanService(workflowBeanService);
                    ncGenesIncidentalVariantCallingWorkflow.setWorkflowRunAttempt(attempt);
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
