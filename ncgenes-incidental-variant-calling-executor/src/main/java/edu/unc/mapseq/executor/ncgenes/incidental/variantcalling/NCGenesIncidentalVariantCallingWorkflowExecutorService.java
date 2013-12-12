package edu.unc.mapseq.executor.ncgenes.incidental.variantcalling;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NCGenesIncidentalVariantCallingWorkflowExecutorService {

    private final Logger logger = LoggerFactory.getLogger(NCGenesIncidentalVariantCallingWorkflowExecutorService.class);

    private final Timer mainTimer = new Timer();

    private NCGenesIncidentalVariantCallingWorkflowExecutorTask task;

    private Long period = Long.valueOf(5);

    public NCGenesIncidentalVariantCallingWorkflowExecutorService() {
        super();
    }

    public void start() throws Exception {
        logger.info("ENTERING start()");
        long delay = 1 * 60 * 1000; // 1 minute
        mainTimer.scheduleAtFixedRate(task, delay, period * 60 * 1000);
    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        mainTimer.purge();
        mainTimer.cancel();
    }

    public NCGenesIncidentalVariantCallingWorkflowExecutorTask getTask() {
        return task;
    }

    public void setTask(NCGenesIncidentalVariantCallingWorkflowExecutorTask task) {
        this.task = task;
    }

    public Long getPeriod() {
        return period;
    }

    public void setPeriod(Long period) {
        this.period = period;
    }

}
