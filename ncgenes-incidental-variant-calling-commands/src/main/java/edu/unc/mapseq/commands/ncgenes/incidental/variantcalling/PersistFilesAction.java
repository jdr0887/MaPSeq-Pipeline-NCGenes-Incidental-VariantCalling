package edu.unc.mapseq.commands.ncgenes.incidental.variantcalling;

import java.util.concurrent.Executors;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncgenes.incidental.variantcalling.RegisterToIRODSRunnable;
import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;

@Command(scope = "ncgenes-incidental-variantcalling", name = "persist-files", description = "Persist Files")
@Service
public class PersistFilesAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(PersistFilesAction.class);

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Argument(index = 0, name = "sampleId", required = true, multiValued = false)
    private Long sampleId;

    public PersistFilesAction() {
        super();
    }

    @Override
    public Object execute() {
        logger.debug("ENTERING execute()");
        RegisterToIRODSRunnable runnable = new RegisterToIRODSRunnable();
        runnable.setMaPSeqDAOBeanService(maPSeqDAOBeanService);
        runnable.setSampleId(sampleId);
        Executors.newSingleThreadExecutor().execute(runnable);
        return null;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

}
