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

@Command(scope = "ncgenes-incidental-variantcalling", name = "register-to-irods", description = "Register to iRODS")
@Service
public class RegisterToIRODSAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(RegisterToIRODSAction.class);

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Reference
    private MaPSeqConfigurationService maPSeqConfigurationService;

    @Argument(index = 0, name = "sampleId", required = true, multiValued = false)
    private Long sampleId;

    @Argument(index = 1, name = "version", required = true, multiValued = false)
    private String version;

    @Argument(index = 2, name = "incidental", required = true, multiValued = false)
    private String incidental;

    public RegisterToIRODSAction() {
        super();
    }

    @Override
    public Object execute() {
        logger.debug("ENTERING execute()");
        RegisterToIRODSRunnable runnable = new RegisterToIRODSRunnable();
        runnable.setMaPSeqDAOBeanService(maPSeqDAOBeanService);
        runnable.setRunMode(maPSeqConfigurationService.getRunMode());
        runnable.setSampleId(sampleId);
        runnable.setIncidental(incidental);
        runnable.setVersion(version);
        Executors.newSingleThreadExecutor().execute(runnable);
        return null;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getIncidental() {
        return incidental;
    }

    public void setIncidental(String incidental) {
        this.incidental = incidental;
    }

}
