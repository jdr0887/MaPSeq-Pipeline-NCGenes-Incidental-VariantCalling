package edu.unc.mapseq.commons.ncgenes.incidental.variantcalling;

import java.io.File;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class AssertExpectedOutputFilesExistRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(AssertExpectedOutputFilesExistRunnable.class);

    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    private Sample sample;

    private String version;

    private String incidental;

    public AssertExpectedOutputFilesExistRunnable(MaPSeqDAOBeanService maPSeqDAOBeanService, Sample sample, String version,
            String incidental) {
        super();
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
        this.sample = sample;
        this.version = version;
        this.incidental = incidental;
    }

    @Override
    public void run() {
        logger.debug("ENTERING run()");
        Workflow workflow = null;
        try {
            workflow = maPSeqDAOBeanService.getWorkflowDAO().findByName("NCGenesIncidentalVariantCalling").get(0);
        } catch (MaPSeqDAOException e1) {
            e1.printStackTrace();
        }

        File outputDirectory = SequencingWorkflowUtil.createOutputDirectory(sample, workflow);

        String rootFileName = String.format("%s_%s_L%03d.fixed-rg.deduped.realign.fixmate.recal", sample.getFlowcell().getName(),
                sample.getBarcode(), sample.getLaneIndex());

        File incidentalVcf = new File(outputDirectory, String.format("%s.incidental-%s.v-%s.vcf", rootFileName, incidental, version));

        for (File f : Arrays.asList(incidentalVcf)) {
            if (!f.exists()) {
                logger.warn(f.getAbsolutePath());
            }
        }
    }

    public MaPSeqDAOBeanService getMaPSeqDAOBeanService() {
        return maPSeqDAOBeanService;
    }

    public void setMaPSeqDAOBeanService(MaPSeqDAOBeanService maPSeqDAOBeanService) {
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
    }

    public Sample getSample() {
        return sample;
    }

    public void setSample(Sample sample) {
        this.sample = sample;
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
