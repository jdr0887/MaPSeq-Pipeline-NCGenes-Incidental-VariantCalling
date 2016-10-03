package edu.unc.mapseq.commons.ncgenes.incidental.variantcalling;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Job;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.sequencing.gatk.GATKUnifiedGenotyper;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.sequencing.IRODSBean;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class RegisterToIRODSRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RegisterToIRODSRunnable.class);

    private MaPSeqDAOBeanService mapseqDAOBeanService;

    private WorkflowRunAttempt workflowRunAttempt;

    public RegisterToIRODSRunnable() {
        super();
    }

    public RegisterToIRODSRunnable(MaPSeqDAOBeanService mapseqDAOBeanService, WorkflowRunAttempt workflowRunAttempt) {
        super();
        this.mapseqDAOBeanService = mapseqDAOBeanService;
        this.workflowRunAttempt = workflowRunAttempt;
    }

    @Override
    public void run() {
        final WorkflowRun workflowRun = workflowRunAttempt.getWorkflowRun();
        final Workflow workflow = workflowRun.getWorkflow();

        try {

            Set<Sample> sampleSet = SequencingWorkflowUtil.getAggregatedSamples(mapseqDAOBeanService, workflowRunAttempt);
            if (CollectionUtils.isEmpty(sampleSet)) {
                logger.warn("No Samples found");
                return;
            }

            String version = null;
            String incidental = null;

            Set<Attribute> attributeSet = workflowRun.getAttributes();
            if (attributeSet != null && !attributeSet.isEmpty()) {
                Iterator<Attribute> attributeIter = attributeSet.iterator();
                while (attributeIter.hasNext()) {
                    Attribute attribute = attributeIter.next();
                    if ("version".equals(attribute.getName())) {
                        version = attribute.getValue();
                    }
                    if ("incidental".equals(attribute.getName())) {
                        incidental = attribute.getValue();
                    }
                }
            }

            if (version == null & incidental == null) {
                logger.warn("Both version and incidental id were null...returning empty irods post-run registration dag");
                return;
            }

            for (Sample sample : sampleSet) {
                File outputDirectory = SequencingWorkflowUtil.createOutputDirectory(sample, workflow);
                File tmpDir = new File(outputDirectory, "tmp");
                if (!tmpDir.exists()) {
                    tmpDir.mkdirs();
                }

                // assumption: a dash is used as a delimiter between a participantId
                // and the external code
                int idx = sample.getName().lastIndexOf("-");
                String participantId = idx != -1 ? sample.getName().substring(0, idx) : sample.getName();

                String irodsDirectory = String.format("/MedGenZone/%s/sequencing/ncgenes/analysis/%s/L%03d_%s/%s/%s",
                        workflow.getSystem().getValue(), sample.getFlowcell().getName(), sample.getLaneIndex(), sample.getBarcode(),
                        workflow.getName(), version);

                CommandOutput commandOutput = null;

                List<CommandInput> commandInputList = new ArrayList<CommandInput>();

                CommandInput commandInput = new CommandInput();
                commandInput.setExitImmediately(Boolean.FALSE);
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("$IRODS_HOME/imkdir -p %s%n", irodsDirectory));
                sb.append(String.format("$IRODS_HOME/imeta add -C %s Project NCGENES%n", irodsDirectory));
                sb.append(String.format("$IRODS_HOME/imeta add -C %s ParticipantId %s NCGENES%n", irodsDirectory, participantId));
                commandInput.setCommand(sb.toString());
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                List<IRODSBean> files2RegisterToIRODS = new LinkedList<IRODSBean>();

                String rootFileName = String.format("%s_%s_L%03d.fixed-rg.deduped.realign.fixmate.recal", sample.getFlowcell().getName(),
                        sample.getBarcode(), sample.getLaneIndex());

                List<ImmutablePair<String, String>> attributeList = Arrays.asList(
                        new ImmutablePair<String, String>("ParticipantId", participantId),
                        new ImmutablePair<String, String>("MaPSeqWorkflowVersion", version),
                        new ImmutablePair<String, String>("MaPSeqWorkflowName", workflow.getName()),
                        new ImmutablePair<String, String>("MaPSeqStudyName", sample.getStudy().getName()),
                        new ImmutablePair<String, String>("MaPSeqSampleId", sample.getId().toString()),
                        new ImmutablePair<String, String>("MaPSeqSystem", workflow.getSystem().getValue()),
                        new ImmutablePair<String, String>("MaPSeqFlowcellId", sample.getFlowcell().getId().toString()),
                        new ImmutablePair<String, String>("IncidentalId", incidental),
                        new ImmutablePair<String, String>("IncidentalVersion", version));

                List<ImmutablePair<String, String>> attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", GATKUnifiedGenotyper.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_VCF.toString()));
                File incidentalVcf = new File(outputDirectory,
                        String.format("%s.incidental-%s.v-%s.vcf", rootFileName, incidental, version));
                Job job = SequencingWorkflowUtil.findJob(mapseqDAOBeanService, workflowRunAttempt.getId(),
                        GATKUnifiedGenotyper.class.getName(), incidentalVcf);
                if (job != null) {
                    attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobId", job.getId().toString()));
                    if (StringUtils.isNotEmpty(job.getCommandLine())) {
                        attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobCommandLine", job.getCommandLine()));
                    }
                } else {
                    logger.warn(String.format("Couldn't find job for: %d, %s", workflowRunAttempt.getId(),
                            GATKUnifiedGenotyper.class.getName()));
                }
                files2RegisterToIRODS.add(new IRODSBean(incidentalVcf, attributeListWithJob));

                for (IRODSBean bean : files2RegisterToIRODS) {

                    File f = bean.getFile();
                    if (!f.exists()) {
                        logger.warn("file to register doesn't exist: {}", f.getAbsolutePath());
                        continue;
                    }

                    commandInput = new CommandInput();
                    commandInput.setExitImmediately(Boolean.FALSE);

                    StringBuilder registerCommandSB = new StringBuilder();
                    String registrationCommand = String.format("$IRODS_HOME/ireg -f %s %s/%s", bean.getFile().getAbsolutePath(),
                            irodsDirectory, bean.getFile().getName());
                    String deRegistrationCommand = String.format("$IRODS_HOME/irm -U %s/%s", irodsDirectory, bean.getFile().getName());
                    registerCommandSB.append(registrationCommand).append("\n");
                    registerCommandSB
                            .append(String.format("if [ $? != 0 ]; then %s; %s; fi%n", deRegistrationCommand, registrationCommand));
                    commandInput.setCommand(registerCommandSB.toString());
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);

                    commandInput = new CommandInput();
                    commandInput.setExitImmediately(Boolean.FALSE);
                    sb = new StringBuilder();
                    for (ImmutablePair<String, String> attribute : bean.getAttributes()) {
                        sb.append(String.format("$IRODS_HOME/imeta add -d %s/%s %s %s NCGenes%n", irodsDirectory, bean.getFile().getName(),
                                attribute.getLeft(), attribute.getRight()));
                    }
                    commandInput.setCommand(sb.toString());
                    commandInput.setWorkDir(tmpDir);
                    commandInputList.add(commandInput);

                }

                File mapseqrc = new File(System.getProperty("user.home"), ".mapseqrc");
                Executor executor = BashExecutor.getInstance();

                for (CommandInput ci : commandInputList) {
                    try {
                        logger.info("ci.getCommand(): {}", ci.getCommand());
                        commandOutput = executor.execute(ci, mapseqrc);
                        logger.info("commandOutput.getExitCode(): {}", commandOutput.getExitCode());
                        logger.debug("commandOutput.getStdout(): {}", commandOutput.getStdout());
                    } catch (ExecutorException e) {
                        if (commandOutput != null) {
                            logger.warn("commandOutput.getStderr(): {}", commandOutput.getStderr());
                        }
                    }
                }

            }

        } catch (WorkflowException | MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

    public MaPSeqDAOBeanService getMapseqDAOBeanService() {
        return mapseqDAOBeanService;
    }

    public void setMapseqDAOBeanService(MaPSeqDAOBeanService mapseqDAOBeanService) {
        this.mapseqDAOBeanService = mapseqDAOBeanService;
    }

    public WorkflowRunAttempt getWorkflowRunAttempt() {
        return workflowRunAttempt;
    }

    public void setWorkflowRunAttempt(WorkflowRunAttempt workflowRunAttempt) {
        this.workflowRunAttempt = workflowRunAttempt;
    }

}
