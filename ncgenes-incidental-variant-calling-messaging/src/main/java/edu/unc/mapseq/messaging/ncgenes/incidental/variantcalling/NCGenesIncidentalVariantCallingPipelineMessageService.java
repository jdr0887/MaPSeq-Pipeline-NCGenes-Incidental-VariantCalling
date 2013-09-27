package edu.unc.mapseq.messaging.ncgenes.incidental.variantcalling;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.pipeline.PipelineBeanService;

public class NCGenesIncidentalVariantCallingPipelineMessageService {

    private final Logger logger = LoggerFactory.getLogger(NCGenesIncidentalVariantCallingPipelineMessageService.class);

    private ConnectionFactory connectionFactory;

    private PipelineBeanService pipelineBeanService;

    private String destinationName;

    private Connection connection;

    private Session session;

    public NCGenesIncidentalVariantCallingPipelineMessageService() {
        super();
    }

    public void start() throws Exception {
        logger.debug("ENTERING start()");
        this.connection = connectionFactory.createConnection();
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = this.session.createQueue(this.destinationName);
        MessageConsumer consumer = session.createConsumer(destination);
        NCGenesIncidentalVariantCallingPipelineMessageListener messageListener = new NCGenesIncidentalVariantCallingPipelineMessageListener();
        messageListener.setPipelineBeanService(pipelineBeanService);
        consumer.setMessageListener(messageListener);
        this.connection.start();
    }

    public void stop() throws Exception {
        logger.debug("ENTERING stop()");
        if (this.session != null) {
            this.session.close();
        }
        if (this.connection != null) {
            this.connection.stop();
            this.connection.close();
        }
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public PipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(PipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

}
