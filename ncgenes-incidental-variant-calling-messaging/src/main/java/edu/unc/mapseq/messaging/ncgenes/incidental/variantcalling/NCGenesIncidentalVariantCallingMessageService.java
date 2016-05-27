package edu.unc.mapseq.messaging.ncgenes.incidental.variantcalling;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NCGenesIncidentalVariantCallingMessageService {

    private static final Logger logger = LoggerFactory.getLogger(NCGenesIncidentalVariantCallingMessageService.class);

    private ConnectionFactory connectionFactory;

    private NCGenesIncidentalVariantCallingMessageListener messageListener;

    private String destinationName;

    private Connection connection;

    private Session session;

    public NCGenesIncidentalVariantCallingMessageService() {
        super();
    }

    public void start() throws Exception {
        logger.debug("ENTERING start()");
        this.connection = connectionFactory.createConnection();
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = this.session.createQueue(this.destinationName);
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(getMessageListener());
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

    public NCGenesIncidentalVariantCallingMessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(NCGenesIncidentalVariantCallingMessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

}
