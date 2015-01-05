package edu.unc.mapseq.messaging;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

public class MessagingTest {

    @Test
    public void testQueue() {
        // String host = "localhost";
        String host = "152.19.198.146";
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                host));
        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/ncgenes.incidental.variantcalling");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            // String format =
            // "{\"entities\":[{\"entityType\":\"Sample\",\"id\":\"%d\"},{\"entityType\":\"WorkflowRun\",\"name\":\"test-%d\"}]}";
            // producer.send(session.createTextMessage(String.format(format, System.getProperty("user.name"), 123123,
            // 123123)));

            String format = "{\"entities\":[{\"entityType\":\"Sample\",\"id\":\"%d\"},{\"entityType\":\"WorkflowRun\",\"name\":\"%s\",\"attributes\":[{\"name\":\"version\",\"value\":\"%d\"},{\"name\":\"incidental\",\"value\":\"%d\"}]}]}";
            String message = String.format(format, 1940353, "NCG_00609_V13_Incidental9_41926.57625135", 13, 9);
            System.out.println(message);
            // producer.send(session.createTextMessage(message));
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

    }
}
