package org.example;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Enumeration;

public class Main {
    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String[] QUEUES = {"queue1", "queue2", "queue3"};

    public static void main(String[] args) {
        try {
            for (String queue : QUEUES) {
                sendMessage(queue, "Message for " + queue);
            }

            for (String queue : QUEUES) {
                browseMessages(queue);
            }

            for (String queue : QUEUES) {
                receiveMessage(queue);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sendMessage(String queueName, String messageText) {
        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
            Connection connection = factory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            TextMessage message = session.createTextMessage(messageText);
            producer.send(message);
            System.out.println("Sent message to " + queueName + ": " + messageText);
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public static void receiveMessage(String queueName) {
        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
            Connection connection = factory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName);
            MessageConsumer consumer = session.createConsumer(destination);
            Message message = consumer.receive(5000);
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                System.out.println("Received from " + queueName + ": " + textMessage.getText());
                callback(queueName, textMessage.getText());
            }
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    private static void callback(String queueName, String message) {
        System.out.println("Callback executed for " + queueName + " with message: " + message);
    }

    public static void browseMessages(String queueName) {
        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
            Connection connection = factory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);
            QueueBrowser browser = session.createBrowser(queue);

            System.out.println("Browsing messages in " + queueName + ":");
            Enumeration<?> messages = browser.getEnumeration();
            while (messages.hasMoreElements()) {
                Message message = (Message) messages.nextElement();
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    System.out.println(" - " + textMessage.getText());
                }
            }

            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}