package org.example;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Enumeration;

public class Main {
    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String[] QUEUES = {"queue1", "queue2", "queue3"};
    private static final String[] TOPICS = {"topic1", "topic2", "topic3"};

    public static void main(String[] args) {
        try {
            // ارسال پیام‌ها به Queues و Topics
            for (String queue : QUEUES) {
                sendMessage(queue, "Message for " + queue, false);
            }

            for (String topic : TOPICS) {
                sendMessage(topic, "Message for " + topic, true);
            }

            // مشاهده پیام‌ها در Queue ها
            for (String queue : QUEUES) {
                browseMessages(queue, false);
            }

            // مشاهده پیام‌ها در Topic ها
            for (String topic : TOPICS) {
                browseMessages(topic, true);
            }

            // ایجاد چند مصرف‌کننده برای Queue ها
            for (String queue : QUEUES) {
                receiveMessage(queue, false, "consumer1");
                receiveMessage(queue, false, "consumer2");
            }

            // ایجاد چند مصرف‌کننده برای Topic ها
            for (String topic : TOPICS) {
                // مصرف‌کننده‌های مختلف برای Topic
                receiveMessage(topic, true, "durableConsumer1");
                receiveMessage(topic, true, "durableConsumer2");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void sendMessage(String destinationName, String messageText, boolean isTopic) {
        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
            Connection connection = factory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = isTopic ? session.createTopic(destinationName) : session.createQueue(destinationName);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            TextMessage message = session.createTextMessage(messageText);
            producer.send(message);
            System.out.println("Sent message to " + destinationName + ": " + messageText);
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public static void receiveMessage(String destinationName, boolean isTopic, String consumerId) {
        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
            Connection connection = factory.createConnection();
            connection.setClientID("uniqueClientID-" + consumerId);  // تنظیم clientID
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = isTopic ? session.createTopic(destinationName) : session.createQueue(destinationName);

            MessageConsumer consumer;
            if (isTopic) {
                // برای Topic، از Durable Subscription استفاده می‌کنیم
                consumer = session.createDurableSubscriber((Topic) destination, consumerId);
            } else {
                // برای Queue، مصرف‌کنندگان به طور مشترک پیام‌ها را دریافت می‌کنند
                consumer = session.createConsumer(destination);
            }

            Message message = consumer.receive(5000);
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                System.out.println("Received from " + destinationName + ": " + textMessage.getText());
                callback(destinationName, textMessage.getText());
            }
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }


    private static void callback(String destinationName, String message) {
        System.out.println("Callback executed for " + destinationName + " with message: " + message);
    }

    public static void browseMessages(String destinationName, boolean isTopic) {
        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
            Connection connection = factory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = isTopic ? session.createTopic(destinationName) : session.createQueue(destinationName);

            // اگر Topic باشد از MessageConsumer استفاده می شود
            if (isTopic) {
                MessageConsumer consumer = session.createConsumer(destination);
                System.out.println("Receiving messages from Topic: " + destinationName);
                Message message = consumer.receive(5000);
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    System.out.println("Received from " + destinationName + ": " + textMessage.getText());
                }
            } else {
                // اگر Queue باشد از QueueBrowser استفاده می شود
                QueueBrowser browser = session.createBrowser((Queue) destination);
                System.out.println("Browsing messages in " + destinationName + ":");
                Enumeration<?> messages = browser.getEnumeration();
                while (messages.hasMoreElements()) {
                    Message message = (Message) messages.nextElement();
                    if (message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        System.out.println(" - " + textMessage.getText());
                    }
                }
            }

            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }


}
