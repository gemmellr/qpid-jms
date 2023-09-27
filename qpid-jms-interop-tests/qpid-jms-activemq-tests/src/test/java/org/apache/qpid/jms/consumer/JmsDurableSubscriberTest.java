/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import jakarta.jms.TopicSubscriber;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.policy.JmsDefaultPresettlePolicy;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Durable Topic Subscriber functionality.
 */
public class JmsDurableSubscriberTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsMessageConsumerTest.class);

    private static final int MSG_COUNT = 10;

    @Override
    public boolean isPersistent() {
        return true;
    }

    public String getSubscriptionName() {
        return testMethodName + "-subscriber";
    }

    @Test
    @Timeout(60)
    public void testCreateDurableSubscriber() throws Exception {
        connection = createAmqpConnection();
        connection.setClientID("DURABLE-AMQP");
        connection.start();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Topic topic = session.createTopic(testMethodName);
        MessageConsumer consumer = session.createDurableSubscriber(topic, getSubscriptionName());

        TopicViewMBean proxy = getProxyToTopic(testMethodName);
        assertEquals(0, proxy.getQueueSize());

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        consumer.close();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        session.unsubscribe(getSubscriptionName());
    }

    @Test
    @Timeout(60)
    public void testDurableSubscriptionUnsubscribe() throws Exception {
        connection = createAmqpConnection();
        connection.setClientID("DURABLE-AMQP");
        connection.start();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Topic topic = session.createTopic(testMethodName);
        session.createDurableSubscriber(topic, getSubscriptionName()).close();

        BrokerViewMBean broker = getProxyToBroker();
        assertEquals(1, broker.getInactiveDurableTopicSubscribers().length);

        session.unsubscribe(getSubscriptionName());

        assertEquals(0, broker.getInactiveDurableTopicSubscribers().length);
        assertEquals(0, broker.getDurableTopicSubscribers().length);
    }

    @Test
    @Timeout(60)
    public void testDurableSubscriptionUnsubscribeNoExistingSubThrowsJMSEx() throws Exception {
        connection = createAmqpConnection();
        connection.setClientID("DURABLE-AMQP");
        connection.start();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);

        BrokerViewMBean broker = getProxyToBroker();
        assertEquals(0, broker.getDurableTopicSubscribers().length);
        assertEquals(0, broker.getInactiveDurableTopicSubscribers().length);

        try {
            session.unsubscribe(getSubscriptionName());
            fail("Should have thrown an InvalidDestinationException");
        } catch (InvalidDestinationException ide) {
        }
    }

    @Test
    @Timeout(60)
    public void testDurableSubscriptionUnsubscribeInUseThrowsAndRecovers() throws Exception {
        connection = createAmqpConnection();
        connection.setClientID("DURABLE-AMQP");
        connection.start();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Topic topic = session.createTopic(testMethodName);
        MessageConsumer consumer = session.createDurableSubscriber(topic, getSubscriptionName());
        assertNotNull(consumer);

        BrokerViewMBean broker = getProxyToBroker();
        assertEquals(1, broker.getDurableTopicSubscribers().length);
        assertEquals(0, broker.getInactiveDurableTopicSubscribers().length);

        try {
            session.unsubscribe(getSubscriptionName());
            fail("Should have thrown a JMSException");
        } catch (JMSException ex) {
        }

        assertEquals(1, broker.getDurableTopicSubscribers().length);
        assertEquals(0, broker.getInactiveDurableTopicSubscribers().length);

        consumer.close();

        assertEquals(0, broker.getDurableTopicSubscribers().length);
        assertEquals(1, broker.getInactiveDurableTopicSubscribers().length);

        session.unsubscribe(getSubscriptionName());

        assertEquals(0, broker.getDurableTopicSubscribers().length);
        assertEquals(0, broker.getInactiveDurableTopicSubscribers().length);
    }

    @Test
    @Timeout(60)
    public void testDurableGoesOfflineAndReturns() throws Exception {
        connection = createAmqpConnection();
        connection.setClientID("DURABLE-AMQP");
        connection.start();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Topic topic = session.createTopic(testMethodName);
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, getSubscriptionName());

        TopicViewMBean proxy = getProxyToTopic(testMethodName);
        assertEquals(0, proxy.getQueueSize());

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        subscriber.close();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        subscriber = session.createDurableSubscriber(topic, getSubscriptionName());

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        subscriber.close();

        session.unsubscribe(getSubscriptionName());
    }

    @Test
    @Timeout(60)
    public void testOfflineSubscriberGetsItsMessages() throws Exception {
        connection = createAmqpConnection();
        connection.setClientID("DURABLE-AMQP");
        connection.start();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        final int MSG_COUNT = 5;

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Topic topic = session.createTopic(testMethodName);
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, getSubscriptionName());

        TopicViewMBean proxy = getProxyToTopic(testMethodName);
        assertEquals(0, proxy.getQueueSize());
        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        subscriber.close();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        MessageProducer producer = session.createProducer(topic);
        for (int i = 0; i < MSG_COUNT; i++) {
            producer.send(session.createTextMessage("Message: " + i));
        }
        producer.close();

        LOG.info("Bringing offline subscription back online.");
        subscriber = session.createDurableSubscriber(topic, getSubscriptionName());

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        final CountDownLatch messages = new CountDownLatch(MSG_COUNT);
        subscriber.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                LOG.info("Consumer got a message: {}", message);
                messages.countDown();
            }
        });

        assertTrue(messages.await(30, TimeUnit.SECONDS), "Only recieved messages: " + messages.getCount());

        subscriber.close();

        session.unsubscribe(getSubscriptionName());
    }

    @Test
    public void testDurableResubscribeWithNewNoLocalValue() throws Exception {
        connection = createAmqpConnection();
        connection.setClientID("DURABLE-AMQP");
        connection.start();

        JmsConnection jmsConnection = (JmsConnection) connection;
        ((JmsDefaultPresettlePolicy) jmsConnection.getPresettlePolicy()).setPresettleAll(true);

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(getDestinationName());

        // Create a Durable Topic Subscription with noLocal set to true.
        MessageConsumer durableSubscriber = session.createDurableSubscriber(topic, getSubscriptionName(), null, true);

        // Create a Durable Topic Subscription with noLocal set to true.
        MessageConsumer nonDurableSubscriber = session.createConsumer(topic);

        // Public first set, only the non durable sub should get these.
        publishToTopic(session, topic);

        LOG.debug("Testing that noLocal=true subscription doesn't get any messages.");

        // Standard subscriber should receive them
        for (int i = 0; i < MSG_COUNT; ++i) {
            Message message = nonDurableSubscriber.receive(5000);
            assertNotNull(message);
        }

        // Durable noLocal=true subscription should not receive them
        {
            Message message = durableSubscriber.receive(250);
            assertNull(message);
        }

        // Public second set for testing durable sub changed.
        publishToTopic(session, topic);

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        // Durable now goes inactive.
        durableSubscriber.close();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getDurableTopicSubscribers().length == 0;
            }
        }), "Should have no durables.");
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getInactiveDurableTopicSubscribers().length == 1;
            }
        }), "Should have an inactive sub.");

        LOG.debug("Testing that updated noLocal=false subscription does get any messages.");

        // Recreate a Durable Topic Subscription with noLocal set to false.
        durableSubscriber = session.createDurableSubscriber(topic, getSubscriptionName(), null, false);

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        // Durable noLocal=false subscription should not receive them as the subscriptions should
        // have been removed and recreated to update the noLocal flag.
        {
            Message message = durableSubscriber.receive(250);
            assertNull(message);
        }

        // Public third set which should get queued for the durable sub with noLocal=false
        publishToTopic(session, topic);

        // Durable subscriber should receive them
        for (int i = 0; i < MSG_COUNT; ++i) {
            Message message = durableSubscriber.receive(5000);
            assertNotNull(message, "Should get local messages now");
        }

        durableSubscriber.close();

        session.unsubscribe(getSubscriptionName());
    }

    private void publishToTopic(Session session, Topic destination) throws Exception {
        MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < MSG_COUNT; ++i) {
            producer.send(session.createMessage());
        }

        producer.close();
    }
}
