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
package org.apache.qpid.jms.failover;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test various client behaviors when the connection has gone offline.
 */
public class JmsOfflineBehaviorTests extends AmqpTestSupport {

    @Test
    @Timeout(60)
    void testConnectionCloseDoesNotBlock() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();
        stopPrimaryBroker();
        connection.close();
    }

    @Test
    @Timeout(60)
    void testSessionCloseDoesNotBlock() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        stopPrimaryBroker();
        session.close();
        connection.close();
    }

    @Test
    @Timeout(60)
    void testClientAckDoesNotBlock() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        MessageConsumer consumer = session.createConsumer(queue);
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Test"));

        Message message = consumer.receive(5000);
        assertNotNull(message);
        stopPrimaryBroker();
        message.acknowledge();

        connection.close();
    }

    @Test
    @Timeout(60)
    void testProducerCloseDoesNotBlock() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);

        stopPrimaryBroker();
        producer.close();
        connection.close();
    }

    @Test
    @Timeout(60)
    void testConsumerCloseDoesNotBlock() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        MessageConsumer consumer = session.createConsumer(queue);

        stopPrimaryBroker();
        consumer.close();
        connection.close();
    }

    @Test
    @Timeout(60)
    void testSessionCloseWithOpenResourcesDoesNotBlock() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        session.createConsumer(queue);
        session.createProducer(queue);

        stopPrimaryBroker();
        session.close();
        connection.close();
    }

    @Test
    @Timeout(60)
    void testGetRemoteURI() throws Exception {

        startNewBroker();

        URI brokerURI = new URI(getAmqpFailoverURI() + "failover.randomize=false");
        final JmsConnection connection = (JmsConnection) createAmqpConnection(brokerURI);
        connection.start();

        URI connectedURI = connection.getConnectedURI();
        assertNotNull(connectedURI);

        final List<URI> brokers = getBrokerURIs();
        assertEquals(brokers.get(0), connectedURI);

        stopPrimaryBroker();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                URI current = connection.getConnectedURI();
                if (current != null && current.equals(brokers.get(1))) {
                    return true;
                }

                return false;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)), "Should connect to secondary URI.");

        connection.close();
    }

    @Test
    @Timeout(60)
    void testClosedReourcesAreNotRestored() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI() + "?failover.maxReconnectDelay=500");
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        session.createConsumer(queue);
        session.createProducer(queue);

        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getQueueProducers().length);

        stopPrimaryBroker();
        session.close();
        restartPrimaryBroker();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getCurrentConnectionsCount() == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)), "Should have a new connection.");

        assertEquals(0, brokerService.getAdminView().getQueueSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getQueueProducers().length);

        connection.close();
    }
}
