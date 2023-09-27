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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TransactionRolledBackException;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test MessageProducer behavior when in a TX and failover occurs.
 */
public class JmsTxProducerFailoverTest extends AmqpTestSupport {

    @Override
    protected boolean isPersistent() {
        return true;
    }

    /*
     * Test that the TX doesn't start until the first send so a failover
     * before then should allow Commit to work as expected.
     */
    @Test
    @Timeout(60)
    void testTxProducerSendAfterFailoverCommits() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());

        connection = createAmqpConnection(brokerURI);
        connection.start();

        final int MSG_COUNT = 5;
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(testMethodName);
        final MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(0, proxy.getQueueSize());

        stopPrimaryBroker();
        restartPrimaryBroker();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getCurrentConnectionsCount() == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)), "Should have a new connection.");

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getQueueProducers().length == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(50)), "Should have a recovered producer.");

        for (int i = 0; i < MSG_COUNT; ++i) {
            LOG.debug("Producer sening message #{}", i + 1);
            producer.send(session.createTextMessage("Message: " + i));
        }

        proxy = getProxyToQueue(testMethodName);
        assertEquals(0, proxy.getQueueSize());

        try {
            session.commit();
            LOG.info("Transacted commit ok after failover.");
        } catch (TransactionRolledBackException rb) {
            fail("Session commit should not have failed with TX rolled back.");
        }

        assertEquals(MSG_COUNT, proxy.getQueueSize());
    }

    /*
     * Tests that even if all sends complete prior to failover the commit that follows
     * will fail and the message are not present on the broker.
     */
    @Test
    @Timeout(60)
    void testTxProducerSendsThenFailoverCommitFails() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());

        connection = createAmqpConnection(brokerURI);
        connection.start();

        final int MSG_COUNT = 5;
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(testMethodName);
        final MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(0, proxy.getQueueSize());

        for (int i = 0; i < MSG_COUNT; ++i) {
            LOG.debug("Producer sening message #{}", i + 1);
            producer.send(session.createTextMessage("Message: " + i));
        }

        assertEquals(0, proxy.getQueueSize());

        stopPrimaryBroker();
        restartPrimaryBroker();

        proxy = getProxyToQueue(testMethodName);
        assertEquals(0, proxy.getQueueSize());

        try {
            session.commit();
            fail("Session commit should have failed with TX rolled back.");
        } catch (TransactionRolledBackException rb) {
            LOG.info("Transacted commit failed after failover: {}", rb.getMessage());
        }

        assertEquals(0, proxy.getQueueSize());
    }

    @Test
    @Timeout(60)
    void testTxProducerRollbackAfterFailoverGetsNoErrors() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());

        connection = createAmqpConnection(brokerURI);
        connection.start();

        JmsConnection jmsConnection = (JmsConnection) connection;
        jmsConnection.setForceSyncSend(true);

        final int MSG_COUNT = 5;
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(testMethodName);
        final MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(0, proxy.getQueueSize());

        for (int i = 0; i < MSG_COUNT; ++i) {
            LOG.debug("Producer sening message #{}", i + 1);
            producer.send(session.createTextMessage("Message: " + i));
        }

        assertEquals(0, proxy.getQueueSize());

        stopPrimaryBroker();
        restartPrimaryBroker();

        proxy = getProxyToQueue(testMethodName);
        assertEquals(0, proxy.getQueueSize());

        try {
            session.rollback();
            LOG.info("Transacted rollback after failover ok");
        } catch (JMSException ex) {
            LOG.warn("Error on rollback not expected: ", ex);
            fail("Session rollback should not have failed: " + ex.getMessage());
        }

        assertEquals(0, proxy.getQueueSize());

        LOG.info("Test {} compelted without error as expected:", getTestName());
    }

    /*
     * Tests that if some sends happen and then a failover followed by additional
     * sends the commit will fail and no messages are left on the broker.
     */
    @Test
    @Timeout(60)
    void testTxProducerSendWorksButCommitFails() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());

        connection = createAmqpConnection(brokerURI);
        connection.start();

        final int MSG_COUNT = 10;
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(testMethodName);
        final MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(0, proxy.getQueueSize());

        for (int i = 0; i < MSG_COUNT / 2; ++i) {
            LOG.debug("Producer sening message #{}", i + 1);
            producer.send(session.createTextMessage("Message: " + i));
        }

        assertEquals(0, proxy.getQueueSize());

        stopPrimaryBroker();
        restartPrimaryBroker();

        proxy = getProxyToQueue(testMethodName);
        assertEquals(0, proxy.getQueueSize());

        for (int i = MSG_COUNT / 2; i < MSG_COUNT; ++i) {
            LOG.debug("Producer sening message #{}", i + 1);
            producer.send(session.createTextMessage("Message: " + i));
        }

        try {
            session.commit();
            fail("Session commit should have failed with TX rolled back.");
        } catch (TransactionRolledBackException rb) {
            LOG.info("Transacted commit failed after failover: {}", rb.getMessage());
        }

        assertEquals(0, proxy.getQueueSize());
    }
}
