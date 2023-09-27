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
package org.apache.qpid.jms.transactions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import jakarta.jms.Destination;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Basic tests for Session in Transacted mode.
 */
public class JmsTransactedSessionTest extends AmqpTestSupport {

    @Test
    @Timeout(60)
    public void testCreateTxSession() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        assertNotNull(session);
        assertTrue(session.getTransacted());

        session.close();
    }

    @Test
    @Timeout(60)
    public void testCommitOnSessionWithNoWork() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        assertNotNull(session);
        assertTrue(session.getTransacted());

        session.commit();
    }

    @Test
    @Timeout(60)
    public void testRollbackOnSessionWithNoWork() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        assertNotNull(session);
        assertTrue(session.getTransacted());

        session.rollback();
    }

    @Test
    @Timeout(60)
    public void testCloseSessionRollsBack() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        sendToAmqQueue(2);

        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(2, proxy.getQueueSize());

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(testMethodName);
        MessageConsumer consumer = session.createConsumer(queue);
        Message message = consumer.receive(5000);
        assertNotNull(message);
        message = consumer.receive(5000);
        assertNotNull(message);

        assertEquals(2, proxy.getQueueSize());
        session.close();
        assertEquals(2, proxy.getQueueSize());
    }

    @Test
    @Timeout(60)
    public void testRollbackSentMessagesThenConsumeWithTopic() throws Exception {
        doRollbackSentMessagesThenConsumeTestImpl(true);
    }

    @Test
    @Timeout(60)
    public void testRollbackSentMessagesThenConsumeWithQueue() throws Exception {
        doRollbackSentMessagesThenConsumeTestImpl(false);
    }

    private void doRollbackSentMessagesThenConsumeTestImpl(boolean topic) throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination dest = null;
        if(topic) {
            dest = session.createTopic(getTestName());
        } else {
            dest = session.createQueue(getTestName());
        }

        MessageProducer producer = session.createProducer(dest);
        MessageConsumer consumer = session.createConsumer(dest);

        int messageCount = 3;
        TextMessage msg = null;
        // Send messages and call rollback
        for (int i = 1; i <= messageCount; i++) {
            String msgText = "Message " + i;
            msg = session.createTextMessage(msgText);
            producer.send(msg);
            LOG.info(msgText + " sent");
        }

        session.rollback();

        // Should not consume any messages since rollback() was called
        msg = (TextMessage) consumer.receive(200);
        if (msg != null) {
            fail("Received unexpected message");
        }

        // Send messages and call commit
        for (int i = 1; i <= messageCount; i++) {
            String msgText = "Message " + i;
            msg = session.createTextMessage(msgText);
            producer.send(msg);
            LOG.info(msgText + " sent again");
        }

        session.commit();

        // consume all messages
        for (int i = 1; i <= messageCount; i++) {
            msg = (TextMessage) consumer.receive(3000);
            if (msg == null) {
                fail("receive() returned null, message " + i + " was not received");
            } else if (!msg.getText().equals("Message " + i)) {
                fail("Received '" + msg.getText() + "' but expected 'Message " + i + "'");
            } else {
                LOG.info("Received: " + msg.getText());
            }
        }

        session.commit();
    }
}
