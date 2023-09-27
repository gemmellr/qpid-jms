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
package org.apache.qpid.jms.destinations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import jakarta.jms.Connection;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test functionality of Temporary Queues.
 */
public class JmsTemporaryQueueTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsTemporaryQueueTest.class);

    @Test
    @Timeout(60)
    public void testCreateTemporaryQueue() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        TemporaryQueue queue = session.createTemporaryQueue();
        session.createConsumer(queue);

        assertEquals(1, brokerService.getAdminView().getTemporaryQueues().length);
    }

    @Test
    @Timeout(60)
    public void testCantConsumeFromTemporaryQueueCreatedOnAnotherConnection() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue tempQueue = session.createTemporaryQueue();
        session.createConsumer(tempQueue);

        Connection connection2 = createAmqpConnection();
        try {
            Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try {
                session2.createConsumer(tempQueue);
                fail("should not be able to consumer from temporary queue from another connection");
            } catch (InvalidDestinationException ide) {
                // expected
            }
        } finally {
            connection2.close();
        }
    }

    @Test
    @Timeout(60)
    public void testCantSendToTemporaryQueueFromClosedConnection() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue tempQueue = session.createTemporaryQueue();

        Connection connection2 = createAmqpConnection();
        try {
            Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Message msg = session2.createMessage();
            MessageProducer producer = session2.createProducer(tempQueue);

            // Close the original connection
            connection.close();

            try {
                producer.send(msg);
                fail("should not be able to send to temporary queue from closed connection");
            } catch (IllegalStateException ide) {
                // expected
            }
        } finally {
            connection2.close();
        }
    }

    @Test
    @Timeout(60)
    public void testCantDeleteTemporaryQueueWithConsumers() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue tempQueue = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(tempQueue);

        try {
            tempQueue.delete();
            fail("should not be able to delete temporary queue with active consumers");
        } catch (IllegalStateException ide) {
            // expected
        }

        consumer.close();

        // Now it should be allowed
        tempQueue.delete();
    }
}
