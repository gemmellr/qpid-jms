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
package org.apache.qpid.jms.usecases;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.BytesMessage;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsLargeMessagesInGroupsTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsLargeMessagesInGroupsTest.class);

    private static final int ITERATIONS = 10;
    private static final int MESSAGE_COUNT = 10;
    private static final int MESSAGE_SIZE = 200 * 1024;
    private static final int RECEIVE_TIMEOUT = 5000;
    private static final String JMSX_GROUP_ID = "JmsGroupsTest";

    @Test
    @Timeout(60)
    public void testGroupSeqIsNeverLost() throws Exception {
        AtomicInteger sequenceCounter = new AtomicInteger();

        for (int i = 0; i < ITERATIONS; ++i) {
            connection = createAmqpConnection();
            {
                sendMessagesToBroker(MESSAGE_COUNT, sequenceCounter);
                connection.start();
                readMessagesOnBroker(MESSAGE_COUNT);
            }
            connection.close();
        }
    }

    protected void readMessagesOnBroker(int count) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageConsumer consumer = session.createConsumer(queue);

        for (int i = 0; i < MESSAGE_COUNT; ++i) {
            Message message = consumer.receive(RECEIVE_TIMEOUT);
            assertNotNull(message);
            LOG.info("Read message #{}: type = {}", i, message.getClass().getSimpleName());
            String gid = message.getStringProperty("JMSXGroupID");
            String seq = message.getStringProperty("JMSXGroupSeq");
            LOG.info("Message assigned JMSXGroupID := {}", gid);
            LOG.info("Message assigned JMSXGroupSeq := {}", seq);
        }

        consumer.close();
    }

    protected void sendMessagesToBroker(int count, AtomicInteger sequence) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageProducer producer = session.createProducer(queue);

        byte[] buffer = new byte[MESSAGE_SIZE];
        for (count = 0; count < MESSAGE_SIZE; count++) {
            String s = String.valueOf(count % 10);
            Character c = s.charAt(0);
            int value = c.charValue();
            buffer[count] = (byte) value;
        }

        LOG.info("Sending {} messages to destination: {}", MESSAGE_COUNT, queue);
        for (int i = 1; i <= MESSAGE_COUNT; i++) {
            BytesMessage message = session.createBytesMessage();
            message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
            message.setStringProperty("JMSXGroupID", JMSX_GROUP_ID);
            message.setIntProperty("JMSXGroupSeq", sequence.incrementAndGet());
            message.writeBytes(buffer);
            producer.send(message);
        }

        producer.close();
    }
}
