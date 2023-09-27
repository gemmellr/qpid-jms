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

import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.policy.JmsDefaultRedeliveryPolicy;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test for MessageConsumer that has a prefetch value of zero.
 */
public class JmsZeroPrefetchTest extends AmqpTestSupport {

    @Override
    public String getAmqpConnectionURIOptions() {
        return "jms.prefetchPolicy.all=0";
    }

    @Test
    @Timeout(60)
    public void testBlockingReceivesUnBlocksOnMessageSend() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());

        final MessageProducer producer = session.createProducer(queue);

        Thread producerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    Thread.sleep(1500);
                    producer.send(session.createTextMessage("Hello World! 1"));
                } catch (Exception e) {
                }
            }
        });
        producerThread.start();

        MessageConsumer consumer = session.createConsumer(queue);
        Message answer = consumer.receive();
        assertNotNull(answer, "Should have received a message!");

        final QueueViewMBean queueView = getProxyToQueue(getDestinationName());

        // Assert that we only pulled one message and that we didn't cause
        // the other message to be dispatched.
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return queueView.getQueueSize() == 0;
            }
        }));
    }

    @Test
    @Timeout(60)
    public void testRepeatedPullAttempts() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello World!"));

        // now lets receive it
        MessageConsumer consumer = session.createConsumer(queue);
        Message answer = consumer.receive(5000);
        assertNotNull(answer, "Should have received a message!");

        // check if method will return at all and will return a null
        answer = consumer.receive(1);
        assertNull(answer, "Should have not received a message!");
        answer = consumer.receiveNoWait();
        assertNull(answer, "Should have not received a message!");
    }

    @Test
    @Timeout(60)
    public void testTwoConsumers() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Msg1"));
        producer.send(session.createTextMessage("Msg2"));

        // now lets receive it
        MessageConsumer consumer1 = session.createConsumer(queue);
        MessageConsumer consumer2 = session.createConsumer(queue);
        TextMessage answer = (TextMessage)consumer1.receive(5000);
        assertNotNull(answer);
        assertEquals(answer.getText(), "Msg1", "Should have received a message!");
        answer = (TextMessage)consumer2.receive(5000);
        assertNotNull(answer);
        assertEquals(answer.getText(), "Msg2", "Should have received a message!");

        answer = (TextMessage)consumer2.receiveNoWait();
        assertNull(answer, "Should have not received a message!");
    }

    @Test
    @Timeout(60)
    public void testConsumerWithNoMessageDoesNotHogMessages() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());

        // Try and receive one message which will fail
        MessageConsumer consumer1 = session.createConsumer(queue);
        assertNull(consumer1.receive(10));

        // Now Producer a message
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Msg1"));

        // now lets receive it with the second consumer, the first should
        // not be accepting messages now and the broker should give it to
        // consumer 2.
        MessageConsumer consumer2 = session.createConsumer(queue);
        TextMessage answer = (TextMessage)consumer2.receive(3000);
        assertNotNull(answer);
        assertEquals(answer.getText(), "Msg1", "Should have received a message!");
    }

    @Test
    @Timeout(60)
    public void testConsumerReceivePrefetchZeroRedeliveryZero() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        // push message to queue
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("test.prefetch.zero");
        MessageProducer producer = session.createProducer(queue);
        TextMessage textMessage = session.createTextMessage("test Message");
        producer.send(textMessage);
        session.close();

        // consume and rollback - increase redelivery counter on message
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = session.createConsumer(queue);
        Message message = consumer.receive(3000);
        assertNotNull(message);
        session.rollback();
        session.close();

        JmsDefaultRedeliveryPolicy redeliveryPolicy = new JmsDefaultRedeliveryPolicy();
        redeliveryPolicy.setMaxRedeliveries(0);

        // Reconnect with zero prefetch and zero redeliveries allowed.
        connection.close();
        connection = createAmqpConnection();
        ((JmsConnection)connection).setRedeliveryPolicy(redeliveryPolicy);
        connection.start();

        // try consume with timeout - expect it to timeout and return NULL message
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
        consumer = session.createConsumer(queue);
        message = consumer.receive(1000);

        assertNull(message);
    }
}
