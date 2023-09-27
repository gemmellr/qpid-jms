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
package org.apache.qpid.jms.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import jakarta.jms.Destination;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import jakarta.jms.TopicPublisher;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsTopicSession;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.provider.mock.MockRemotePeer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

public class JmsTopicPublisherTest extends JmsConnectionTestSupport {

    private JmsTopicSession session;
    private final MockRemotePeer remotePeer = new MockRemotePeer();

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        remotePeer.start();
        connection = createConnectionToMockProvider();
        session = (JmsTopicSession) connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        try {
            remotePeer.terminate();
        } finally {
            super.tearDown();
        }
    }

    @Test
    @Timeout(10)
    public void testMultipleCloseCallsNoErrors() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(topic);
        publisher.close();
        publisher.close();
    }

    @Test
    @Timeout(10)
    public void testGetTopic() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(topic);
        assertSame(topic, publisher.getTopic());
    }

    @Test
    @Timeout(10)
    public void testPublishMessage() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(topic);
        Message message = session.createMessage();
        publisher.publish(message);

        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        Destination destination = message.getJMSDestination();
        assertEquals(topic, destination);
    }

    @Test
    @Timeout(10)
    public void testPublishMessageOnProvidedTopic() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(null);
        Message message = session.createMessage();
        publisher.publish(topic, message);

        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        Destination destination = message.getJMSDestination();
        assertEquals(topic, destination);
    }

    @Test
    @Timeout(10)
    public void testPublishMessageOnProvidedTopicWhenNotAnonymous() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(topic);
        Message message = session.createMessage();

        try {
            publisher.publish(session.createTopic(getTestName() + "1"), message);
            fail("Should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException uoe) {}

        try {
            publisher.publish((Topic) null, message);
            fail("Should throw InvalidDestinationException");
        } catch (InvalidDestinationException ide) {}
    }

    @Test
    @Timeout(10)
    public void testPublishMessageWithDeliveryOptions() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(topic);
        Message message = session.createMessage();
        publisher.publish(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        Destination destination = message.getJMSDestination();
        assertEquals(topic, destination);
    }

    @Test
    @Timeout(10)
    public void testPublishMessageWithOptionsOnProvidedTopic() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(null);
        Message message = session.createMessage();
        publisher.publish(topic, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        Destination destination = message.getJMSDestination();
        assertEquals(topic, destination);
    }

    @Test
    @Timeout(10)
    public void testPublishMessageWithOptionsOnProvidedTopicWhenNotAnonymous() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(topic);
        Message message = session.createMessage();

        try {
            publisher.publish(session.createTopic(getTestName() + "1"), message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException uoe) {}

        try {
            publisher.publish((Topic) null, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Should throw InvalidDestinationException");
        } catch (InvalidDestinationException ide) {}
    }
}
