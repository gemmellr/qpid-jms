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

import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import jakarta.jms.TopicSession;
import jakarta.jms.TopicSubscriber;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Test the basic contract of the TopicSubscriber
 */
public class JmsTopicSubscriberTest extends JmsConnectionTestSupport {

    protected TopicSession session;
    protected Topic topic;
    protected TopicSubscriber subscriber;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        topicConnection = createTopicConnectionToMockProvider();
        session = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic(_testMethodName);
        subscriber = session.createSubscriber(topic);
    }

    @Test
    @Timeout(30)
    public void testMultipleCloseCalls() throws Exception {
        subscriber.close();
        subscriber.close();
    }

    @Test
    @Timeout(30)
    public void testGetQueue() throws Exception {
        assertEquals(topic, subscriber.getTopic());
    }

    @Test
    @Timeout(30)
    public void testGetMessageListener() throws Exception {
        assertNull(subscriber.getMessageListener());
        subscriber.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
            }
        });
        assertNotNull(subscriber.getMessageListener());
    }

    @Test
    @Timeout(30)
    public void testGetMessageSelector() throws Exception {
        assertNull(subscriber.getMessageSelector());
    }
}
