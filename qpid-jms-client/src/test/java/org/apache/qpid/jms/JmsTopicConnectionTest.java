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
package org.apache.qpid.jms;

import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.ServerSessionPool;
import jakarta.jms.Session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Test various contract aspects of the TopicConnection implementation
 */
public class JmsTopicConnectionTest extends JmsConnectionTestSupport {

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);

        topicConnection = createTopicConnectionToMockProvider();
        topicConnection.start();
    }

    @Test
    @Timeout(30)
    public void testCreateConnectionConsumerOnTopicConnection() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            topicConnection.createConnectionConsumer(new JmsQueue(), "subscriptionName", (ServerSessionPool) null, 1);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateQueueSessionOnTopicConnection() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            topicConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        });
    }
}