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
package org.apache.qpid.jms.session;

import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.Queue;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueReceiver;
import jakarta.jms.QueueSender;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Tests behaviour after a QueueSession is closed.
 */
public class JmsQueueSessionClosedTest extends JmsConnectionTestSupport {

    private QueueSession session;
    private QueueSender sender;
    private QueueReceiver receiver;

    protected void createTestResources() throws Exception {
        connection = createQueueConnectionToMockProvider();

        session = ((QueueConnection) connection).createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(_testMethodName);

        sender = session.createSender(destination);
        receiver = session.createReceiver(destination);

        // Close the session explicitly, without closing the above.
        session.close();
    }

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        createTestResources();
    }

    @Test
    @Timeout(30)
    public void testSessionCloseAgain() throws Exception {
        // Close it again
        session.close();
    }

    @Test
    @Timeout(30)
    public void testReceiverCloseAgain() throws Exception {
        // Close it again (closing the session should have closed it already).
        receiver.close();
    }

    @Test
    @Timeout(30)
    public void testSenderCloseAgain() throws Exception {
        // Close it again (closing the session should have closed it already).
        sender.close();
    }

    @Test
    @Timeout(30)
    public void testReceiverGetQueueFails() throws Exception {
        assertThrows(jakarta.jms.IllegalStateException.class, () -> {
            receiver.getQueue();
        });
    }

    @Test
    @Timeout(30)
    public void testSenderGetQueueFails() throws Exception {
        assertThrows(jakarta.jms.IllegalStateException.class, () -> {
            sender.getQueue();
        });
    }
}
