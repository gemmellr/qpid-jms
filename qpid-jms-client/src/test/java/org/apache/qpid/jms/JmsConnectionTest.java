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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URI;

import jakarta.jms.ConnectionMetaData;
import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;

import org.apache.qpid.jms.meta.JmsConnectionId;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.mock.MockProvider;
import org.apache.qpid.jms.provider.mock.MockProviderFactory;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test basic functionality around JmsConnection
 */
public class JmsConnectionTest {

    private final IdGenerator clientIdGenerator = new IdGenerator();

    private MockProvider provider;
    private JmsConnection connection;
    private JmsConnectionInfo connectionInfo;

    @BeforeEach
    public void setUp() throws Exception {
        provider = (MockProvider) MockProviderFactory.create(new URI("mock://localhost"));
        connectionInfo = new JmsConnectionInfo(new JmsConnectionId("ID:TEST:1"));
        connectionInfo.setClientId(clientIdGenerator.generateId(), false);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @Timeout(30)
    public void testJmsConnectionThrowsJMSExceptionProviderStartFails() throws JMSException, IllegalStateException, IOException {
        assertThrows(JMSException.class, () -> {
            provider.getConfiguration().setFailOnStart(true);
            try (JmsConnection connection = new JmsConnection(connectionInfo, provider);) {}
        });
    }

    @Test
    @Timeout(30)
    public void testStateAfterCreate() throws JMSException {
        connection = new JmsConnection(connectionInfo, provider);

        assertFalse(connection.isStarted());
        assertFalse(connection.isClosed());
        assertFalse(connection.isConnected());
    }

    @Test
    @Timeout(30)
    public void testGetExceptionListener() throws JMSException {
        connection = new JmsConnection(connectionInfo, provider);

        assertNull(connection.getExceptionListener());
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
            }
        });

        assertNotNull(connection.getExceptionListener());
    }

    @Test
    @Timeout(30)
    public void testReplacePrefetchPolicy() throws JMSException {
        connection = new JmsConnection(connectionInfo, provider);

        JmsDefaultPrefetchPolicy newPolicy = new JmsDefaultPrefetchPolicy();
        newPolicy.setAll(1);

        assertNotSame(newPolicy, connection.getPrefetchPolicy());
        connection.setPrefetchPolicy(newPolicy);
        assertEquals(newPolicy, connection.getPrefetchPolicy());
    }

    @Test
    @Timeout(30)
    public void testGetConnectionId() throws JMSException {
        connection = new JmsConnection(connectionInfo, provider);
        assertEquals("ID:TEST:1", connection.getId().toString());
    }

    @Test
    @Timeout(30)
    public void testAddConnectionListener() throws JMSException {
        connection = new JmsConnection(connectionInfo, provider);
        JmsConnectionListener listener = new JmsDefaultConnectionListener();
        assertFalse(connection.removeConnectionListener(listener));
        connection.addConnectionListener(listener);
        assertTrue(connection.removeConnectionListener(listener));
    }

    @Test
    @Timeout(30)
    public void testConnectionStart() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);

        assertFalse(connection.isConnected());
        connection.start();
        assertTrue(connection.isConnected());
    }

    @Test
    @Timeout(30)
    public void testConnectionMulitpleStartCalls() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);

        assertFalse(connection.isConnected());
        connection.start();
        assertTrue(connection.isConnected());
        connection.start();
        assertTrue(connection.isConnected());
    }

    @Test
    @Timeout(30)
    public void testConnectionStartAndStop() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);

        assertFalse(connection.isConnected());
        connection.start();
        assertTrue(connection.isConnected());
        connection.stop();
        assertTrue(connection.isConnected());
    }

    @Test
    @Timeout(30)
    public void testSetClientIDFromNull() throws JMSException, IOException {
        assertThrows(InvalidClientIDException.class, () -> {
            connection = new JmsConnection(connectionInfo, provider);
            assertFalse(connection.isConnected());
            connection.setClientID("");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateNonTXSessionWithTXAckMode() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        try {
            connection.createSession(false, Session.SESSION_TRANSACTED);
            fail("Should not allow non-TX session with mode SESSION_TRANSACTED");
        } catch (JMSException ex) {
        }
    }

    @Test
    @Timeout(30)
    public void testCreateNonTXSessionWithUnknownAckMode() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        try {
            connection.createSession(false, 99);
            fail("Should not allow unkown Ack modes.");
        } catch (JMSException ex) {
        }
    }

    @Test
    @Timeout(30)
    public void testCreateSessionWithUnknownAckMode() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        try {
            connection.createSession(99);
            fail("Should not allow unkown Ack modes.");
        } catch (JMSException ex) {
        }
    }

    @Test
    @Timeout(30)
    public void testCreateSessionDefaultMode() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        JmsSession session = (JmsSession) connection.createSession();
        assertEquals(session.getSessionMode(), Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    @Timeout(30)
    public void testSetClientIDFromEmptyString() throws JMSException, IOException {
        assertThrows(InvalidClientIDException.class, () -> {
            connection = new JmsConnection(connectionInfo, provider);
            assertFalse(connection.isConnected());
            connection.setClientID(null);
        });
    }

    @Test
    @Timeout(30)
    public void testSetClientIDFailsOnSecondCall() throws JMSException, IOException {
        assertThrows(IllegalStateException.class, () -> {
            connection = new JmsConnection(connectionInfo, provider);

            assertFalse(connection.isConnected());
            connection.setClientID("TEST-ID");
            assertTrue(connection.isConnected());
            connection.setClientID("TEST-ID");
        });
    }

    @Test
    @Timeout(30)
    public void testSetClientIDFailsAfterStart() throws JMSException, IOException {
        assertThrows(IllegalStateException.class, () -> {
            connection = new JmsConnection(connectionInfo, provider);

            assertFalse(connection.isConnected());
            connection.start();
            assertTrue(connection.isConnected());
            connection.setClientID("TEST-ID");
        });
    }

    @Test
    @Timeout(30)
    public void testDeleteOfTempQueueOnClosedConnection() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue tempQueue = session.createTemporaryQueue();
        assertNotNull(tempQueue);

        connection.close();
        try {
            tempQueue.delete();
            fail("Should have thrown an IllegalStateException");
        } catch (IllegalStateException ex) {
        }
    }

    @Test
    @Timeout(30)
    public void testDeleteOfTempTopicOnClosedConnection() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryTopic tempTopic = session.createTemporaryTopic();
        assertNotNull(tempTopic);

        connection.close();
        try {
            tempTopic.delete();
            fail("Should have thrown an IllegalStateException");
        } catch (IllegalStateException ex) {
        }
    }

    @Test
    @Timeout(30)
    public void testConnectionCreatedSessionRespectsAcknowledgementMode() throws Exception {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        JmsSession session = (JmsSession) connection.createSession(Session.SESSION_TRANSACTED);
        try {
            session.acknowledge(ACK_TYPE.ACCEPTED);
            fail("Should be in TX mode and not allow explicit ACK.");
        } catch (IllegalStateException ise) {
        }
    }

    @Test
    @Timeout(30)
    public void testConnectionMetaData() throws Exception {
        connection = new JmsConnection(connectionInfo, provider);

        ConnectionMetaData metaData = connection.getMetaData();

        assertNotNull(metaData);
        assertEquals(3, metaData.getJMSMajorVersion());
        assertEquals(1, metaData.getJMSMinorVersion());
        assertEquals("3.1", metaData.getJMSVersion());
        assertNotNull(metaData.getJMSXPropertyNames());

        assertNotNull(metaData.getProviderVersion());
        assertNotNull(metaData.getJMSProviderName());

        int major = metaData.getProviderMajorVersion();
        int minor = metaData.getProviderMinorVersion();
        assertTrue((major + minor) != 0, "Expected non-zero provider major(" + major + ") / minor(" + minor +") version.");
    }
}
