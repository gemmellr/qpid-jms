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
package org.apache.qpid.jms.provider.failover;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.DefaultProviderListener;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderFutureFactory;
import org.apache.qpid.jms.test.Wait;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test behavior of the FailoverProvider
 */
public class FailoverProviderTest extends FailoverProviderTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverProviderTest.class);

    private final ProviderFutureFactory futuresFactory = ProviderFutureFactory.create(Collections.emptyMap());

    private List<URI> uris;
    private FailoverProvider provider;
    private JmsConnectionInfo connection;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);

        uris = new ArrayList<URI>();

        uris.add(new URI("mock://192.168.2.1:5672"));
        uris.add(new URI("mock://192.168.2.2:5672"));
        uris.add(new URI("mock://192.168.2.3:5672"));
        uris.add(new URI("mock://192.168.2.4:5672"));

        connection = createConnectionInfo();
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        if (provider != null) {
            provider.close();
        }
        super.tearDown();
    }

    @Test
    @Timeout(30)
    public void testCreateProviderOnlyUris() {
        provider = new FailoverProvider(uris, Collections.emptyMap(), futuresFactory);
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, provider.isRandomize());
        assertNull(provider.getRemoteURI());
        assertNotNull(provider.getNestedOptions());
        assertTrue(provider.getNestedOptions().isEmpty());
    }

    @Test
    @Timeout(30)
    public void testCreateProviderOnlyNestedOptions() {
        Map<String, String> options = new HashMap<String, String>();
        options.put("transport.tcpNoDelay", "true");

        provider = new FailoverProvider(Collections.emptyList(), options, futuresFactory);
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, provider.isRandomize());
        assertNull(provider.getRemoteURI());
        assertNotNull(provider.getNestedOptions());
        assertFalse(provider.getNestedOptions().isEmpty());
        assertTrue(provider.getNestedOptions().containsKey("transport.tcpNoDelay"));
    }

    @Test
    @Timeout(30)
    public void testCreateProviderWithNestedOptions() {
        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap(), futuresFactory);
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, provider.isRandomize());
        assertNull(provider.getRemoteURI());
        assertNotNull(provider.getNestedOptions());
        assertTrue(provider.getNestedOptions().isEmpty());
    }

    @Test
    @Timeout(30)
    public void testProviderListener() {
        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap(), futuresFactory);
        assertNull(provider.getProviderListener());
        provider.setProviderListener(new DefaultProviderListener());
        assertNotNull(provider.getProviderListener());
    }

    @Test
    @Timeout(30)
    public void testGetRemoteURI() throws Exception {
        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap(), futuresFactory);

        assertNull(provider.getRemoteURI());
        provider.connect(connection);
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return provider.getRemoteURI() != null;
            }
        }, TimeUnit.SECONDS.toMillis(20), 10), "Should have a remote URI after connect");
    }

    @Test
    @Timeout(30)
    public void testToString() throws Exception {
        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap(), futuresFactory);

        assertNotNull(provider.toString());
        provider.connect(connection);
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                LOG.info("FailoverProvider: toString = {}", provider.toString());
                return provider.toString().contains("mock://");
            }
        }, TimeUnit.SECONDS.toMillis(20), 10), "Should have a mock scheme after connect");
    }

    @Test
    @Timeout(30)
    public void testConnectToMock() throws Exception {
        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap(), futuresFactory);
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, provider.isRandomize());
        assertNull(provider.getRemoteURI());

        final CountDownLatch connected = new CountDownLatch(1);

        provider.setProviderListener(new DefaultProviderListener() {

            @Override
            public void onConnectionEstablished(URI remoteURI) {
                connected.countDown();
            }
        });

        provider.connect(connection);

        ProviderFuture request = provider.newProviderFuture();
        provider.create(createConnectionInfo(), request);

        request.sync(10, TimeUnit.SECONDS);

        assertTrue(request.isComplete());

        provider.close();

        assertEquals(1, mockPeer.getContextStats().getProvidersCreated());
        assertEquals(1, mockPeer.getContextStats().getConnectionAttempts());
    }

    @Test
    @Timeout(30)
    public void testCannotStartWithoutListener() throws Exception {
        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap(), futuresFactory);
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, provider.isRandomize());
        assertNull(provider.getRemoteURI());
        provider.connect(connection);

        try {
            provider.start();
            fail("Should not allow a start if no listener added yet.");
        } catch (IllegalStateException ex) {
        }

        provider.close();
    }

    @Test
    @Timeout(30)
    public void testStartupMaxReconnectAttempts() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost?mock.failOnConnect=true)" +
            "?failover.startupMaxReconnectAttempts=5" +
            "&failover.useReconnectBackOff=false");

        Connection connection = null;
        try {
            connection = factory.createConnection();
            connection.start();
            fail("Should have stopped after five retries.");
        } catch (JMSException ex) {
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

        assertEquals(5, mockPeer.getContextStats().getProvidersCreated());
        assertEquals(5, mockPeer.getContextStats().getConnectionAttempts());
        assertEquals(5, mockPeer.getContextStats().getCloseAttempts());
    }

    @Test
    @Timeout(30)
    public void testMaxReconnectAttemptsWithOneURI() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost1?mock.failOnConnect=true)" +
            "?failover.maxReconnectAttempts=5" +
            "&failover.useReconnectBackOff=false");

        Connection connection = null;
        try {
            connection = factory.createConnection();
            connection.start();
            fail("Should have stopped after five retries.");
        } catch (JMSException ex) {
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

        assertEquals(5, mockPeer.getContextStats().getProvidersCreated());
        assertEquals(5, mockPeer.getContextStats().getConnectionAttempts());
        assertEquals(5, mockPeer.getContextStats().getCloseAttempts());
    }

    @Test
    @Timeout(30)
    public void testMaxReconnectAttemptsWithMultipleURIs() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://192.168.2.1?mock.failOnConnect=true," +
                      "mock://192.168.2.2?mock.failOnConnect=true," +
                      "mock://192.168.2.3?mock.failOnConnect=true)" +
            "?failover.maxReconnectAttempts=5" +
            "&failover.reconnectDelay=1" +
            "&failover.useReconnectBackOff=false");

        Connection connection = null;
        try {
            connection = factory.createConnection();
            connection.start();
            fail("Should have stopped after five retries.");
        } catch (JMSException ex) {
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

        // The number should scale by the number of URIs in the list
        assertEquals(15, mockPeer.getContextStats().getProvidersCreated());
        assertEquals(15, mockPeer.getContextStats().getConnectionAttempts());
        assertEquals(15, mockPeer.getContextStats().getCloseAttempts());
    }

    @Test
    @Timeout(30)
    public void testMaxReconnectAttemptsWithBackOff() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost?mock.failOnConnect=true)" +
            "?failover.maxReconnectAttempts=5" +
            "&failover.maxReconnectDelay=60" +
            "&failover.reconnectDelay=10" +
            "&failover.useReconnectBackOff=true");

        Connection connection = null;
        try {
            connection = factory.createConnection();
            connection.start();
            fail("Should have stopped after five retries.");
        } catch (JMSException ex) {
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

        assertEquals(5, mockPeer.getContextStats().getProvidersCreated());
        assertEquals(5, mockPeer.getContextStats().getConnectionAttempts());
        assertEquals(5, mockPeer.getContextStats().getCloseAttempts());
    }

    @Test
    @Timeout(30)
    public void testFailureOnCloseIsSwallowed() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost?mock.failOnClose=true)");

        Connection connection = factory.createConnection();
        connection.start();
        connection.close();

        assertEquals(1, mockPeer.getContextStats().getProvidersCreated());
        assertEquals(1, mockPeer.getContextStats().getConnectionAttempts());
        assertEquals(1, mockPeer.getContextStats().getCloseAttempts());
    }

    @Test
    @Timeout(30)
    public void testSessionLifeCyclePassthrough() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost)");

        Connection connection = factory.createConnection();
        connection.start();
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE).close();
        connection.close();

        assertEquals(1, mockPeer.getContextStats().getCreateResourceCalls(JmsSessionInfo.class));
        assertEquals(1, mockPeer.getContextStats().getDestroyResourceCalls(JmsSessionInfo.class));
    }

    @Test
    @Timeout(30)
    public void testConsumerLifeCyclePassthrough() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost)");

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(_testMethodName);
        session.createConsumer(destination).close();
        connection.close();

        assertEquals(1, mockPeer.getContextStats().getCreateResourceCalls(JmsConsumerInfo.class));
        assertEquals(1, mockPeer.getContextStats().getStartResourceCalls(JmsConsumerInfo.class));
        assertEquals(1, mockPeer.getContextStats().getDestroyResourceCalls(JmsConsumerInfo.class));
    }

    @Test
    @Timeout(30)
    public void testProducerLifeCyclePassthrough() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost)");

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(_testMethodName);
        session.createProducer(destination).close();
        connection.close();

        assertEquals(1, mockPeer.getContextStats().getCreateResourceCalls(JmsProducerInfo.class));
        assertEquals(1, mockPeer.getContextStats().getDestroyResourceCalls(JmsProducerInfo.class));
    }

    @Test
    @Timeout(30)
    public void testSessionRecoverPassthrough() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost)");

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        session.recover();
        connection.close();

        assertEquals(1, mockPeer.getContextStats().getRecoverCalls());
    }

    @Test
    @Timeout(30)
    public void testSessionUnsubscribePassthrough() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost)");

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        session.unsubscribe("some-subscription");
        connection.close();

        assertEquals(1, mockPeer.getContextStats().getUnsubscribeCalls());
    }

    @Test
    @Timeout(30)
    public void testSendMessagePassthrough() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost)");

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(getTestName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createMessage());

        connection.close();

        assertEquals(1, mockPeer.getContextStats().getSendCalls());
    }

    @Test
    @Timeout(10)
    public void testTimeoutsSetFromConnectionInfo() throws Exception {
        final long CONNECT_TIMEOUT = TimeUnit.SECONDS.toMillis(4);
        final long CLOSE_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
        final long SEND_TIMEOUT = TimeUnit.SECONDS.toMillis(6);
        final long REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(7);

        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap(), futuresFactory);
        provider.setProviderListener(new DefaultProviderListener() {

            @Override
            public void onConnectionEstablished(URI remoteURI) {
            }
        });

        provider.connect(connection);
        provider.start();

        JmsConnectionInfo connectionInfo = createConnectionInfo();

        connectionInfo.setConnectTimeout(CONNECT_TIMEOUT);
        connectionInfo.setCloseTimeout(CLOSE_TIMEOUT);
        connectionInfo.setSendTimeout(SEND_TIMEOUT);
        connectionInfo.setRequestTimeout(REQUEST_TIMEOUT);

        ProviderFuture request = provider.newProviderFuture();
        provider.create(connectionInfo, request);
        request.sync();

        assertEquals(CLOSE_TIMEOUT, provider.getCloseTimeout());
        assertEquals(SEND_TIMEOUT, provider.getSendTimeout());
        assertEquals(REQUEST_TIMEOUT, provider.getRequestTimeout());
    }

    @Test
    @Timeout(30)
    public void testAmqpOpenServerListActionDefault() {
        provider = new FailoverProvider(uris, Collections.emptyMap(), futuresFactory);
        assertEquals("REPLACE", provider.getAmqpOpenServerListAction());
    }

    @Test
    @Timeout(30)
    public void testSetGetAmqpOpenServerListAction() {
        provider = new FailoverProvider(uris, Collections.emptyMap(), futuresFactory);
        String action = "ADD";
        assertFalse(action.equals(provider.getAmqpOpenServerListAction()));

        provider.setAmqpOpenServerListAction(action);
        assertEquals(action, provider.getAmqpOpenServerListAction());
    }

    @Test
    @Timeout(30)
    public void testSetInvalidAmqpOpenServerListActionThrowsIAE() {
        provider = new FailoverProvider(uris, Collections.emptyMap(), futuresFactory);
        try {
            provider.setAmqpOpenServerListAction("invalid");
            fail("no exception was thrown");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }
}
