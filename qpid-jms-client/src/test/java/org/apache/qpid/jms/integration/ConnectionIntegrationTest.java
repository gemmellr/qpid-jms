/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.integration;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.NETWORK_HOST;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.OPEN_HOSTNAME;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.PORT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.ResourceAllocationException;
import jakarta.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionExtensions;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsConnectionRemotelyClosedException;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionRedirectedException;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.basictypes.ConnectionError;
import org.apache.qpid.jms.test.testpeer.matchers.CoordinatorMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.util.MetaDataSupport;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.transaction.TxnCapability;
import org.apache.qpid.proton.engine.impl.AmqpHeader;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ConnectionIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test
    @Timeout(20)
    public void testCreateAndCloseConnection() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(10)
    public void testCreateConnectionToNonSaslPeer() throws Exception {
        doConnectionWithUnexpectedHeaderTestImpl(AmqpHeader.HEADER);
    }

    @Test
    @Timeout(10)
    public void testCreateConnectionToNonAmqpPeer() throws Exception {
        byte[] responseHeader = new byte[] { 'N', 'O', 'T', '-', 'A', 'M', 'Q', 'P' };
        doConnectionWithUnexpectedHeaderTestImpl(responseHeader);
    }

    private void doConnectionWithUnexpectedHeaderTestImpl(byte[] responseHeader) throws Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectHeader(AmqpHeader.SASL_HEADER, responseHeader);

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort());
            try {
                factory.createConnection("guest", "guest");
                fail("Expected connection creation to fail");
            } catch (JMSException jmse) {
                assertThat(jmse.getMessage(), containsString("SASL header mismatch"));
            }
        }
    }

    @Test
    @Timeout(20)
    public void testCloseConnectionTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setCloseTimeout(500);

            testPeer.expectClose(false);

            connection.start();
            assertNotNull(connection, "Connection should not be null");
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCloseConnectionCompletesWhenConnectionDropsBeforeResponse() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);

            testPeer.expectClose(false);
            testPeer.dropAfterLastHandler();

            connection.start();
            assertNotNull(connection, "Connection should not be null");
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateConnectionWithClientId() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, false, null, null, null, true);
            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testCreateAutoAckSession() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertNotNull(session, "Session should not be null");
            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testCreateAutoAckSessionByDefault() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            Session session = connection.createSession();
            assertNotNull(session, "Session should not be null");
            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testCreateAutoAckSessionUsingAckModeOnlyMethod() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            assertNotNull(session, "Session should not be null");
            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testCreateTransactedSession() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            // Expect the session, with an immediate link to the transaction coordinator
            // using a target with the expected capabilities only.
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            txCoordinatorMatcher.withCapabilities(arrayContaining(TxnCapability.LOCAL_TXN));
            testPeer.expectSenderAttach(txCoordinatorMatcher, false, false);

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId);
            testPeer.expectDischarge(txnId, true);
            testPeer.expectClose();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            assertNotNull(session, "Session should not be null");

            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testCreateTransactedSessionUsingAckModeOnlyMethod() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            // Expect the session, with an immediate link to the transaction coordinator
            // using a target with the expected capabilities only.
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            txCoordinatorMatcher.withCapabilities(arrayContaining(TxnCapability.LOCAL_TXN));
            testPeer.expectSenderAttach(txCoordinatorMatcher, false, false);

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId);
            testPeer.expectDischarge(txnId, true);
            testPeer.expectClose();

            Session session = connection.createSession(Session.SESSION_TRANSACTED);
            assertNotNull(session, "Session should not be null");

            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testCreateTransactedSessionFailsWhenNoDetachResponseSent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            ((JmsConnection) connection).setRequestTimeout(500);

            testPeer.expectBegin();
            // Expect the session, with an immediate link to the transaction coordinator
            // using a target with the expected capabilities only.
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            txCoordinatorMatcher.withCapabilities(arrayContaining(TxnCapability.LOCAL_TXN));
            testPeer.expectSenderAttach(notNullValue(), txCoordinatorMatcher, true, true, false, 0, null, null);
            testPeer.expectDetach(true, false, false);
            // Expect the AMQP session to be closed due to the JMS session creation failure.
            testPeer.expectEnd();

            try {
                connection.createSession(true, Session.SESSION_TRANSACTED);
                fail("Session create should have failed.");
            } catch (JMSException ex) {
                // Expected
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyCloseConnectionDuringSessionCreation() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            // Expect the begin, then explicitly close the connection with an error
            testPeer.expectBegin(notNullValue(), false);
            testPeer.remotelyCloseConnection(true, AmqpError.NOT_ALLOWED, BREAD_CRUMB);

            try {
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
                assertNotNull(jmse.getMessage(), "Expected exception to have a message");
                assertTrue(jmse.getMessage().contains(BREAD_CRUMB), "Expected breadcrumb to be present in message");
            }

            testPeer.waitForAllHandlersToComplete(3000);

            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyDropConnectionDuringSessionCreation() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            // Expect the begin, then drop connection without without a close frame.
            testPeer.expectBegin(notNullValue(), false);
            testPeer.dropAfterLastHandler();

            try {
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
            }

            testPeer.waitForAllHandlersToComplete(3000);

            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyDropConnectionDuringSessionCreationTransacted() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + "?jms.clientID=foo");
            Connection connection = factory.createConnection();

            CountDownLatch exceptionListenerFired = new CountDownLatch(1);
            connection.setExceptionListener(ex -> exceptionListenerFired.countDown());

            // Expect the begin, then drop connection without without a close frame before the tx-coordinator setup.
            testPeer.expectBegin();
            testPeer.dropAfterLastHandler();

            try {
                connection.createSession(true, Session.SESSION_TRANSACTED);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
            }

            assertTrue(exceptionListenerFired.await(5, TimeUnit.SECONDS), "Exception listener did not fire");

            testPeer.waitForAllHandlersToComplete(3000);

            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testConnectionPropertiesContainExpectedMetaData() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            Matcher<?> connPropsMatcher = allOf(hasEntry(AmqpSupport.PRODUCT, MetaDataSupport.PROVIDER_NAME),
                    hasEntry(AmqpSupport.VERSION, MetaDataSupport.PROVIDER_VERSION),
                    hasEntry(AmqpSupport.PLATFORM, MetaDataSupport.PLATFORM_DETAILS));

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen(connPropsMatcher, null, false);
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + "?jms.clientID=foo");
            Connection connection = factory.createConnection();

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testMaxFrameSizeOptionCommunicatedInOpen() throws Exception {
        int frameSize = 39215;
        doMaxFrameSizeOptionTestImpl(frameSize, UnsignedInteger.valueOf(frameSize));
    }

    @Test
    @Timeout(20)
    public void testMaxFrameSizeOptionCommunicatedInOpenDefault() throws Exception {
        doMaxFrameSizeOptionTestImpl(-1, UnsignedInteger.MAX_VALUE);
    }

    private void doMaxFrameSizeOptionTestImpl(int uriOption, UnsignedInteger transmittedValue) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslLayerDisabledConnect(equalTo(transmittedValue));
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            String uri = "amqp://localhost:" + testPeer.getServerPort() + "?amqp.saslLayer=false&amqp.maxFrameSize=" + uriOption;
            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();
            connection.start();

            testPeer.waitForAllHandlersToComplete(3000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testMaxFrameSizeInfluencesOutgoingFrameSize() throws Exception {
        doMaxFrameSizeInfluencesOutgoingFrameSizeTestImpl(1000, 10001, 11);
        doMaxFrameSizeInfluencesOutgoingFrameSizeTestImpl(1500, 6001, 5);
    }

    private void doMaxFrameSizeInfluencesOutgoingFrameSizeTestImpl(int frameSize, int bytesPayloadSize, int numFrames) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslLayerDisabledConnect(equalTo(UnsignedInteger.valueOf(frameSize)));
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            String uri = "amqp://localhost:" + testPeer.getServerPort() + "?amqp.saslLayer=false&amqp.maxFrameSize=" + frameSize;
            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            // Expect n-1 transfers of maxFrameSize
            for (int i = 1; i < numFrames; i++) {
                testPeer.expectTransfer(frameSize);
            }
            // Plus one more of unknown size (framing overhead).
            testPeer.expectTransfer(0);

            // Send the message
            byte[] orig = createBytePyload(bytesPayloadSize);
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(orig);

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    private static byte[] createBytePyload(int sizeInBytes) {
        Random rand = new Random(System.currentTimeMillis());

        byte[] payload = new byte[sizeInBytes];
        for (int i = 0; i < sizeInBytes; i++) {
            payload[i] = (byte) (64 + 1 + rand.nextInt(9));
        }

        return payload;
    }

    @Test
    @Timeout(20)
    public void testAmqpHostnameSetByDefault() throws Exception {
        doAmqpHostnameTestImpl("localhost", false, equalTo("localhost"));
    }

    @Test
    @Timeout(20)
    public void testAmqpHostnameSetByVhostOption() throws Exception {
        String vhost = "myAmqpHost";
        doAmqpHostnameTestImpl(vhost, true, equalTo(vhost));
    }

    @Test
    @Timeout(20)
    public void testAmqpHostnameNotSetWithEmptyVhostOption() throws Exception {
        doAmqpHostnameTestImpl("", true, nullValue());
    }

    private void doAmqpHostnameTestImpl(String amqpHostname, boolean setHostnameOption, Matcher<?> hostnameMatcher) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen(null, hostnameMatcher, false);
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            String uri = "amqp://localhost:" + testPeer.getServerPort();
            if(setHostnameOption) {
                uri += "?amqp.vhost=" + amqpHostname;
            }

            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyEndConnectionListenerInvoked() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch done = new CountDownLatch(1);

            // Don't set a ClientId, so that the underlying AMQP connection isn't established yet
            Connection connection = testFixture.establishConnecton(testPeer, false, null, null, null, false);

            // Tell the test peer to close the connection when executing its last handler
            testPeer.remotelyCloseConnection(true);

            // Add the exception listener
            connection.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    done.countDown();
                }
            });

            // Trigger the underlying AMQP connection
            connection.start();

            assertTrue(done.await(5, TimeUnit.SECONDS), "Connection should report failure");

            testPeer.waitForAllHandlersToComplete(1000);

            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyEndConnectionWithRedirect() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch done = new CountDownLatch(1);
            final AtomicReference<JMSException> asyncError = new AtomicReference<JMSException>();

            final String redirectVhost = "vhost";
            final String redirectNetworkHost = "localhost";
            final int redirectPort = 5677;

            // Don't set a ClientId, so that the underlying AMQP connection isn't established yet
            Connection connection = testFixture.establishConnecton(testPeer, false, null, null, null, false);

            // Tell the test peer to close the connection when executing its last handler
            Map<Symbol, Object> errorInfo = new HashMap<Symbol, Object>();
            errorInfo.put(OPEN_HOSTNAME, redirectVhost);
            errorInfo.put(NETWORK_HOST, redirectNetworkHost);
            errorInfo.put(PORT, 5677);

            testPeer.remotelyCloseConnection(true, ConnectionError.REDIRECT, "Connection redirected", errorInfo);

            // Add the exception listener
            connection.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    asyncError.set(exception);
                    done.countDown();
                }
            });

            // Trigger the underlying AMQP connection
            connection.start();

            assertTrue(done.await(5, TimeUnit.SECONDS), "Connection should report failure");

            assertTrue(asyncError.get() instanceof JMSException);
            assertTrue(asyncError.get().getCause() instanceof ProviderConnectionRedirectedException);

            ProviderConnectionRedirectedException redirect = (ProviderConnectionRedirectedException) asyncError.get().getCause();
            URI redirectionURI = redirect.getRedirectionURI();

            assertNotNull(redirectionURI);
            assertTrue(redirectionURI.getQuery().contains("amqp.vhost=" + redirectVhost), "Unexpected query, got: " + redirectionURI.getQuery());
            assertEquals(redirectNetworkHost, redirectionURI.getHost());
            assertEquals(redirectPort, redirectionURI.getPort());

            testPeer.waitForAllHandlersToComplete(1000);

            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyEndConnectionWithSessionWithConsumer() throws Exception {
        final String BREAD_CRUMB = "ErrorMessage";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a consumer, then remotely end the connection afterwards.
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.remotelyCloseConnection(true, AmqpError.RESOURCE_LIMIT_EXCEEDED, BREAD_CRUMB);

            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            testPeer.waitForAllHandlersToComplete(1000);
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return !((JmsConnection) connection).isConnected();
                }
            }, 10000, 10), "connection never closed.");

            try {
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String message = jmsise.getCause().getMessage();
                assertTrue(message.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()));
                assertTrue(message.contains(BREAD_CRUMB));
            }

            // Verify the session is now marked closed
            try {
                session.getAcknowledgeMode();
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String message = jmsise.getCause().getMessage();
                assertTrue(message.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()));
                assertTrue(message.contains(BREAD_CRUMB));
            }

            // Verify the consumer is now marked closed
            try {
                consumer.getMessageListener();
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String message = jmsise.getCause().getMessage();
                assertTrue(message.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()));
                assertTrue(message.contains(BREAD_CRUMB));
            }

            // Try closing them explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            consumer.close();
            session.close();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyEndConnectionWithSessionWithProducerWithSendWaitingOnCredit() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Expect producer creation, don't give it credit.
            testPeer.expectSenderAttachWithoutGrantingCredit();

            // Producer has no credit so the send should block waiting for it.
            testPeer.remotelyCloseConnection(true, AmqpError.RESOURCE_LIMIT_EXCEEDED, BREAD_CRUMB, 50);

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            Message message = session.createTextMessage("myMessage");

            try {
                producer.send(message);
                fail("Expected exception to be thrown");
            } catch (ResourceAllocationException jmse) {
                // Expected
                assertNotNull(jmse.getMessage(), "Expected exception to have a message");
                assertTrue(jmse.getMessage().contains(BREAD_CRUMB), "Expected breadcrumb to be present in message");
            } catch (Throwable t) {
                fail("Caught unexpected exception: " + t);
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyEndConnectionWithSessionWithProducerWithSendWaitingOnOutcome() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Expect producer creation, and a message to be sent, but don't return a disposition.
            // Instead, close the connection.
            testPeer.expectSenderAttach();
            testPeer.expectTransferButDoNotRespond(new TransferPayloadCompositeMatcher());
            testPeer.remotelyCloseConnection(true, ConnectionError.CONNECTION_FORCED, BREAD_CRUMB, 50);

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            Message message = session.createTextMessage("myMessage");

            try {
                producer.send(message);
                fail("Expected exception to be thrown");
            } catch (JmsConnectionRemotelyClosedException jmse) {
                // Expected
                assertNotNull(jmse.getMessage(), "Expected exception to have a message");
                assertTrue(jmse.getMessage().contains(BREAD_CRUMB), "Expected breadcrumb to be present in message");
            } catch (Throwable t) {
                fail("Caught unexpected exception: " + t);
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateConnectionWithServerSendingPreemptiveData() throws Exception {
        boolean sendServerSaslHeaderPreEmptively = true;
        try (TestAmqpPeer testPeer = new TestAmqpPeer(null, false, sendServerSaslHeaderPreEmptively);) {
            // Don't use test fixture, handle the connection directly to control sasl behaviour
            testPeer.expectSaslAnonymousWithPreEmptiveServerHeader();
            testPeer.expectOpen();
            testPeer.expectBegin();

            JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort());
            Connection connection = factory.createConnection();
            connection.start();

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testDontAwaitClientIDBeforeOpen() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            String uri = "amqp://localhost:" + testPeer.getServerPort() + "?jms.awaitClientID=false";

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();

            // Verify that all handlers complete, i.e. the awaitClientID=false option
            // setting was effective in provoking the AMQP Open immediately even
            // though it has no ClientID and we haven't used the Connection.
            testPeer.waitForAllHandlersToComplete(3000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testWaitForClientIDDoesNotOpenUntilPromptedWithSetClientID() throws Exception {
        doTestWaitForClientIDDoesNotOpenUntilPrompted(true);
    }

    @Test
    @Timeout(20)
    public void testWaitForClientIDDoesNotOpenUntilPromptedWithStart() throws Exception {
        doTestWaitForClientIDDoesNotOpenUntilPrompted(false);
    }

    private void doTestWaitForClientIDDoesNotOpenUntilPrompted(boolean setClientID) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            String uri = "amqp://localhost:" + testPeer.getServerPort() + "?jms.awaitClientID=true";

            testPeer.expectSaslAnonymous();

            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectOpen();
            testPeer.expectBegin();

            if (setClientID) {
                connection.setClientID("client-id");
            } else {
                connection.start();
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test
    @Timeout(20)
    public void testUseDaemonThreadURIOption() throws Exception {
        doUseDaemonThreadTestImpl(null);
        doUseDaemonThreadTestImpl(false);
        doUseDaemonThreadTestImpl(true);
    }

    private void doUseDaemonThreadTestImpl(Boolean useDaemonThread) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            String remoteURI = "amqp://localhost:" + testPeer.getServerPort();
            if (useDaemonThread != null) {
                remoteURI += "?jms.useDaemonThread=" + useDaemonThread;
            }

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            final CountDownLatch connectionEstablished = new CountDownLatch(1);
            final AtomicBoolean daemonThread = new AtomicBoolean(false);

            ConnectionFactory factory = new JmsConnectionFactory(remoteURI);
            JmsConnection connection = (JmsConnection) factory.createConnection();

            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    // Record whether the thread is daemon or not
                    daemonThread.set(Thread.currentThread().isDaemon());

                    connectionEstablished.countDown();
                }
            });

            connection.start();

            assertTrue(connectionEstablished.await(5, TimeUnit.SECONDS), "Connection established callback didn't trigger");

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);

            if (useDaemonThread == null) {
                // Expect default to be false when not configured
                assertFalse(daemonThread.get());
            } else {
                // Else expect to match the URI option value
                assertEquals(useDaemonThread, daemonThread.get());
            }
        }
    }

    @Test
    @Timeout(20)
    public void testConnectionWithPreemptiveServerOpen() throws Exception {

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Ensure the Connection awaits a ClientID being set or not, giving time for the preemptive server Open
            String uri = "amqp://localhost:" + testPeer.getServerPort() + "?jms.awaitClientID=true";

            testPeer.expectSaslAnonymousWithServerAmqpHeaderSentPreemptively();
            testPeer.sendPreemptiveServerAmqpHeader();
            testPeer.sendPreemptiveServerOpenFrame();
            // Then expect the clients header to arrive, but defer responding since the servers was already sent.
            testPeer.expectHeader(AmqpHeader.HEADER, null);

            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();

            // Then expect the clients Open frame to arrive, but defer responding since the servers was already sent
            // before the clients AMQP connection open is provoked.
            testPeer.expectOpen(null, null, true);
            testPeer.expectBegin();

            Thread.sleep(10); // Gives a little more time for the preemptive Open to actually arrive.

            // Use the connection to provoke the Open
            connection.setClientID("client-id");

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test
    @Timeout(20)
    public void testConnectionPropertiesExtensionAddedValues() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final String property1 = "property1";
            final String property2 = "property2";

            final String value1 = UUID.randomUUID().toString();
            final String value2 = UUID.randomUUID().toString();

            Matcher<?> connPropsMatcher = allOf(
                    hasEntry(Symbol.valueOf(property1), value1),
                    hasEntry(Symbol.valueOf(property2), value2),
                    hasEntry(AmqpSupport.PRODUCT, MetaDataSupport.PROVIDER_NAME),
                    hasEntry(AmqpSupport.VERSION, MetaDataSupport.PROVIDER_VERSION),
                    hasEntry(AmqpSupport.PLATFORM, MetaDataSupport.PLATFORM_DETAILS));

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen(connPropsMatcher, null, false);
            testPeer.expectBegin();

            final URI remoteURI = new URI("amqp://localhost:" + testPeer.getServerPort());

            JmsConnectionFactory factory = new JmsConnectionFactory(remoteURI);

            factory.setExtension(JmsConnectionExtensions.AMQP_OPEN_PROPERTIES.toString(), (connection, uri) -> {
                Map<String, Object> properties = new HashMap<>();

                properties.put(property1, value1);
                properties.put(property2, value2);

                return properties;
            });

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testConnectionPropertiesExtensionAddedValuesOfNonString() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final String property1 = "property1";
            final String property2 = "property2";

            final UUID value1 = UUID.randomUUID();
            final UUID value2 = UUID.randomUUID();

            Matcher<?> connPropsMatcher = allOf(
                    hasEntry(Symbol.valueOf(property1), value1),
                    hasEntry(Symbol.valueOf(property2), value2));

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen(connPropsMatcher, null, false);
            testPeer.expectBegin();

            final URI remoteURI = new URI("amqp://localhost:" + testPeer.getServerPort());

            JmsConnectionFactory factory = new JmsConnectionFactory(remoteURI);

            factory.setExtension(JmsConnectionExtensions.AMQP_OPEN_PROPERTIES.toString(), (connection, uri) -> {
                Map<String, Object> properties = new HashMap<>();

                properties.put(property1, value1);
                properties.put(property2, value2);

                return properties;
            });

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.waitForAllHandlersToComplete(1000);
            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testConnectionPropertiesExtensionProtectsClientProperties() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            Matcher<?> connPropsMatcher = allOf(
                    hasEntry(AmqpSupport.PRODUCT, MetaDataSupport.PROVIDER_NAME),
                    hasEntry(AmqpSupport.VERSION, MetaDataSupport.PROVIDER_VERSION),
                    hasEntry(AmqpSupport.PLATFORM, MetaDataSupport.PLATFORM_DETAILS));

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen(connPropsMatcher, null, false);
            testPeer.expectBegin();

            final URI remoteURI = new URI("amqp://localhost:" + testPeer.getServerPort());

            JmsConnectionFactory factory = new JmsConnectionFactory(remoteURI);

            factory.setExtension(JmsConnectionExtensions.AMQP_OPEN_PROPERTIES.toString(), (connection, uri) -> {
                Map<String, Object> properties = new HashMap<>();

                properties.put(AmqpSupport.PRODUCT.toString(), "Super-Duper-Qpid-JMS");
                properties.put(AmqpSupport.VERSION.toString(), "5.0.32.Final");
                properties.put(AmqpSupport.PLATFORM.toString(), "Commodore 64");

                return properties;
            });

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.waitForAllHandlersToComplete(1000);
            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testConnectionFailsWhenUserSuppliesIllegalProperties() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.setSuppressReadExceptionOnClose(true);
            testPeer.expectSaslAnonymous();

            final URI remoteURI = new URI("amqp://localhost:" + testPeer.getServerPort());

            JmsConnectionFactory factory = new JmsConnectionFactory(remoteURI);

            factory.setExtension(JmsConnectionExtensions.AMQP_OPEN_PROPERTIES.toString(), (connection, uri) -> {
                Map<String, Object> properties = new HashMap<>();

                properties.put("not-amqp-encodable", factory);

                return properties;
            });

            Connection connection = factory.createConnection();

            try {
                connection.start();
                fail("Should not be able to connect when illegal types are in the properties");
            } catch (JMSException ex) {
            } catch (Exception unexpected) {
                fail("Caught unexpected error from connnection.start() : " + unexpected);
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Disabled("Disabled due to requirement of hard coded port")
    @Test
    @Timeout(20)
    public void testLocalPortOption() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            int localPort = 5671;
            String uri = "amqp://localhost:" + testPeer.getServerPort() + "?transport.localPort=" + localPort;
            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();
            connection.start();

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectClose();
            connection.close();

            int clientPort = testPeer.getClientSocket().getPort();
            assertEquals(localPort, clientPort);
        }
    }
}
