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

import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TransactionRolledBackException;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.exceptions.ProviderIOException;
import org.apache.qpid.jms.provider.mock.ResourceLifecycleFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Test that calls into the FailoverProvider when it is not connected works
 * as expected based on the call and the resource type in question.
 */
public class FailoverProviderOfflineBehaviorTest extends FailoverProviderTestSupport {

    private final JmsConnectionFactory factory = new JmsConnectionFactory("failover:(mock://localhost)");

    private JmsConnection connection;
    private CountDownLatch connectionInterrupted;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        connectionInterrupted = new CountDownLatch(1);
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        connection.close();
        super.tearDown();
    }

    @Test
    @Timeout(20)
    public void testConnectionCloseDoesNotBlock() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.addConnectionListener(new ConnectionInterruptionListener());
        connection.start();
        mockPeer.shutdown();
        connectionInterrupted.await(9, TimeUnit.SECONDS);
        connection.close();
    }

    @Test
    @Timeout(20)
    public void testSessionCloseDoesNotBlock() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.addConnectionListener(new ConnectionInterruptionListener());
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        mockPeer.shutdown();
        connectionInterrupted.await(9, TimeUnit.SECONDS);
        session.close();
        connection.close();
    }

    @Test
    @Timeout(20)
    public void testProducerCloseDoesNotBlock() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.addConnectionListener(new ConnectionInterruptionListener());
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(_testMethodName);
        MessageProducer producer = session.createProducer(queue);

        mockPeer.shutdown();
        connectionInterrupted.await(9, TimeUnit.SECONDS);

        producer.close();
        connection.close();
    }

    @Test
    @Timeout(20)
    public void testConsumerCloseDoesNotBlock() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.addConnectionListener(new ConnectionInterruptionListener());
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(_testMethodName);
        MessageConsumer consumer = session.createConsumer(queue);

        mockPeer.shutdown();
        connectionInterrupted.await(9, TimeUnit.SECONDS);

        consumer.close();
        connection.close();
    }

    @Test
    @Timeout(20)
    public void testSessionCloseWhenDestroyCallFailsDoesNotBlock() throws Exception {
        mockPeer.setResourceDestroyFilter(new ResourceLifecycleFilter() {

            @Override
            public void onLifecycleEvent(JmsResource resource) throws Exception {
                if (resource instanceof JmsSessionInfo) {
                    mockPeer.shutdownQuietly();
                    throw new ProviderIOException("Failure closing session");
                }
            }
        });

        connection = (JmsConnection) factory.createConnection();
        connection.addConnectionListener(new ConnectionInterruptionListener());
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        session.close();

        connection.close();
    }

    @Test
    @Timeout(20)
    public void testSessionCloseWhenProviderSuddenlyClosesDoesNotBlock() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.addConnectionListener(new ConnectionInterruptionListener());
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        mockPeer.silentlyCloseConnectedProviders();

        session.close();
    }

    @Test
    @Timeout(20)
    public void testSessionCloseWithOpenResourcesDoesNotBlock() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.addConnectionListener(new ConnectionInterruptionListener());
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(_testMethodName);
        session.createConsumer(queue);
        session.createProducer(queue);

        mockPeer.shutdown();
        connectionInterrupted.await(9, TimeUnit.SECONDS);

        session.close();
        connection.close();
    }

    @Test
    @Timeout(20)
    public void testSessionRecoverDoesNotBlock() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.addConnectionListener(new ConnectionInterruptionListener());
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        mockPeer.shutdown();
        connectionInterrupted.await(9, TimeUnit.SECONDS);

        session.recover();
        connection.close();
    }

    @Test
    @Timeout(20)
    public void testTransactionCommitFails() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.addConnectionListener(new ConnectionInterruptionListener());
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(_testMethodName);
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createMessage());

        mockPeer.shutdown();
        connectionInterrupted.await(9, TimeUnit.SECONDS);

        try {
            session.commit();
            fail("Should not allow a commit while offline.");
        } catch (TransactionRolledBackException ex) {}

        connection.close();
    }

    @Test
    @Timeout(20)
    public void testTransactionRollbackSucceeds() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.addConnectionListener(new ConnectionInterruptionListener());
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(_testMethodName);
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createMessage());

        mockPeer.shutdown();
        connectionInterrupted.await(9, TimeUnit.SECONDS);

        try {
            session.rollback();
        } catch (TransactionRolledBackException ex) {
            fail("Should allow a rollback while offline.");
        }

        connection.close();
    }

    private class ConnectionInterruptionListener extends JmsDefaultConnectionListener {

        @Override
        public void onConnectionInterrupted(URI remoteURI) {
            connectionInterrupted.countDown();
        }
    }
}
