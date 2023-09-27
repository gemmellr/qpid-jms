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

import java.net.ServerSocket;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import javax.net.ServerSocketFactory;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests failover reconnect behavior when the remote side is not accepting socket connections
 * in a normal manner.
 */
public class FailoverSocketLevelAcceptFailures extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverSocketLevelAcceptFailures.class);

    private ServerSocket server;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        server = ServerSocketFactory.getDefault().createServerSocket(0);
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        try {
            server.close();
            server = null;
        } catch (Exception ignored) {
        }

        super.tearDown();
    }

    @Test
    @Timeout(40)
    void testFailoverHandlesSocketNotAccepted() throws Exception {
        final String remoteURI = "failover:(amqp://localhost:" + server.getLocalPort() +
            ")?jms.connectTimeout=666&failover.maxReconnectAttempts=1&failover.startupMaxReconnectAttempts=1";

        try {
            ConnectionFactory cf = new JmsConnectionFactory(remoteURI);
            Connection connection = cf.createConnection();
            connection.start();
            fail("Should throw error once the connection starts");
        } catch (Exception ex) {
            LOG.info("Error on connect:", ex);
        }
    }
}
