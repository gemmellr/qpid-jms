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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import jakarta.jms.JMSException;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.netty.NettyBlackHoleServer;
import org.apache.qpid.jms.transports.netty.NettySimpleAmqpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class WsIntegrationTest extends QpidJmsTestCase {

    private static final String BROKER_PKCS12_KEYSTORE = "src/test/resources/broker-pkcs12.keystore";
    private static final String PASSWORD = "password";

    @Test
    @Timeout(30)
    public void testNonSslWebSocketConnectionFailsToSslServer() throws Exception {
        TransportOptions serverOptions = new TransportOptions();
        serverOptions.setKeyStoreLocation(BROKER_PKCS12_KEYSTORE);
        serverOptions.setKeyStorePassword(PASSWORD);
        serverOptions.setVerifyHost(false);

        try (NettySimpleAmqpServer server = new NettySimpleAmqpServer(serverOptions, true, false, true)) {
            server.start();

            JmsConnectionFactory factory = new JmsConnectionFactory("amqpws://localhost:" + server.getServerPort());

            try {
                factory.createConnection();
                fail("should not have connected");
            }
            catch (JMSException jmse) {
                String message = jmse.getMessage();
                assertNotNull(message);
                assertTrue(message.contains("Connection failed"), "Unexpected message: " + message);
            }
        }
    }

    @Test
    @Timeout(30)
    public void testWebsocketConnectionToBlackHoleServerTimesOut() throws Exception {
        TransportOptions serverOptions = new TransportOptions();

        try (NettyBlackHoleServer server = new NettyBlackHoleServer(serverOptions, false)) {
            server.start();

            JmsConnectionFactory factory = new JmsConnectionFactory("amqpws://localhost:" + server.getServerPort() + "?transport.connectTimeout=25");

            try {
                factory.createConnection();
                fail("should not have connected");
            }
            catch (JMSException jmse) {
                String message = jmse.getMessage();
                assertNotNull(message);
                assertTrue(message.contains("WebSocket handshake timed out"), "Unexpected message: " + message);
            }
        }
    }
}
