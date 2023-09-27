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
package org.apache.qpid.jms.discovery;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.URI;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.qpid.jms.provider.DefaultProviderListener;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.discovery.DiscoveryProviderFactory;
import org.apache.qpid.jms.support.MulticastTestSupport;
import org.apache.qpid.jms.support.MulticastTestSupport.MulticastSupportResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic discovery of remote brokers
 */
public class JmsDiscoveryProviderTest {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsDiscoveryProviderTest.class);

    private static boolean multicastWorking = false;

    static
    {
        MulticastSupportResult msr = MulticastTestSupport.checkMulticastWorking();
        multicastWorking = msr.isMulticastWorking();
    }

    private BrokerService broker;

    @BeforeEach
    public void setup() throws Exception {
        // Check assumptions *before* trying to start
        // the broker, which may fail otherwise
        assumeTrue(multicastWorking, "Multicast does not seem to be working, skip!");

        broker = createBroker();
        try {
            broker.start();
            broker.waitUntilStarted();
        } catch (Exception e) {
            try {
                broker.stop();
            } catch (Exception ignore) {}

            throw e;
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    @Test
    @Timeout(30)
    public void testCreateDiscoveryProvider() throws Exception {
        URI discoveryUri = new URI("discovery:multicast://default");
        Provider provider = DiscoveryProviderFactory.create(discoveryUri);
        assertNotNull(provider);

        DefaultProviderListener listener = new DefaultProviderListener();
        provider.setProviderListener(listener);
        provider.start();
        provider.close();
    }

    @Test
    @Timeout(30)
    public void testStartFailsWithNoListener() throws Exception {
        URI discoveryUri = new URI("discovery:multicast://default");
        Provider provider =
            DiscoveryProviderFactory.create(discoveryUri);
        assertNotNull(provider);
        try {
            provider.start();
            fail("Should have thrown IllegalStateException");
        } catch (IllegalStateException expected) {
        } catch (Exception unexpected) {
            fail("Should have thrown IllegalStateException");
        }
        provider.close();
    }

    @Test
    @Timeout(30)
    public void testCreateFailsWithUnknownAgent() throws Exception {
        assertThrows(IOException.class, () -> {
            URI discoveryUri = new URI("discovery:unknown://default");
            Provider provider = DiscoveryProviderFactory.create(discoveryUri);
            provider.close();
        });
    }

    protected BrokerService createBroker() throws Exception {

        BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName("localhost");
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setUseJmx(false);

        TransportConnector connector = brokerService.addConnector("amqp://0.0.0.0:0");
        connector.setName("amqp");
        connector.setDiscoveryUri(new URI("multicast://default"));

        return brokerService;
    }
}
