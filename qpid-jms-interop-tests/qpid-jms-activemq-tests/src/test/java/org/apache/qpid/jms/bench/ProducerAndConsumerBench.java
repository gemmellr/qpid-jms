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
package org.apache.qpid.jms.bench;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.jms.BytesMessage;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Disabled
public class ProducerAndConsumerBench extends AmqpTestSupport  {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerAndConsumerBench.class);

    public static final int PAYLOAD_SIZE = 64 * 1024;
    public static final int ioBuffer = 2 * PAYLOAD_SIZE;
    public static final int socketBuffer = 64 * PAYLOAD_SIZE;

    private final byte[] payload = new byte[PAYLOAD_SIZE];
    private final int parallelProducer = 1;
    private final int parallelConsumer = 1;
    private final Vector<Throwable> exceptions = new Vector<Throwable>();
    private ConnectionFactory factory;

    private final long NUM_SENDS = 30000;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);

        for (int i = 0; i < PAYLOAD_SIZE; ++i) {
            payload[i] = (byte) (i % 255);
        }
    }

    @Test
    void testProduceConsume() throws Exception {
        this.factory = createAmqpConnectionFactory();

        final AtomicLong sharedSendCount = new AtomicLong(NUM_SENDS);
        final AtomicLong sharedReceiveCount = new AtomicLong(NUM_SENDS);

        Thread.sleep(2000);

        long start = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(parallelConsumer + parallelProducer);

        for (int i = 0; i < parallelConsumer; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        consumeMessages(sharedReceiveCount);
                    } catch (Throwable e) {
                        exceptions.add(e);
                    }
                }
            });
        }
        for (int i = 0; i < parallelProducer; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        publishMessages(sharedSendCount);
                    } catch (Throwable e) {
                        exceptions.add(e);
                    }
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.MINUTES);
        assertTrue(executorService.isTerminated(), "Producers done in time");
        assertTrue(exceptions.isEmpty(), "No exceptions: " + exceptions);

        double duration = System.currentTimeMillis() - start;
        LOG.info("Duration:            " + duration + "ms");
        LOG.info("Rate:                " + (NUM_SENDS * 1000 / duration) + "m/s");
    }

    private void consumeMessages(AtomicLong count) throws Exception {
        JmsConnection connection = (JmsConnection) factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, ActiveMQSession.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageConsumer consumer = session.createConsumer(queue);
        long v;
        while ((v = count.decrementAndGet()) > 0) {
            if ((count.get() % 10000) == 0) {
                LOG.info("Received message: {}", NUM_SENDS - count.get());
            }
            assertNotNull(consumer.receive(15000), "got message " + v);
        }
        consumer.close();
    }

    private void publishMessages(AtomicLong count) throws Exception {
        JmsConnection connection = (JmsConnection) factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());

        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        while (count.getAndDecrement() > 0) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(payload);
            producer.send(message);
            if ((count.get() % 10000) == 0) {
                LOG.info("Sent message: {}", NUM_SENDS - count.get());
            }
        }
        producer.close();
        connection.close();
    }

    @Override
    protected void configureBrokerPolicies(BrokerService broker) {
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
        policyEntry.setPrioritizedMessages(false);
        policyEntry.setExpireMessagesPeriod(0);
        policyEntry.setEnableAudit(false);
        policyEntry.setOptimizedDispatch(true);
        policyEntry.setQueuePrefetch(1); // ensure no contention on add with
                                         // matched producer/consumer

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);
    }

    @Override
    protected boolean isForceAsyncSends() {
        return false;
    }

    @Override
    protected boolean isForceSyncSends() {
        return false;
    }

    @Override
    protected String getAmqpTransformer() {
        return "jms";
    }

    @Override
    protected boolean isForceAsyncAcks() {
        return true;
    }

    @Override
    public String getAmqpConnectionURIOptions() {
        return "jms.presettlePolicy.presettleAll=false";
    }

    @Override
    protected int getSocketBufferSize() {
        return socketBuffer;
    }

    @Override
    protected int getIOBufferSize() {
        return ioBuffer;
    }
}
