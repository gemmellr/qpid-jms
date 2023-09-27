/*
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
 */
package org.apache.qpid.jms.integration;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.MessageListener;
import jakarta.jms.Queue;

import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.DataDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.MessageAnnotationsDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSConsumerIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(JMSConsumerIntegrationTest.class);

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test
    @Timeout(20)
    public void testCreateConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);
            testPeer.expectBegin();
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            Queue queue = context.createQueue("test");
            JMSConsumer consumer = context.createConsumer(queue);
            assertNotNull(consumer);

            testPeer.expectEnd();
            testPeer.expectClose();
            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyCloseJMSConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);

            testPeer.expectBegin();

            // Create a consumer, then remotely end it afterwards.
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, AmqpError.RESOURCE_DELETED, "resource closed");

            Queue queue = context.createQueue("myQueue");
            final JMSConsumer consumer = context.createConsumer(queue);

            // Verify the consumer gets marked closed
            testPeer.waitForAllHandlersToComplete(1000);
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        consumer.getMessageListener();
                    } catch (IllegalStateRuntimeException jmsise) {
                        return true;
                    }
                    return false;
                }
            }, 10000, 10), "JMSConsumer never closed.");

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            consumer.close();

            testPeer.expectEnd();
            testPeer.expectClose();
            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testReceiveMessageWithReceiveZeroTimeout() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);

            testPeer.expectBegin();

            Queue queue = context.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            JMSConsumer messageConsumer = context.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(0);

            assertNotNull(receivedMessage, "A message should have been recieved");

            testPeer.expectEnd();
            testPeer.expectClose();
            context.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test
    @Timeout(20)
    public void testConsumerReceiveNoWaitThrowsIfConnectionLost() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);

            testPeer.expectBegin();

            Queue queue = context.createQueue("queue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow(false, notNullValue(UnsignedInteger.class));
            testPeer.expectLinkFlow(true, notNullValue(UnsignedInteger.class));
            testPeer.dropAfterLastHandler();

            final JMSConsumer consumer = context.createConsumer(queue);

            try {
                consumer.receiveNoWait();
                fail("An exception should have been thrown");
            } catch (JMSRuntimeException jmsre) {
                // Expected
            }

            try {
                context.close();
            } catch (Throwable ignored) {
            }
        }
    }

    @Test
    @Timeout(20)
    public void testNoReceivedMessagesWhenConnectionNotStarted() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);
            context.setAutoStart(false);

            testPeer.expectBegin();

            Queue destination = context.createQueue(getTestName());

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 3);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            JMSConsumer consumer = context.createConsumer(destination);

            assertNull(consumer.receive(100));

            context.start();

            assertNotNull(consumer.receive(2000));

            testPeer.expectEnd();
            testPeer.expectClose();
            context.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test
    @Timeout(60)
    public void testSyncReceiveFailsWhenListenerSet() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);

            testPeer.expectBegin();

            Queue destination = context.createQueue(getTestName());

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            JMSConsumer consumer = context.createConsumer(destination);

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message m) {
                    LOG.warn("Async consumer got unexpected Message: {}", m);
                }
            });

            try {
                consumer.receive();
                fail("Should have thrown an exception.");
            } catch (JMSRuntimeException ex) {
            }

            try {
                consumer.receive(1000);
                fail("Should have thrown an exception.");
            } catch (JMSRuntimeException ex) {
            }

            try {
                consumer.receiveNoWait();
                fail("Should have thrown an exception.");
            } catch (JMSRuntimeException ex) {
            }

            testPeer.expectEnd();
            testPeer.expectClose();
            context.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test
    @Timeout(20)
    public void testReceiveBodyMapMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);

            testPeer.expectBegin();

            Queue queue = context.createQueue("myQueue");

            // Prepare an AMQP message for the test peer to send, containing an
            // AmqpValue section holding a map with entries for each supported type,
            // and annotated as a JMS map message.
            String myBoolKey = "myBool";
            boolean myBool = true;
            String myByteKey = "myByte";
            byte myByte = 4;
            String myBytesKey = "myBytes";
            byte[] myBytes = myBytesKey.getBytes();
            String myCharKey = "myChar";
            char myChar = 'd';
            String myDoubleKey = "myDouble";
            double myDouble = 1234567890123456789.1234;
            String myFloatKey = "myFloat";
            float myFloat = 1.1F;
            String myIntKey = "myInt";
            int myInt = Integer.MAX_VALUE;
            String myLongKey = "myLong";
            long myLong = Long.MAX_VALUE;
            String myShortKey = "myShort";
            short myShort = 25;
            String myStringKey = "myString";
            String myString = myStringKey;

            Map<String, Object> map = new LinkedHashMap<String, Object>();
            map.put(myBoolKey, myBool);
            map.put(myByteKey, myByte);
            map.put(myBytesKey, new Binary(myBytes));// the underlying AMQP message uses Binary rather than byte[] directly.
            map.put(myCharKey, myChar);
            map.put(myDoubleKey, myDouble);
            map.put(myFloatKey, myFloat);
            map.put(myIntKey, myInt);
            map.put(myLongKey, myLong);
            map.put(myShortKey, myShort);
            map.put(myStringKey, myString);

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_MAP_MESSAGE);

            DescribedType amqpValueSectionContent = new AmqpValueDescribedType(map);

            // receive the message from the test peer
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, null, null, amqpValueSectionContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();
            testPeer.expectEnd();
            testPeer.expectClose();

            JMSConsumer messageConsumer = context.createConsumer(queue);
            @SuppressWarnings("unchecked")
            Map<String, Object> receivedMap = messageConsumer.receiveBody(Map.class, 3000);

            // verify the content is as expected
            assertNotNull(receivedMap, "Map was not received");

            assertEquals(myBool, receivedMap.get(myBoolKey), "Unexpected boolean value");
            assertEquals(myByte, receivedMap.get(myByteKey), "Unexpected byte value");
            byte[] readBytes = (byte[]) receivedMap.get(myBytesKey);
            assertTrue(Arrays.equals(myBytes, readBytes), "Read bytes were not as expected: " + Arrays.toString(readBytes));
            assertEquals(myChar, receivedMap.get(myCharKey), "Unexpected char value");
            assertEquals(myDouble, (double) receivedMap.get(myDoubleKey), 0.0, "Unexpected double value");
            assertEquals(myFloat, (float) receivedMap.get(myFloatKey), 0.0, "Unexpected float value");
            assertEquals(myInt, receivedMap.get(myIntKey), "Unexpected int value");
            assertEquals(myLong, receivedMap.get(myLongKey), "Unexpected long value");
            assertEquals(myShort, receivedMap.get(myShortKey), "Unexpected short value");
            assertEquals(myString, receivedMap.get(myStringKey), "Unexpected UTF value");

            context.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testReceiveBodyTextMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);

            testPeer.expectBegin();

            final String content = "Message-Content";
            Queue queue = context.createQueue("myQueue");

            DescribedType amqpValueContent = new AmqpValueDescribedType(content);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();
            testPeer.expectEnd();
            testPeer.expectClose();

            JMSConsumer messageConsumer = context.createConsumer(queue);
            String received = messageConsumer.receiveBody(String.class, 3000);

            assertNotNull(received);
            assertEquals(content, received);

            context.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testReceiveBodyObjectMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);

            testPeer.expectBegin();

            Queue queue = context.createQueue("myQueue");

            PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);

            String expectedContent = "expectedContent";

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(expectedContent);
            oos.flush();
            oos.close();
            byte[] bytes = baos.toByteArray();

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_OBJECT_MESSAGE);

            DescribedType dataContent = new DataDescribedType(new Binary(bytes));

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, properties, null, dataContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();
            testPeer.expectEnd();
            testPeer.expectClose();

            JMSConsumer messageConsumer = context.createConsumer(queue);
            String received = messageConsumer.receiveBody(String.class, 3000);

            assertNotNull(received);
            assertEquals(expectedContent, received);

            context.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testReceiveBodyBytesMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);

            testPeer.expectBegin();

            Queue queue = context.createQueue("myQueue");

            PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE);

            MessageAnnotationsDescribedType msgAnnotations = null;
            msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_BYTES_MESSAGE);

            final byte[] expectedContent = "expectedContent".getBytes();
            DescribedType dataContent = new DataDescribedType(new Binary(expectedContent));

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, properties, null, dataContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            JMSConsumer messageConsumer = context.createConsumer(queue);
            byte[] received = messageConsumer.receiveBody(byte[].class, 3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(received);
            assertTrue(Arrays.equals(expectedContent, received));

            testPeer.expectEnd();
            testPeer.expectClose();

            context.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testReceiveBodyFailsDoesNotAcceptMessageAutoAck() throws Exception {
        doTestReceiveBodyFailsDoesNotAcceptMessage(JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Test
    @Timeout(20)
    public void testReceiveBodyFailsDoesNotAcceptMessageDupsOk() throws Exception {
        doTestReceiveBodyFailsDoesNotAcceptMessage(JMSContext.DUPS_OK_ACKNOWLEDGE);
    }

    @Test
    @Timeout(20)
    public void testReceiveBodyFailsDoesNotAcceptMessageClientAck() throws Exception {
        doTestReceiveBodyFailsDoesNotAcceptMessage(JMSContext.CLIENT_ACKNOWLEDGE);
    }

    public void doTestReceiveBodyFailsDoesNotAcceptMessage(int sessionMode) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);

            testPeer.expectBegin();

            final String content = "Message-Content";
            Queue queue = context.createQueue("myQueue");

            DescribedType amqpValueContent = new AmqpValueDescribedType(content);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueContent);
            testPeer.expectEnd();
            testPeer.expectClose();

            JMSConsumer messageConsumer = context.createConsumer(queue);
            try {
                messageConsumer.receiveBody(Boolean.class, 3000);
                fail("Should not read as Boolean type");
            } catch (MessageFormatRuntimeException mfre) {
            }

            context.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testReceiveBodyFailsThenAcceptsOnSuccessfullyNextCallAutoAck() throws Exception {
        doTestReceiveBodyFailsDoesNotAcceptMessage(JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Test
    @Timeout(20)
    public void testReceiveBodyFailsThenAcceptsOnSuccessfullyNextCallDupsOk() throws Exception {
        doTestReceiveBodyFailsDoesNotAcceptMessage(JMSContext.DUPS_OK_ACKNOWLEDGE);
    }

    @Test
    @Timeout(20)
    public void testReceiveBodyFailsThenGetNullOnNextAttemptClientAck() throws Exception {
        doTestReceiveBodyFailsDoesNotAcceptMessage(JMSContext.CLIENT_ACKNOWLEDGE);
    }

    public void doTestReceiveBodyFailsThenCalledWithCorrectType(int sessionMode) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);

            testPeer.expectBegin();

            final String content = "Message-Content";
            Queue queue = context.createQueue("myQueue");

            DescribedType amqpValueContent = new AmqpValueDescribedType(content);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueContent);

            JMSConsumer messageConsumer = context.createConsumer(queue);
            try {
                messageConsumer.receiveBody(Boolean.class, 3000);
                fail("Should not read as Boolean type");
            } catch (MessageFormatRuntimeException mfre) {
            }

            testPeer.waitForAllHandlersToComplete(3000);

            if (sessionMode == JMSContext.AUTO_ACKNOWLEDGE ||
                sessionMode == JMSContext.DUPS_OK_ACKNOWLEDGE) {

                testPeer.expectDispositionThatIsAcceptedAndSettled();
            }

            String received = messageConsumer.receiveBody(String.class, 3000);

            if (sessionMode == JMSContext.AUTO_ACKNOWLEDGE ||
                sessionMode == JMSContext.DUPS_OK_ACKNOWLEDGE) {

                assertNotNull(received);
                assertEquals(content, received);
            } else {
                assertNull(received);
            }

            testPeer.expectEnd();
            testPeer.expectClose();

            context.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testReceiveBodyBytesMessageFailsWhenWrongTypeRequested() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);

            testPeer.expectBegin();

            Queue queue = context.createQueue("myQueue");

            PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE);

            MessageAnnotationsDescribedType msgAnnotations = null;
            msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_BYTES_MESSAGE);

            final byte[] expectedContent = "expectedContent".getBytes();
            DescribedType dataContent = new DataDescribedType(new Binary(expectedContent));

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, properties, null, dataContent, 2);
            testPeer.expectDispositionThatIsAcceptedAndSettled();
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            JMSConsumer messageConsumer = context.createConsumer(queue);
            try {
                messageConsumer.receiveBody(String.class, 3000);
                fail("Should not read as String type");
            } catch (MessageFormatRuntimeException mfre) {
            }

            byte[] received1 = messageConsumer.receiveBody(byte[].class, 3000);

            try {
                messageConsumer.receiveBody(Map.class, 3000);
                fail("Should not read as Map type");
            } catch (MessageFormatRuntimeException mfre) {
            }

            byte[] received2 = (byte[]) messageConsumer.receiveBody(Object.class, 3000);

            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(received1);
            assertNotNull(received2);
            assertTrue(Arrays.equals(expectedContent, received1));
            assertTrue(Arrays.equals(expectedContent, received2));

            testPeer.expectEnd();
            testPeer.expectClose();

            context.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }
}
