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
package org.apache.qpid.jms.example;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.tracing.JmsTracer;
import org.apache.qpid.jms.tracing.opentracing.OpenTracingTracerFactory;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

// NOTE: Hacky temporary additions to HelloWorld for ad-hoc tracing 'testing'
public class HelloWorld {

    private static boolean REGISTER_GLOBAL_TRACER = true;
    private static boolean SET_TRACER_ON_FACTORY = false;

    private static boolean PRODUCER = true;
    private static boolean CONSUMER = true;
    private static boolean USE_ONMESSAGE = true;

    private static long TIMEOUT = 5000;

    public static void main(String[] args) throws Exception {
        if(!PRODUCER && !CONSUMER) {
            throw new IllegalStateException("Not configured for producing or consuming!");
        }

        if (REGISTER_GLOBAL_TRACER) {
            String  serviceName = "some-global-tracer"; // Normally set via env variable, hard coding for testing.
            Configuration configuration = Configuration.fromEnv(serviceName);

            doHackyTestingConfiguration(configuration);

            Tracer tracer = configuration.getTracer();
            GlobalTracer.registerIfAbsent(tracer);
        }

        JmsTracer factorySet = null;
        if (SET_TRACER_ON_FACTORY) {
            String  serviceName = "some-factory-set-tracer"; // Normally set via env variable, hard coding for testing.
            Configuration configuration = Configuration.fromEnv(serviceName);

            doHackyTestingConfiguration(configuration);

            Tracer tracer = configuration.getTracer();

            factorySet = OpenTracingTracerFactory.create(tracer);
        }

        try {
            // The configuration for the Qpid InitialContextFactory has been supplied in
            // a jndi.properties file in the classpath, which results in it being picked
            // up automatically by the InitialContext constructor.
            Context context = new InitialContext();

            JmsConnectionFactory factory = (JmsConnectionFactory) context.lookup("myFactoryLookup");
            Destination queue = (Destination) context.lookup("myQueueLookup");

            if(factorySet != null) {
                factory.setTracer(factorySet);
            }

            Connection connection = factory.createConnection(System.getProperty("USER", "guest"), System.getProperty("PASSWORD", "guest"));
            connection.setExceptionListener(new MyExceptionListener());
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer messageConsumer = null;
            if(CONSUMER) {
                messageConsumer = session.createConsumer(queue);
            }

            MessageProducer messageProducer = null;
            if(PRODUCER) {
                messageProducer = session.createProducer(queue);

                TextMessage message = session.createTextMessage("Hello world!");
                messageProducer.send(message, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            if(CONSUMER) {
                if(USE_ONMESSAGE) {
                    CountDownLatch latch = new CountDownLatch(1);
                    messageConsumer.setMessageListener(new MessageListener() {

                        @Override
                        public void onMessage(Message message) {
                            try {
                                Thread.sleep(100);
                                String body = message.getBody(String.class);
                                System.out.println(body);
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                latch.countDown();
                            }
                        }
                    });

                    if(!latch.await(TIMEOUT, TimeUnit.MILLISECONDS)) {
                        System.out.println("No message received within the given timeout!");
                    }
                } else {
                    Thread.sleep(100);

                    TextMessage receivedMessage = (TextMessage) messageConsumer.receive(TIMEOUT);

                    if (receivedMessage != null) {
                        System.out.println(receivedMessage.getText());
                    } else {
                        System.out.println("No message received within the given timeout!");
                    }
                }
            }

            connection.close();
        } catch (Exception exp) {
            System.out.println("Caught exception, exiting.");
            exp.printStackTrace(System.out);
            System.exit(1);
        }

    }

    private static class MyExceptionListener implements ExceptionListener {
        @Override
        public void onException(JMSException exception) {
            System.out.println("Connection ExceptionListener fired, exiting.");
            exception.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private static void doHackyTestingConfiguration(Configuration configuration) {
        // Everything below would normally be configured remotely or by env variable, hack here for testing only
        SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv();
        samplerConfig.withType("const");
        samplerConfig.withParam(1);

        ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv();
        reporterConfig.withLogSpans(true);

        configuration.withReporter(reporterConfig);
        configuration.withSampler(samplerConfig);
    }
}
