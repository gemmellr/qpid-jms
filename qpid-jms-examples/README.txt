# Modified example for demonstrating OpenTracing usage with Jaeger.

The JMS client code relating to this is mostly in https://github.com/apache/qpid-jms/blob/main/qpid-jms-client/src/main/java/org/apache/qpid/jms/tracing/opentracing/OpenTracingTracer.java and the other files under directory https://github.com/apache/qpid-jms/tree/main/qpid-jms-client/src/main/java/org/apache/qpid/jms/tracing


# Start Jaeger, for example:

 docker run -it --rm -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 -p 5775:5775/udp -p 6831:6831/udp -p 6832:6832/udp -p 5778:5778 -p 16686:16686 -p 14268:14268 -p 14250:14250 -p 9411:9411 docker.io/jaegertracing/all-in-one:1.23

#  Start an AMQP server on port 5672, for example:

 docker run -it --rm -p 8161:8161 -p 61616:61616 -p 5672:5672 -e ARTEMIS_USERNAME=guest -e ARTEMIS_PASSWORD=guest vromero/activemq-artemis:2.16.0-alpine

# Build and run the HelloWorld example:

 cd qpid-jms-examples
 mvn clean package dependency:copy-dependencies -DincludeScope=runtime -DskipTests
 java -DUSER=guest -DPASSWORD=guest -cp "target/classes/:target/dependency/*" org.apache.qpid.jms.example.HelloWorld

# Look in the Jaeger UI:
 Browse to http://localhost:16686
 Select the "myTracedHelloWorld" from the Service dropdown at the top of the Search options (if not already selected).
 Hit Find Traces.

The search results will show traces that you can then click through and play around with. There will be one for each run of HelloWorld if you run it many times (go to Search and d Find Traces again for updates).

Each run will result in 2 'spans', one from the sending of the message, and another from receiving the message. The send span will be many millseconds long, as it covers the period from the start of sending to the point the client receives the result (e.g accepted) from the server. The receive span is effectively instantaneous, it just marks the point in time at which the consumer.receive() method got a message to return to the application.

(If the example were modified to use an asynchronous MessageListener to receive the message, the recieve span would cover the time taken to deliver the message to the application...which is what e.g the Proton clients [will] do).




===========================
Running the client examples
===========================

Use maven to build the module, and additionally copy the dependencies
alongside their output:

  mvn clean package dependency:copy-dependencies -DincludeScope=runtime -DskipTests

Now you can run the examples using commands of the format:

  Linux:   java -cp "target/classes/:target/dependency/*" org.apache.qpid.jms.example.HelloWorld

  Windows: java -cp "target\classes\;target\dependency\*" org.apache.qpid.jms.example.HelloWorld

NOTE: The examples expect to use a Queue named "queue". You may need to create
this before running the examples, depending on the broker/peer you are using.

NOTE: By default the examples can only connect anonymously. A username and
password with which the connection can authenticate with the server may be set
through system properties named USER and PASSWORD respectively. E.g:

  Linux:   java -DUSER=guest -DPASSWORD=guest -cp "target/classes/:target/dependency/*" org.apache.qpid.jms.example.HelloWorld

  Windows: java -DUSER=guest -DPASSWORD=guest -cp "target\classes\;target\dependency\*" org.apache.qpid.jms.example.HelloWorld

NOTE: You can configure the connection and queue details used by updating the
JNDI configuration file before building. It can be found at:
src/main/resources/jndi.properties

NOTE: The earlier build command will cause Maven to resolve the client artifact
dependencies against its local and remote repositories. If you wish to use a
locally-built client, ensure to "mvn install" it in your local repo first.

