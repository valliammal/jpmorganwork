# Apache Kafka Message passing for the Product sale for Java

Product sale and get that info code for connecting to a Apache Kafka cluster and authenticate with SSL_SASL and SCRAM. 

To easily test this code you can create a free Apacha Kafka instance at https://www.cloudkarafka.com

## Running locally

If you just want to test it out.

### Configure

All of the authentication settings can be found in the Details page for your CloudKarafka instance.

```
export CLOUDKARAFKA_BROKERS=broker1:9094,broker2:9094,broker3:9094
export CLOUDKARAFKA_USERNAME=<username>
export CLOUDKARAFKA_PASSWORD=<password>
```

### Run

```
mvn clean compile assembly:single
java -jar target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar
```

This will start a Java application that pushes messages to Kafka in one Thread and read messages in the main Thread. 
The output you will see in the terminal is the messages received in the consumer.
The ConsumerForPurchase.java is executed in another command line and then both will have the authentication 
and get the results of send and receive.