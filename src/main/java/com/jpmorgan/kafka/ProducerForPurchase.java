package com.jpmorgan.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerForPurchase {

  public static void main(String[] args) {
	  
    Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.33.10:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Producer<String, String> producer = null;
    try {
      producer = new KafkaProducer<>(props);
      
      String message = "";

      message = "apples 20 Price in Euros : 10";
      producer.send(new ProducerRecord<String, String>("PurchasedTopic", message));
      System.out.println("Sent:" + message);

      message = "oranges 10 Price in Euros : 5";
      producer.send(new ProducerRecord<String, String>("PurchasedTopic", message));
      System.out.println("Sent:" + message);

      message = "bananas 20 Price in Euros : 8";
      producer.send(new ProducerRecord<String, String>("PurchasedTopic", message));
      System.out.println("Sent:" + message);

      message = "grapes 1kg Price in Euros : 7";
      producer.send(new ProducerRecord<String, String>("PurchasedTopic", message));
      System.out.println("Sent:" + message);

      message = "pomegranite Price in Euros : 2";
      producer.send(new ProducerRecord<String, String>("PurchasedTopic", message));
      System.out.println("Sent:" + message);

      message = "peries 5 Price in Euros : 2";
      producer.send(new ProducerRecord<String, String>("PurchasedTopic", message));
      System.out.println("Sent:" + message);
      
    } catch (Exception e) {
      e.printStackTrace();

    } finally {
      producer.close();
    }

  }

}
