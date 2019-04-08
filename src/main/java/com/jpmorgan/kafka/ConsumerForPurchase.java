package com.jpmorgan.kafka;

import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerForPurchase {



	public static void main(String[] args) {

		Logger logger=Logger.getLogger(ConsumerForPurchase.class.getName(), "C:\\jpmorgan\\log.txt");
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.33.10:9092");
		props.put("group.id", "group-1");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		@SuppressWarnings("resource")
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
		kafkaConsumer.subscribe(Arrays.asList("PurchasedTopic"));
		int sumOfPurchase = 0;
		int sumOfapplesPrice = 0;
		int sumOforangesPrice = 0;
		int sumOfbananasPrice = 0;
		int sumOfgrapesPrice = 0;
		int sumOfpomegranitePrice = 0;
		int sumOfperiesPrice = 0;
		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
			int numOfConsumerRecords = 0;
			for (ConsumerRecord<String, String> record : records) {
				System.out.println("Partition: " + record.partition() + " Offset: " + record.offset()
				+ " Value: " + record.value() + " ThreadID: " + Thread.currentThread().getId());
				// Parse the value and then do the additions of Euro
				String valueOfMessage = record.value();
				String purchaseValue = valueOfMessage.substring(valueOfMessage.indexOf(":")).trim();
				sumOfPurchase += Integer.parseInt(purchaseValue);
				numOfConsumerRecords++;
				if (valueOfMessage.contains("apples")) {
					sumOfapplesPrice++;
				} else if (valueOfMessage.contains("oranges")) {
					sumOforangesPrice++;
				} else if (valueOfMessage.contains("bananas")) {
					sumOfbananasPrice++;
				} else if (valueOfMessage.contains("grapes")) {
					sumOfgrapesPrice++;
				} else if (valueOfMessage.contains("pomegranite")) {
					sumOfpomegranitePrice++;
				} else if (valueOfMessage.contains("peries")) {
					sumOfperiesPrice++;
				} 
				if (numOfConsumerRecords == 10) {
					
					logger.info("total products " + sumOfPurchase);
					logger.info("total apples price" + sumOfapplesPrice);
					logger.info("total oranges price" + sumOforangesPrice);
					logger.info("total bananas price" + sumOfbananasPrice);
					logger.info("total grapes price" + sumOfgrapesPrice);
					logger.info("total pomegranite price" + sumOfpomegranitePrice);
					logger.info("total peries price" + sumOfperiesPrice);
					
				} else if (numOfConsumerRecords == 50) {
					logger.info("Adjustments are run for the consumer records to be success");
				}

			}
			System.out.println("The value of the sum of purchase " + sumOfPurchase);

		}

	}

}
