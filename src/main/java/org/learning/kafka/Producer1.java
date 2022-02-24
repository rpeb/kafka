package org.learning.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer1 {

	public static void main(String[] args) {
		final String topic = "twitterTopic";
		final String bootstrapServer = "http://localhost:29092";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		for (int i = 10; i < 16; ++i) {
			String message = "hello" + Integer.toString(i);
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);
			producer.send(record);
		}
		producer.close();
	}

}
