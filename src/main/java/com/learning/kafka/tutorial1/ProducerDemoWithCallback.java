package com.learning.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        // producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create producer record
        for (int i = 0; i < 10; ++i) {
            ProducerRecord<String, String> record = new ProducerRecord<>("new-topic", "hello world " + Integer.toString(i));

            // send data
            producer.send(record, (recordMetadata, e) -> {
                // executes everytime a record is successfully sent or error occurs
                if (e != null) {
                    logger.error("Error while producing: " + e);
                } else {
                    logger.info("\nReceived new metadata: \nTopic = " + recordMetadata.topic() + "\nPartition = " + recordMetadata.partition() + "\nOffsets = " + recordMetadata.offset() + "\nTimestamp = " + recordMetadata.timestamp());
                }
            });
        }

        // flush and close producer
        producer.close();
    }
}
