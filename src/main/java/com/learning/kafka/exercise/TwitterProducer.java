package com.learning.kafka.exercise;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100);

        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndPoint = new StatusesFilterEndpoint();

        List<Long> followings = Arrays.asList(1234L,22132L);
        List<String> terms = Arrays.asList("twitter", "api");

        hosebirdEndPoint.trackTerms(terms);

        Properties props = null;
        FileInputStream fis = null;
        try {
            fis = new FileInputStream("src/main/resources/twitter.properties");
            props = new Properties();
            props.load(fis);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        Authentication auth = new OAuth1(
                props.getProperty("api_key"),
                props.getProperty("api_secret"),
                props.getProperty("token"),
                props.getProperty("secret")
        );
        ClientBuilder builder = new ClientBuilder()
                .name("kafka")
                .hosts(hosts)
                .authentication(auth)
                .endpoint(hosebirdEndPoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client client = builder.build();
        client.connect();
        for (int i = 0; i < 100; ++i) {
            try {
                String message = msgQueue.take();
                Properties properties = new Properties();
                properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
                properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

                ProducerRecord<String, String> record = new ProducerRecord<>("twitter_topic", message);
                producer.send(record);
                producer.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
