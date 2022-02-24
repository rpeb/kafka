package org.learning.kafka;

import io.github.redouane59.twitter.TwitterClient;
import io.github.redouane59.twitter.dto.tweet.TweetList;
import io.github.redouane59.twitter.dto.tweet.TweetV2;
import io.github.redouane59.twitter.signature.TwitterCredentials;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import org.learning.kafka.util.PropertiesFileReader;

import java.util.Properties;

public class TwitterProducer {
    public static TwitterClient getTwitterClient(String twitterPropertiesFile) {
        Properties props = PropertiesFileReader.getProperties(twitterPropertiesFile);
        final String apiKey = props.getProperty("api_key");
        final String apiSecret = props.getProperty("api_secret");
        final String token = props.getProperty("token");
        final String secret = props.getProperty("secret");
        System.out.println("apikey: " + apiKey + "apiSecret: " + apiSecret);
        TwitterClient twitterClient = new TwitterClient(TwitterCredentials.builder()
                .accessToken(token)
                .accessTokenSecret(secret)
                .apiKey(apiKey)
                .apiSecretKey(apiSecret)
                .build());
        return twitterClient;
    }

    public static KafkaProducer<String, String> getKafkaProducer() {
        final String bootstrapServer = "http://localhost:29092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    public static void main(String[] args) {
        final String twitterPropertiesFile = "src/main/resources/twitter.properties";
        final String kafkaTopic = "twitter_topic_2";
        TwitterClient twitterClient = getTwitterClient(twitterPropertiesFile);
        TweetList tweetList = twitterClient.searchTweets("from:naval");
        for (TweetV2.TweetData tweetData: tweetList.getData()) {
            JSONObject tweetBody = new JSONObject();
//            tweetData.getId()
            tweetBody.put("id", tweetData.getId());
            tweetBody.put("author", tweetData.getAuthorId());
            tweetBody.put("text",tweetData.getText());
            String message = tweetBody.toJSONString();
            KafkaProducer<String, String> producer = getKafkaProducer();
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaTopic, message);
            producer.send(record);
            producer.close();
        }
    }
}