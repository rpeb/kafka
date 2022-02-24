package org.learning.kafka;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import io.github.redouane59.twitter.TwitterClient;
import io.github.redouane59.twitter.dto.tweet.Tweet;
import io.github.redouane59.twitter.dto.tweet.TweetList;
import io.github.redouane59.twitter.dto.tweet.TweetV2;
import io.github.redouane59.twitter.signature.TwitterCredentials;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.json.simple.JSONObject;
import org.learning.kafka.util.PropertiesFileReader;

public class TwitterProducer {
    public static Client getHosebirdClient(String twitterPropertiesFile) {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndPoint = new StatusesFilterEndpoint();
        List<Long> followings = Arrays.asList(1234L,22132L);
        List<String> terms = Arrays.asList("twitter", "api");
        hosebirdEndPoint.trackTerms(terms);
        hosebirdEndPoint.followings(followings);
        Properties props = PropertiesFileReader.getProperties(twitterPropertiesFile);
        final String apiKey = props.getProperty("api_key");
        final String apiSecret = props.getProperty("api_secret");
        final String token = props.getProperty("token");
        final String secret = props.getProperty("secret");
        Authentication auth = new OAuth1(apiKey,apiSecret,token,secret);
        ClientBuilder builder = new ClientBuilder()
                .name("kafka")
                .hosts(hosts)
                .authentication(auth)
                .endpoint(hosebirdEndPoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        Client client = builder.build();
        return client;
    }

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