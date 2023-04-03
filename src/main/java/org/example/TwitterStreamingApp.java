package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Arrays;

public class TwitterStreamingApp {

    public static void main(String[] args) throws InterruptedException {
        // Set up Twitter credentials using environment variables
        String consumerKey = System.getenv("TWITTER_CONSUMER_KEY");
        String consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET");
        String accessToken = System.getenv("TWITTER_ACCESS_TOKEN");
        String accessTokenSecret = System.getenv("TWITTER_ACCESS_SECRET");
        System.setProperty("twitter4j.config", "twitter4j.properties");


        // Create a Twitter configuration
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret);
        Authorization authorization = AuthorizationFactory.getInstance(configurationBuilder.build());

        // Create a Spark configuration
        SparkConf conf = new SparkConf().setAppName("TwitterStream");

        // Create a Streaming context with a batch interval of 5 seconds
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // Create a Twitter DStream of your own tweets
        JavaDStream<Status> stream = TwitterUtils.createStream(ssc, authorization)
                .filter(status -> status.getUser().getScreenName().equals("PeacefullyPizza"));

        // Extract hashtags from tweets
        JavaDStream<String> hashtags = stream.flatMap(status -> Arrays.asList(status.getText().split(" ")).iterator())
                .filter(word -> word.startsWith("#"));

        // Count occurrences of each hashtag
        JavaPairDStream<String, Long> counts = hashtags.countByValue();

        // Print the results
        counts.print();

        // Start the streaming context
        ssc.start();
        ssc.awaitTermination();
    }
}