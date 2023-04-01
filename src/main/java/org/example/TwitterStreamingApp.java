package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import java.util.Arrays;

public class TwitterStreamingApp {

    public static void main(String[] args) throws InterruptedException {
        // Set up Twitter credentials
        System.setProperty("twitter4j.oauth.consumerKey", "YOUR_CONSUMER_KEY");
        System.setProperty("twitter4j.oauth.consumerSecret", "YOUR_CONSUMER_SECRET");
        System.setProperty("twitter4j.oauth.accessToken", "YOUR_ACCESS_TOKEN");
        System.setProperty("twitter4j.oauth.accessTokenSecret", "YOUR_ACCESS_SECRET");

        // Create a Spark configuration
        SparkConf conf = new SparkConf().setAppName("TwitterStream");

        // Create a Streaming context with a batch interval of 5 seconds
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // Create a Twitter DStream
        JavaDStream<twitter4j.Status> stream = TwitterUtils.createStream(ssc);

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
