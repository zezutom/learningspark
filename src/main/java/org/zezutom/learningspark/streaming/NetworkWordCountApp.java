package org.zezutom.learningspark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Counts the number of words in text data received from a data server listening on a TCP socket.
 *
 * How to Run:
 * 1. Run Netcat (found in most Unix-like systems) as a data server: nc -lk 9999
 * 2. In a different terminal, start this example as:
 *
 * $SPARK_HOME/bin/spark-submit \
 * --class "org.zezutom.learningspark.streaming.NetworkWordCountApp" \
 * target/learningspark-1.0-SNAPSHOT.jar
 *
 * 3. Then, any lines typed in the terminal running the Netcat server will be counted and printed on screen every second:
 *
 *   $ nc -lk 9999
 *   hello world
 *
 * Prints:
 *   -------------------------------------------
 *   Time: 1438265763000 ms
 *   -------------------------------------------
 *   (hello,1)
 *   (world,1)
 *
 * http://spark.apache.org/docs/latest/streaming-programming-guide.html
 */
public class NetworkWordCountApp {

    public static void main(String[] args) {

        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCountApp");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // Create a DStream that will connect to hostname:port, like localhost:9999
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaPairDStream<String, Integer> wordCounts = NetworkWordCount.wc(lines);

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        // Start the computation
        jssc.start();

        // Wait for the computation to terminate
        jssc.awaitTermination();
    }
}
