package org.zezutom.learningspark.streaming;

import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import scala.Tuple2;

import java.util.Arrays;

public class NetworkWordCount {

    public static JavaPairDStream<String, Integer> wc(JavaReceiverInputDStream<String> lines) {
        return lines
                .flatMap(s -> Arrays.asList(s.split("\\s")))        // Split on any white character
                .filter(word -> !word.isEmpty())                    // Only count words as non-empty strings
                .map(word -> word.replaceAll(",", "")
                        .replaceAll("\\.", "").toLowerCase())       // Remove punctuation and make all words lowercase
                .mapToPair(s -> new Tuple2<>(s, 1))                 // Count each word in each batch
                .reduceByKey((i1, i2) -> i1 + i2);
    }
}
