package org.zezutom.learningspark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static JavaPairRDD<String, Integer> wc(JavaRDD<String> lines) {
        return lines
                .flatMap(s -> Arrays.asList(s.split("\\s")))        // Split on any white character
                .filter(word -> !word.isEmpty())                    // Only count words as non-empty strings
                .map(word -> word.replaceAll(",", "")
                        .replaceAll("\\.", "").toLowerCase())         // Remove punctuation and make all words lowercase
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2)
                .sortByKey();
    }
}
