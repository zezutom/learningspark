package org.zezutom.learningspark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCountAndJoin {

    public static List<Tuple2<String, Tuple2<Integer, Integer>>> wc_join(String keyword, JavaRDD<String> rdd1, JavaRDD<String> rdd2) {
        JavaPairRDD<String, Integer> wc_out1 = wc(keyword, rdd1);
        JavaPairRDD<String, Integer> wc_out2 = wc(keyword, rdd2);
        return wc_out1.join(wc_out2).collect();
    }

    private static JavaPairRDD<String, Integer> wc(String keyword, JavaRDD<String> rdd) {
        return rdd
                .flatMap(s -> Arrays.asList(s.split("\\s")))    // Split on any white character
                .map(s -> s.replaceAll(",", "")
                        .replaceAll("\\.", "").toLowerCase())   // Remove punctuation and make all words lowercase
                .filter(s -> s.equals(keyword.toLowerCase()))
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
    }
}
