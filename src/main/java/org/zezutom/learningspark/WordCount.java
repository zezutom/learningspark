package org.zezutom.learningspark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;

public class WordCount {

    private final JavaSparkContext sc;

    public WordCount(JavaSparkContext sc) {
        this.sc = sc;
    }

    public JavaPairRDD<String, Integer> wc(String file) {
        if (!new File(file).exists())
            throw new IllegalArgumentException("No such file: " + file);

        return wc(sc.textFile(file, 2));
    }

    public JavaPairRDD<String, Integer> wc(JavaRDD<String> lines) {
        return lines.flatMap(s -> Arrays.asList(s.split(" "))).mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);
    }
}
