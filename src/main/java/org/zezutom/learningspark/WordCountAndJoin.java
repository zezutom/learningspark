package org.zezutom.learningspark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class WordCountAndJoin {

    private final JavaSparkContext sc;

    public WordCountAndJoin(JavaSparkContext sc) {
        this.sc = sc;
    }

    public List<Tuple2<String, Tuple2<Integer, Integer>>> wc_join(String keyword, String file1, String file2) {
        JavaPairRDD<String, Integer> wc_out1 = wc(keyword, file1);
        JavaPairRDD<String, Integer> wc_out2 = wc(keyword, file2);
        return wc_out1.join(wc_out2).collect();
    }

    private JavaPairRDD<String, Integer> wc(String keyword, String file) {
        if (!new File(file).exists())
            throw new IllegalArgumentException("No such file: " + file);
        return sc.textFile(file).flatMap(s -> Arrays.asList(s.split(" "))).filter(s -> s.equals(keyword)).mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);
    }
}
