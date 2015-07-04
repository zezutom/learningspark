package org.zezutom.learningspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;

/**
 * Counts the number of words in the given file and saves the result into an output directory.
 *
 * How to Run:
 *
 * $SPARK_HOME/bin/spark-submit \
 * --class "org.zezutom.learningspark.WordCount" \
 * --master local[4] \
 * target/learningspark-1.0-SNAPSHOT.jar src/main/resources/README.md
 *
 */
public class WordCount {

    public static void main(String[] args) {
        if (args.length == 0)
            throw new IllegalArgumentException("Please provide a file to parse");

        String file = args[0];

        if (!new File(file).exists())
            throw new IllegalArgumentException("No such file: " + file);

        SparkConf conf = new SparkConf().setAppName("Word Count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> data = sc.textFile(file, 2);

        JavaPairRDD<String, Integer> wc;
        wc = data.flatMap(s -> Arrays.asList(s.split(" "))).mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);

        wc.saveAsTextFile("wc_out.txt");
    }
}
