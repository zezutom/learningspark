package org.zezutom.learningspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;

/**
 * Counts the number of words in the given file and saves the result into an output directory.
 *
 * How to Run:
 *
 * $SPARK_HOME/bin/spark-submit \
 * --class "org.zezutom.learningspark.WordCountApp" \
 * --master local[4] \
 * target/learningspark-1.0-SNAPSHOT.jar src/main/resources/README.md
 *
 */
public class WordCountApp {

    public static void main(String[] args) {
        if (args.length == 0)
            throw new IllegalArgumentException("Please provide a file to parse");

        final String file = args[0];

        if (!new File(file).exists())
            throw new IllegalArgumentException("No such file: " + file);

        SparkConf conf = new SparkConf().setAppName("WordCount App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        WordCount.wc(sc.textFile(file, 2).cache()).saveAsTextFile("wc_out.txt");
    }
}
