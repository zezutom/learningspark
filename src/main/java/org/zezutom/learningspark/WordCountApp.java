package org.zezutom.learningspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

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

        SparkConf conf = new SparkConf().setAppName("WordCount App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        new WordCount(sc).wc(args[0]).saveAsTextFile("wc_out.txt");
    }
}
