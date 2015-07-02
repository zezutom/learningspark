package org.zezutom.learningspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;

/**
 * A simple app demo.
 *
 * How to Run:
 *
 * $SPARK_HOME/bin/spark-submit \
 * --class "org.zezutom.learningspark.SimpleApp" \
 * --master local[4] \
 * target/learningspark-1.0-SNAPSHOT.jar README.md
 *
 */
public class SimpleApp {

    public static void main(String[] args) {
        if (args.length == 0)
            throw new IllegalArgumentException("Please provide a file to parse");

        String logFile = args[0];

        if (!new File(logFile).exists())
            throw new IllegalArgumentException("No such file: " + logFile);

        SparkConf conf = new SparkConf().setAppName("Simple App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile, 2).cache();

        long numAs = logData.filter(line -> line.contains("a")).count();
        long numBs = logData.filter(line -> line.contains("b")).count();

        System.out.println(String.format("Lines with a: %s, Lines with b: %s", numAs, numBs));
    }
}
