package org.zezutom.learningspark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Demonstrates the use of an Accumulator variable.
 *
 * Docs:
 * An accumulator is created from an initial value 'v' by calling SparkContext.accumulator(v).
 * Tasks running on the cluster can then add to it using the add method (Java) or the += operator
 * (Scala and Python). However, they cannot read its value. Only the driver program can read
 * the accumulator’s value, using its value method.
 *
 * How to Run:
 *
 * $SPARK_HOME/bin/spark-submit \
 * --class "org.zezutom.learningspark.AccumApp" \
 * --master local[4] \
 * target/learningspark-1.0-SNAPSHOT.jar
 *
 */
public class AccumApp {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCount App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Accumulator<Integer> accum = sc.accumulator(0, "My Accumulator");
        System.out.println(Accum.sum(sc.parallelize(Arrays.asList(1, 2, 3, 4)), accum));
    }
}
