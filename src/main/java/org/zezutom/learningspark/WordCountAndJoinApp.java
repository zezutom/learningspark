package org.zezutom.learningspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;

/**
 * Using the README.md and CHANGES.txt files (see: src/main/resources):
 *
 * #1 Create RDDs to filter each file for the keyword "Spark"
 *
 * #2 Perform a WordCount on each, i.e. so the results are (K, V) pairs of (word, count)
 *
 * #3 Join the two RDDs
 *
 * How to Run:
 *
 * $SPARK_HOME/bin/spark-submit \
 * --class "org.zezutom.learningspark.WordCountAndJoinApp" \
 * --master local[4] \
 * target/learningspark-1.0-SNAPSHOT.jar
 *
 */
public class WordCountAndJoinApp {

    private static final SparkConf conf = new SparkConf().setAppName("Word Count and Join");
    private static final JavaSparkContext sc = new JavaSparkContext(conf);

    public static void main(String[] args) {

        List<Tuple2<String, Tuple2<Integer, Integer>>> wc_join_out =
                WordCountAndJoin.wc_join("Spark", readFile("README.md"), readFile("CHANGES.txt"));

        StringBuilder sb = new StringBuilder();
        wc_join_out.stream().forEach(
                t -> sb.append(t._1())
                        .append(" (")
                        .append(t._2()._1())
                        .append(",")
                        .append(t._2()._2())
                        .append(")"));
        System.out.println(sb);
    }

    private static JavaRDD<String> readFile(String file) {
        String resource = Paths.get("src", "main", "resources", file).toAbsolutePath().toString();
        if (!new File(resource).exists())
            throw new IllegalArgumentException("No such file: " + file);
        return sc.textFile(resource);
    }
}
