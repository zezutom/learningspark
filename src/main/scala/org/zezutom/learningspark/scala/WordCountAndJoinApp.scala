package org.zezutom.learningspark.scala

import org.apache.spark.{SparkContext, SparkConf}

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
 * --class "org.zezutom.learningspark.scala.WordCountAndJoinApp" \
 * --master local[4] \
 * target/learningspark-1.0-SNAPSHOT.jar
 *
 */
object WordCountAndJoinApp {

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount and Join")
    val sc = new SparkContext(conf)

    val wc_join_out = new WordCountAndJoin(sc)
      .wc_join("Spark", "src/main/resources/README.md", "src/main/resources/CHANGES.txt")

    println(wc_join_out.mkString("\n"))
  }

}
