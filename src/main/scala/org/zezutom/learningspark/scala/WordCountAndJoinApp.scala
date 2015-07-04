package org.zezutom.learningspark.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Counts the number of words in the given file and saves the result into an output directory.
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
    val conf = new SparkConf().setAppName("Word Count and Join")
    val sc = new SparkContext(conf)

    val wc_join_out = new WordCountAndJoin(sc)
      .wc_join("Spark", "src/main/resources/README.md", "src/main/resources/CHANGES.txt")

    println(wc_join_out.mkString("\n"))

  }

}
