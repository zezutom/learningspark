package org.zezutom.learningspark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Counts the number of words in the given file and saves the result into an output directory.
 *
 * How to Run:
 *
 * $SPARK_HOME/bin/spark-submit \
 * --class "org.zezutom.learningspark.scala.WordCountApp" \
 * --master local[4] \
 * target/learningspark-1.0-SNAPSHOT.jar src/main/resources/README.md
 *
 */
object WordCountApp {

  def main (args: Array[String]) {

    if (args.length == 0)
      throw new IllegalArgumentException("Please provide a file to parse")

    val conf = new SparkConf().setAppName("WordCount App")
    val sc = new SparkContext(conf)

    new WordCount(sc).wc(args(0)).saveAsTextFile("wc_out.txt")
  }

}
