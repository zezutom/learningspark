package org.zezutom.learningspark.scala

import java.io.File

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

    val file = args(0)

    if (!new File(file).exists())
      throw new IllegalArgumentException(s"No such file: $file")

    val conf = new SparkConf().setAppName("WordCount App")
    val sc = new SparkContext(conf)

    WordCount.wc(sc.textFile(file, 2).cache()).saveAsTextFile("wc_out.txt")
  }

}
