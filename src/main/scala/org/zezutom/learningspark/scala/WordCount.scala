package org.zezutom.learningspark.scala

import java.io.File

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Counts the number of words in the given file and saves the result into an output directory.
 *
 * How to Run:
 *
 * $SPARK_HOME/bin/spark-submit \
 * --class "org.zezutom.learningspark.scala.WordCount" \
 * --master local[4] \
 * target/learningspark-1.0-SNAPSHOT.jar README.md
 *
 */
object WordCount {

  def main (args: Array[String]) {

    if (args.length == 0)
      throw new IllegalArgumentException("Please provide a file to parse")

    val file = args(0)

    if (!new File(file).exists())
      throw new IllegalArgumentException(s"No such file: $file")

    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    val data = sc.textFile(file, 2)
    val wc = data.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    wc.saveAsTextFile("wc_out.txt")
  }

}
