package org.zezutom.learningspark.scala

import java.io.File
import java.nio.file.Paths

import org.apache.spark.rdd.RDD
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

  val conf = new SparkConf().setAppName("WordCount and Join")
  val sc = new SparkContext(conf)

  def main (args: Array[String]) {

    val wc_join_out = WordCountAndJoin
      .wc_join("Spark", readFile("README.md"), readFile("CHANGES.txt"))

    println(wc_join_out.mkString("\n"))
  }

  private def readFile(file:String): RDD[String] = {
    val resource = Paths.get("src", "main", "resources", file).toAbsolutePath.toString
    if (!new File(resource).exists())
      throw new IllegalArgumentException(s"No such file: $file")
    sc.textFile(resource)
  }

}
