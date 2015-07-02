package org.zezutom.learningspark

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

/**
 * A simple app demo.
 *
 * Note that applications should define a main() method instead of extending scala.App.
 * Subclasses of scala.App may not work correctly.
 *
 * How to Run:
 *
 * $SPARK_HOME/bin/spark-submit \
 * --class "org.zezutom.learningspark.SimpleApp" \
 * --master local[4] \
 * target/learningspark-1.0-SNAPSHOT.jar README.md
 * 
 */
object SimpleApp {
  def main(args: Array[String]): Unit = {
    if (args.length == 0)
      throw new IllegalArgumentException("Please provide a file to parse")

    val logFile = args(0)

    if (!new File(logFile).exists())
      throw new IllegalArgumentException(s"No such file: $logFile")

    val conf = new SparkConf().setAppName("Simple App")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()

    println("Lines with a: %s, Lines with b: %b".format(numAs, numBs))
  }
}
