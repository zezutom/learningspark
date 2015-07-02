package org.zezutom.learningspark

import org.apache.spark.{SparkContext, SparkConf}

/**
 * A simple app demo.
 *
 * Note that applications should define a main() method instead of extending scala.App. Subclasses of scala.App may not work correctly.
 */
object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile = args{0}
    val conf = new SparkConf().setAppName("Simple App")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %b".format(numAs, numBs))
  }
}
