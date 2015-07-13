package org.zezutom.learningspark.scala

import org.apache.spark.{SparkContext, SparkConf}

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
 * --class "org.zezutom.learningspark.scala.AccumApp" \
 * --master local[4] \
 * target/learningspark-1.0-SNAPSHOT.jar
 *
 */
object AccumApp {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Accumulator App")
    val sc = new SparkContext(conf)
    val accum = sc.accumulator(0, "My Accumulator")
    println(Accum.sum(sc.parallelize(Array(1, 2, 3, 4)), accum))
  }
}
