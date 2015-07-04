package org.zezutom.learningspark.scala

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Shows how to join two different data sources.
 *
 * How to Run:
 *
 * $SPARK_HOME/bin/spark-submit \
 * --class "org.zezutom.learningspark.scala.JoinDemoApp" \
 * --master local[4] \
 * target/learningspark-1.0-SNAPSHOT.jar
 *
 */
object JoinDemoApp {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Join Demo")
    val sc = new SparkContext(conf)
    val format = new SimpleDateFormat("yyyy-MM-dd")

    case class Register(d: Date, uuid: String, custId: String, lat: Float, lng: Float)
    case class Click(d: Date, uuid: String, landingPage: Int)

    val reg = sc.textFile("src/main/resources/reg.tsv").map(_.split("\t")).map(
      r => (r(1), Register(format.parse(r(0)), r(1), r(2), r(3).toFloat, r(4).toFloat))
    )

    val clk = sc.textFile("src/main/resources/clk.tsv").map(_.split("\t")).map(
      c => (c(1), Click(format.parse(c(0)), c(1), c(2).trim.toInt))
    )

    println(reg.join(clk).toDebugString/*.saveAsTextFile("join_out.txt")*/)
  }

}
