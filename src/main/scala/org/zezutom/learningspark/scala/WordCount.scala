package org.zezutom.learningspark.scala

import org.apache.spark.rdd.RDD

object WordCount {
  def wc(lines:RDD[String]): RDD[(String, Int)] = {
    lines.flatMap(line => line.split(" ")).filter(!_.isEmpty).map(word => (word, 1)).reduceByKey(_ + _)
  }
}
