package org.zezutom.learningspark.scala

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

class WordCount(val sc:SparkContext) {

  def wc(file: String): RDD[(String, Int)] = {
    if (!new File(file).exists())
      throw new IllegalArgumentException(s"No such file: $file")

    wc(sc.textFile(file, 2).cache())
  }

  def wc(lines:RDD[String]): RDD[(String, Int)] = {
    lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
  }
}
