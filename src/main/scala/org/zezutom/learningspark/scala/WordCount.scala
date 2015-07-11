package org.zezutom.learningspark.scala

import org.apache.spark.rdd.RDD

object WordCount {
  def wc(lines:RDD[String]): RDD[(String, Int)] = {
    lines.flatMap(_.split("\\s"))                             // Split on any white character
      .filter(!_.isEmpty)                                     // Only count words as non-empty strings
      .map(_.stripSuffix(",").stripSuffix(".").toLowerCase)   // Remove punctuation and make all words lowercase
      .map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._1)
  }
}
