package org.zezutom.learningspark.scala.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object NetworkWordCount {
  def wc(lines:ReceiverInputDStream[String]): DStream[(String, Int)] = {
    lines.flatMap(_.split("\\s"))                             // Split on any white character
      .filter(!_.isEmpty)                                     // Only count words as non-empty strings
      .map(_.stripSuffix(",").stripSuffix(".").toLowerCase)   // Count each word in each batch, remove punctuation and make all words lowercase
      .map(word => (word, 1)).reduceByKey(_ + _)
  }
}
