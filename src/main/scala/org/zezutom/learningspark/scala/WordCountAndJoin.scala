package org.zezutom.learningspark.scala

import org.apache.spark.rdd.RDD

object WordCountAndJoin {

  def wc_join(keyword: String, rdd1: RDD[String], rdd2: RDD[String]): Array[(String, (Int, Int))] = {
    val wc_out1 = wc(keyword, rdd1)
    val wc_out2 = wc(keyword, rdd2)
    wc_out1.join(wc_out2).collect()
  }

  private def wc(keyword: String, rdd: RDD[String]): RDD[(String, Int)] =
    rdd
      .flatMap(line => line.split("\\s"))                     // Split on any white character
      .map(_.stripSuffix(",").stripSuffix(".").toLowerCase)   // Remove punctuation and make all words lowercase
      .filter(_.equals(keyword.toLowerCase))
      .map(word => (word, 1)).reduceByKey(_ + _)
  
}
