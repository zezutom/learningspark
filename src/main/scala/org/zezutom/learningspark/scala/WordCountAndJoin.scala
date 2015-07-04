package org.zezutom.learningspark.scala

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountAndJoin(val sc:SparkContext) {

  def wc_join(keyword: String, file1: String, file2: String): Array[(String, (Int, Int))] = {
    val wc_out1 = wc(keyword, file1)
    val wc_out2 = wc(keyword, file2)
    wc_out1.join(wc_out2).collect()
  }

  private def wc(keyword: String, file: String): RDD[(String, Int)] = {
    if (!new File(file).exists())
      throw new IllegalArgumentException(s"No such file: $file")
    sc.textFile(file)
      .flatMap(line => line.split(" "))
      .filter(_.equals(keyword))
      .map(word => (word, 1)).reduceByKey(_ + _)
  }
}
