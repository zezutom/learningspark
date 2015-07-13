package org.zezutom.learningspark.scala

import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

object Accum {
  def sum(nums: RDD[Int], accum: Accumulator[Int]): Int = {
    nums.foreach(x => accum += x)
    accum.value
  }
}
