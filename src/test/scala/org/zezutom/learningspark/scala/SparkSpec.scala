package org.zezutom.learningspark.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSpec extends BeforeAndAfterAll {
  this: Suite =>

  private var _sc: SparkContext = _

  def sc = _sc

  val conf: SparkConf = new SparkConf()
    .setMaster("local[4]").set("spark.driver.host", "localhost")
    .setAppName(getClass.getSimpleName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    _sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }
    super.afterAll()
  }

}
