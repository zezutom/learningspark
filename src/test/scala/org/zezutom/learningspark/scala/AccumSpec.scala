package org.zezutom.learningspark.scala

import org.apache.spark.Accumulator
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, FlatSpec, GivenWhenThen}

// https://github.com/mkuthan/example-spark/blob/master/src/test/scala/example/SparkExampleSpec.scala
@RunWith(classOf[JUnitRunner])
class AccumSpec extends FlatSpec with SparkSpec with GivenWhenThen with Matchers {

  "Empty set" should "yield zero" in {
    Given("empty set")
    val nums:Array[Int] = Array()

    When("sum up numbers")
    val accum: Accumulator[Int] = sc.accumulator(0, "Test Accumulator")
    val sum = Accum.sum(sc.parallelize(nums), accum)

    Then("sum of zero")
    sum should equal(0)
  }

  "Numeric sequence" should "be summed up" in {
    Given("sequence of 1 to 4")
    val nums = Array(1, 2, 3, 4)

    When("sum up numbers")
    val accum: Accumulator[Int] = sc.accumulator(0, "Test Accumulator")
    val sum = Accum.sum(sc.parallelize(nums), accum)

    Then("sum of ten")
    sum should equal(10)
  }
}
