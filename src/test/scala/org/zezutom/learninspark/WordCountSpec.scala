package org.zezutom.learninspark

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, GivenWhenThen, FlatSpec}
import org.zezutom.learningspark.scala.WordCount

// https://github.com/mkuthan/example-spark/blob/master/src/test/scala/example/SparkExampleSpec.scala
@RunWith(classOf[JUnitRunner])
class WordCountSpec extends FlatSpec with SparkSpec with GivenWhenThen with Matchers {

  "Empty set" should "be counted" in {
    Given("empty set")
    val lines = Array("")

    When("count words")
    val wordCounts = WordCount.wc(sc.parallelize(lines)).collect()

    Then("empty count")
    wordCounts shouldBe empty
  }
}
