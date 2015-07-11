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

  "Robert Frost's famous quote" should "be counted" in {
    Given("quote")
    val lines = Array(
      "Two roads diverged in a wood, and I,",
      "I took the one less traveled by",
      "And that has made all the difference.")

    When("count words")
    val wordCounts = WordCount.wc(sc.parallelize(lines)).collect()

    Then("words counted")
    wordCounts should equal(Array(
      ("a", 1), ("all", 1), ("and", 2), ("by", 1), ("difference", 1), ("diverged", 1),
      ("has", 1), ("i", 2), ("in", 1), ("less", 1), ("made", 1), ("one", 1),
      ("roads", 1), ("that", 1), ("the" ,2), ("took" ,1), ("traveled" ,1), ("two", 1), ("wood", 1)))
  }
}
