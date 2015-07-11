package org.zezutom.learninspark

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.zezutom.learningspark.scala.WordCountAndJoin

@RunWith(classOf[JUnitRunner])
class WordCountAndJoinSpec extends FlatSpec with SparkSpec with GivenWhenThen with Matchers {

  "The word \"Spark\" " should "be counted" in {
    Given("About Apache Spark")
    // https://spark.apache.org/docs/latest/index.html
    val lines1 = Array(
      "Apache Spark is a fast and general-purpose cluster computing system.",
      "It also supports a rich set of higher-level tools including Spark SQL and Spark Streaming."
    )

    Given("About spark as an expression")
    // https://en.wikipedia.org/wiki/Spark
    val lines2 = Array(
      "Spark (fire), a small glowing particle or ember.",
      "In pyrotechnics, iron filings and metal alloys such as magnalium may be used to create sparks.",
      "Scorched linen was commonly used as tinder to catch the spark and start ",
      "the fire but producing a good spark could take much time."
    )

    When("count word \"Spark\"")
    val wordCounts = WordCountAndJoin.wc_join("Spark", sc.parallelize(lines1), sc.parallelize(lines2))

    Then("occurrences counted")
    wordCounts should equal(
      Array(("spark",(3,3)))
    )
  }
}
