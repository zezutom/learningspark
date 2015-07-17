package org.zezutom.learningspark;

import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCountAndJoinTest extends SparkTest {

    @Test
    public void the_word_Spark_should_be_counted() {
        // Given "About Apache Spark"
        List<String> lines1 = Arrays.asList(
            "Apache Spark is a fast and general-purpose cluster computing system.",
            "It also supports a rich set of higher-level tools including Spark SQL and Spark Streaming."
        );

        // Given "About spark as an expression"
        List<String> lines2 = Arrays.asList(
                "Spark (fire), a small glowing particle or ember.",
                "In pyrotechnics, iron filings and metal alloys such as magnalium may be used to create sparks.",
                "Scorched linen was commonly used as tinder to catch the spark and start ",
                "the fire but producing a good spark could take much time."
        );

        // When "count word \"Spark\""
        List wordCounts = WordCountAndJoin.wc_join("Spark", sc.parallelize(lines1), sc.parallelize(lines2));

        // Then "occurrences counted"
        Assert.assertEquals(Arrays.asList(new Tuple2<>("spark", new Tuple2<>(3, 3))), wordCounts);
    }
}
