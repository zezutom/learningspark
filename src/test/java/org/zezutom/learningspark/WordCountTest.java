package org.zezutom.learningspark;

import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WordCountTest extends SparkTest {

    @Test
    public void empty_set_should_be_counted() {
        // Given "empty set"
        List<String> lines = Collections.emptyList();

        // When "count words"
        List wordCounts = WordCount.wc(sc.parallelize(lines)).collect();

        // Then "empty count"
        assertTrue(wordCounts.isEmpty());
    }

    @Test
    public void robert_frosts_famous_quote_should_be_counted() {
        // Given "quote"
        List<String> lines = Arrays.asList(
                "Two roads diverged in a wood, and I,",
                "I took the one less traveled by",
                "And that has made all the difference.");

        // When "count words"
        List wordCounts = WordCount.wc(sc.parallelize(lines)).collect();

        // Then "words counted"
        assertEquals(Arrays.asList(
                tuple("a", 1),
                tuple("all", 1),
                tuple("and", 2),
                tuple("by", 1),
                tuple("difference", 1),
                tuple("diverged", 1),
                tuple("has", 1),
                tuple("i", 2),
                tuple("in", 1),
                tuple("less", 1),
                tuple("made", 1),
                tuple("one", 1),
                tuple("roads", 1),
                tuple("that", 1),
                tuple("the", 2),
                tuple("took", 1),
                tuple("traveled", 1),
                tuple("two", 1),
                tuple("wood", 1)
        ), wordCounts);
    }

    private Tuple2 tuple(String word, int count) {
        return new Tuple2<>(word, count);
    }
}
