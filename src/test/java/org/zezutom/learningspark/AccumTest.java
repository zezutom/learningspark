package org.zezutom.learningspark;

import org.apache.spark.Accumulator;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class AccumTest extends SparkTest {

    private final Accumulator<Integer> accum = sc.accumulator(0, "Test Accumulator");

    @Test
    public void empty_set_should_yield_zero() {
        // Given "empty set"
        List<Integer> nums = Collections.emptyList();

        // When "sum up numbers"
        int sum = Accum.sum(sc.parallelize(nums), accum);

        // Then "sum of zero"
        assertTrue(sum == 0);
    }

    @Test
    public void numeric_sequence_should_be_summed_up() {
        // Given "sequence of 1 to 4"
        List<Integer> nums = Arrays.asList(1, 2, 3, 4);

        // When "sum up numbers"
        int sum = Accum.sum(sc.parallelize(nums), accum);

        // Then "sum of ten"
        assertTrue(sum == 10);
    }


}
