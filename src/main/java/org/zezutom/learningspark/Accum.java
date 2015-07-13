package org.zezutom.learningspark;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;

public class Accum {

    public static int sum(JavaRDD<Integer> nums, Accumulator<Integer> accum) {
        nums.foreach(x -> accum.add(x));
        return accum.value();
    }
}
