package org.zezutom.learningspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class SparkTest {

    protected static JavaSparkContext sc;

    @BeforeClass
    public static void setUp() {
        SparkConf conf = new SparkConf()
                .setMaster("local[4]").set("spark.driver.host", "localhost")
                .setAppName(SparkTest.class.getSimpleName());
        sc = new JavaSparkContext(conf);
    }

    @AfterClass
    public static void tearDown() {
        if (sc != null) {
            sc.stop();
            sc = null;
        }
    }
}
