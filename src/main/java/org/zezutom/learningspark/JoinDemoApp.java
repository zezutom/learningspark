package org.zezutom.learningspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Shows how to join two different data sources.
 * <p/>
 * How to Run:
 * <p/>
 * $SPARK_HOME/bin/spark-submit \
 * --class "org.zezutom.learningspark.JoinDemoApp" \
 * --master local[4] \
 * target/learningspark-1.0-SNAPSHOT.jar
 */
public class JoinDemoApp {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Simple App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        JavaPairRDD<String, Register> reg = sc.textFile("src/main/resources/reg.tsv")
                .map(s -> s.split("\t")).mapToPair(s -> new Tuple2<>(s[1], new Register(format.parse(s[0]), s[1], s[2], Float.parseFloat(s[3].trim()), Float.parseFloat(s[4].trim()))));
        JavaPairRDD<String, Click> clk = sc.textFile("src/main/resources/clk.tsv")
                .map(s -> s.split("\t")).mapToPair(s -> new Tuple2<>(s[1], new Click(format.parse(s[0]), s[1], Integer.parseInt(s[2].trim()))));

        System.out.println(reg.join(clk).toDebugString());
    }

    static class Register {
        Date date;
        String uuid;
        String custId;
        Float lat;
        Float lng;

        Register(Date date, String uuid, String custId, Float lat, Float lng) {
            this.date = date;
            this.uuid = uuid;
            this.custId = custId;
            this.lat = lat;
            this.lng = lng;
        }
    }

    static class Click {
        Date date;
        String uuid;
        Integer landingPage;

        Click(Date date, String uuid, Integer landingPage) {
            this.date = date;
            this.uuid = uuid;
            this.landingPage = landingPage;
        }
    }
}
