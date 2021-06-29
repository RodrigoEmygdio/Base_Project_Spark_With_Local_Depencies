package org.example;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.List;

public class TestSpark {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> javaRDD = sc.parallelize(integers);
        Integer reduce = javaRDD.reduce((a, b) -> a + b);
        System.out.println("Result is: " + reduce);
    }
}
