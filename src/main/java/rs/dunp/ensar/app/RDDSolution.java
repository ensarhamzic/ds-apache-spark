package rs.dunp.ensar.app;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        var sparkConf = new SparkConf().setAppName("DistribuiraniSistemi").setMaster("local[*]");
        var sparkContext = new SparkContext(sparkConf);

        JavaRDD<String> inputFile = sparkContext.textFile("C:\\spark-3.5.0-bin-hadoop3\\kupovina.csv", 1).toJavaRDD();

        JavaPairRDD<String, Double> purchases = inputFile.mapToPair(line -> {
            String[] parts = line.split(",");
            String customerId = parts[0];
            double price = Double.parseDouble(parts[2]);
            return new Tuple2<>(customerId, price);
        });

        JavaPairRDD<String, Double> totalSpentByCustomer = purchases.reduceByKey(Double::sum);

        totalSpentByCustomer.collect().forEach(System.out::println);
    }
}