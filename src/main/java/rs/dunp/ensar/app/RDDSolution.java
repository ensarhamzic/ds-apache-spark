package rs.dunp.ensar.app;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;

public class RDDSolution {
    public static void main(String[] args) {
        var sparkConf = new SparkConf().setAppName("DistribuiraniSistemi").setMaster("local[*]");
        var sparkContext = new JavaSparkContext(sparkConf);

        String path = Objects.requireNonNull(RDDSolution.class.getResource("/kupovina.csv")).getPath();

        JavaRDD<String> inputFile = sparkContext.textFile(path);

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