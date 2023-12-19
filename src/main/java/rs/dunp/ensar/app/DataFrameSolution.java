package rs.dunp.ensar.app;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Objects;

public class DataFrameSolution {
    public static void main(String[] args) {
        var sparkConf = new SparkConf().setAppName("DistribuiraniSistemi").setMaster("local[*]");
        var spark = SparkSession.builder().config(sparkConf).getOrCreate();

        String path = Objects.requireNonNull(DataFrameSolution.class.getResource("/kupovina.csv")).getPath();

        Dataset<Row> df = spark.read().option("header", "false").csv(path);

        df = df.withColumnRenamed("_c0", "customerId")
            .withColumnRenamed("_c1", "productId")
            .withColumnRenamed("_c2", "price");

        df = df.withColumn("price", df.col("price").cast("double"));

        Dataset<Row> totalSpentByCustomer = df.groupBy("customerId").agg(functions.sum("price").alias("totalSpent"));

        totalSpentByCustomer.show(1000);
    }
}
