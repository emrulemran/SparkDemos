package com.javaspark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CSVDataFrame {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("CSVDataFrame").master("local[*]").getOrCreate();
        String dataPath = "C:\\Users\\Emran\\IdeaProjects\\JavaSpark\\data\\salesdata2009.csv";
        Dataset<Row> df = spark
                .read()
                .format("org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2")
                .option("header", true)
                .option("inferSchema", true)
                .load(dataPath);

        // if 2 different libraries installed to handle CSV data, we have to use: .format("org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2")
        // not: .format(""csv)
        df.show();
    }
}
