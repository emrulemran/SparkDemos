package com.javaspark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaSparkJDBC {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("JDBCDataFrame")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark
                .read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/world")  // .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "country")
                .option("user", "root")
                .option("password", "pwd")     //  .option("serverTimezone", "EST")
                .load();
        ((Dataset<Row>) df).show(10);
    }
}




/*
<!-- https://mvnrepository.com/artifact/com.mysql/mysql-connector-j -->
<dependency>
    <groupId>com.mysql</groupId>
    <artifactId>mysql-connector-j</artifactId>
    <version>8.2.0</version>
</dependency>

*/
