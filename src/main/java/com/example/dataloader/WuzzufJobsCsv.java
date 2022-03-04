package com.example.dataloader;

import com.example.dataloader.dao.DataLoaderDAO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * WuzzufJobsCsv
 */
public class WuzzufJobsCsv implements DataLoaderDAO {

    public Dataset<Row> load(String filename) {
        SparkSession spark = SparkSession.builder()
                .getOrCreate();
        // spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> dataset = spark.read()
                .option("header", "true")
                .csv(filename);
        dataset.show(5);
        System.out.println("+++++========++++++++Done");
        // dataset.createOrReplaceTempView("WUZZUF_DATA");
        // Dataset<Row> typedDataset = spark.sql("SELECT "
        //         + "cast (Title as string) Title, "
        //         + "cast (Company as string) Company, "
        //         + "cast (Location as string) Location, "
        //         + "cast (Type as string) Type, "
        //         + "cast (Level as string) Level, "
        //         + "cast (YearsExp as string) YearsExp, "
        //         + "cast (Country as string) Country, "
        //         + "cast (Skills as string) Skills FROM WUZZUF_DATA");
        // return typedDataset;
        return null;
    }
}