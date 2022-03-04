package com.example.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;

import com.example.dataloader.WuzzufJobsCsv;
import com.example.dataloader.dao.DataLoaderDAO;
import com.example.utils.UDFUtils;
import static com.example.utils.Constants.COLUMN_PARSE_YEARS_OF_EXP_UDF_NAME;
import static com.example.utils.Constants.COLUMN_PARSE_MAX_UDF_NAME;
import static com.example.utils.Constants.COLUMN_PARSE_MIN_UDF_NAME;

public class WuzzufJobsAnalysis {
    Dataset<Row> wuzzufData;
    SparkSession spark;
    UDFUtils udfUtil;

    public WuzzufJobsAnalysis() {
        spark = SparkSession.builder().getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        udfUtil = new UDFUtils(spark.sqlContext());
        udfUtil.registerColumnProcessYearsExpUdf();
        udfUtil.registerColumnMaxExpUdf();
        udfUtil.registerColumnMinExpUdf();
    }

    public void readData() {
        DataLoaderDAO loader = new WuzzufJobsCsv();
        wuzzufData = loader.load("src/main/resources/Wuzzuf_Jobs.csv");
        wuzzufData = createTempView(wuzzufData);
        wuzzufData = encodeCategories(wuzzufData);
        wuzzufData = factoriesYearsOfExp(wuzzufData);
        System.out.println("+++++========++++++++Done");
        wuzzufData.printSchema();
        System.out.println("+++++========++++++++Done");
        wuzzufData.show();
        System.out.println("+++++========++++++++Done");
    }

    public Dataset<Row> createTempView(Dataset<Row> df) {
        df.createOrReplaceTempView("WUZZUF_DATA");

        Dataset<Row> typedDataset = spark.sql("SELECT "
                + "cast (Title as string) Title, "
                + "cast (Company as string) Company, "
                + "cast (Location as string) Location, "
                + "cast (Type as string) Type, "
                + "cast (Level as string) Level, "
                + "cast (YearsExp as string) YearsExp, "
                + "cast (Country as string) Country, "
                + "cast (Skills as string) Skills FROM WUZZUF_DATA");
        return typedDataset;
    }

    public Dataset<Row> encodeCategories(Dataset<Row> df) {
        StringIndexer typeIndexer = new StringIndexer().setInputCol("Type").setOutputCol("type-factorized")
                .setHandleInvalid("keep");
        df = typeIndexer.fit(df).transform(df);
        StringIndexer levelIndexer = new StringIndexer().setInputCol("Level").setOutputCol("level-factorized")
                .setHandleInvalid("keep");
        df = levelIndexer.fit(df).transform(df);
        df = df.drop("Type", "Level");

        return df;
    }

    public Dataset<Row> factoriesYearsOfExp(Dataset<Row> df) {
        df = df.withColumn("MinMaxYearsExp",
                callUDF(COLUMN_PARSE_YEARS_OF_EXP_UDF_NAME, col("YearsExp")));
        df = df.withColumn("MaxYearsExp",
                callUDF(COLUMN_PARSE_MAX_UDF_NAME, col("MinMaxYearsExp")));
        df = df.withColumn("MinYearsExp",
                callUDF(COLUMN_PARSE_MIN_UDF_NAME, col("MinMaxYearsExp")));
        df = df.drop("MinMaxYearsExp", "YearsExp");

        return df;
    }
}
