package com.example.analysis;

import com.example.dataloader.WuzzufJobsCsv;
import com.example.dataloader.dao.DataLoaderDAO;
import com.example.utils.UDFUtils;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static com.example.utils.Constants.*;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

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
        wuzzufData.printSchema();
        System.out.println("+++++========++++++++Done");
        System.out.println("+++++========++++++++Done");
        wuzzufData.show();
        System.out.println("+++++========++++++++Done");
        wuzzufData.describe("type-factorized", "level-factorized", "MaxYearsExp", "MinYearsExp").show();
        System.out.println("+++++========++++++++Done");
        // clean Data.
        //cleanData();
        this.jobsByCompany();

        // Count the jobs for each company and display that in order

    }
    //clean data
    private void cleanData() {
        // Remove Duplicates:
        wuzzufData = wuzzufData.dropDuplicates();
        // Remove Nulls from YearsOfExp column
        String sql = "Select * FROM WuzzufJobs WHERE YearsExp != \"null Yrs of Exp\"";
        wuzzufData.createOrReplaceTempView ("WuzzufJobs");
        wuzzufData = spark.sql(sql);
    }
    // return some of first rows from data ---- it need number of line that you want to display.
    public Dataset<Row> getHeadData(int number){
        // Create view and execute query to display first n rows of WuzzufJobs data:
        this.wuzzufData.createOrReplaceTempView ("WuzzufJobs");
        return this.spark.sql("SELECT * FROM WuzzufJobs LIMIT "+ number+";");
    }
    public String jobsByCompany(){
        Dataset<Row> company = wuzzufData.groupBy("Company").count().orderBy(col("count").desc()).limit(20);
        company.printSchema();
        //System.exit(1);
        PieCharts.popularCompanies(company);
        List<Row> top_Companies = company.collectAsList();
        return null;
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
