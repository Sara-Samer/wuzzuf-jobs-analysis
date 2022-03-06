package com.example.analysis;

import org.apache.spark.sql.*;
import org.apache.spark.ml.feature.StringIndexer;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;

import com.example.dataloader.WuzzufJobsCsv;
import com.example.dataloader.dao.DataLoaderDAO;
import com.example.utils.UDFUtils;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;

import java.util.ArrayList;
import java.util.List;

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
        wuzzufData.printSchema();
        System.out.println("+++++========++++++++Done");
        System.out.println("+++++========++++++++Done");
        wuzzufData.show();
        System.out.println("+++++========++++++++Done");
        wuzzufData.describe("type-factorized", "level-factorized", "MaxYearsExp", "MinYearsExp").show();
        System.out.println("+++++========++++++++Done");

        Dataset<Row> MostTitles =MostPopularTitles(wuzzufData);
        MostTitles.show();
        JobTitlesBarGraph(wuzzufData);
        System.out.println("+++++========++++++++Done");
        Dataset<Row> MostAreas = MostPopularAreas(wuzzufData);
        MostAreas.show();
        AreasCountBarGraph(wuzzufData);
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

    public Dataset<Row> FindMostPopular(Dataset<Row> df,String ColName)
    {
        return df.groupBy(ColName).count().sort(col("count").desc());
    }


    public Dataset<Row> MostPopularTitles(Dataset<Row> df)
    {
        return FindMostPopular(df,"Title");
    }

    public Dataset<Row> MostPopularAreas(Dataset<Row> df)
    {
        return FindMostPopular(df,"Location");
    }


    public void DrawBarChart(Dataset<Row> df,String Xcol,String Ycol,String titleName, String xAxisTitle,String yAxisTitle,String SeriesName)
    {
       Dataset<Row>  Popular_df = df.limit(5);
       List<String> Col_Selection = Popular_df.select(Xcol).as(Encoders.STRING()).collectAsList() ;
       List<Long> counts = Popular_df .select(Ycol).as(Encoders.LONG()).collectAsList();

       CategoryChart chart = new CategoryChartBuilder().width (900).height (600).title (titleName).xAxisTitle (xAxisTitle).yAxisTitle (yAxisTitle).build ();
       chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideN);
       chart.getStyler ().setHasAnnotations (true);
       chart.getStyler ().setStacked (true);

       chart.addSeries (SeriesName, Col_Selection, counts);

        new SwingWrapper(chart).displayChart ();
    }

    public void JobTitlesBarGraph(Dataset<Row> df)
    {
        Dataset<Row> MostTitles_df= MostPopularTitles( df);
        DrawBarChart(MostTitles_df,"Title","count","Most Popular Title","Titles","Count","Titles's Count");

    }

    public void AreasCountBarGraph(Dataset<Row> df)
    {
        Dataset<Row> MostAreas_df= MostPopularAreas( df);
        DrawBarChart(MostAreas_df,"Location","count","Most Popular Areas","Areas","Count","Area's Count");

    }


}
