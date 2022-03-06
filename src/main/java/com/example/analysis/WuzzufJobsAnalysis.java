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

import org.apache.spark.sql.*;
import org.apache.spark.ml.feature.StringIndexer;

import static java.util.stream.Collectors.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;

import com.example.dataloader.WuzzufJobsCsv;
import com.example.dataloader.dao.DataLoaderDAO;
import com.example.utils.UDFUtils;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;
import org.apache.spark.sql.Row;

import java.util.*;
import java.util.function.Function;

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
        // clean Data.
        cleanData();
        this.jobsByCompany();

        // Count the jobs for each company and display that in order


        Dataset<Row> MostTitles =MostPopularTitles(wuzzufData);
        MostTitles.show();
        JobTitlesBarGraph(wuzzufData);
        System.out.println("+++++========++++++++Done");
        Dataset<Row> MostAreas = MostPopularAreas(wuzzufData);
        MostAreas.show();
        AreasCountBarGraph(wuzzufData);
        System.out.println("+++++========++++++++Done");
        mostPopularSkills(wuzzufData);

        //Map skills_map = mostPopularSkills(wuzzufData);

        // this is a change in intellij
        //Dataset<Row> ww= mostPopularSkills(wuzzufData);
        //ww.show();
        //JobSkillsBarGraph (wuzzufData);

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

    public void JobSkillsBarGraph(Dataset<Row> df)
    {

        Dataset<Row> MostTitles_df = (Dataset<Row>) mostPopularSkills( df);
        DrawBarChart(MostTitles_df,"Title","count","Most Popular Title","Titles","Count","Titles's Count");

    }

    public void AreasCountBarGraph(Dataset<Row> df)
    {
        Dataset<Row> MostAreas_df= MostPopularAreas( df);
        DrawBarChart(MostAreas_df,"Location","count","Most Popular Areas","Areas","Count","Area's Count");

    }

    public Map<String, Integer> mostPopularSkills(Dataset<Row> df) {

        List<Row> skillSet = df.collectAsList();
        List<String> allSkils = new ArrayList<String>();
        String skill;
        for (Row row : skillSet) {
            skill = row.get(4).toString();
            String[] subs = skill.split(",");
            for (String word : subs) {
                allSkils.add(word);
            }
        }
        Map<String, Integer> mapAllSkills =
                allSkils.stream().collect(groupingBy(Function.identity(), summingInt(e -> 1)));
        //Sort the map descending
        Map<String, Integer> sorted_skillset = mapAllSkills
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(100)
                .collect(
                        toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                                LinkedHashMap::new));
        int idx=0;

        System.out.println("=============== Most Repaeated Skills ==============");
        for (Map.Entry<String, Integer> entry : sorted_skillset.entrySet()) {
            System.out.println(entry.getKey()+" : "+entry.getValue());
            if (idx==30)
            {
                break;
            }
            idx++;
        }
        return (sorted_skillset);
    }



}
