package com.example.main;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.Column;

// import com.example.dataloader.WuzzufJobsCsv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
// import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;

// import org.springframework.boot.SpringApplication;
// import org.springframework.boot.autoconfigure.SpringBootApplication;
// import org.apache.log4j.Level;
// import org.apache.log4j.LogManager;
// import org.apache.log4j.Logger;
// import org.apache.log4j.BasicConfigurator;  

// @SpringBootApplication
public class WuzzufJobsAnalysisApplication {

	// private static final Logger logger =
	// LogManager.getLogger(WuzzufJobsAnalysisApplication.class);
	public static void main(String[] args) {
		// Logger.getLogger(SparkSession.class).setLevel(Level.OFF);
		// BasicConfigurator.configure();
		// logger.info("Hello world");
		// logger.error("we are in logger info mode");
		// SparkConf conf = new SparkConf()
		// .setAppName("Main")
		// .setMaster("local[2]");
		// JavaSparkContext sc = new JavaSparkContext(conf);

		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark ML project")
				.master("local[2]")
				.config("spark.master", "local")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		// spark.sparkContext().setLogLevel(Level.);
		Dataset<Row> dataset = spark.read()
				.option("header", "true")
				.csv("src/main/resources/Wuzzuf_Jobs.csv");

		dataset.createOrReplaceTempView("WUZZUF_DATA");

		Dataset<Row> typedDataset = spark.sql("SELECT "
				+ "cast (Title as string) Title, "
				+ "cast (Company as string) Company, "
				+ "cast (Location as string) Location, "
				+ "cast (Type as string) Type, "
				+ "cast (Level as string) Level, "
				+ "cast (YearsExp as string) YearsExp, "
				+ "cast (Country as string) Country, "
				+ "cast (Skills as string) Skills FROM WUZZUF_DATA");
		// dataset = dataset.cache();
		// typedDataset.show(6);
		StringIndexer typeIndexer = new StringIndexer().setInputCol("Type").setOutputCol("type-factorized")
				.setHandleInvalid("keep");
		typedDataset = typeIndexer.fit(typedDataset).transform(typedDataset);
		System.out.println("+++++========++++++++Done");
		typedDataset.printSchema();

		StringIndexer levelIndexer = new StringIndexer().setInputCol("Level").setOutputCol("level-factorized")
				.setHandleInvalid("keep");
		typedDataset = levelIndexer.fit(typedDataset).transform(typedDataset);
		System.out.println("+++++========++++++++Done");
		typedDataset.printSchema();

		typedDataset = typedDataset.drop("Type", "Level");
		System.out.println("+++++========++++++++Done");
		// typedDataset.select("YearsExp").rdd().map(row -> row).collect().toList();
		// typedDataset.select("YearsExp").as.collect().toList();
		// String yearsLength = "Yrs of Exp";
		String minmaxCol = "columnUppercase";
		spark.sqlContext().udf().register(minmaxCol, (UDF1<String, String>) (yearsExp) -> {
			String years = yearsExp.toString();
			// Get 1-3 | 6-12 | null | 8+ | 14+ | 9 ->
			// means remove Yrs of Exp string w 5alas
			years = years.split(" ", 2)[0];
			if (years.equals("null"))
				return null;
			else if (years.indexOf('-') != -1) {
				String[] minmax = years.split("-");
				return minmax[0] + " " + minmax[1];
			} else if (years.indexOf('+') != -1) {
				years = years.substring(0, years.length() - 1);
				return years + " " + "10";
				// return years + " " + years;
			} else {
				return years + " " + years;
			}
		}, DataTypes.StringType);
		typedDataset = typedDataset.withColumn("MinMaxYearsExp", callUDF(minmaxCol, col("YearsExp")));
		typedDataset.printSchema();
		System.out.println("+++++========++++++++Done");
		typedDataset.show();
		System.out.println("+++++========++++++++Done");


		// List<String> yearsToSplit = typedDataset.select("YearsExp")
		// 		.collectAsList()
		// 		.stream()
		// 		.parallel()
		// 		.map(row -> {
		// 			String years = row.get(0).toString();
		// 			// Get 1-3 | 6-12 | null | 8+ | 14+ | 9 ->
		// 			// means remove Yrs of Exp string w 5alas
		// 			years = years.split(" ", 2)[0];
		// 			if (years.equals("null"))
		// 				return null;
		// 			else if (years.indexOf('-') != -1) {
		// 				String[] minmax = years.split("-");
		// 				return minmax[0] + " " + minmax[1];
		// 			} else if (years.indexOf('+') != -1) {
		// 				years = years.substring(0, years.length() - 1);
		// 				return years + " " + years;
		// 			} else {
		// 				return years + " " + years;
		// 			}
		// 		})
		// 		.collect(Collectors.toList());

		// UserDefinedFunction udfUppercase = udf(
		// 		(String s) -> s.toUpperCase(), DataTypes.StringType);

		// typedDataset.select(udfUppercase.apply(col("name")));
		// Column cl = new Column;
		// typedDataset.withColumn("MinYearsExp", functions.);
		// typedDataset.toJavaRDD().
		// Column maxYearsExp = typedDataset.col("YearsExp").apply(x=>"");
		// or ERROR only
		// Logger.getLogger("org").setLevel(Level.ERROR);
		// (new WuzzufJobsCsv()).load("src/main/resources/Wuzzuf_Jobs.csv");
		// SpringApplication.run(WuzzufJobsAnalysisApplication.class, args);
	}

}
