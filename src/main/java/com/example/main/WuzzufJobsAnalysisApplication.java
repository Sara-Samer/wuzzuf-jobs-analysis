package com.example.main;

import com.example.dataloader.WuzzufJobsCsv;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
// import org.apache.log4j.Level;
// import org.apache.log4j.LogManager;
// import org.apache.log4j.Logger;
// import org.apache.log4j.BasicConfigurator;  

@SpringBootApplication
public class WuzzufJobsAnalysisApplication {

	// private static final Logger logger = LogManager.getLogger(WuzzufJobsAnalysisApplication.class);  
	public static void main(String[] args) {
		// Logger.getLogger(SparkSession.class).setLevel(Level.OFF);
		// BasicConfigurator.configure();  
		// logger.info("Hello world");  
		// logger.error("we are in logger info mode");  
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark ML project")
				.master("local[4]")
				.config("spark.master", "local")
				.getOrCreate();
				spark.sparkContext().setLogLevel("ERROR"); 

		// or ERROR only
		// Logger.getLogger("org").setLevel(Level.ERROR);
		(new WuzzufJobsCsv()).load("src/main/resources/Wuzzuf_Jobs.csv");
		// SpringApplication.run(WuzzufJobsAnalysisApplication.class, args);
	}

}
