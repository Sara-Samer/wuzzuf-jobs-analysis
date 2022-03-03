package com.example.WuzzufJobsAnalysis;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WuzzufJobsAnalysisApplication {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark ML project")
				.master("local[4]")
				.config("spark.master", "local")
				.getOrCreate();
		SpringApplication.run(WuzzufJobsAnalysisApplication.class, args);
	}

}
