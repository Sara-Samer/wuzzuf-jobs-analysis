package com.example.main;

// import com.example.analysis.WuzzufJobsAnalysis;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SpringBootApplication
public class WuzzufJobsAnalysisApplication implements WebMvcConfigurer {

	public static void main(String[] args) {
		SparkSession
				.builder()
				.appName("Java Spark ML project")
				.master("local[2]")
				.config("spark.master", "local")
				.getOrCreate();
    //spark.sparkContext().setLogLevel("ERROR");
		SpringApplication.run(WuzzufJobsAnalysisApplication.class, args);
	}

	@Override
    public void addResourceHandlers(ResourceHandlerRegistry registry){ 
            registry.addResourceHandler("/**")
                 .addResourceLocations("classpath:/");
    }
}
