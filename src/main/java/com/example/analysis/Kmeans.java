package com.example.analysis;

import javassist.expr.Cast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class Kmeans {
    public static String calculateKMeans(Dataset<Row> dataset) {
        // Loads data.
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        sparkSession.sparkContext();
        Dataset<Row> convert = sparkSession.sql("select Title, Company from WUZZUF_DATA");
        StringIndexerModel indexer = new StringIndexer()
                .setInputCols(new String[] { "Title", "Company"})
                .setOutputCols(new String[] { "TitleConv", "CompanyConv"})
                .fit(convert);

        Dataset<Row> indexed = indexer.transform(convert);
        indexed.show();
        indexed.printSchema();
        Dataset<Row> factorized = indexed.select("TitleConv", "CompanyConv");
        factorized.show();

        System.out.println("+++++========++++++++kmeans");

        JavaRDD<Vector> parsedData = factorized.javaRDD().map((Function<Row, Vector>) s -> {
            double[] values = new double[2];

            values[0] = Double.parseDouble(s.get(0).toString());
            values[1] = Double.parseDouble(s.get(1).toString());

            return Vectors.dense(values);});

        parsedData.cache();
        // Cluster the data into four classes using KMeans
        int numClusters = 4;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WithinError = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WithinError);

        String output = "<html><body>";
        output += "<h1>"+"Calculate KMeans Clustering:"+"</h1><br><p>";
        System.out.println("Cluster centers:");
        for (Vector str : clusters.clusterCenters()){
            System.out.println(" " + str);
            output += str.toString();
            output += "<br>";
        }

        output += "<br>" + "Within Set Sum of Square Error: " + WithinError+"<br>";
        output += "</p></body></html>";
        return output;

    }
}
