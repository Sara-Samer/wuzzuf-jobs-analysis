package com.example.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class AnalysisHelper {
    Dataset<Row> wuzzufData;
    private static AnalysisHelper instance;

    private AnalysisHelper() {

    }

    public static AnalysisHelper getInstance() {
        if (instance == null) {
            instance = new AnalysisHelper();
        }
        return instance;
    }

    public String readData(){
        WuzzufJobsAnalysis inst = WuzzufJobsAnalysis.getInstance();
        wuzzufData = inst.readData();
        wuzzufData = inst.factoriesYearsOfExp(wuzzufData);
        String html = this.datasetToTable(wuzzufData.limit(100));
        return html;
    }
    public String cleanData(){
        // wuzzufData = WuzzufJobsAnalysis.getInstance().readData();
        return "";
    }
    public String getStructure(){
        String html = String.format("<h1>Wuzzuf Data Schema</h1>") +
                String.format("<h2 style=\"text-align:center;\"> Total records = %d</h2>", wuzzufData.count()) +
                String.format("<h2 style=\"text-align:center;\"> Schema </h2>") +
                "<table style=\"width:100%;text-align: center\">";
        String[] st = wuzzufData.schema().toDDL().split(",");

        for (String st1 : st) {
            html += "<tr><td><br>" + st1 + "</tr></td>";
        }
        html += "</table>";
        return html;
    }
    private String datasetToTable(Dataset<Row> df){
        String html = "<table style=\"width:100%;text-align: center\">";
        html += "<thead>";
        html += "<tr>";
        for (String col : df.columns()) {
            html += ("<th>" + col + "</th>");
        }
        html += "</tr>";
        html += "</thead>";
        for (Row row : df.collectAsList()) {
            html += "<tr>";
            for (int i = 0; i < row.length(); i++) {
                html += ("<td>" + row.get(i) + "</td>");
            }
            html += "</tr>";
        }
        html += "</tbody>";

        html += "</tbody>";
        html += "</table>";
        return html;
    }
    public String getSummary(){
        WuzzufJobsAnalysis inst = WuzzufJobsAnalysis.getInstance();
        Dataset<Row> summary = inst.encodeCategories(wuzzufData).describe("type-factorized", "level-factorized", "MaxYearsExp", "MinYearsExp");
        String html = String.format("<h1>Wuzzuf Data Summary</h1>");
        html += this.datasetToTable(summary);
        return html;
    }
    public String getTitlesChart(){
        return "";
    }
    public String geSkillsChart(){
        return "";
    }
    public String getAreasChart(){
        return "";
    }
    public String getJobsChart(){
        return "";
    }
    public String getTitlesTable(){
        return "";
    }
    public String geSkillsTable(){
        return "";
    }
    public String getAreasTable(){
        return "";
    }
    public String getJobsTable(){
        return "";
    }
}
