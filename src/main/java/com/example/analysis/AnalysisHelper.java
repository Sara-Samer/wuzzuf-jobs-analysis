package com.example.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static com.example.utils.Constants.POPULAR_TITLES_CHART;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;

import static com.example.utils.Constants.POPULAR_SKILLS_CHART;
import static com.example.utils.Constants.POPULAR_AREAS_CHART;
import static com.example.utils.Constants.IMG_PATH;;

public class AnalysisHelper {
    Dataset<Row> wuzzufData;
    String fileExtention = ".jpg";

    private static AnalysisHelper instance;

    private AnalysisHelper() {
        wuzzufData = WuzzufJobsAnalysis.getInstance().readData();
        wuzzufData = WuzzufJobsAnalysis.getInstance().factoriesYearsOfExp(wuzzufData);
    }

    public static AnalysisHelper getInstance() {
        if (instance == null) {
            instance = new AnalysisHelper();
        }
        return instance;
    }

    public String readData() {
        String html = this.datasetToTable(wuzzufData.limit(100));
        return html;
    }

    public String cleanData() {
        // wuzzufData = WuzzufJobsAnalysis.getInstance().readData();
        return "";
    }

    public String getStructure() {
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

    private String datasetToTable(Dataset<Row> df) {
        String html = "<table style=\"width:100%;text-align: center\">";
        html += "<thead>";
        html += "<tr>";
        for (String col : df.columns()) {
            html += ("<th>" + col + "</th>");
        }
        html += "</tr>";
        html += "</thead>";

        html += "<tbody>";
        for (Row row : df.collectAsList()) {
            html += "<tr>";
            for (int i = 0; i < row.length(); i++) {
                html += ("<td>" + row.get(i) + "</td>");
            }
            html += "</tr>";
        }
        html += "</tbody>";

        html += "</table>";
        return html;
    }

    public String getSummary() {
        WuzzufJobsAnalysis inst = WuzzufJobsAnalysis.getInstance();
        Dataset<Row> summary = inst.encodeCategories(wuzzufData).describe("type-factorized", "level-factorized",
                "MaxYearsExp", "MinYearsExp");
        String html = String.format("<h1>Wuzzuf Data Summary</h1>");
        html += this.datasetToTable(summary);
        return html;
    }

    public String getTitlesChart() {
        String path = IMG_PATH + POPULAR_TITLES_CHART + this.fileExtention;
        File file = new File(path);
        if (!file.exists()) {
            WuzzufJobsAnalysis.getInstance().JobTitlesBarGraph(wuzzufData);
        }
        String html = String.format("<h1>%s</h1>", POPULAR_TITLES_CHART.replace("-", " ")) +
                String.format("<img src=\"%s\">", "/" + POPULAR_TITLES_CHART + this.fileExtention);
        return html;
    }

    public String getSkillsChart() {
        String path = IMG_PATH + POPULAR_SKILLS_CHART + this.fileExtention;
        File file = new File(path);
        if (!file.exists()) {
            WuzzufJobsAnalysis.getInstance().JobSkillsBarGraph(wuzzufData);
        }
        String html = String.format("<h1>%s</h1>", POPULAR_SKILLS_CHART.replace("-", " ")) +
                String.format("<img src=\"%s\">", "/" + POPULAR_SKILLS_CHART + this.fileExtention);
        return html;
    }

    public String getAreasChart() {
        String path = IMG_PATH + POPULAR_AREAS_CHART + this.fileExtention;
        File file = new File(path);
        if (!file.exists()) {
            WuzzufJobsAnalysis.getInstance().AreasCountBarGraph(wuzzufData);
        }
        String html = String.format("<h1>%s</h1>", POPULAR_AREAS_CHART.replace("-", " ")) +
                String.format("<img src=\"%s\">", "/" + POPULAR_AREAS_CHART + this.fileExtention);
        return html;
    }

    public String getTitlesTable() {
        Dataset<Row> table = WuzzufJobsAnalysis.getInstance().MostPopularTitles(wuzzufData);
        return this.datasetToTable(table);
    }

    public String getSkillsTable() {
        Map<String, Integer> table = WuzzufJobsAnalysis.getInstance().mostPopularSkills(wuzzufData);
        String html = "<table style=\"width:100%;text-align: center\">";
        html += "<thead>";
        html += "<tr>";
        html += ("<th>" + "Skill" + "</th>");
        html += ("<th>" + "Count" + "</th>");
        html += "</tr>";
        html += "</thead>";

        html += "<tbody>";
        for (Entry<String, Integer> entry : table.entrySet()) {
            html += "<tr>";
            html += ("<td>" + entry.getKey() + "</td>");
            html += ("<td>" + entry.getValue() + "</td>");
            html += "</tr>";
        }
        html += "</tbody>";

        html += "</table>";

        return html;
    }

    public String getAreasTable() {
        Dataset<Row> table = WuzzufJobsAnalysis.getInstance().MostPopularAreas(wuzzufData);
        return this.datasetToTable(table);
    }

    public String getJobsChart() {
        return "";
    }

    public String getJobsTable() {
        return "";
    }
}
