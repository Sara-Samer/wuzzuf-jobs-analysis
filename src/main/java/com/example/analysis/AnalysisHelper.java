package com.example.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static com.example.utils.Constants.POPULAR_TITLES_CHART;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.List;

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

        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:LightPink;\">%s</h1>", "Read Data") ;
        html += this.datasetToTable(wuzzufData.limit(100));
        return html;

    }

    public String cleanData() {
        // wuzzufData = WuzzufJobsAnalysis.getInstance().readData();
        return "";
    }

    public String getStructure() {
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:LightPink;\">%s</h1>","Wuzzaf data schema") +
                String.format("<h2 style=\"text-align:center;\"> Total records = %d</h2>", wuzzufData.count()) +
                String.format("<h2 style=\"text-align:center;\"> Schema </h2>") +
                "<table style=\"border:1px solid black;width:100%;text-align: center\">";
        String[] st = wuzzufData.schema().toDDL().split(",");

        for (String st1 : st) {
            html += "<tr><td><br>" + st1 + "</tr></td>";
        }
        html += "</table>";
        return html;
    }

    private String datasetToTable(Dataset<Row> df) {
        String html = "<table style= \"border:1px solid black;border-collapse: collapse; margin-left:auto;margin-right:auto; width:80%;text-align:center\">";
        html += "<thead>";
        html += "<tr>";
        for (String col : df.columns()) {
            html += ("<th style= \" border: 1px solid;\">" + col + "</th>");
        }
        html += "</tr>";
        html += "</thead>";

        html += "<tbody>";
        for (Row row : df.collectAsList()) {
            html += "<tr>";
            for (int i = 0; i < row.length(); i++) {
                html += ("<td style= \" border: 1px solid;\">" + row.get(i) + "</td>");
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
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:LightPink;\">%s</h1>", "Wuzaff data summary") ;
        html += this.datasetToTable(summary);
        return html;
    }

    public String getTitlesChart() {
        String path = IMG_PATH + POPULAR_TITLES_CHART + this.fileExtention;
        File file = new File(path);
        if (!file.exists()) {
            WuzzufJobsAnalysis.getInstance().JobTitlesBarGraph(wuzzufData);
        }
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:LightPink;\">%s</h1>", POPULAR_TITLES_CHART.replace("-", " ")) +
                String.format("<img src=\"%s\">", "/" + POPULAR_TITLES_CHART + this.fileExtention);
        return html;
    }

    public String getSkillsChart() {
        String path = IMG_PATH + POPULAR_SKILLS_CHART + this.fileExtention;
        File file = new File(path);
        if (!file.exists()) {
            WuzzufJobsAnalysis.getInstance().JobSkillsBarGraph(wuzzufData);
        }
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:LightPink;\">%s</h1>", POPULAR_SKILLS_CHART.replace("-", " ")) +
                String.format("<img src=\"%s\">", "/" + POPULAR_SKILLS_CHART + this.fileExtention);
        return html;
    }

    public String getAreasChart() {
        String path = IMG_PATH + POPULAR_AREAS_CHART + this.fileExtention;
        File file = new File(path);
        if (!file.exists()) {
            WuzzufJobsAnalysis.getInstance().AreasCountBarGraph(wuzzufData);
        }
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:LightPink;\">%s</h1>", POPULAR_AREAS_CHART.replace("-", " ")) +
                String.format("<img src=\"%s\">", "/" + POPULAR_AREAS_CHART + this.fileExtention);
        return html;
    }



    public String getTitlesTable() {
        Dataset<Row> table = WuzzufJobsAnalysis.getInstance().MostPopularTitles(wuzzufData);
        List<String> label =dfToList(table);
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:LightPink;\">%s</h1>", "The most popular job titles") +
                "<table style= \"border:1px solid black;border-collapse: collapse; margin-left:auto;margin-right:auto; width:80%;text-align:center\">" +
                "<tr><th style= \" border: 1px solid;\">Title</th><th style= \" border: 1px solid;\">TitleCount</th></tr>";

        for(int i = 0; i< 40; i++){
            String[] row = label.get(i).replace("[", "").replace("]", "").split(",");
            html += "<tr>\n" +"<td style= \" border: 1px solid;\">"+row[0]+"</td>\n" +"<td style= \" border: 1px solid;\">"+row[1]+"</td>\n" +"</td>\n"+"</tr>";
        }

        html += "</table>";

        return html;
    }

    public String getSkillsTable() {
        Map<String, Integer> table = WuzzufJobsAnalysis.getInstance().mostPopularSkills(wuzzufData);
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:LightPink;\">%s</h1>", "The most popular Skills.") +
                "<table style=\"border:1px solid black; border-collapse: collapse;margin-left:auto;margin-right:auto; width:70%;text-align:center; \" >" +
                "<tr ><th style= \" border: 1px solid;\" >Skill</th><th style= \" border: 1px solid;\" >Count</th></tr>";

        ArrayList<Object> l1 = new ArrayList<>();

        ArrayList<Object> l2 = new ArrayList<>();

        for (Entry<String, Integer> it : table.entrySet()) {
            l1.add(it.getKey());
            l2.add(it.getValue());
        }

        for(int i = 0; i< 100; i++){

            html += "<tr >\n" +"<td style= \" border: 1px solid;\">"+l1.get(i)+"</td >\n" +"<td style= \" border: 1px solid;\">"+l2.get(i)+"</td>\n" +"</td>\n"+"</tr>";
        }

        html += "</table>";

        return html;
    }

    public String getAreasTable() {
        Dataset<Row> table = WuzzufJobsAnalysis.getInstance().MostPopularAreas(wuzzufData);
        List<String> label =dfToList(table);

        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:SpringGreen;\">%s</h1>", "The most popular areas") +
                "<table style=\"border:1px solid black; border-collapse: collapse;margin-left:auto;margin-right:auto; width:70%;text-align:center; \" >" +
                "<tr ><th style= \" border: 1px solid;\">Area</th><th style= \" border: 1px solid;\">AreaCount</th></tr>";

        for(int i = 0; i< 40; i++){
            String[] row = label.get(i).replace("[", "").replace("]", "").split(",");
            html += "<tr>\n" +"<td style= \" border: 1px solid;\">"+row[0]+"</td>\n" +"<td style= \" border: 1px solid;\">"+row[1]+"</td>\n" +"</td>\n"+"</tr>";
        }

        html += "</table>";

        return html;
    }

    public String getJobsChart() {
        return "";
    }

    public String getJobsTable() {
        return "";
    }
    public List<String> dfToList(Dataset<Row> df){
        List results = new ArrayList();

        for(Row r:df.collectAsList()){
            results.add(r);
        }
        List<String> label = new ArrayList<>();
        for(int i = 0; i< results.size(); i++){
            label.add((String)((Row)results.get(i)).toString());
        }
        return label;
    }
}

