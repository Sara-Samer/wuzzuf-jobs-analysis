package com.example.main.controllers;

import com.example.analysis.AnalysisHelper;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

// import org.springframework.web;

@RestController
public class WuzzufAnalysisController {
    @GetMapping("/")
    public String index() {
        return "Hi Dear User";
    }

    @GetMapping("/read")
    public ResponseEntity<String> readAndShow() {
        String res = AnalysisHelper.getInstance().readData();
        return ResponseEntity.ok(res);
    }

    @GetMapping("/summary")
    public ResponseEntity<String> summary() {
        String res = AnalysisHelper.getInstance().getSummary();
        return ResponseEntity.ok(res);
    }

    @GetMapping("/struct")
    public ResponseEntity<String> structure() {
        String res = AnalysisHelper.getInstance().getStructure();
        return ResponseEntity.ok(res);
    }

    @GetMapping("/most-popular-skills")
    public ResponseEntity<String> mostPopularSkills() {
        String res = AnalysisHelper.getInstance().getSkillsChart();
        return ResponseEntity.ok(res);
    }

    @GetMapping("/most-popular-titles")
    public ResponseEntity<String> mostPopularTitles() {
        String res = AnalysisHelper.getInstance().getTitlesChart();
        return ResponseEntity.ok(res);
    }

    @GetMapping("/most-popular-areas")
    public ResponseEntity<String> mostPopularAreas() {
        String res = AnalysisHelper.getInstance().getAreasChart();
        return ResponseEntity.ok(res);
    }

}
