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

}
