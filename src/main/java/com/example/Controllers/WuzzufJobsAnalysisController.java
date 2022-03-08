package com.example.Controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WuzzufJobsAnalysisController {

    @GetMapping
    public String ShowData()
    {
        // Analysis.ShowData();
        return"Hello";
    }
}
