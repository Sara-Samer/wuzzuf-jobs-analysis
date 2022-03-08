package com.example.main.controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

// import org.springframework.web;

@RestController
public class WuzzufAnalysisController {
    @GetMapping("/hi")
    public String index(){
        return "Hi Sara";
    }
    
}
