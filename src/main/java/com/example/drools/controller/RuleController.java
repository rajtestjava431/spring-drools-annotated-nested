package com.example.drools.controller;

import com.example.drools.model.Payload;
import com.example.drools.service.RuleEngineService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;
import java.util.List;

@RestController
@RequestMapping("/api")
public class RuleController {

    @Autowired
    private RuleEngineService ruleEngineService;

    @PostMapping("/evaluate")
    public Map<String, List<String>> evaluate(@RequestBody Payload payload) throws IllegalAccessException, InterruptedException, IOException {

        return ruleEngineService.process(payload);
    }
}