package com.example.ds_project.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class ClusterConfig {

    @Value("${nodes}")
    private String nodes;

    public List<String> getAllNodes() {
        return Arrays.asList(nodes.split(","));
    }
}
