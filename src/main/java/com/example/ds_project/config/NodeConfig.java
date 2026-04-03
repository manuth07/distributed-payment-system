package com.example.ds_project.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class NodeConfig {
    @Value("${node.id}")
    private String nodeId;

    @Value("${node.url}")
    private String nodeUrl;

    public String getNodeId(){
        return nodeId;
    }

    public String getNodeUrl(){
        return nodeUrl;
    }
}
