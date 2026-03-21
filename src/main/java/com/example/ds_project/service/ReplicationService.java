package com.example.ds_project.service;

import com.example.ds_project.model.Payment;
import com.example.ds_project.config.ClusterConfig;
import com.example.ds_project.config.NodeConfig;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class ReplicationService {

    private final RestTemplate restTemplate;
    private final ClusterConfig clusterConfig;
    private final NodeConfig nodeConfig;

    public ReplicationService(RestTemplate restTemplate,
                              ClusterConfig clusterConfig,
                              NodeConfig nodeConfig) {
        this.restTemplate = restTemplate;
        this.clusterConfig = clusterConfig;
        this.nodeConfig = nodeConfig;
    }

    public void replicateToOtherNodes(Payment payment) {
        for (String node : clusterConfig.getAllNodes()) {

            // skip self
            if (node.equals(nodeConfig.getNodeUrl()))
                continue;

            try {
                String url = node + "/internal/replicate";
                restTemplate.postForObject(url, payment, Void.class);
            } catch (Exception e) {
                System.out.println("Failed to replicate to " + node);
            }
        }
    }
}
