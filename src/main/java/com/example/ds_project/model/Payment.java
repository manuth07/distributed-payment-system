package com.example.ds_project.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public class Payment {
    private String id;
    private String nodeId;
    private BigDecimal amount;
    private String status;
    private LocalDateTime timestamp;

    public Payment(BigDecimal  amount, String nodeId) {
        this.id = UUID.randomUUID().toString();
        this.amount = amount;
        this.status = "SUCCESS";
        this.timestamp = LocalDateTime.now();
        this.nodeId = nodeId;
    }

    // Getters
    public String getId() {
        return id;
    }
    public BigDecimal  getAmount() {
        return amount;
    }
    public String getStatus() {
        return status;
    }
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    public String getNodeId() {
        return nodeId;
    }
}
