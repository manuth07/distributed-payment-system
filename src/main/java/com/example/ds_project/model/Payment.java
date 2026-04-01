package com.example.ds_project.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Payment model with Time Synchronization support (Phase 3b).
 * 
 * Fields:
 * - timestamp: LocalDateTime for human readability (maintained for backward compatibility)
 * - correctedTimestamp: epoch millis with clock offset applied (for distributed ordering)
 * - clockOffsetAtCreation: offset that was applied when payment was created (audit trail)
 * - publishingNodeId: identifies which node originally created this payment
 */
public class Payment {
    private String id;
    private String nodeId;
    private BigDecimal amount;
    private String status;
    private LocalDateTime timestamp;
    
    // Phase 3b: Time Synchronization tracking
    private long correctedTimestamp;       // Epoch millis with offset applied
    private long clockOffsetAtCreation;    // Offset applied at creation time (ms)
    private String publishingNodeId;       // Node that originally published payment

    public Payment() {} // Default for JSON

    /**
     * Legacy 5-parameter constructor - maintained for backward compatibility.
     */
    public Payment(String id, String nodeId, BigDecimal amount, String status, LocalDateTime timestamp) {
        this.id = id;
        this.nodeId = nodeId;
        this.amount = amount;
        this.status = status;
        this.timestamp = timestamp;
        this.correctedTimestamp = 0;
        this.clockOffsetAtCreation = 0;
        this.publishingNodeId = nodeId;
    }

    /**
     * Legacy 2-parameter constructor - maintained for backward compatibility.
     */
    public Payment(BigDecimal amount, String nodeId) {
        this.id = UUID.randomUUID().toString();
        this.amount = amount;
        this.status = "SUCCESS";
        this.timestamp = LocalDateTime.now();
        this.nodeId = nodeId;
        this.correctedTimestamp = 0;
        this.clockOffsetAtCreation = 0;
        this.publishingNodeId = nodeId;
    }

    /**
     * Extended constructor with Time Synchronization support (Phase 3b).
     * Used when creating payments with known clock offsets.
     */
    public Payment(String id, String nodeId, BigDecimal amount, String status, 
                   LocalDateTime timestamp, long correctedTimestamp, 
                   long clockOffsetAtCreation, String publishingNodeId) {
        this.id = id;
        this.nodeId = nodeId;
        this.amount = amount;
        this.status = status;
        this.timestamp = timestamp;
        this.correctedTimestamp = correctedTimestamp;
        this.clockOffsetAtCreation = clockOffsetAtCreation;
        this.publishingNodeId = publishingNodeId;
    }

    // Getters
    public String getId() {
        return id;
    }
    public BigDecimal getAmount() {
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
    
    // Phase 3b: New getters for Time Synchronization
    public long getCorrectedTimestamp() {
        return correctedTimestamp;
    }
    public long getClockOffsetAtCreation() {
        return clockOffsetAtCreation;
    }
    public String getPublishingNodeId() {
        return publishingNodeId;
    }
    
    // Setters for deserialization and updates
    public void setCorrectedTimestamp(long correctedTimestamp) {
        this.correctedTimestamp = correctedTimestamp;
    }
    public void setClockOffsetAtCreation(long clockOffsetAtCreation) {
        this.clockOffsetAtCreation = clockOffsetAtCreation;
    }
    public void setPublishingNodeId(String publishingNodeId) {
        this.publishingNodeId = publishingNodeId;
    }
}
