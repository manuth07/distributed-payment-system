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
    private String userId;
    private String nodeId;
    private BigDecimal amount;
    private String status;
    private LocalDateTime timestamp;
    
    // Phase 3b: Time Synchronization tracking
    private long correctedTimestamp;       // Epoch millis with offset applied
    private long clockOffsetAtCreation;    // Offset applied at creation time (ms)
    private String publishingNodeId;       // Node that originally published payment
    
    // Part B: Custom Indexing Policy
    // Kafka partition + offset serve as the definitive authoritative storage index
    // correctedTimestamp acts as a logical application-level ordering aid
    private int kafkaPartition = -1;
    private long kafkaOffset = -1;

    public Payment() {} // Default for JSON

    /**
     * Legacy 5-parameter constructor - maintained for backward compatibility.
     */
    public Payment(String id, String nodeId, BigDecimal amount, String status, LocalDateTime timestamp) {
        this.id = id;
        this.userId = "anonymous";
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
        this.userId = "anonymous";
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
        this.userId = "anonymous";
        this.nodeId = nodeId;
        this.amount = amount;
        this.status = status;
        this.timestamp = timestamp;
        this.correctedTimestamp = correctedTimestamp;
        this.clockOffsetAtCreation = clockOffsetAtCreation;
        this.publishingNodeId = publishingNodeId;
    }

    /**
     * Full constructor with userId and Time Synchronization support.
     * Used when creating payments with known user identity and clock offsets.
     */
    public Payment(String id, String userId, String nodeId, BigDecimal amount, String status,
                   LocalDateTime timestamp, long correctedTimestamp,
                   long clockOffsetAtCreation, String publishingNodeId) {
        this.id = id;
        this.userId = userId;
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
    public String getUserId() {
        return userId;
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
    
    // Part B: Kafka Indexing Metadata Getters
    public int getKafkaPartition() {
        return kafkaPartition;
    }
    public long getKafkaOffset() {
        return kafkaOffset;
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
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    // Part B: Kafka Indexing Metadata Setters
    public void setKafkaPartition(int kafkaPartition) {
        this.kafkaPartition = kafkaPartition;
    }
    public void setKafkaOffset(long kafkaOffset) {
        this.kafkaOffset = kafkaOffset;
    }
}
