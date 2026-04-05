package com.example.ds_project.service;

import com.example.ds_project.config.NodeConfig;
import com.example.ds_project.kafka.PaymentEvent;
import com.example.ds_project.model.Payment;
import com.example.ds_project.repository.PaymentRepository;
import com.example.ds_project.timesync.ClockSynchronizationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

@Service
public class PaymentService {
    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository repository;
    private final NodeConfig nodeConfig;
    private final ReplicationService replicationService;
    private final ClockSynchronizationService clockSyncService;  // Phase 3b: Time Synchronization

    @Value("${server.port}")
    private String serverPort;

    public PaymentService(PaymentRepository repository, NodeConfig nodeConfig, 
                         ReplicationService replicationService,
                         ClockSynchronizationService clockSyncService) {  // Phase 3b: Injected
        this.repository = repository;
        this.nodeConfig = nodeConfig;
        this.replicationService = replicationService;
        this.clockSyncService = clockSyncService;
    }
    
    public Payment processPayment(BigDecimal amount) {
        // Phase 3b: Capture clock offset at payment creation time
        long rawTimestampMs = System.currentTimeMillis();
        long clockOffset = clockSyncService.getCurrentOffset();
        long correctedTimestampMs = rawTimestampMs + clockOffset;
        
        Payment newPayment = new Payment(
                UUID.randomUUID().toString(),
                nodeConfig.getNodeId(),
                amount,
                "SUCCESS",
                LocalDateTime.now(),                  // For human readability (backward compat)
                correctedTimestampMs,                 // Corrected timestamp for ordering
                clockOffset,                          // Offset applied at creation
                nodeConfig.getNodeId()                // This node published it
        );
        
        return newPayment;
    }

    /**
     * Part A & F: CUSTOM AGREEMENT / MATERIALIZATION POLICY
     * 
     * This method acts as the local validation and acceptance pipeline.
     * We do NOT blindly trust the network. We explicitly validate the event before
     * accepting it into our local materialized view.
     * 
     * @param event The raw Kafka event
     * @param partition The authoritative storage partition index
     * @param offset The chronological append-only offset index
     */
    public void processAndCommitPayment(PaymentEvent event, int partition, long offset) {
        log.info("Vaildating candidate payment {}", event.paymentId());

        // 1. Part A: Integrity Validation
        if (event.amount() == null || event.amount().compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("Agreement Policy Rejected: Payment {} has invalid amount {}", event.paymentId(), event.amount());
            return;
        }

        // 2. Part D: Idempotency / Deduplication Policy
        // Using paymentId as the canonical idempotency key
        if (repository.findById(event.paymentId().toString()).isPresent()) {
            log.info("Idempotency Policy Triggered: Payment {} already exists. Skipping duplicate replay.", event.paymentId());
            return;
        }

        try {
            // Processing simulation
            Thread.sleep(75);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 3. Part B & F: Materialization and Indexing Binding
        Payment payment = new Payment(
                event.paymentId().toString(),
                event.userId(),
                "node-" + serverPort,
                event.amount(),
                "SUCCESS",
                LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(event.timestamp()), ZoneOffset.UTC),
                event.timestamp(),
                event.clockOffsetApplied(),
                event.publishingNodeId()
        );
        
        // Explicitly bind the authoritative storage index to the local view
        payment.setKafkaPartition(partition);
        payment.setKafkaOffset(offset);

        // 4. Local Commit
        repository.save(payment);

        log.info("Materialization Complete: Payment {} strictly indexed at partition-{} offset-{}", 
                event.paymentId(), partition, offset);
    }



    public List<Payment> getAllPayments() {
        return repository.findAll();
    }

    public long getCount() {
        return repository.count();
    }

    /**
     * Phase 3c: Query payments by correctedTimestamp range.
     * Returns sorted results for time-window queries.
     */
    public List<Payment> getPaymentsByTimestampRange(long startMs, long endMs) {
        return repository.findByCorrectedTimestampBetween(startMs, endMs);
    }

    /**
     * Phase 3c: Query payments by node ID, sorted by correctedTimestamp.
     * Used for per-node payment verification.
     */
    public List<Payment> getPaymentsByNodeId(String nodeId) {
        return repository.findByNodeIdOrderByCorrectedTimestamp(nodeId);
    }

    /**
     * Phase 3c: Query payments by publishing node ID, sorted by correctedTimestamp.
     * Used for audit trails and tracing payment sources.
     */
    public List<Payment> getPaymentsByPublishingNodeId(String publishingNodeId) {
        return repository.findByPublishingNodeIdOrderByCorrectedTimestamp(publishingNodeId);
    }

    /**
     * Phase 3c: Get payment statistics including timestamp ranges and counts.
     * Used for monitoring cluster-wide payment ordering health.
     */
    public java.util.Map<String, Object> getPaymentStatistics() {
        return repository.getTimestampStatistics();
    }

    /**
     * Retrieve transaction history for a specific user from this node's local repository.
     * Returns payments sorted by correctedTimestamp descending (newest first).
     */
    public List<Payment> getTransactionsByUserId(String userId) {
        return repository.findByUserIdOrderByCorrectedTimestampDesc(userId);
    }
}
