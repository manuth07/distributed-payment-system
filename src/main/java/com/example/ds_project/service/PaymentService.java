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
        
        Payment saved = repository.save(newPayment);
        return saved;
    }

    public void processPayment(PaymentEvent event) {
        log.info("Processing payment {}", event.paymentId());

        try {
            // Task 2: Simulate processing
            Thread.sleep(75);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Create Payment and set status
        Payment payment = new Payment(
                event.paymentId().toString(),
                "node-" + serverPort,
                event.amount(),
                "SUCCESS",
                LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(event.timestamp()), ZoneOffset.UTC)
        );
        // Set userId from the Kafka event
        payment.setUserId(event.userId());

        // Phase 8: Kafka is now strictly secondary and informational.
        // We DO NOT save directly to PaymentRepository from Kafka. 
        // This avoids the dual-write problem, as Raft is the authority.
        // repository.save(payment);

        log.info("Kafka audit: Payment {} event received for user {} (No DB write)", event.paymentId(), event.userId());
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
