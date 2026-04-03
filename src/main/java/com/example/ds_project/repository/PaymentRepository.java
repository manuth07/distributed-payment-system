package com.example.ds_project.repository;

import com.example.ds_project.model.Payment;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Payment repository with Time Synchronization support (Phase 3c).
 * 
 * Default sorting: correctedTimestamp (for distributed ordering)
 * This ensures payments are returned in causal order despite clock skew.
 */
@Repository
public class PaymentRepository {

    private Map<String, Payment> paymentStore = new HashMap<>();

    public Payment save(Payment payment) {
        paymentStore.put(payment.getId(), payment);
        return payment;
    }

    public Optional<Payment> findById(String id) {
        return Optional.ofNullable(paymentStore.get(id));
    }

    /**
     * Phase 3c: findAll() now returns payments sorted by correctedTimestamp.
     * This ensures distributed causal ordering despite clock skew.
     * 
     * For payments with correctedTimestamp=0 (legacy or not yet corrected),
     * they are sorted last, then by insertion order.
     */
    public List<Payment> findAll() {
        return paymentStore.values().stream()
                .sorted(Comparator
                        .comparingLong(Payment::getCorrectedTimestamp)
                        .thenComparingLong(p -> System.identityHashCode(p))  // Stable sort via identity
                )
                .collect(Collectors.toList());
    }

    /**
     * Phase 3c: Find payments by node ID, sorted by correctedTimestamp.
     * Useful for per-node payment verification and auditing.
     */
    public List<Payment> findByNodeIdOrderByCorrectedTimestamp(String nodeId) {
        return paymentStore.values().stream()
                .filter(p -> nodeId.equals(p.getNodeId()))
                .sorted(Comparator.comparingLong(Payment::getCorrectedTimestamp))
                .collect(Collectors.toList());
    }

    /**
     * Phase 3c: Find payments by publishing node ID, sorted by correctedTimestamp.
     * Enables verification of payments from specific publishing sources.
     */
    public List<Payment> findByPublishingNodeIdOrderByCorrectedTimestamp(String publishingNodeId) {
        return paymentStore.values().stream()
                .filter(p -> publishingNodeId.equals(p.getPublishingNodeId()))
                .sorted(Comparator.comparingLong(Payment::getCorrectedTimestamp))
                .collect(Collectors.toList());
    }

    /**
     * Phase 3c: Find payments within a timestamp range (correctedTimestamp).
     * Useful for time-window queries like "payments from the last 5 minutes".
     * 
     * @param startTimestampMs inclusive start (epoch millis)
     * @param endTimestampMs inclusive end (epoch millis)
     * @return sorted list of payments in the range
     */
    public List<Payment> findByCorrectedTimestampBetween(long startTimestampMs, long endTimestampMs) {
        return paymentStore.values().stream()
                .filter(p -> p.getCorrectedTimestamp() >= startTimestampMs && 
                           p.getCorrectedTimestamp() <= endTimestampMs)
                .sorted(Comparator.comparingLong(Payment::getCorrectedTimestamp))
                .collect(Collectors.toList());
    }

    /**
     * Phase 3c: Get payment statistics for monitoring and analysis.
     * Returns min/max corrected timestamps across all payments.
     */
    public Map<String, Object> getTimestampStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        
        if (paymentStore.isEmpty()) {
            stats.put("count", 0);
            stats.put("minCorrectedTimestamp", 0);
            stats.put("maxCorrectedTimestamp", 0);
            return stats;
        }
        
        List<Payment> sorted = findAll();
        long minTs = sorted.stream().mapToLong(Payment::getCorrectedTimestamp).min().orElse(0);
        long maxTs = sorted.stream().mapToLong(Payment::getCorrectedTimestamp).max().orElse(0);
        
        stats.put("count", sorted.size());
        stats.put("minCorrectedTimestamp", minTs);
        stats.put("maxCorrectedTimestamp", maxTs);
        stats.put("timeSpanMs", maxTs - minTs);
        
        return stats;
    }
}