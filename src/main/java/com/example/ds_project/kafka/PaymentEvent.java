package com.example.ds_project.kafka;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Kafka event representing a payment transaction.
 * Timestamp is corrected by clock synchronization offset before publishing.
 */
public record PaymentEvent(
    UUID paymentId,              // Unique payment identifier
    String userId,               // User who initiated the payment
    BigDecimal amount,           // Payment amount
    long timestamp,              // Corrected timestamp (epoch ms with offset applied)
    String status,               // Status: PENDING, SUCCESS, FAILED
    long clockOffsetApplied,     // Clock offset that was applied (ms) - for audit trail
    String publishingNodeId      // Which node published this event
) {
    /**
     * Convenience constructor for backward compatibility.
     * Used when offset correction is not needed (e.g., in tests).
     */
    public PaymentEvent(UUID paymentId, BigDecimal amount, long timestamp, String status) {
        this(paymentId, "anonymous", amount, timestamp, status, 0, "unknown");
    }
}
