package com.example.ds_project.kafka;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentEvent(UUID paymentId, BigDecimal amount, long timestamp, String status) {
}
