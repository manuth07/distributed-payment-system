package com.example.ds_project.kafka;

import com.example.ds_project.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KafkaConsumerService {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final PaymentService paymentService;

    @Value("${server.port}")
    private String serverPort;

    // Deduplication
    private final Set<UUID> processedPayments = ConcurrentHashMap.newKeySet();

    public KafkaConsumerService(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    /**
     * Kafka Consumer — ALL nodes process independently.
     * NO leader check. NO Raft dependency.
     * Raft runs in parallel for coordination only.
     */
    @KafkaListener(topics = "payments", groupId = "payment-group")
    public void consume(PaymentEvent event) {
        try {
            // Deduplication
            if (!processedPayments.add(event.paymentId())) {
                log.debug("Duplicate payment {} ignored on node {}", event.paymentId(), serverPort);
                return;
            }

            log.info("Node {} consumed payment {} (amount={})", serverPort, event.paymentId(), event.amount());

            // Process and store — every node does this independently
            paymentService.processPayment(event);

            log.info("Node {} completed pipeline for payment {}", serverPort, event.paymentId());
        } catch (Exception e) {
            log.error("CONSUMER ERROR on node {} for payment {}: {}", serverPort, event.paymentId(), e.getMessage(), e);
        }
    }
}
