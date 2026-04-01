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

    // Task 5: Deduplication
    private final Set<UUID> processedPayments = ConcurrentHashMap.newKeySet();

    public KafkaConsumerService(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = "payments", groupId = "payment-group")
    public void consume(PaymentEvent event) {
        try {
            log.info("=== KAFKA CONSUMER TRIGGERED on Node {} ===", serverPort);
            log.info("Received event: paymentId={}, amount={}", event.paymentId(), event.amount());

            // Task 5: Deduplication
            if (!processedPayments.add(event.paymentId())) {
                log.info("Duplicate ignored for payment {}", event.paymentId());
                return;
            }

            // Task 7: Required log format
            log.info("Node {} consumed payment {}", serverPort, event.paymentId());

            // Process and store — NO leader check
            paymentService.processPayment(event);

            log.info("=== PIPELINE COMPLETE for {} on Node {} ===", event.paymentId(), serverPort);
        } catch (Exception e) {
            log.error("FATAL: Consumer failed to process payment on Node {}", serverPort, e);
        }
    }
}
