package com.example.ds_project.kafka;

import com.example.ds_project.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

@Service
public class KafkaConsumerService {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final PaymentService paymentService;

    @Value("${server.port}")
    private String serverPort;

    public KafkaConsumerService(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    /**
     * Part E: REPLAY & RECOVERY LOGIC
     * Using a dynamic groupId = "${spring.kafka.consumer.group-id}" instead of a fixed string
     * means each node constitutes its own independent consumer group.
     * With auto-offset-reset=earliest, restarting a node naturally forces it to replay
     * the entire Kafka log and mathematically recover its materialized view from scratch.
     */
    @KafkaListener(topics = "payments", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(PaymentEvent event,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) {
        try {
            // Part A: Custom Agreement Pipeline Kickoff
            log.info("Node {} received event {} [partition={}, offset={}]", 
                    serverPort, event.paymentId(), partition, offset);

            // Process and store — passing to custom acceptance layer
            paymentService.processAndCommitPayment(event, partition, offset);

            log.info("Node {} successfully applied payment {}", serverPort, event.paymentId());
        } catch (Exception e) {
            log.error("CONSUMER ERROR on node {} for payment {}: {}", serverPort, event.paymentId(), e.getMessage(), e);
        }
    }
}
