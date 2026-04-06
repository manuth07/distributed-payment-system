package com.example.ds_project.kafka;

import com.example.ds_project.service.PaymentService;
import com.example.ds_project.raft.RaftNode;
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
    private final RaftNode raftNode;

    @Value("${server.port}")
    private String serverPort;

    // Deduplication
    private final Set<UUID> processedPayments = ConcurrentHashMap.newKeySet();

    public KafkaConsumerService(PaymentService paymentService, RaftNode raftNode) {
        this.paymentService = paymentService;
        this.raftNode = raftNode;
    }

    /**
     * Kafka Consumer with Raft specialization.
     * 
     * Optimization (Issue #3): Check leader status FIRST before parsing Kafka messages.
     * Followers skip message processing to reduce CPU overhead.
     * Only leaders actually consume and process Kafka events for Raft replication.
     */
    @KafkaListener(topics = "payments", groupId = "payment-group")
    public void consume(PaymentEvent event) {
        try {
            // ⚡ OPTIMIZATION: Early leader check to skip Kafka parsing on followers
            // This prevents unnecessary CPU usage on non-leader nodes
            if (raftNode.getState() != RaftNode.State.LEADER) {
                log.debug("Skipping Kafka event on follower node {} (payment: {}). Raft leader handles replication.",
                        serverPort, event.paymentId());
                return;
            }

            // Deduplication at ingestion layer
            if (!processedPayments.add(event.paymentId())) {
                log.debug("Duplicate payment {} ignored on leader node {}", event.paymentId(), serverPort);
                return;
            }

            log.info("Leader node {} consuming payment {} (amount={})", serverPort, event.paymentId(), event.amount());

            // Process and publish to Raft — only leader does this
            paymentService.processPayment(event);

            log.debug("Leader node {} completed Raft consensus for payment {}", serverPort, event.paymentId());
        } catch (Exception e) {
            log.error("CONSUMER ERROR on node {} for payment {}: {}", serverPort, event.paymentId(), e.getMessage(), e);
        }
    }
}
