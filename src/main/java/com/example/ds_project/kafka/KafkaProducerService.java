package com.example.ds_project.kafka;

import com.example.ds_project.raft.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private static final String TOPIC = "payments";

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;
    private final RaftNode raftNode;

    @Value("${raft.node.id}")
    private String nodeId;

    @Value("${node.url}")
    private String nodeUrl;

    @Value("${raft.quorum.size:3}")
    private int quorumSize;

    public KafkaProducerService(KafkaTemplate<String, PaymentEvent> kafkaTemplate, RaftNode raftNode) {
        this.kafkaTemplate = kafkaTemplate;
        this.raftNode = raftNode;
    }

    public PaymentResponse publishPayment(BigDecimal amount) {
        UUID paymentId = UUID.randomUUID();
        long ts = System.currentTimeMillis();
        PaymentEvent event = new PaymentEvent(paymentId, amount, ts, "PENDING");

        String raftStatus = "PENDING";

        // Publish to Kafka
        try {
            CompletableFuture<SendResult<String, PaymentEvent>> future =
                    kafkaTemplate.send(TOPIC, paymentId.toString(), event);
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("KAFKA SEND FAILED for payment {}: {}", paymentId, ex.getMessage());
                } else {
                    log.info("Payment {} delivered to Kafka topic='{}' partition={} offset={}",
                            paymentId, TOPIC,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });
        } catch (Exception e) {
            log.error("KAFKA SEND EXCEPTION for payment {}", paymentId, e);
            raftStatus = "KAFKA_ERROR";
        }

        // Raft metadata (informational only — does NOT block Kafka)
        String leaderNodeId = raftNode.getState() == RaftNode.State.LEADER ? nodeId : "unknown";
        String leaderUrl = raftNode.getState() == RaftNode.State.LEADER ? nodeUrl : "unknown";

        return PaymentResponse.builder()
                .paymentId(paymentId)
                .amount(amount)
                .timestamp(ts)
                .raftStatus(raftStatus)
                .raftLeaderNodeId(leaderNodeId)
                .raftLeaderUrl(leaderUrl)
                .replicatedToNodes(1)
                .quorumRequired(quorumSize)
                .consensusReached(false)
                .raftTerm(raftNode.getCurrentTerm())
                .logIndex(raftNode.getLastApplied())
                .kafkaTopic(TOPIC)
                .kafkaConsumerGroup("payment-group")
                .receivingNode(nodeId)
                .build();
    }
}
