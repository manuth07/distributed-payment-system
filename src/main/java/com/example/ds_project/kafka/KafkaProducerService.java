package com.example.ds_project.kafka;

import com.example.ds_project.raft.RaftNode;
import com.example.ds_project.timesync.ClockSynchronizationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class KafkaProducerService {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private static final String TOPIC = "payments";

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;
    private final RaftNode raftNode;
    private final ClockSynchronizationService clockSyncService;

    @Value("${raft.node.id}")
    private String nodeId;

    @Value("${node.url}")
    private String nodeUrl;

    @Value("${raft.quorum.size:3}")
    private int quorumSize;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    public KafkaProducerService(KafkaTemplate<String, PaymentEvent> kafkaTemplate, RaftNode raftNode,
                                 ClockSynchronizationService clockSyncService) {
        this.kafkaTemplate = kafkaTemplate;
        this.raftNode = raftNode;
        this.clockSyncService = clockSyncService;
    }

    public PaymentResponse publishPayment(BigDecimal amount) {
        UUID paymentId = UUID.randomUUID();
        long rawTimestamp = System.currentTimeMillis();
        
        // Phase 3a: Apply clock offset correction BEFORE publishing to Kafka
        long clockOffset = clockSyncService.getCurrentOffset();
        long correctedTimestamp = rawTimestamp + clockOffset;
        
        log.debug("Publishing payment {}: rawTs={}, offset={}ms, correctedTs={}",
                paymentId, rawTimestamp, clockOffset, correctedTimestamp);
        
        PaymentEvent event = new PaymentEvent(
                paymentId,
                amount,
                correctedTimestamp,      // <- CORRECTED TIMESTAMP
                "PENDING",
                clockOffset,             // <- AUDIT TRAIL: offset applied
                nodeId                   // <- AUDIT TRAIL: publishing node
        );

        // Track whether Kafka delivery actually succeeded
        AtomicReference<Throwable> kafkaError = new AtomicReference<>();
        CompletableFuture<SendResult<String, PaymentEvent>> future =
                kafkaTemplate.send(TOPIC, paymentId.toString(), event);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                kafkaError.set(ex);
                log.error("Failed to deliver payment {} to Kafka: {}", paymentId, ex.getMessage());
            } else {
                log.info("Payment {} delivered → Kafka topic='{}' partition={} offset={}",
                        paymentId, TOPIC,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        // Gather real-time Raft metadata
        String leaderNodeId = determineLeaderNodeId();
        String leaderUrl    = determineLeaderUrl();
        int replicatedCount = countReplicatedNodes();
        boolean consensus   = replicatedCount >= quorumSize;
        long logIndex       = raftNode.getLastApplied(); // last applied index at this node

        return PaymentResponse.builder()
                .paymentId(paymentId)
                .amount(amount)
                .timestamp(correctedTimestamp)  // <- Use corrected timestamp in response
                .raftStatus(kafkaError.get() != null ? "KAFKA_ERROR" : "PENDING")
                .raftLeaderNodeId(leaderNodeId)
                .raftLeaderUrl(leaderUrl)
                .replicatedToNodes(replicatedCount)
                .quorumRequired(quorumSize)
                .consensusReached(consensus)
                .raftTerm(raftNode.getCurrentTerm())
                .logIndex(logIndex)
                .kafkaTopic(TOPIC)
                .kafkaConsumerGroup(consumerGroupId)
                .receivingNode(nodeId)
                .clockOffsetApplied(clockOffset)     // <- NEW: track offset in response
                .build();
    }

    private String determineLeaderNodeId() {
        if (raftNode.getState() == RaftNode.State.LEADER) {
            return nodeId;
        }
        // Derive leader node ID from the matchIndex keys that are fully up-to-date
        return "unknown (awaiting Raft sync)";
    }

    private String determineLeaderUrl() {
        if (raftNode.getState() == RaftNode.State.LEADER) {
            return nodeUrl;
        }
        return "unknown";
    }

    /**
     * Count how many peer nodes have a matchIndex >= our commitIndex
     * (i.e., they have received at least all entries this node has committed)
     */
    private int countReplicatedNodes() {
        long commitIndex = raftNode.getCommitIndex();
        // Self is always "replicated"
        int count = 1;
        for (long matchIdx : raftNode.getMatchIndex().values()) {
            if (matchIdx >= commitIndex) count++;
        }
        return count;
    }
}
