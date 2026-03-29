package com.example.ds_project.kafka;

import com.example.ds_project.model.Payment;
import com.example.ds_project.raft.LogEntry;
import com.example.ds_project.raft.RaftLog;
import com.example.ds_project.raft.RaftNode;
import com.example.ds_project.raft.RaftLeaderManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KafkaConsumerService {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final RaftNode raftNode;
    private final RaftLog raftLog;
    private final RaftLeaderManager raftLeaderManager;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Task 3: Deduplication memory caching
    private final Set<UUID> processedPayments = ConcurrentHashMap.newKeySet();

    public KafkaConsumerService(RaftNode raftNode, RaftLog raftLog, RaftLeaderManager raftLeaderManager) {
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.raftLeaderManager = raftLeaderManager;
        // Registers LocalTime modules if needed, but our Payment uses LocalDateTime
        this.objectMapper.findAndRegisterModules(); 
    }

    @KafkaListener(topics = "payments", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(PaymentEvent event) {
        String state = raftNode.getState().name();
        log.info("[Node: {} | State: {}] Consumed payment {} from Kafka topic='payments'", 
                raftNode.getClass().getSimpleName().equals("RaftNode") ? System.getProperty("node.id", "local") : "node", 
                state, event.paymentId());

        if (!processedPayments.add(event.paymentId())) {
            log.debug("Payment {} already processed locally, skipping.", event.paymentId());
            return;
        }

        if (raftNode.getState() == RaftNode.State.LEADER) {
            try {
                log.info("I am LEADER. Packaging {} into Raft Log for consensus.", event.paymentId());
                
                // Phase 3b: Create payment with Time Synchronization metadata
                Payment payment = new Payment(
                        event.paymentId().toString(),
                        "cluster-consensus",
                        event.amount(),
                        "SUCCESS",
                        LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(event.timestamp()), ZoneOffset.UTC),
                        event.timestamp(),
                        event.clockOffsetApplied(),
                        event.publishingNodeId()
                );

                String payload = objectMapper.writeValueAsString(payment);
                
                LogEntry entry = new LogEntry(
                        raftLog.getLastLogIndex() + 1,
                        raftNode.getCurrentTerm(),
                        event.paymentId().toString(),
                        payload,
                        event.timestamp(),
                        LogEntry.LogStatus.PENDING
                );

                raftLog.appendEntry(entry);
                log.info("Successfully appended log entry {} for payment {}. Triggering replication.",
                        entry.getIndex(), event.paymentId());
                
                raftLeaderManager.replicateToAll();
                
            } catch (Exception e) {
                log.error("CRITICAL: Failed to append payment {} to Raft log", event.paymentId(), e);
            }
        } else {
            log.info("I am {}. Ignoring Kafka message {}; waiting for replication from the Leader.", 
                    state, event.paymentId());
        }
    }
}
