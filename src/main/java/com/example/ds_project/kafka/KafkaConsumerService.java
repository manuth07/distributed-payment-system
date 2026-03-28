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
        if (!processedPayments.add(event.paymentId())) {
            return;  // Deduplication: skip if already processed
        }

        log.info("Consumed payment {} from Kafka stream (published by {}, offset applied: {}ms)",
                event.paymentId(), event.publishingNodeId(), event.clockOffsetApplied());

        if (raftNode.getState() == RaftNode.State.LEADER) {
            try {
                log.info("I am LEADER. Packaging {} into Raft Log for consensus.", event.paymentId());
                
                // Create a success-intent payment object
                // Note: event.timestamp() is already corrected by ClockSynchronizationService in producer
                Payment payment = new Payment(
                        event.paymentId().toString(),
                        "cluster-consensus",
                        event.amount(),
                        "SUCCESS",
                        LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(event.timestamp()), ZoneOffset.UTC)
                );

                String payload = objectMapper.writeValueAsString(payment);
                
                // Create LogEntry with the event's timestamp (already corrected)
                // This ensures Raft log entries also have corrected timestamps
                LogEntry entry = new LogEntry(
                        raftLog.getLastLogIndex() + 1,
                        raftNode.getCurrentTerm(),
                        event.paymentId().toString(),
                        payload,
                        event.timestamp(),  // <- Use corrected timestamp from event
                        LogEntry.LogStatus.PENDING
                );

                raftLog.appendEntry(entry);
                log.info("Appended log entry {} for payment {} (timestamp: {}ms)",
                        entry.getIndex(), event.paymentId(), event.timestamp());
                
                // Speed up consensus by triggering replication immediately
                raftLeaderManager.replicateToAll();
                
            } catch (Exception e) {
                log.error("Failed to append payment {} to Raft log", event.paymentId(), e);
            }
        } else {
            log.info("I am {}. Acknowledged payment {} but waiting for replication from Leader.", 
                    raftNode.getState(), event.paymentId());
        }
    }
}
