package com.example.ds_project.service;

import com.example.ds_project.kafka.PaymentResponse;
import com.example.ds_project.model.Payment;
import com.example.ds_project.raft.LogEntry;
import com.example.ds_project.raft.RaftLeaderManager;
import com.example.ds_project.raft.RaftLog;
import com.example.ds_project.raft.RaftNode;
import com.example.ds_project.timesync.ClockSynchronizationService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class PaymentConsensusService {
    private static final Logger log = LoggerFactory.getLogger(PaymentConsensusService.class);

    private final RaftNode raftNode;
    private final RaftLog raftLog;
    private final RaftLeaderManager raftLeaderManager;
    private final ClockSynchronizationService clockSyncService;
    private final ObjectMapper objectMapper;

    @Value("${raft.node.id}")
    private String nodeId;

    @Value("${node.url}")
    private String nodeUrl;

    @Value("${raft.quorum.size:3}")
    private int quorumSize;

    public PaymentConsensusService(RaftNode raftNode, RaftLog raftLog, RaftLeaderManager raftLeaderManager, ClockSynchronizationService clockSyncService) {
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.raftLeaderManager = raftLeaderManager;
        this.clockSyncService = clockSyncService;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules(); // Support java.time
    }

    public PaymentResponse appendAndReplicate(BigDecimal amount, String userId) {
        if (raftNode.getState() != RaftNode.State.LEADER) {
            throw new IllegalStateException("Node is not the Raft leader. Cannot accept payment storage writes.");
        }

        UUID paymentId = UUID.randomUUID();
        long rawTs = System.currentTimeMillis();
        long clockOffset = clockSyncService.getCurrentOffset();
        long correctedTs = rawTs + clockOffset;

        Payment payment = new Payment(
                paymentId.toString(),
                userId,
                nodeId,
                amount,
                "SUCCESS",
                LocalDateTime.now(),
                correctedTs,
                clockOffset,
                nodeId
        );

        String payload;
        try {
            payload = objectMapper.writeValueAsString(payment);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize payment", e);
            throw new RuntimeException("Serialization failure", e);
        }

        long logIndex;
        long term;

        ReentrantLock lock = raftNode.getLock();
        lock.lock();
        try {
            term = raftNode.getCurrentTerm();
            logIndex = raftLog.getLastLogIndex() + 1;

            LogEntry entry = new LogEntry(logIndex, term, paymentId.toString(), payload, rawTs, LogEntry.LogStatus.PENDING);
            entry.setUserId(userId);
            entry.setAmount(amount);
            entry.setCorrectedTimestamp(correctedTs);
            entry.setClockOffsetAtCreation(clockOffset);
            entry.setPublishingNodeId(nodeId);

            raftLog.appendEntry(entry);
        } finally {
            lock.unlock();
        }

        log.info("Appended payment {} to Raft log at index {}, term {}", paymentId, logIndex, term);

        // Actively trigger replication to followers
        raftLeaderManager.replicateToAll();

        // Wait for consensus (quorum ACK)
        boolean consensusReached = waitForConsensus(logIndex, 5000); // Wait up to 5 seconds

        if (!consensusReached) {
            log.warn("Consensus failed or timed out for payment {} at index {}. Not committed.", paymentId, logIndex);
            return buildResponse(payment, logIndex, term, false, "QUORUM_FAILED");
        }

        log.info("Consensus REACHED for payment {} at index {}. Advance commitIndex triggered.", paymentId, logIndex);
        return buildResponse(payment, logIndex, term, true, "COMMITTED");
    }

    private boolean waitForConsensus(long expectedIndex, long timeoutMs) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            if (raftNode.getCommitIndex() >= expectedIndex) {
                return true;
            }
            try {
                Thread.sleep(10); // Polling interval
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private PaymentResponse buildResponse(Payment payment, long logIndex, long term, boolean consensusReached, String status) {
        long replicatedNodes = raftNode.getMatchIndex().values().stream().filter(idx -> idx >= logIndex).count() + 1; // +1 for self

        return PaymentResponse.builder()
                .paymentId(UUID.fromString(payment.getId()))
                .amount(payment.getAmount())
                .timestamp(payment.getCorrectedTimestamp())
                .raftStatus(status)
                .raftLeaderNodeId(nodeId)
                .raftLeaderUrl(nodeUrl)
                .replicatedToNodes((int) replicatedNodes)
                .quorumRequired(quorumSize)
                .consensusReached(consensusReached)
                .raftTerm(term)
                .logIndex(logIndex)
                .kafkaTopic("N/A - Raft Authoritative") // Show it's using Raft now
                .kafkaConsumerGroup("N/A")
                .receivingNode(nodeId)
                .clockOffsetApplied(payment.getClockOffsetAtCreation())
                .build();
    }
}
