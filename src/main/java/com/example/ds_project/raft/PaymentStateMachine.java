package com.example.ds_project.raft;

import com.example.ds_project.model.Payment;
import com.example.ds_project.repository.PaymentRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Step 6: Payment State Machine
 * 
 * Watches the Raft commitIndex. When it advances, it applies the newly 
 * committed log entries to the local PaymentRepository.
 */
@Service
public class PaymentStateMachine {
    private static final Logger log = LoggerFactory.getLogger(PaymentStateMachine.class);

    private final RaftNode raftNode;
    private final RaftLog raftLog;
    private final PaymentRepository repository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PaymentStateMachine(RaftNode raftNode, RaftLog raftLog, PaymentRepository repository) {
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.repository = repository;
    }

    // Temporarily Disabled for Milestone 1 (Pure Kafka Evaluation)
    // @Scheduled(fixedDelay = 200) 
    public void applyCommittedEntries() {
        long commitIndex = raftNode.getCommitIndex();
        long lastApplied = raftNode.getLastApplied();

        if (commitIndex > lastApplied) {
            log.debug("Applying committed entries from {} to {}", lastApplied + 1, commitIndex);
            for (long i = lastApplied + 1; i <= commitIndex; i++) {
                LogEntry entry = raftLog.getEntry(i);
                if (entry != null && entry.getPayload() != null) {
                    applyEntry(entry);
                }
                raftNode.setLastApplied(i);
            }
        }
    }

    private void applyEntry(LogEntry entry) {
        try {
            // Reconstruct the payment from the JSON data stored in the Raft log
            Payment payment = objectMapper.readValue(entry.getPayload(), Payment.class);
            
            // Deduplication: Check if this payment ID has already been applied to the state machine
            if (repository.findById(payment.getId()).isPresent()) {
                log.warn("Cluster Deduplication: Payment {} already exists in state machine (Log Index: {}). Skipping.", 
                        payment.getId(), entry.getIndex());
                return;
            }
            
            // Persist to the local ledger (State Machine)
            repository.save(payment);
            
            log.info("State Machine: Applied payment {} (Index: {})", payment.getId(), entry.getIndex());
        } catch (Exception e) {
            log.error("Failed to apply log entry at index {}", entry.getIndex(), e);
        }
    }
}
