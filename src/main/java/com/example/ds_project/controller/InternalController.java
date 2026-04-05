package com.example.ds_project.controller;

import com.example.ds_project.model.ConsensusNodeResponse;
import com.example.ds_project.model.Payment;
import com.example.ds_project.raft.RaftNode;
import com.example.ds_project.repository.PaymentRepository;
import com.example.ds_project.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@RestController
@RequestMapping("/internal")
public class InternalController {
    private static final Logger log = LoggerFactory.getLogger(InternalController.class);

    private final PaymentRepository repository;
    private final PaymentService paymentService;
    private final RaftNode raftNode;

    @Value("${raft.node.id}")
    private String nodeId;

    public InternalController(PaymentRepository repository, PaymentService paymentService,
                              RaftNode raftNode) {
        this.repository = repository;
        this.paymentService = paymentService;
        this.raftNode = raftNode;
    }

    @PostMapping("/replicate")
    public void replicatePayment(@RequestBody Payment payment) {

        // Deduplication check
        if (repository.findById(payment.getId()).isPresent()) {
            return;
        }

        repository.save(payment);
    }

    //Fault tolerance base
    @GetMapping("/health")
    public String health() {
        return "OK";
    }

    /**
     * Part G: CONSISTENCY VALIDATION FEATURE
     * Internal endpoint to explicitly verify node state against the cluster.
     * This proves that local materialized views independently converged to the exact same dataset
     * from the authoritative Kafka log.
     */
    @GetMapping("/consistency-check")
    public java.util.Map<String, Object> getConsistencyCheck() {
        List<Payment> allPayments = repository.findAll();
        
        long maxOffset = allPayments.stream()
                .mapToLong(Payment::getKafkaOffset)
                .max()
                .orElse(-1);
                
        long maxCorrectedTimestamp = allPayments.stream()
                .mapToLong(Payment::getCorrectedTimestamp)
                .max()
                .orElse(-1);
                
        String datasetHash = computeTransactionHash(allPayments);

        java.util.Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("nodeId", nodeId);
        result.put("paymentCount", allPayments.size());
        result.put("maxKafkaOffset", maxOffset);
        result.put("maxCorrectedTimestamp", maxCorrectedTimestamp);
        result.put("datasetHash", datasetHash);
        
        return result;
    }

    /**
     * Internal endpoint for node-to-node user transaction queries.
     * Called by followers that forward user transaction history requests to this node (the leader).
     * Returns the leader's local transaction list for the given user.
     */
    @GetMapping("/users/{userId}/transactions")
    public List<Payment> getTransactionsByUserId(@PathVariable String userId) {
        return paymentService.getTransactionsByUserId(userId);
    }

    /**
     * Internal consensus endpoint for quorum-based retrieval validation.
     *
     * <p>Called by the ZooKeeper-elected leader during a quorum read. Each node returns:
     * <ul>
     *   <li>Its Raft metadata (nodeId, term, commitIndex, state) for freshness validation</li>
     *   <li>Its local user transactions for the requested userId</li>
     *   <li>A deterministic SHA-256 hash of the transaction list for data integrity</li>
     * </ul>
     *
     * <p>The leader uses term and commitIndex to filter out stale nodes, then merges
     * transactions from quorum-accepted nodes only.</p>
     *
     * <p><b>Note:</b> This is a quorum-based retrieval consensus layer over distributed
     * local repositories. It is NOT a true Raft linearizable read, because payment state
     * is stored via Kafka consumer partitioning, not via the Raft-applied state machine.</p>
     */
    @GetMapping("/users/{userId}/transactions/consensus")
    public ConsensusNodeResponse getConsensusTransactions(@PathVariable String userId) {
        List<Payment> transactions = paymentService.getTransactionsByUserId(userId);
        String dataHash = computeTransactionHash(transactions);

        return new ConsensusNodeResponse(
                nodeId,
                raftNode.getCurrentTerm(),
                raftNode.getCommitIndex(),
                raftNode.getState().name(),
                transactions.size(),
                dataHash,
                transactions
        );
    }

    /**
     * Computes a deterministic SHA-256 hash over stable transaction fields.
     *
     * <p>Uses id, userId, amount, status, and correctedTimestamp — fields that are
     * immutable once a payment is created. Object identity, toString, or insertion
     * order are explicitly avoided to ensure determinism across JVM instances.</p>
     *
     * @param transactions the list of transactions to hash
     * @return hex-encoded SHA-256 digest string
     */
    private String computeTransactionHash(List<Payment> transactions) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (Payment p : transactions) {
                // Build a canonical string from stable, immutable payment fields
                String canonical = String.join("|",
                        p.getId() != null ? p.getId() : "",
                        p.getUserId() != null ? p.getUserId() : "",
                        p.getAmount() != null ? p.getAmount().toPlainString() : "0",
                        p.getStatus() != null ? p.getStatus() : "",
                        String.valueOf(p.getCorrectedTimestamp())
                );
                digest.update(canonical.getBytes(StandardCharsets.UTF_8));
            }
            byte[] hashBytes = digest.digest();
            // Convert to hex string
            StringBuilder hex = new StringBuilder();
            for (byte b : hashBytes) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            log.error("SHA-256 not available", e);
            return "HASH_ERROR";
        }
    }
}