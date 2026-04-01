package com.example.ds_project.controller;

import com.example.ds_project.kafka.KafkaProducerService;
import com.example.ds_project.kafka.PaymentResponse;
import com.example.ds_project.model.Payment;
import com.example.ds_project.raft.RaftLog;
import com.example.ds_project.raft.RaftNode;
import com.example.ds_project.raft.LogEntry;
import com.example.ds_project.service.PaymentService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/payments")
public class PaymentController {
    private final PaymentService service;
    private final KafkaProducerService kafkaProducerService;
    private final RaftNode raftNode;
    private final RaftLog raftLog;

    @Value("${raft.node.id}")
    private String nodeId;

    @Value("${raft.quorum.size:3}")
    private int quorumSize;

    public PaymentController(PaymentService service,
                             KafkaProducerService kafkaProducerService,
                             RaftNode raftNode,
                             RaftLog raftLog) {
        this.service = service;
        this.kafkaProducerService = kafkaProducerService;
        this.raftNode = raftNode;
        this.raftLog = raftLog;
    }

    /**
     * POST /payments?amount=500
     * Publishes to Kafka and returns rich cluster metadata in the response.
     */
    @PostMapping
    public ResponseEntity<PaymentResponse> makePayment(@RequestParam BigDecimal amount) {
        PaymentResponse response = kafkaProducerService.publishPayment(amount);
        return ResponseEntity.ok(response);
    }

    /**
     * GET /payments
     * Returns all committed payments in this node's state machine.
     */
    @GetMapping
    public List<Payment> getAllPayments() {
        return service.getAllPayments();
    }

    @GetMapping("/count")
    public long getCount() {
        return service.getCount();
    }

    /**
     * GET /node/info
     * Returns: port, nodeId
     */
    @GetMapping("/node/info")
    public Map<String, String> getNodeInfo() {
        Map<String, String> info = new LinkedHashMap<>();
        info.put("port", System.getenv("SERVER_PORT")); // Better fallback might be possible but SERVER_PORT is set
        info.put("nodeId", nodeId);
        return info;
    }

    /**
     * GET /payments/cluster-status
     * Returns a live snapshot of this node's view of the entire Raft cluster.
     */
    @GetMapping("/cluster-status")
    public ResponseEntity<Map<String, Object>> clusterStatus() {
        Map<String, Object> status = new LinkedHashMap<>();

        // This node's own state
        status.put("thisNode", nodeId);
        status.put("raftState", raftNode.getState().name());
        status.put("raftTerm", raftNode.getCurrentTerm());
        status.put("commitIndex", raftNode.getCommitIndex());
        status.put("lastApplied", raftNode.getLastApplied());
        status.put("logSize", raftLog.getLastLogIndex() + 1);

        // Leader info
        boolean isLeader = raftNode.getState() == RaftNode.State.LEADER;
        status.put("isLeader", isLeader);

        // Replication status per peer (only meaningful on leader)
        Map<String, Object> peerStatus = new LinkedHashMap<>();
        raftNode.getNextIndex().forEach((peer, nextIdx) -> {
            Map<String, Long> peerData = new LinkedHashMap<>();
            peerData.put("nextIndex", nextIdx);
            peerData.put("matchIndex", raftNode.getMatchIndex().getOrDefault(peer, -1L));
            peerStatus.put(peer, peerData);
        });
        status.put("peerReplication", peerStatus);

        // Total replicated peers at commit index
        long commitIndex = raftNode.getCommitIndex();
        long replicatedPeers = raftNode.getMatchIndex().values()
                .stream().filter(m -> m >= commitIndex).count();
        status.put("replicatedNodeCount", replicatedPeers + 1); // +1 for self
        status.put("quorumRequired", quorumSize);
        status.put("quorumReached", (replicatedPeers + 1) >= quorumSize);

        // Recent Raft log entries (last 5)
        long lastIdx = raftLog.getLastLogIndex();
        List<Map<String, Object>> recentLog = new java.util.ArrayList<>();
        for (long i = Math.max(0, lastIdx - 4); i <= lastIdx; i++) {
            LogEntry e = raftLog.getEntry(i);
            if (e != null) {
                Map<String, Object> logEntry = new LinkedHashMap<>();
                logEntry.put("index", e.getIndex());
                logEntry.put("term", e.getTerm());
                logEntry.put("paymentId", e.getPaymentId());
                logEntry.put("status", e.getStatus());
                recentLog.add(logEntry);
            }
        }
        status.put("recentRaftLog", recentLog);

        return ResponseEntity.ok(status);
    }
}
