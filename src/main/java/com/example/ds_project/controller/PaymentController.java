package com.example.ds_project.controller;

import com.example.ds_project.config.ClusterConfig;
import com.example.ds_project.coordination.LeaderState;
import com.example.ds_project.kafka.KafkaProducerService;
import com.example.ds_project.kafka.PaymentResponse;
import com.example.ds_project.model.ConsensusNodeResponse;
import com.example.ds_project.model.Payment;
import com.example.ds_project.raft.RaftLog;
import com.example.ds_project.raft.RaftNode;
import com.example.ds_project.raft.LogEntry;
import com.example.ds_project.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/payments")
public class PaymentController {
    private static final Logger log = LoggerFactory.getLogger(PaymentController.class);

    /**
     * Maximum allowed gap between a node's commitIndex and the highest observed commitIndex
     * across all responding nodes. Nodes whose commitIndex lags by more than this threshold
     * are considered stale and excluded from the quorum.
     *
     * <p>A value of 1 means a node can be at most 1 index behind the most up-to-date node
     * and still be accepted. This accounts for minor replication delays between heartbeat
     * cycles without allowing significantly stale data into the consensus set.</p>
     */
    private static final long COMMIT_INDEX_STALENESS_THRESHOLD = 1;

    private final PaymentService service;
    private final KafkaProducerService kafkaProducerService;
    private final RaftNode raftNode;
    private final RaftLog raftLog;
    private final LeaderState leaderState;
    private final ClusterConfig clusterConfig;
    private final RestTemplate restTemplate;

    @Value("${raft.node.id}")
    private String nodeId;

    @Value("${node.url}")
    private String nodeUrl;

    @Value("${raft.quorum.size:3}")
    private int quorumSize;

    public PaymentController(PaymentService service,
                             KafkaProducerService kafkaProducerService,
                             RaftNode raftNode,
                             RaftLog raftLog,
                             LeaderState leaderState,
                             ClusterConfig clusterConfig,
                             RestTemplate restTemplate) {
        this.service = service;
        this.kafkaProducerService = kafkaProducerService;
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.leaderState = leaderState;
        this.clusterConfig = clusterConfig;
        this.restTemplate = restTemplate;
    }

    /**
     * POST /payments?amount=500&userId=user123
     * Publishes to Kafka and returns rich cluster metadata in the response.
     * userId is optional — defaults to "anonymous" if not provided.
     */
    @PostMapping
    public ResponseEntity<PaymentResponse> makePayment(
            @RequestParam BigDecimal amount,
            @RequestParam(required = false, defaultValue = "anonymous") String userId) {
        PaymentResponse response = kafkaProducerService.publishPayment(amount, userId);
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
        info.put("port", System.getenv("SERVER_PORT"));
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

    /**
     * Phase 3c: GET /payments/by-timestamp?startMs=X&endMs=Y
     */
    @GetMapping("/by-timestamp")
    public ResponseEntity<List<Payment>> getPaymentsByTimestampRange(
            @RequestParam(required = false) Long startMs,
            @RequestParam(required = false) Long endMs) {
        if (startMs == null || endMs == null) {
            return ResponseEntity.badRequest().body(List.of());
        }
        List<Payment> payments = service.getPaymentsByTimestampRange(startMs, endMs);
        return ResponseEntity.ok(payments);
    }

    /**
     * Phase 3c: GET /payments/by-node?nodeId=node1
     */
    @GetMapping("/by-node")
    public ResponseEntity<List<Payment>> getPaymentsByNode(@RequestParam String nodeId) {
        List<Payment> payments = service.getPaymentsByNodeId(nodeId);
        return ResponseEntity.ok(payments);
    }

    /**
     * Phase 3c: GET /payments/by-publishing-node?publishingNodeId=node2
     */
    @GetMapping("/by-publishing-node")
    public ResponseEntity<List<Payment>> getPaymentsByPublishingNode(@RequestParam String publishingNodeId) {
        List<Payment> payments = service.getPaymentsByPublishingNodeId(publishingNodeId);
        return ResponseEntity.ok(payments);
    }

    /**
     * Phase 3c: GET /payments/statistics
     */
    @GetMapping("/statistics")
    public ResponseEntity<Map<String, Object>> getPaymentStatistics() {
        Map<String, Object> stats = service.getPaymentStatistics();
        return ResponseEntity.ok(stats);
    }

    // ═════════════════════════════════════════════════════════════════════════════
    // User Transaction History with Quorum-Based Retrieval Consensus
    // ═════════════════════════════════════════════════════════════════════════════

    /**
     * GET /payments/users/{userId}/transactions
     *
     * <p>Retrieves the transaction history for a specific user using <b>quorum-based
     * retrieval consensus</b>. This provides stronger consistency than simple leader
     * aggregation by validating the freshness of each peer node's Raft metadata
     * before including its data in the merged result.</p>
     *
     * <h3>Consistency Model</h3>
     * <ul>
     *   <li><b>Follower path:</b> Forwards the request to the ZooKeeper-elected leader.</li>
     *   <li><b>Leader path:</b> Queries all cluster nodes via the internal consensus endpoint,
     *       validates each response's Raft term and commitIndex against the cluster-wide maximum,
     *       requires a quorum (≥3 of 5 nodes) of fresh responses, then merges and deduplicates
     *       the accepted transactions.</li>
     * </ul>
     *
     * <h3>Staleness filtering</h3>
     * <p>A node's response is <b>accepted</b> if and only if:</p>
     * <ol>
     *   <li>Its Raft term equals the highest term observed across all responses</li>
     *   <li>Its commitIndex is within {@link #COMMIT_INDEX_STALENESS_THRESHOLD} of the
     *       highest commitIndex observed across all responses</li>
     * </ol>
     *
     * <p><b>Important:</b> This is a quorum-validated read over distributed local repositories.
     * It is NOT a true Raft linearizable read, because payment state is stored via Kafka
     * consumer partitioning, not via the Raft-applied state machine.</p>
     *
     * @param userId the user whose transaction history to retrieve
     * @param status optional filter by payment status (e.g., SUCCESS, PENDING)
     * @param from   optional filter: correctedTimestamp >= from (epoch millis)
     * @param to     optional filter: correctedTimestamp <= to (epoch millis)
     * @return JSON with transactions and consensus metadata
     */
    @GetMapping("/users/{userId}/transactions")
    public ResponseEntity<Map<String, Object>> getUserTransactions(
            @PathVariable String userId,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) Long from,
            @RequestParam(required = false) Long to) {

        // ── Step 1: Follower path — forward to leader ─────────────────────────
        if (!leaderState.isLeader()) {
            return forwardToLeader(userId, status, from, to);
        }

        // ── Step 2: Leader path — quorum-based consensus read ─────────────────
        return buildConsensusResponse(userId, status, from, to);
    }

    /**
     * Forwards the user transaction history request to the ZooKeeper-elected leader.
     * Uses the same RestTemplate pattern established in ReplicationService.
     */
    private ResponseEntity<Map<String, Object>> forwardToLeader(
            String userId, String status, Long from, Long to) {

        String leaderUrl = leaderState.getLeaderUrl();
        if (leaderUrl == null || leaderUrl.isEmpty()) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("error", "NO_LEADER_ELECTED");
            error.put("message", "No leader is currently elected. Please retry shortly.");
            error.put("sourceNode", nodeId);
            error.put("raftState", raftNode.getState().name());
            error.put("term", raftNode.getCurrentTerm());
            return ResponseEntity.status(503).body(error);
        }

        try {
            StringBuilder url = new StringBuilder(leaderUrl)
                    .append("/payments/users/").append(userId).append("/transactions");

            List<String> params = new ArrayList<>();
            if (status != null) params.add("status=" + status);
            if (from != null)   params.add("from=" + from);
            if (to != null)     params.add("to=" + to);
            if (!params.isEmpty()) {
                url.append("?").append(String.join("&", params));
            }

            log.info("Forwarding user transaction request for userId={} to leader at {}",
                    userId, leaderUrl);

            ResponseEntity<Map<String, Object>> leaderResponse = restTemplate.exchange(
                    url.toString(),
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<Map<String, Object>>() {}
            );

            Map<String, Object> body = new LinkedHashMap<>();
            if (leaderResponse.getBody() != null) {
                body.putAll(leaderResponse.getBody());
            }
            body.put("forwardedBy", nodeId);
            body.put("forwardedFrom", nodeUrl);

            return ResponseEntity.ok(body);

        } catch (Exception e) {
            log.error("Failed to forward transaction request to leader at {}: {}",
                    leaderUrl, e.getMessage());

            Map<String, Object> fallback = buildLocalResponse(userId, status, from, to);
            fallback.put("warning", "LEADER_UNREACHABLE");
            fallback.put("message", "Failed to reach leader at " + leaderUrl +
                    ". Returning local (potentially stale) data.");
            fallback.put("consensusReached", false);
            return ResponseEntity.ok(fallback);
        }
    }

    /**
     * Leader-side: Performs quorum-based retrieval consensus across all cluster nodes.
     *
     * <p>Algorithm:</p>
     * <ol>
     *   <li>Query all nodes (including self) via {@code GET /internal/users/{userId}/transactions/consensus}</li>
     *   <li>Collect {@link ConsensusNodeResponse} from each reachable node</li>
     *   <li>Determine the highest observed Raft term and commitIndex</li>
     *   <li>Accept only responses where {@code term == highestTerm} AND
     *       {@code commitIndex >= maxCommitIndex - COMMIT_INDEX_STALENESS_THRESHOLD}</li>
     *   <li>If accepted responses &lt; {@link #quorumSize}, return HTTP 503</li>
     *   <li>Otherwise, merge transactions from accepted nodes, deduplicate by payment ID,
     *       apply optional filters, sort, and return</li>
     * </ol>
     */
    private ResponseEntity<Map<String, Object>> buildConsensusResponse(
            String userId, String status, Long from, Long to) {

        // ── Phase 1: Collect responses from all cluster nodes ─────────────────
        List<ConsensusNodeResponse> allResponses = new ArrayList<>();
        List<String> unreachableNodes = new ArrayList<>();

        for (String node : clusterConfig.getAllNodes()) {
            try {
                String url = node + "/internal/users/" + userId + "/transactions/consensus";
                ResponseEntity<ConsensusNodeResponse> resp = restTemplate.exchange(
                        url,
                        HttpMethod.GET,
                        null,
                        new ParameterizedTypeReference<ConsensusNodeResponse>() {}
                );
                if (resp.getBody() != null) {
                    allResponses.add(resp.getBody());
                    log.debug("Consensus response from {}: term={}, commitIndex={}, count={}, hash={}",
                            node, resp.getBody().getTerm(), resp.getBody().getCommitIndex(),
                            resp.getBody().getCount(), resp.getBody().getDataHash());
                }
            } catch (Exception e) {
                log.warn("Consensus: node {} unreachable: {}", node, e.getMessage());
                unreachableNodes.add(node);
            }
        }

        int responsesReceived = allResponses.size();

        // ── Phase 2: Determine cluster-wide freshness baseline ───────────────
        long highestTerm = allResponses.stream()
                .mapToLong(ConsensusNodeResponse::getTerm)
                .max()
                .orElse(-1);

        long maxCommitIndex = allResponses.stream()
                .mapToLong(ConsensusNodeResponse::getCommitIndex)
                .max()
                .orElse(-1);

        // ── Phase 3: Filter stale responses ──────────────────────────────────
        //
        // Acceptance criteria:
        //   1) term == highestTerm                       (node is in the current election epoch)
        //   2) commitIndex >= maxCommitIndex - threshold (node's log is sufficiently replicated)
        //
        // The commitIndex threshold (default: 1) absorbs minor replication lag between
        // heartbeat cycles. A node that is 0 or 1 index behind the leader is likely
        // processing the same committed data; a node more than 1 behind may have missed
        // recent commits and could serve stale reads.

        long commitFloor = maxCommitIndex - COMMIT_INDEX_STALENESS_THRESHOLD;

        List<ConsensusNodeResponse> acceptedResponses = new ArrayList<>();
        List<Map<String, Object>> rejectedDetails = new ArrayList<>();

        for (ConsensusNodeResponse resp : allResponses) {
            boolean termOk = resp.getTerm() == highestTerm;
            boolean commitOk = resp.getCommitIndex() >= commitFloor;

            if (termOk && commitOk) {
                acceptedResponses.add(resp);
            } else {
                // Record why this node was rejected for observability
                Map<String, Object> rejectInfo = new LinkedHashMap<>();
                rejectInfo.put("nodeId", resp.getNodeId());
                rejectInfo.put("term", resp.getTerm());
                rejectInfo.put("commitIndex", resp.getCommitIndex());
                List<String> reasons = new ArrayList<>();
                if (!termOk) reasons.add("term " + resp.getTerm() + " != highestTerm " + highestTerm);
                if (!commitOk) reasons.add("commitIndex " + resp.getCommitIndex()
                        + " < commitFloor " + commitFloor);
                rejectInfo.put("reasons", reasons);
                rejectedDetails.add(rejectInfo);

                log.info("Consensus: rejected node {} (term={}, commitIndex={}, reasons={})",
                        resp.getNodeId(), resp.getTerm(), resp.getCommitIndex(), reasons);
            }
        }

        int responsesAccepted = acceptedResponses.size();
        List<String> acceptedNodeIds = acceptedResponses.stream()
                .map(ConsensusNodeResponse::getNodeId)
                .collect(Collectors.toList());

        // ── Phase 4: Quorum check ────────────────────────────────────────────
        if (responsesAccepted < quorumSize) {
            log.warn("Consensus FAILED for userId={}: accepted={}/{}, required={}",
                    userId, responsesAccepted, responsesReceived, quorumSize);

            Map<String, Object> error = new LinkedHashMap<>();
            error.put("error", "CONSENSUS_NOT_REACHED");
            error.put("message", "Insufficient fresh nodes to form a quorum. " +
                    "Only " + responsesAccepted + " of " + responsesReceived +
                    " responding nodes passed staleness check (need " + quorumSize + ").");
            error.put("userId", userId);
            error.put("sourceNode", nodeId);
            error.put("quorumRequired", quorumSize);
            error.put("responsesReceived", responsesReceived);
            error.put("responsesAccepted", responsesAccepted);
            error.put("highestTerm", highestTerm);
            error.put("maxCommitIndex", maxCommitIndex);
            error.put("commitIndexStalenessThreshold", COMMIT_INDEX_STALENESS_THRESHOLD);
            error.put("acceptedNodes", acceptedNodeIds);
            error.put("rejectedNodes", rejectedDetails);
            error.put("unreachableNodes", unreachableNodes);
            return ResponseEntity.status(503).body(error);
        }

        // ── Phase 5: Merge, deduplicate, filter, sort ────────────────────────
        log.info("Consensus REACHED for userId={}: accepted={}/{}, term={}, maxCommitIndex={}",
                userId, responsesAccepted, responsesReceived, highestTerm, maxCommitIndex);

        // Merge all transactions from accepted nodes
        List<Payment> mergedTransactions = new ArrayList<>();
        for (ConsensusNodeResponse accepted : acceptedResponses) {
            if (accepted.getTransactions() != null) {
                mergedTransactions.addAll(accepted.getTransactions());
            }
        }

        // Deduplicate by payment ID
        Map<String, Payment> deduped = new LinkedHashMap<>();
        for (Payment p : mergedTransactions) {
            deduped.putIfAbsent(p.getId(), p);
        }
        List<Payment> dedupedList = new ArrayList<>(deduped.values());

        // Apply optional filters
        if (status != null && !status.isEmpty()) {
            dedupedList = dedupedList.stream()
                    .filter(p -> status.equalsIgnoreCase(p.getStatus()))
                    .collect(Collectors.toList());
        }
        if (from != null) {
            dedupedList = dedupedList.stream()
                    .filter(p -> p.getCorrectedTimestamp() >= from)
                    .collect(Collectors.toList());
        }
        if (to != null) {
            dedupedList = dedupedList.stream()
                    .filter(p -> p.getCorrectedTimestamp() <= to)
                    .collect(Collectors.toList());
        }

        // Sort by correctedTimestamp descending (newest first)
        dedupedList.sort(Comparator.comparingLong(Payment::getCorrectedTimestamp).reversed());

        // ── Phase 6: Build response with consensus metadata ──────────────────
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("userId", userId);
        response.put("sourceNode", nodeId);
        response.put("leaderNode", nodeId);
        response.put("consensusReached", true);
        response.put("quorumRequired", quorumSize);
        response.put("responsesReceived", responsesReceived);
        response.put("responsesAccepted", responsesAccepted);
        response.put("highestTerm", highestTerm);
        response.put("maxCommitIndex", maxCommitIndex);
        response.put("commitIndexStalenessThreshold", COMMIT_INDEX_STALENESS_THRESHOLD);
        response.put("acceptedNodes", acceptedNodeIds);
        response.put("rejectedNodes", rejectedDetails);
        response.put("unreachableNodes", unreachableNodes);
        response.put("count", dedupedList.size());
        response.put("transactions", dedupedList);

        return ResponseEntity.ok(response);
    }

    /**
     * Builds a response from LOCAL data only (used as fallback when leader is unreachable).
     */
    private Map<String, Object> buildLocalResponse(
            String userId, String status, Long from, Long to) {

        List<Payment> transactions = service.getTransactionsByUserId(userId);

        if (status != null && !status.isEmpty()) {
            transactions = transactions.stream()
                    .filter(p -> status.equalsIgnoreCase(p.getStatus()))
                    .collect(Collectors.toList());
        }
        if (from != null) {
            transactions = transactions.stream()
                    .filter(p -> p.getCorrectedTimestamp() >= from)
                    .collect(Collectors.toList());
        }
        if (to != null) {
            transactions = transactions.stream()
                    .filter(p -> p.getCorrectedTimestamp() <= to)
                    .collect(Collectors.toList());
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("userId", userId);
        response.put("sourceNode", nodeId);
        response.put("leaderNode", leaderState.getLeaderUrl());
        response.put("isLeader", false);
        response.put("term", raftNode.getCurrentTerm());
        response.put("commitIndex", raftNode.getCommitIndex());
        response.put("raftState", raftNode.getState().name());
        response.put("count", transactions.size());
        response.put("transactions", transactions);

        return response;
    }
}
