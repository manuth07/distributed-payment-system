package com.example.ds_project.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Step 5: Raft Leader Manager
 *
 * Responsibilities:
 *  1. Maintain an election timeout; if no heartbeat is received within it, start an election.
 *  2. When LEADER: send periodic heartbeats (AppendEntries with no entries) to all peers.
 *  3. Coordinate vote collection via RequestVote RPCs.
 */
@Component
public class RaftLeaderManager {
    private static final Logger log = LoggerFactory.getLogger(RaftLeaderManager.class);

    private final RaftNode raftNode;
    private final RaftLog raftLog;
    private final RestTemplate restTemplate;

    @Value("${raft.node.id}")
    private String nodeId;

    @Value("${node.url}")
    private String nodeUrl;

    @Value("#{'${raft.peers}'.split(',')}")
    private List<String> peers;

    @Value("${raft.heartbeat.interval.ms:100}")
    private long heartbeatIntervalMs;

    @Value("${raft.election.timeout.min.ms:300}")
    private long electionTimeoutMin;

    @Value("${raft.election.timeout.max.ms:600}")
    private long electionTimeoutMax;

    @Value("${raft.quorum.size:3}")
    private int quorumSize;

    /** When the election clock was last reset (ms since epoch) */
    private final AtomicLong lastHeartbeatTime = new AtomicLong(System.currentTimeMillis());

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ExecutorService ioPool = Executors.newCachedThreadPool();

    private volatile long currentElectionTimeout;

    public RaftLeaderManager(RaftNode raftNode, RaftLog raftLog, RestTemplateBuilder builder) {
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.restTemplate = builder
                .setConnectTimeout(Duration.ofMillis(200))
                .setReadTimeout(Duration.ofMillis(500))
                .build();
    }

    @PostConstruct
    public void start() {
        resetElectionTimeout();
        // 1. Election-timer loop (runs every 50ms, light CPU cost)
        scheduler.scheduleAtFixedRate(this::checkElectionTimeout, 150, 50, TimeUnit.MILLISECONDS);
        // 2. Heartbeat sender loop (runs every heartbeatIntervalMs when LEADER)
        scheduler.scheduleAtFixedRate(this::maybeSendHeartbeats, 150, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
        log.info("[{}] RaftLeaderManager started. Election timeout: {}ms", nodeId, currentElectionTimeout);
    }

    @PreDestroy
    public void stop() {
        scheduler.shutdownNow();
        ioPool.shutdownNow();
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Called externally by RaftRpcService when a valid AppendEntries is received
    // ─────────────────────────────────────────────────────────────────────────────
    public void resetElectionTimeout() {
        currentElectionTimeout = electionTimeoutMin +
                (long) (Math.random() * (electionTimeoutMax - electionTimeoutMin));
        lastHeartbeatTime.set(System.currentTimeMillis());
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Election Timeout Check
    // ─────────────────────────────────────────────────────────────────────────────
    private void checkElectionTimeout() {
        if (raftNode.getState() == RaftNode.State.LEADER) {
            return; // Leaders don't time out
        }
        long elapsed = System.currentTimeMillis() - lastHeartbeatTime.get();
        if (elapsed >= currentElectionTimeout) {
            log.info("[{}] Election timeout after {}ms. Starting election!", nodeId, elapsed);
            startElection();
        }
    }

    /** Step 6: Trigger immediate replication of new entries for the Leader */
    public void replicateToAll() {
        if (raftNode.getState() != RaftNode.State.LEADER) return;
        maybeSendHeartbeats(); // Trigger replication cycle immediately
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Leader Heartbeat Sender
    // ─────────────────────────────────────────────────────────────────────────────
    private void maybeSendHeartbeats() {
        if (raftNode.getState() != RaftNode.State.LEADER) {
            return;
        }
        // Send AppendEntries to every peer in parallel
        for (String peer : peers) {
            if (peer.equals(nodeUrl)) continue; // Skip self
            ioPool.submit(() -> replicateToPeer(peer));
        }
        // Step 6: Periodically check if we can advance commitIndex based on peer responses
        advanceCommitIndex();
    }

    private void replicateToPeer(String peer) {
        try {
            long lastIndex = raftLog.getLastLogIndex();
            long nextIdx = raftNode.getNextIndex().getOrDefault(peer, 1L);
            
            // If the follower is behind, send the entries starting from nextIndex
            List<LogEntry> entriesToSend = null;
            if (lastIndex >= nextIdx) {
                entriesToSend = new ArrayList<>();
                for (long i = nextIdx; i <= lastIndex; i++) {
                    LogEntry e = raftLog.getEntry(i);
                    if (e != null) entriesToSend.add(e);
                }
            }

            long prevLogIndex = nextIdx - 1;
            long prevLogTerm = 0;
            if (prevLogIndex >= 0) {
                LogEntry prev = raftLog.getEntry(prevLogIndex);
                if (prev != null) prevLogTerm = prev.getTerm();
            }

            AppendEntriesRequest request = new AppendEntriesRequest(
                    raftNode.getCurrentTerm(),
                    nodeId,
                    prevLogIndex,
                    prevLogTerm,
                    entriesToSend,
                    raftNode.getCommitIndex()
            );

            AppendEntriesResponse resp = restTemplate.postForObject(peer + "/raft/append-entries", request, AppendEntriesResponse.class);
            
            if (resp != null) {
                if (resp.term() > raftNode.getCurrentTerm()) {
                    raftNode.setCurrentTerm(resp.term());
                    raftNode.setVotedFor(null);
                    raftNode.setState(RaftNode.State.FOLLOWER);
                    return;
                }

                if (resp.success()) {
                    // Replication succeeded for this chunk
                    long newNext = nextIdx + (entriesToSend == null ? 0 : entriesToSend.size());
                    raftNode.getNextIndex().put(peer, newNext);
                    raftNode.getMatchIndex().put(peer, newNext - 1);
                } else {
                    // Log mismatch; decrement nextIndex and retry later (§5.3)
                    long conflictIdx = resp.conflictIndex();
                    raftNode.getNextIndex().put(peer, Math.max(0, conflictIdx));
                }
            }
        } catch (Exception e) {
            log.debug("[{}] Replication/Heartbeat to {} failed: {}", nodeId, peer, e.getMessage());
        }
    }

    private void advanceCommitIndex() {
        long currentCommit = raftNode.getCommitIndex();
        long lastIndex = raftLog.getLastLogIndex();

        // Raft paper §5.4.1: Find N > commitIndex such that majority of matchIndex[i] >= N
        for (long n = lastIndex; n > currentCommit; n--) {
            LogEntry entry = raftLog.getEntry(n);
            if (entry == null || entry.getTerm() != raftNode.getCurrentTerm()) {
                continue; // Only commit entries from the current term (§5.4)
            }

            int count = 1; // Count self
            for (String peer : peers) {
                if (peer.equals(nodeUrl)) continue;
                if (raftNode.getMatchIndex().getOrDefault(peer, -1L) >= n) {
                    count++;
                }
            }

            if (count >= quorumSize) {
                log.info("[{}] Consensus reached on index {}. Updating commitIndex.", nodeId, n);
                raftNode.setCommitIndex(n);
                break;
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Raft Paper §5.2: Leader Election
    // ─────────────────────────────────────────────────────────────────────────────
    private void startElection() {
        ReentrantLock lock = raftNode.getLock();  // Use existing public lock on RaftNode
        if (!lock.tryLock()) return; // Another thread already handling election
        try {
            // Double-check we're still eligible
            if (raftNode.getState() == RaftNode.State.LEADER) return;

            raftNode.setState(RaftNode.State.CANDIDATE);
            long newTerm = raftNode.getCurrentTerm() + 1;
            raftNode.setCurrentTerm(newTerm);
            raftNode.setVotedFor(nodeId); // Vote for self
            resetElectionTimeout();

            log.info("[{}] Started election for term {}", nodeId, newTerm);

            AtomicInteger votes = new AtomicInteger(1); // Count self-vote
            CountDownLatch latch = new CountDownLatch(peers.size() - 1);

            RequestVoteRequest voteReq = new RequestVoteRequest(
                    newTerm, nodeId,
                    raftLog.getLastLogIndex(),
                    raftLog.getLastLogTerm()
            );

            for (String peer : peers) {
                if (peer.equals(nodeUrl)) { latch.countDown(); continue; }
                final String p = peer;
                ioPool.submit(() -> {
                    try {
                        RequestVoteResponse resp = restTemplate.postForObject(
                                p + "/raft/request-vote", voteReq, RequestVoteResponse.class);
                        if (resp != null && resp.voteGranted()) {
                            int total = votes.incrementAndGet();
                            log.info("[{}] Vote granted from {}. Total: {}/{}", nodeId, p, total, quorumSize);
                        } else if (resp != null && resp.term() > newTerm) {
                            // Discovered a higher term — revert to follower
                            raftNode.setCurrentTerm(resp.term());
                            raftNode.setVotedFor(null);
                            raftNode.setState(RaftNode.State.FOLLOWER);
                        }
                    } catch (Exception e) {
                        log.debug("[{}] RequestVote to {} failed: {}", nodeId, p, e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
            }

            // Wait up to half election timeout for votes to come back
            latch.await(currentElectionTimeout / 2, TimeUnit.MILLISECONDS);

            // Check if we won (only if still candidate & term didn't change)
            if (raftNode.getState() == RaftNode.State.CANDIDATE
                    && raftNode.getCurrentTerm() == newTerm
                    && votes.get() >= quorumSize) {
                raftNode.setState(RaftNode.State.LEADER);
                raftNode.initializeLeaderState(peers, raftLog.getLastLogIndex());
                log.info("[{}] *** BECAME RAFT LEADER for term {} with {}/{} votes ***",
                        nodeId, newTerm, votes.get(), quorumSize);
                // Immediately assert leadership with heartbeats
                maybeSendHeartbeats();
            } else if (raftNode.getState() == RaftNode.State.CANDIDATE) {
                log.info("[{}] Election failed ({} votes, need {}). Reverting to FOLLOWER.",
                        nodeId, votes.get(), quorumSize);
                raftNode.setState(RaftNode.State.FOLLOWER);
                resetElectionTimeout();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }
}
