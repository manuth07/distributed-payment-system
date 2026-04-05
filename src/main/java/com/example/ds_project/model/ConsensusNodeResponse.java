package com.example.ds_project.model;

import java.util.List;

/**
 * Response DTO returned by each node during quorum-based retrieval consensus.
 *
 * <p>This is NOT a Raft AppendEntries/RequestVote response. It is a retrieval-time consensus
 * DTO used by the ZooKeeper-elected leader to validate freshness of peer nodes before
 * merging their local transaction data. Each node returns its Raft metadata alongside
 * its local user transactions and a deterministic SHA-256 hash of that transaction list,
 * enabling the leader to detect stale nodes and enforce quorum-based read consistency.</p>
 *
 * <p>Important: This provides quorum-validated reads over distributed local repositories.
 * It is NOT a true Raft linearizable read, because payment state is stored via Kafka
 * consumer partitioning, not via the Raft-applied state machine.</p>
 */
public class ConsensusNodeResponse {

    private String nodeId;
    private long term;
    private long commitIndex;
    private String raftState;
    private int count;
    private String dataHash;
    private List<Payment> transactions;

    public ConsensusNodeResponse() {} // Default for JSON deserialization

    public ConsensusNodeResponse(String nodeId, long term, long commitIndex, String raftState,
                                 int count, String dataHash, List<Payment> transactions) {
        this.nodeId = nodeId;
        this.term = term;
        this.commitIndex = commitIndex;
        this.raftState = raftState;
        this.count = count;
        this.dataHash = dataHash;
        this.transactions = transactions;
    }

    // ─── Getters ─────────────────────────────────────────────────────────────
    public String getNodeId()          { return nodeId; }
    public long getTerm()              { return term; }
    public long getCommitIndex()       { return commitIndex; }
    public String getRaftState()       { return raftState; }
    public int getCount()              { return count; }
    public String getDataHash()        { return dataHash; }
    public List<Payment> getTransactions() { return transactions; }

    // ─── Setters (for Jackson deserialization) ───────────────────────────────
    public void setNodeId(String nodeId)             { this.nodeId = nodeId; }
    public void setTerm(long term)                   { this.term = term; }
    public void setCommitIndex(long commitIndex)     { this.commitIndex = commitIndex; }
    public void setRaftState(String raftState)       { this.raftState = raftState; }
    public void setCount(int count)                  { this.count = count; }
    public void setDataHash(String dataHash)         { this.dataHash = dataHash; }
    public void setTransactions(List<Payment> transactions) { this.transactions = transactions; }
}
