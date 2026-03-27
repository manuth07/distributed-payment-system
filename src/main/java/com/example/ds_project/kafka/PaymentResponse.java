package com.example.ds_project.kafka;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Rich API response for payment submission.
 * Exposes real-time cluster consensus metadata alongside payment info.
 */
public class PaymentResponse {
    private UUID paymentId;
    private BigDecimal amount;
    private long timestamp;

    // Raft Consensus Metadata
    private String raftStatus;          // PENDING | COMMITTED
    private String raftLeaderNodeId;    // which node is currently the leader
    private String raftLeaderUrl;       // URL of the leader
    private int replicatedToNodes;      // how many nodes this was replicated to (matchIndex count)
    private int quorumRequired;         // how many needed for consensus
    private boolean consensusReached;   // true if replicatedToNodes >= quorumRequired
    private long raftTerm;              // current Raft term
    private long logIndex;              // index in the Raft log (-1 if not yet appended)

    // Kafka Metadata
    private String kafkaTopic;          // the Kafka topic the payment was published to
    private String kafkaConsumerGroup;  // the consumer group handling the message

    // Receiving node
    private String receivingNode;       // which node this request was received by

    public PaymentResponse() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final PaymentResponse r = new PaymentResponse();
        public Builder paymentId(UUID v)         { r.paymentId = v; return this; }
        public Builder amount(BigDecimal v)       { r.amount = v; return this; }
        public Builder timestamp(long v)          { r.timestamp = v; return this; }
        public Builder raftStatus(String v)       { r.raftStatus = v; return this; }
        public Builder raftLeaderNodeId(String v) { r.raftLeaderNodeId = v; return this; }
        public Builder raftLeaderUrl(String v)    { r.raftLeaderUrl = v; return this; }
        public Builder replicatedToNodes(int v)   { r.replicatedToNodes = v; return this; }
        public Builder quorumRequired(int v)      { r.quorumRequired = v; return this; }
        public Builder consensusReached(boolean v){ r.consensusReached = v; return this; }
        public Builder raftTerm(long v)           { r.raftTerm = v; return this; }
        public Builder logIndex(long v)           { r.logIndex = v; return this; }
        public Builder kafkaTopic(String v)       { r.kafkaTopic = v; return this; }
        public Builder kafkaConsumerGroup(String v){ r.kafkaConsumerGroup = v; return this; }
        public Builder receivingNode(String v)    { r.receivingNode = v; return this; }
        public PaymentResponse build()            { return r; }
    }

    // ─── Getters ─────────────────────────────────────────────────────────────────
    public UUID getPaymentId()          { return paymentId; }
    public BigDecimal getAmount()       { return amount; }
    public long getTimestamp()          { return timestamp; }
    public String getRaftStatus()       { return raftStatus; }
    public String getRaftLeaderNodeId() { return raftLeaderNodeId; }
    public String getRaftLeaderUrl()    { return raftLeaderUrl; }
    public int getReplicatedToNodes()   { return replicatedToNodes; }
    public int getQuorumRequired()      { return quorumRequired; }
    public boolean isConsensusReached() { return consensusReached; }
    public long getRaftTerm()           { return raftTerm; }
    public long getLogIndex()           { return logIndex; }
    public String getKafkaTopic()       { return kafkaTopic; }
    public String getKafkaConsumerGroup(){ return kafkaConsumerGroup; }
    public String getReceivingNode()    { return receivingNode; }
}
