package com.example.ds_project.raft;

import com.example.ds_project.timesync.Timestamped;

/**
 * Raft log entry with support for out-of-order reordering (Phase 4).
 * Implements Timestamped to work with LogReorderBuffer for distributed ordering.
 */
public class LogEntry implements Timestamped {
    private long index;
    private long term;
    private String paymentId;
    private String payload;
    private long timestamp;
    private LogStatus status;

    public enum LogStatus {
        PENDING, COMMITTED, APPLIED
    }

    public LogEntry() {
    }

    public LogEntry(long index, long term, String paymentId, String payload, long timestamp, LogStatus status) {
        this.index = index;
        this.term = term;
        this.paymentId = paymentId;
        this.payload = payload;
        this.timestamp = timestamp;
        this.status = status;
    }

    public long getIndex() { return index; }
    public void setIndex(long index) { this.index = index; }

    public long getTerm() { return term; }
    public void setTerm(long term) { this.term = term; }

    public String getPaymentId() { return paymentId; }
    public void setPaymentId(String paymentId) { this.paymentId = paymentId; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public LogStatus getStatus() { return status; }
    public void setStatus(LogStatus status) { this.status = status; }
    
    /**
     * Phase 4: Implement Timestamped interface for out-of-order log reordering.
     * Returns the corrected timestamp for ordering purposes.
     * 
     * @return correctedTimestamp (stored in the timestamp field)
     */
    @Override
    public long getCorrectedTimestamp() {
        return timestamp;
    }
}
