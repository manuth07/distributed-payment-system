package com.example.ds_project.timesync;

import lombok.Getter;
import lombok.Setter;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Stores clock synchronization metadata for a node.
 * Tracks offset, skew, and synchronization history.
 */
@Getter
@Setter
public class ClockMetadata {
    private String nodeId;
    
    @JsonProperty("clockOffset")
    private long clockOffset = 0;  // milliseconds, positive = this node is ahead
    
    @JsonProperty("lastSyncTime")
    private long lastSyncTime = 0;  // epoch milliseconds when last sync occurred
    
    @JsonProperty("syncCount")
    private int syncCount = 0;  // number of successful syncs
    
    @JsonProperty("lastSyncError")
    private String lastSyncError = null;  // last error message, if any
    
    @JsonProperty("estimatedSkew")
    private long estimatedSkew = 0;  // milliseconds, max offset across cluster (for this node's calculation)
    
    @JsonProperty("roundTripDelay")
    private long roundTripDelay = 0;  // milliseconds, estimated network RTD from latest sync

    public ClockMetadata() {}

    public ClockMetadata(String nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Update clock offset based on NTP calculation.
     * @param offset calculated offset in milliseconds
     * @param rtd round-trip delay in milliseconds
     */
    public void updateOffset(long offset, long rtd) {
        this.clockOffset = offset;
        this.roundTripDelay = rtd;
        this.lastSyncTime = System.currentTimeMillis();
        this.syncCount++;
        this.lastSyncError = null;
    }

    /**
     * Record a sync failure.
     * @param errorMessage description of the error
     */
    public void recordSyncError(String errorMessage) {
        this.lastSyncError = errorMessage;
    }

    /**
     * Apply the current offset to a given timestamp.
     * @param timestamp raw timestamp in milliseconds
     * @return corrected timestamp (timestamp + offset)
     */
    public long applyCorrectionToTimestamp(long timestamp) {
        return timestamp + this.clockOffset;
    }
}
