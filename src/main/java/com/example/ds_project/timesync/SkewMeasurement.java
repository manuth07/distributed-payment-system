package com.example.ds_project.timesync;

import lombok.Getter;
import lombok.Setter;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Represents a single clock skew measurement at a point in time.
 * Stores snapshot of all node offsets and calculated cluster skew.
 */
@Getter
@Setter
public class SkewMeasurement {
    
    @JsonProperty("timestamp")
    private long timestamp;  // milliseconds since epoch when measurement was taken
    
    @JsonProperty("clusterSkew")
    private long clusterSkew;  // milliseconds - max(offsets) - min(offsets)
    
    @JsonProperty("severity")
    private String severity;  // "OK", "WARNING", "CRITICAL"
    
    @JsonProperty("numNodes")
    private int numNodes;  // number of nodes in cluster
    
    @JsonProperty("nodeOffsets")
    private java.util.Map<String, Long> nodeOffsets;  // nodeId -> offset in ms
    
    @JsonProperty("affectedNodes")
    private List<String> affectedNodes;  // nodes with highest/lowest offsets
    
    @JsonProperty("maxOffsetNode")
    private NodeOffsetInfo maxOffsetNode;  // node with highest positive offset
    
    @JsonProperty("minOffsetNode")
    private NodeOffsetInfo minOffsetNode;  // node with highest negative offset (most behind)

    public SkewMeasurement() {}

    public SkewMeasurement(long timestamp, long clusterSkew, String severity, int numNodes,
                          java.util.Map<String, Long> nodeOffsets, List<String> affectedNodes,
                          NodeOffsetInfo maxOffsetNode, NodeOffsetInfo minOffsetNode) {
        this.timestamp = timestamp;
        this.clusterSkew = clusterSkew;
        this.severity = severity;
        this.numNodes = numNodes;
        this.nodeOffsets = nodeOffsets;
        this.affectedNodes = affectedNodes;
        this.maxOffsetNode = maxOffsetNode;
        this.minOffsetNode = minOffsetNode;
    }

    /**
     * Nested class for node offset information.
     */
    @Getter
    @Setter
    public static class NodeOffsetInfo {
        @JsonProperty("nodeId")
        public String nodeId;
        
        @JsonProperty("offset")
        public long offset;

        public NodeOffsetInfo() {}

        public NodeOffsetInfo(String nodeId, long offset) {
            this.nodeId = nodeId;
            this.offset = offset;
        }
    }
}
