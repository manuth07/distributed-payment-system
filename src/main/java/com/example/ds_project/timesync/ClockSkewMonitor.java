package com.example.ds_project.timesync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Monitors clock skew across the cluster.
 * Tracks measurements, identifies anomalies, and persists history.
 */
@Slf4j
@Service
public class ClockSkewMonitor {
    
    @Autowired
    private ClockSynchronizationService clockSyncService;
    
    @Value("${node.id:node-unknown}")
    private String nodeId;
    
    @Value("${timesync.skew-threshold:100}")
    private long skewThresholdMs;  // milliseconds - warn if skew exceeds this
    
    @Value("${timesync.data-dir:./data}")
    private String dataDir;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String SKEW_HISTORY_FILE = "clock-skew-history.jsonl";
    private SkewMeasurement lastMeasurement = null;
    
    private static final long CRITICAL_SKEW_THRESHOLD = 1000;  // 1 second = critical
    private static final long WARNING_SKEW_THRESHOLD = 500;    // 500ms = warning
    
    /**
     * Measure current cluster skew and record if significant.
     * Called periodically by a scheduler or on-demand.
     * 
     * @return the latest SkewMeasurement
     */
    public SkewMeasurement measureClusterSkew() {
        long timestamp = System.currentTimeMillis();
        long clusterSkew = clockSyncService.calculateClusterSkew();
        
        // Determine severity
        String severity = "OK";
        if (clusterSkew >= CRITICAL_SKEW_THRESHOLD) {
            severity = "CRITICAL";
        } else if (clusterSkew >= WARNING_SKEW_THRESHOLD) {
            severity = "WARNING";
        } else if (clusterSkew >= skewThresholdMs) {
            severity = "ALERT";
        }
        
        // Get all node metadata
        Map<String, ClockMetadata> allMetadata = clockSyncService.getAllNodeMetadata();
        int numNodes = allMetadata.size();
        
        // Extract offsets
        Map<String, Long> nodeOffsets = allMetadata.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().getClockOffset()
                ));
        
        // Find affected nodes (min/max)
        SkewMeasurement.NodeOffsetInfo maxOffsetNode = null;
        SkewMeasurement.NodeOffsetInfo minOffsetNode = null;
        List<String> affectedNodes = new ArrayList<>();
        
        if (!nodeOffsets.isEmpty()) {
            String maxNode = Collections.max(nodeOffsets.entrySet(), 
                    Comparator.comparingLong(Map.Entry::getValue)).getKey();
            String minNode = Collections.min(nodeOffsets.entrySet(),
                    Comparator.comparingLong(Map.Entry::getValue)).getKey();
            
            maxOffsetNode = new SkewMeasurement.NodeOffsetInfo(maxNode, nodeOffsets.get(maxNode));
            minOffsetNode = new SkewMeasurement.NodeOffsetInfo(minNode, nodeOffsets.get(minNode));
            
            affectedNodes.add(maxNode);
            if (!maxNode.equals(minNode)) {
                affectedNodes.add(minNode);
            }
        }
        
        SkewMeasurement measurement = new SkewMeasurement(
                timestamp,
                clusterSkew,
                severity,
                numNodes,
                nodeOffsets,
                affectedNodes,
                maxOffsetNode,
                minOffsetNode
        );
        
        this.lastMeasurement = measurement;
        
        // Log warnings
        if (!"OK".equals(severity)) {
            log.warn("Clock skew anomaly detected: skew={}ms, severity={}, maxNode={}({}ms), minNode={}({}ms)",
                    clusterSkew, severity,
                    maxOffsetNode != null ? maxOffsetNode.nodeId : "N/A",
                    maxOffsetNode != null ? maxOffsetNode.offset : 0,
                    minOffsetNode != null ? minOffsetNode.nodeId : "N/A",
                    minOffsetNode != null ? minOffsetNode.offset : 0);
        }
        
        // Persist to history file
        persistMeasurement(measurement);
        
        return measurement;
    }
    
    /**
     * Get the most recent skew measurement.
     * 
     * @return latest SkewMeasurement or null if none recorded yet
     */
    public SkewMeasurement getLatestMeasurement() {
        if (lastMeasurement == null) {
            // Try to load from file as fallback
            return loadLatestFromHistory();
        }
        return lastMeasurement;
    }
    
    /**
     * Get skew measurements from the last N hours.
     * 
     * @param hours number of hours to retrieve
     * @return list of SkewMeasurements from the specified time window
     */
    public List<SkewMeasurement> getSkewHistory(int hours) {
        List<SkewMeasurement> history = new ArrayList<>();
        long cutoffTime = System.currentTimeMillis() - (hours * 3600_000L);
        
        try {
            String filePath = Paths.get(dataDir, SKEW_HISTORY_FILE).toString();
            Files.lines(Paths.get(filePath))
                    .map(line -> {
                        try {
                            return objectMapper.readValue(line, SkewMeasurement.class);
                        } catch (Exception e) {
                            log.warn("Failed to parse skew history line: {}", line, e);
                            return null;
                        }
                    })
                    .filter(measurement -> measurement != null && measurement.getTimestamp() >= cutoffTime)
                    .forEach(history::add);
            
            log.debug("Loaded {} skew measurements from last {} hours", history.size(), hours);
        } catch (IOException e) {
            log.debug("Skew history file not found or empty: {}", dataDir + "/" + SKEW_HISTORY_FILE);
        }
        
        return history;
    }
    
    /**
     * Get summary statistics from skew history.
     * 
     * @param hours number of hours to analyze
     * @return SkewStatistics with min, max, average skew
     */
    public SkewStatistics getSkewStatistics(int hours) {
        List<SkewMeasurement> history = getSkewHistory(hours);
        
        if (history.isEmpty()) {
            return new SkewStatistics(0, 0, 0, 0);
        }
        
        long minSkew = history.stream().mapToLong(SkewMeasurement::getClusterSkew).min().orElse(0);
        long maxSkew = history.stream().mapToLong(SkewMeasurement::getClusterSkew).max().orElse(0);
        long avgSkew = Math.round(history.stream()
                .mapToLong(SkewMeasurement::getClusterSkew)
                .average()
                .orElse(0));
        
        return new SkewStatistics(minSkew, maxSkew, avgSkew, history.size());
    }
    
    /**
     * Persist a skew measurement to the history file (JSONL format).
     */
    private void persistMeasurement(SkewMeasurement measurement) {
        try {
            String filePath = Paths.get(dataDir, SKEW_HISTORY_FILE).toString();
            String json = objectMapper.writeValueAsString(measurement);
            Files.write(
                    Paths.get(filePath),
                    (json + "\n").getBytes(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
            log.debug("Skew measurement persisted: {}ms", measurement.getClusterSkew());
        } catch (IOException e) {
            log.error("Failed to persist skew measurement", e);
        }
    }
    
    /**
     * Load the latest measurement from history file.
     */
    private SkewMeasurement loadLatestFromHistory() {
        try {
            String filePath = Paths.get(dataDir, SKEW_HISTORY_FILE).toString();
            List<String> lines = Files.readAllLines(Paths.get(filePath));
            
            if (!lines.isEmpty()) {
                String lastLine = lines.get(lines.size() - 1);
                return objectMapper.readValue(lastLine, SkewMeasurement.class);
            }
        } catch (IOException e) {
            log.debug("Could not load latest skew measurement from history");
        }
        
        return null;
    }
    
    /**
     * DTO for skew statistics.
     */
    public record SkewStatistics(
        long minSkew,      // minimum skew in the period (ms)
        long maxSkew,      // maximum skew in the period (ms)
        long averageSkew,  // average skew (ms)
        int measurementCount  // number of measurements
    ) {}
}
