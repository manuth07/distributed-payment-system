package com.example.ds_project.timesync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Core NTP-style clock synchronization service.
 * Calculates clock offsets, manages per-node metadata, persists sync history.
 */
@Slf4j
@Service
public class ClockSynchronizationService {
    
    private final ConcurrentHashMap<String, ClockMetadata> nodeOffsets = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String dataDir;
    private final String nodeId;
    
    // Configuration defaults
    private static final long MAX_OFFSET_TOLERANCE = 5000;  // 5 seconds - reject offsets larger than this
    private static final String METADATA_FILE_NAME = "clock-metadata.json";
    
    public ClockSynchronizationService(String nodeId, String dataDir) {
        this.nodeId = nodeId;
        this.dataDir = dataDir;
        ensureDataDirectoryExists();
        loadMetadataFromDisk();
    }
    
    /**
     * NTP-style offset calculation using round-trip delay estimation.
     * Formula: offset = ((serverReceiveTime - clientSendTime) + (serverSendTime - clientReceiveTime)) / 2
     * 
     * @param request the client's sync request
     * @return TimeSyncResponse with calculated offset
     */
    public TimeSyncResponse calculateSyncResponse(TimeSyncRequest request) {
        long serverReceiveTime = System.currentTimeMillis();  // When server received request
        long clientSendTime = request.clientSendTime();
        
        // Sanity check: reject if client time is wildly off (more than 1 hour in future)
        if (clientSendTime > System.currentTimeMillis() + 3600_000) {
            log.warn("Rejecting sync request from {} with future timestamp: {}",
                    request.nodeId(), clientSendTime);
            return new TimeSyncResponse(serverReceiveTime, System.currentTimeMillis(), 0);
        }
        
        long serverSendTime = System.currentTimeMillis();
        
        // NTP offset calculation: how much to add to client's clock to match server
        // offset = ((T2 - T1) + (T3 - T4)) / 2
        // where T1 = clientSendTime, T2 = serverReceiveTime, T3 = serverSendTime, T4 = clientReceiveTime
        // Client will calculate T4 when it receives this response
        // So we return T2 and T3; client computes the full offset
        long estimatedOffset = (serverReceiveTime - clientSendTime + serverSendTime - System.currentTimeMillis()) / 2;
        
        // Simpler approach: just use one-way delay estimate
        // offset = serverReceiveTime - clientSendTime (assumes symmetric network delay)
        long simpleOffset = serverReceiveTime - clientSendTime;
        
        log.debug("TimeSyncResponse for {}: clientSendTime={}, serverReceiveTime={}, estimatedOffset={}ms",
                request.nodeId(), clientSendTime, serverReceiveTime, simpleOffset);
        
        return new TimeSyncResponse(serverReceiveTime, serverSendTime, simpleOffset);
    }
    
    /**
     * Process a sync response received from the server (leader).
     * Calculates and stores the clock offset.
     * 
     * @param response the server's sync response
     * @param clientSendTime what the client sent (for accurate calculation)
     * @return true if offset was successfully updated, false if invalid or out of tolerance
     */
    public boolean processTimeSyncResponse(TimeSyncResponse response, long clientSendTime) {
        long clientReceiveTime = System.currentTimeMillis();
        
        // Full NTP offset calculation
        long serverReceiveTime = response.serverReceiveTime();
        long serverSendTime = response.serverSendTime();
        
        // offset = ((T2 - T1) + (T3 - T4)) / 2
        long offset = ((serverReceiveTime - clientSendTime) + (serverSendTime - clientReceiveTime)) / 2;
        long roundTripDelay = (clientReceiveTime - clientSendTime) - (serverSendTime - serverReceiveTime);
        
        // Use response's pre-calculated offset if preferred
        offset = response.estimatedClockOffset();
        
        // Validate offset is within tolerance
        if (Math.abs(offset) > MAX_OFFSET_TOLERANCE) {
            log.warn("Rejecting clock offset {} ms for {} - exceeds tolerance of {} ms",
                    offset, nodeId, MAX_OFFSET_TOLERANCE);
            return false;
        }
        
        // Update local node metadata
        ClockMetadata metadata = nodeOffsets.computeIfAbsent(
                nodeId,
                k -> new ClockMetadata(nodeId)
        );
        metadata.updateOffset(offset, Math.abs(roundTripDelay));
        
        log.info("Clock offset updated for {}: {} ms (RTD: {} ms, syncCount: {})",
                nodeId, offset, Math.abs(roundTripDelay), metadata.getSyncCount());
        
        persistMetadataToDisk();
        return true;
    }
    
    /**
     * Get the current clock offset for this node.
     * @return offset in milliseconds (positive = this node is fast, negative = slow)
     */
    public long getCurrentOffset() {
        return nodeOffsets.getOrDefault(nodeId, new ClockMetadata(nodeId)).getClockOffset();
    }
    
    /**
     * Get the current clock metadata for this node.
     */
    public ClockMetadata getNodeMetadata() {
        return nodeOffsets.getOrDefault(nodeId, new ClockMetadata(nodeId));
    }
    
    /**
     * Get metadata for a specific node (used by leader for monitoring).
     */
    public ClockMetadata getMetadata(String node) {
        return nodeOffsets.get(node);
    }
    
    /**
     * Update metadata for a remote node (leader aggregating from followers).
     * @param nodeId the node whose metadata to update
     * @param metadata the metadata to store
     */
    public void updateRemoteNodeMetadata(String nodeId, ClockMetadata metadata) {
        nodeOffsets.put(nodeId, metadata);
        persistMetadataToDisk();
    }
    
    /**
     * Get all stored node metadata (snapshots from cluster).
     */
    public ConcurrentHashMap<String, ClockMetadata> getAllNodeMetadata() {
        return new ConcurrentHashMap<>(nodeOffsets);
    }
    
    /**
     * Calculate current cluster clock skew.
     * @return maximum difference between any two nodes' offsets
     */
    public long calculateClusterSkew() {
        if (nodeOffsets.isEmpty()) return 0;
        
        long maxOffset = nodeOffsets.values().stream()
                .mapToLong(ClockMetadata::getClockOffset)
                .max()
                .orElse(0);
        
        long minOffset = nodeOffsets.values().stream()
                .mapToLong(ClockMetadata::getClockOffset)
                .min()
                .orElse(0);
        
        return maxOffset - minOffset;
    }
    
    /**
     * Apply clock offset to a timestamp for correction.
     * @param timestamp raw timestamp in milliseconds
     * @return corrected timestamp
     */
    public long applyClockCorrection(long timestamp) {
        long offset = getCurrentOffset();
        return timestamp + offset;
    }
    
    /**
     * Record a synchronization error for this node.
     * @param errorMessage description of the error
     */
    public void recordSyncError(String errorMessage) {
        ClockMetadata metadata = nodeOffsets.computeIfAbsent(
                nodeId,
                k -> new ClockMetadata(nodeId)
        );
        metadata.recordSyncError(errorMessage);
        persistMetadataToDisk();
    }
    
    /**
     * Persist clock metadata to disk for durability.
     */
    private synchronized void persistMetadataToDisk() {
        try {
            String metadataPath = Paths.get(dataDir, METADATA_FILE_NAME).toString();
            String json = objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(nodeOffsets);
            Files.write(Paths.get(metadataPath), json.getBytes());
            log.debug("Clock metadata persisted to {}", metadataPath);
        } catch (IOException e) {
            log.error("Failed to persist clock metadata", e);
        }
    }
    
    /**
     * Load clock metadata from disk on service startup.
     */
    private void loadMetadataFromDisk() {
        try {
            String metadataPath = Paths.get(dataDir, METADATA_FILE_NAME).toString();
            File file = new File(metadataPath);
            
            if (file.exists()) {
                String json = new String(Files.readAllBytes(Paths.get(metadataPath)));
                ConcurrentHashMap<String, ClockMetadata> loaded = objectMapper.readValue(
                        json,
                        ConcurrentHashMap.class
                );
                nodeOffsets.putAll(loaded);
                log.info("Clock metadata loaded from disk: {} nodes", nodeOffsets.size());
            } else {
                log.info("No existing clock metadata file found at {}", metadataPath);
            }
        } catch (Exception e) {
            log.warn("Failed to load clock metadata from disk", e);
        }
    }
    
    /**
     * Ensure data directory exists.
     */
    private void ensureDataDirectoryExists() {
        try {
            Files.createDirectories(Paths.get(dataDir));
        } catch (IOException e) {
            log.error("Failed to create data directory: {}", dataDir, e);
        }
    }
}
