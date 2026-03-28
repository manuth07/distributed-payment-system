package com.example.ds_project.timesync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * REST Controller for time synchronization endpoints.
 * Exposes NTP-style request/response endpoints and monitoring status.
 */
@Slf4j
@RestController
@RequestMapping("/timesync")
public class TimeSyncController {
    
    @Autowired
    private ClockSynchronizationService clockSyncService;
    
    /**
     * NTP-style time sync request endpoint.
     * Followers POST to this endpoint on the leader to request synchronization.
     * 
     * @param request the TimeSyncRequest with client's local time
     * @return TimeSyncResponse with server times and calculated offset
     */
    @PostMapping("/request")
    public TimeSyncResponse handleTimeSyncRequest(@RequestBody TimeSyncRequest request) {
        log.debug("Received time sync request from {}, clientSendTime={}", 
                request.nodeId(), request.clientSendTime());
        
        TimeSyncResponse response = clockSyncService.calculateSyncResponse(request);
        
        log.debug("Sending time sync response: offset={}ms", response.estimatedClockOffset());
        return response;
    }
    
    /**
     * Get the current clock synchronization status for this node.
     * 
     * @return ClockMetadata containing offset, last sync time, and statistics
     */
    @GetMapping("/status")
    public ClockMetadata getClockStatus() {
        ClockMetadata metadata = clockSyncService.getNodeMetadata();
        log.debug("Clock status requested: offset={}ms, lastSync={}", 
                metadata.getClockOffset(), metadata.getLastSyncTime());
        return metadata;
    }
    
    /**
     * Get cluster-wide clock synchronization status (leader-only view).
     * Returns all known node offsets and current cluster skew.
     * 
     * @return ClusterClockStatus containing all node offsets and cluster skew
     */
    @GetMapping("/cluster-status")
    public ClusterClockStatus getClusterStatus() {
        long clusterSkew = clockSyncService.calculateClusterSkew();
        ClusterClockStatus status = new ClusterClockStatus(
                clusterSkew,
                clockSyncService.getAllNodeMetadata()
        );
        
        log.debug("Cluster clock status requested: skew={}ms, nodeCount={}",
                clusterSkew, status.nodeMetadata().size());
        return status;
    }
}

/**
 * DTO for cluster-wide clock status.
 */
record ClusterClockStatus(
    long clusterSkew,  // milliseconds
    java.util.Map<String, ClockMetadata> nodeMetadata
) {}
