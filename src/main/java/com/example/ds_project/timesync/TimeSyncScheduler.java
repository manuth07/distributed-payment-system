package com.example.ds_project.timesync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.RestClientException;

import com.example.ds_project.coordination.LeaderState;

/**
 * Background scheduler for periodic time synchronization.
 * Runs on all nodes; followers sync to leader every N seconds.
 */
@Slf4j
@Service
public class TimeSyncScheduler {
    
    @Autowired
    private ClockSynchronizationService clockSyncService;
    
    @Autowired
    private LeaderState leaderState;
    
    @Autowired
    private RestTemplate restTemplate;
    
    @Value("${node.id:node-unknown}")
    private String nodeId;
    
    @Value("${timesync.interval:5000}")
    private long syncInterval;  // milliseconds
    
    @Value("${timesync.remote-sync-timeout:200}")
    private long remoteSyncTimeout;  // milliseconds
    
    private int retryCount = 0;
    private static final int MAX_RETRIES = 3;
    private static final long BACKOFF_MULTIPLIER = 2;  // exponential backoff: 1s, 2s, 4s, 8s...
    
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info("Time synchronization scheduler initialized for node {}", nodeId);
    }
    
    /**
     * Periodic time synchronization task.
     * Runs on all nodes every {timesync.interval} milliseconds.
     * Followers sync to leader; leader aggregates and broadcasts.
     */
    @Scheduled(fixedRateString = "${timesync.interval:5000}")
    public void synchronizeClock() {
        try {
            if (leaderState.isLeader()) {
                // Leader role: aggregate metrics from followers if needed
                // For Phase 1, leader just maintains its own offset
                log.debug("[{}] Leader node - clock offset: {}ms",
                        nodeId, clockSyncService.getCurrentOffset());
            } else {
                // Follower role: sync to leader
                syncToLeader();
            }
        } catch (Exception e) {
            log.error("Unexpected error in time sync scheduler", e);
            clockSyncService.recordSyncError("Scheduler error: " + e.getMessage());
        }
    }
    
    /**
     * Follower synchronizes its clock to the leader.
     */
    private void syncToLeader() {
        String leaderUrl = leaderState.getLeaderUrl();
        
        if (leaderUrl == null || leaderUrl.isEmpty()) {
            log.debug("[{}] No leader URL available yet, skipping sync", nodeId);
            return;
        }
        
        String syncUrl = leaderUrl + "/timesync/request";
        
        try {
            // Create sync request with current local time
            long clientSendTime = System.currentTimeMillis();
            TimeSyncRequest request = new TimeSyncRequest(nodeId, clientSendTime);
            
            // Send to leader and receive response
            TimeSyncResponse response = restTemplate.postForObject(
                    syncUrl,
                    request,
                    TimeSyncResponse.class
            );
            
            if (response != null) {
                // Process the response and update our offset
                boolean success = clockSyncService.processTimeSyncResponse(response, clientSendTime);
                
                if (success) {
                    log.info("[{}] Clock sync succeeded with leader. Offset: {}ms",
                            nodeId, clockSyncService.getCurrentOffset());
                    resetRetryCount();
                } else {
                    log.warn("[{}] Clock sync response rejected (offset out of tolerance)", nodeId);
                    handleSyncFailure("Offset out of tolerance");
                }
            } else {
                log.warn("[{}] Clock sync received null response from leader", nodeId);
                handleSyncFailure("Null response from leader");
            }
            
        } catch (RestClientException e) {
            log.warn("[{}] Clock sync failed - cannot reach leader at {}: {}",
                    nodeId, leaderUrl, e.getMessage());
            handleSyncFailure("Cannot reach leader: " + e.getMessage());
        } catch (Exception e) {
            log.error("[{}] Unexpected error during clock sync", nodeId, e);
            handleSyncFailure("Unexpected error: " + e.getMessage());
        }
    }
    
    /**
     * Handle a synchronization failure with exponential backoff.
     */
    private void handleSyncFailure(String errorMessage) {
        clockSyncService.recordSyncError(errorMessage);
        
        if (retryCount < MAX_RETRIES) {
            retryCount++;
            long backoffMs = (long) (1000 * Math.pow(BACKOFF_MULTIPLIER, retryCount - 1));
            log.warn("[{}] Sync failed (attempt {}): {}. Backing off {}ms",
                    nodeId, retryCount, errorMessage, backoffMs);
        } else {
            log.error("[{}] Sync failed {} times. Waiting for next scheduled sync cycle",
                    nodeId, MAX_RETRIES);
        }
    }
    
    /**
     * Reset retry counter on successful sync.
     */
    private void resetRetryCount() {
        if (retryCount > 0) {
            retryCount = 0;
            log.debug("[{}] Retry counter reset", nodeId);
        }
    }
}
