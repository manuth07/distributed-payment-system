package com.example.ds_project.timesync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Scheduler for periodic clock skew measurements.
 * Runs separately from time sync to provide continuous monitoring.
 */
@Slf4j
@Service
public class SkewMonitoringScheduler {
    
    @Autowired
    private ClockSkewMonitor clockSkewMonitor;
    
    @Value("${node.id:node-unknown}")
    private String nodeId;
    
    @Value("${timesync.skew-monitoring-interval:10000}")
    private long monitoringInterval;  // milliseconds, default 10 seconds
    
    /**
     * Periodic skew measurement task.
     * Runs every {timesync.skew-monitoring-interval} milliseconds.
     */
    @Scheduled(fixedRateString = "${timesync.skew-monitoring-interval:10000}")
    public void monitorSkew() {
        try {
            SkewMeasurement measurement = clockSkewMonitor.measureClusterSkew();
            
            log.debug("[{}] Skew measurement: {}ms (severity: {})",
                    nodeId, measurement.getClusterSkew(), measurement.getSeverity());
        } catch (Exception e) {
            log.error("Error in skew monitoring scheduler", e);
        }
    }
}
