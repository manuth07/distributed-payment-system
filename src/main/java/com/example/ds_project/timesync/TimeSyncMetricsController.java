package com.example.ds_project.timesync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST Controller for clock skew monitoring and metrics.
 * Exposes endpoints for viewing skew measurements, history, and statistics.
 */
@Slf4j
@RestController
@RequestMapping("/timesync")
public class TimeSyncMetricsController {
    
    @Autowired
    private ClockSkewMonitor clockSkewMonitor;
    
    /**
     * Get the latest clock skew measurement.
     * Includes current skew, affected nodes, and severity.
     * 
     * @return latest SkewMeasurement
     */
    @GetMapping("/skew-report")
    public SkewMeasurement getSkewReport() {
        SkewMeasurement measurement = clockSkewMonitor.getLatestMeasurement();
        
        if (measurement == null) {
            // No measurement yet, return a minimal one
            measurement = new SkewMeasurement(
                    System.currentTimeMillis(),
                    0,
                    "UNKNOWN",
                    0,
                    java.util.Collections.emptyMap(),
                    java.util.Collections.emptyList(),
                    null,
                    null
            );
        }
        
        log.debug("Skew report requested: {}ms", measurement.getClusterSkew());
        return measurement;
    }
    
    /**
     * Get clock skew history for the specified number of hours.
     * 
     * @param hours number of hours to retrieve (default 1)
     * @return list of SkewMeasurements
     */
    @GetMapping("/skew-history")
    public List<SkewMeasurement> getSkewHistory(
            @RequestParam(name = "hours", defaultValue = "1") int hours) {
        
        if (hours < 1 || hours > 168) {  // Max 7 days
            hours = 1;
        }
        
        List<SkewMeasurement> history = clockSkewMonitor.getSkewHistory(hours);
        log.debug("Skew history requested for {} hours: {} measurements", hours, history.size());
        return history;
    }
    
    /**
     * Get clock skew statistics for the specified number of hours.
     * 
     * @param hours number of hours to analyze (default 1)
     * @return SkewStatistics with min, max, average values
     */
    @GetMapping("/skew-statistics")
    public ClockSkewMonitor.SkewStatistics getSkewStatistics(
            @RequestParam(name = "hours", defaultValue = "1") int hours) {
        
        if (hours < 1 || hours > 168) {  // Max 7 days
            hours = 1;
        }
        
        ClockSkewMonitor.SkewStatistics stats = clockSkewMonitor.getSkewStatistics(hours);
        log.debug("Skew statistics requested for {} hours: min={}ms, max={}ms, avg={}ms",
                hours, stats.minSkew(), stats.maxSkew(), stats.averageSkew());
        return stats;
    }
}
