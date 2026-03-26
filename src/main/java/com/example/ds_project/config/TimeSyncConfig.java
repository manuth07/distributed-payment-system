package com.example.ds_project.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.example.ds_project.timesync.ClockSynchronizationService;

/**
 * Spring configuration for time synchronization components.
 */
@Slf4j
@Configuration
@EnableScheduling
public class TimeSyncConfig {
    
    @Value("${node.id:node-unknown}")
    private String nodeId;
    
    @Value("${timesync.data-dir:./data}")
    private String dataDir;
    
    @Value("${timesync.enabled:true}")
    private boolean timeSyncEnabled;
    
    /**
     * Create the core ClockSynchronizationService bean.
     */
    @Bean
    public ClockSynchronizationService clockSynchronizationService() {
        if (!timeSyncEnabled) {
            log.info("Time synchronization is disabled via configuration");
        }
        
        String serviceDataDir = dataDir + "/" + nodeId;
        log.info("Initializing ClockSynchronizationService for {} with data dir: {}",
                nodeId, serviceDataDir);
        
        return new ClockSynchronizationService(nodeId, serviceDataDir);
    }
}
