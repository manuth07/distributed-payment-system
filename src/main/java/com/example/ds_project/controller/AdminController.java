package com.example.ds_project.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.io.File;

/**
 * Admin Controller for managing node operations.
 * Provides endpoints for graceful shutdown, node control, and system info.
 */
@RestController
@RequestMapping("/admin")
public class AdminController {
    private static final Logger log = LoggerFactory.getLogger(AdminController.class);

    @Value("${raft.node.id}")
    private String nodeId;

    @Value("${server.port}")
    private String port;

    @Value("${app.version:1.0.0-SNAPSHOT}")
    private String appVersion;

    /**
     * GET /admin/info
     * Returns node and application information
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getInfo() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("nodeId", nodeId);
        info.put("port", port);
        info.put("appVersion", appVersion);
        info.put("javaVersion", System.getProperty("java.version"));
        info.put("osName", System.getProperty("os.name"));
        info.put("uptime", Runtime.getRuntime().totalMemory() / (1024 * 1024) + " MB");
        info.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(info);
    }

    /**
     * GET /admin/system-stats
     * Returns JVM and system statistics
     */
    @GetMapping("/system-stats")
    public ResponseEntity<Map<String, Object>> getSystemStats() {
        Runtime runtime = Runtime.getRuntime();
        Map<String, Object> stats = new LinkedHashMap<>();
        
        stats.put("jvmMemory", Map.of(
            "total_mb", runtime.totalMemory() / (1024 * 1024),
            "free_mb", runtime.freeMemory() / (1024 * 1024),
            "max_mb", runtime.maxMemory() / (1024 * 1024),
            "used_mb", (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)
        ));
        
        stats.put("processors", Runtime.getRuntime().availableProcessors());
        stats.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.ok(stats);
    }

    /**
     * POST /admin/shutdown
     * Gracefully shutdown this node (for testing failover scenarios)
     */
    @PostMapping("/shutdown")
    public ResponseEntity<Map<String, String>> shutdown(
            @RequestParam(required = false, defaultValue = "false") boolean force) {
        
        Map<String, String> response = new LinkedHashMap<>();
        response.put("status", "shutting_down");
        response.put("nodeId", nodeId);
        response.put("timestamp", System.currentTimeMillis() + "");
        
        log.warn("=== SHUTDOWN INITIATED FROM ADMIN API ===");
        
        new Thread(() -> {
            try {
                Thread.sleep(500);
                System.exit(0);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
        
        return ResponseEntity.accepted().body(response);
    }

    /**
     * POST /admin/ready
     * Health check for readiness probes
     */
    @PostMapping("/ready")
    @GetMapping("/ready")
    public ResponseEntity<Map<String, String>> readinessProbe() {
        Map<String, String> response = new LinkedHashMap<>();
        response.put("ready", "true");
        response.put("nodeId", nodeId);
        return ResponseEntity.ok(response);
    }

    /**
     * POST /admin/reset-all
     * Clears local payment repository, Raft log, and deletes persistent data files.
     */
    @PostMapping("/reset-all")
    public ResponseEntity<Map<String, String>> resetAll() {
        log.warn("=== SYSTEM RESET REQUESTED ON NODE {} ===", nodeId);
        try {
            paymentRepository.deleteAll();
            raftNode.setCommitIndex(-1L);
            raftNode.setLastApplied(-1L);
            raftNode.setCurrentTerm(0L);
            raftNode.setVotedFor(null);
            raftLog.getEntries().clear();
            File file = new File(raftLogFilePath);
            if (file.exists()) file.delete();
            log.info("System reset complete on node {}.", nodeId);
        } catch (Exception e) {
            log.error("Reset failed on node {}", nodeId, e);
        }
        Map<String, String> response = new LinkedHashMap<>();
        response.put("status", "reset_complete");
        response.put("nodeId", nodeId);
        return ResponseEntity.ok(response);
    }

    /**
     * POST /admin/simulate-skew
     * Manually introduce clock skew for testing purposes.
     * @param skewMs the amount of skew in milliseconds
     */
    @PostMapping("/simulate-skew")
    public ResponseEntity<Map<String, Object>> simulateSkew(@RequestParam long skewMs) {
        log.warn("=== SIMULATING CLOCK SKEW OF {} MS ON NODE {} ===", skewMs, nodeId);
        clockSyncService.setSimulationSkewMs(skewMs);
        
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "skew_applied");
        response.put("skewMs", skewMs);
        response.put("nodeId", nodeId);
        return ResponseEntity.ok(response);
    }

    /**
     * POST /admin/reset-clocks
     * Resets all clock skews and offsets to zero.
     */
    @PostMapping("/reset-clocks")
    public ResponseEntity<Map<String, String>> resetClocks() {
        log.warn("=== CLOCK RESET REQUESTED ON NODE {} ===", nodeId);
        clockSyncService.resetClocks();
        
        Map<String, String> response = new LinkedHashMap<>();
        response.put("status", "clocks_reset");
        response.put("nodeId", nodeId);
        return ResponseEntity.ok(response);
    }

    /**
     * GET /admin/liveness
     */
    @GetMapping("/liveness")
    public ResponseEntity<Map<String, String>> livenessProbe() {
        Map<String, String> response = new LinkedHashMap<>();
        response.put("alive", "true");
        response.put("nodeId", nodeId);
        return ResponseEntity.ok(response);
    }

    private final com.example.ds_project.repository.PaymentRepository paymentRepository;
    private final com.example.ds_project.raft.RaftNode raftNode;
    private final com.example.ds_project.raft.RaftLog raftLog;
    private final com.example.ds_project.timesync.ClockSynchronizationService clockSyncService;
    
    @Value("${raft.log.file}")
    private String raftLogFilePath;

    public AdminController(com.example.ds_project.repository.PaymentRepository paymentRepository,
                           com.example.ds_project.raft.RaftNode raftNode,
                           com.example.ds_project.raft.RaftLog raftLog,
                           com.example.ds_project.timesync.ClockSynchronizationService clockSyncService) {
        this.paymentRepository = paymentRepository;
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.clockSyncService = clockSyncService;
    }
}
