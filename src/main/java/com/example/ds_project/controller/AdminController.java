package com.example.ds_project.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Admin Controller for managing node operations.
 * Provides endpoints for graceful shutdown, node control, and system info.
 * 
 * Endpoints:
 * - GET /admin/info - System and node information
 * - POST /admin/shutdown - Gracefully shutdown this node
 * - GET /admin/system-stats - System statistics
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
     * 
     * Usage: Tell Docker Compose to stop, or manually trigger JVM exit
     */
    @PostMapping("/shutdown")
    public ResponseEntity<Map<String, String>> shutdown(
            @RequestParam(required = false, defaultValue = "false") boolean force) {
        
        Map<String, String> response = new LinkedHashMap<>();
        response.put("status", "shutting_down");
        response.put("nodeId", nodeId);
        response.put("timestamp", System.currentTimeMillis() + "");
        
        log.warn("=== SHUTDOWN INITIATED FROM ADMIN API ===");
        log.warn("Node: {}", nodeId);
        log.warn("Force: {}", force);
        
        // Give time for response to be sent
        new Thread(() -> {
            try {
                Thread.sleep(500);
                log.info("Executing System.exit(0)...");
                System.exit(0);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
        
        return ResponseEntity.accepted().body(response);
    }

    /**
     * POST /admin/ready-probe
     * Health check for Docker/Kubernetes readiness probes
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
     * GET /admin/liveness
     * Health check for Docker/Kubernetes liveness probes
     */
    @GetMapping("/liveness")
    public ResponseEntity<Map<String, String>> livenessProbe() {
        Map<String, String> response = new LinkedHashMap<>();
        response.put("alive", "true");
        response.put("nodeId", nodeId);
        return ResponseEntity.ok(response);
    }
}
