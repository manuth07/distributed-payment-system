package com.example.ds_project;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

@SpringBootApplication
public class DsProjectApplication {

    private static final Logger log = LoggerFactory.getLogger(DsProjectApplication.class);

    @Value("${node.id:UNKNOWN}")
    private String nodeId;

    @Value("${server.port:8080}")
    private String serverPort;

    public static void main(String[] args) {
        SpringApplication.run(DsProjectApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info("==========================================================");
        log.info("🚀 NODE BOOT COMPLETE");
        log.info("✅ Node ID: {}", nodeId);
        log.info("✅ Listening on Port: {}", serverPort);
        log.info("✅ Custom endpoints (/payments/count, /payments/cluster-status) registered.");
        log.info("==========================================================");
    }
}
