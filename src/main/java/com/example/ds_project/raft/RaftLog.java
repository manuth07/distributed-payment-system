package com.example.ds_project.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Collections;
import java.util.Set;

@Component
public class RaftLog {
    private static final Logger log = LoggerFactory.getLogger(RaftLog.class);

    private final CopyOnWriteArrayList<LogEntry> entries = new CopyOnWriteArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // Phase 2 Optimization: O(1) deduplication index instead of O(N) log scan
    private final Set<String> paymentIdIndex = Collections.synchronizedSet(new java.util.HashSet<>());

    @Value("${raft.log.file}")
    private String logFilePath;

    @PostConstruct
    public void init() {
        loadLog();
    }

    private void loadLog() {
        File file = new File(logFilePath);
        if (!file.exists()) {
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    LogEntry entry = objectMapper.readValue(line, LogEntry.class);
                    // Append-only simulation: latter entries with same index overwrite older ones conceptually
                    if (entry.getIndex() < entries.size()) {
                        entries.set((int) entry.getIndex(), entry);
                    } else if (entry.getIndex() == entries.size()) {
                        entries.add(entry);
                    } else {
                        log.warn("Gap identified in log at index {} vs size {}", entry.getIndex(), entries.size());
                        entries.add(entry);
                    }
                    // Populate paymentIdIndex for O(1) deduplication
                    if (entry.getPaymentId() != null) {
                        paymentIdIndex.add(entry.getPaymentId());
                    }
                }
            }
            log.info("Loaded {} log entries from disk ({} payment IDs indexed).", entries.size(), paymentIdIndex.size());
        } catch (IOException e) {
            log.error("Failed to load Raft log from disk", e);
        }
    }

    public synchronized void appendEntry(LogEntry entry) {
        entries.add(entry);
        // Phase 2 Optimization: Maintain O(1) deduplication index
        if (entry.getPaymentId() != null) {
            paymentIdIndex.add(entry.getPaymentId());
        }
        flushEntryToDisk(entry);
    }

    public synchronized void updateEntryStatus(long index, LogEntry.LogStatus newStatus) {
        if (index >= 0 && index < entries.size()) {
            LogEntry entry = entries.get((int) index);
            entry.setStatus(newStatus);
            // Append updated entry to end of file to record the state mutation
            flushEntryToDisk(entry);
        }
    }

    // Raft paper §5.3: log truncation when followers have conflicting entries
    public synchronized void truncateFromIndex(long index) {
        if (index >= 0 && index < entries.size()) {
            // Rebuild paymentIdIndex after truncation
            paymentIdIndex.clear();
            entries.subList((int) index, entries.size()).clear();
            for (LogEntry entry : entries) {
                if (entry.getPaymentId() != null) {
                    paymentIdIndex.add(entry.getPaymentId());
                }
            }
            rewriteLogFile();
        }
    }

    /**
     * Phase 2 Optimization: O(1) deduplication check instead of O(N) log scan
     * @param paymentId the payment ID to check
     * @return true if payment already exists in the log
     */
    public boolean containsPayment(String paymentId) {
        return paymentIdIndex.contains(paymentId);
    }

    private void flushEntryToDisk(LogEntry entry) {
        try {
            File file = new File(logFilePath);
            file.getParentFile().mkdirs();
            String json = objectMapper.writeValueAsString(entry) + System.lineSeparator();
            Files.writeString(Path.of(logFilePath), json, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Failed to flush log entry to disk", e);
        }
    }
    
    private void rewriteLogFile() {
        try {
            File file = new File(logFilePath);
            file.getParentFile().mkdirs();
            StringBuilder fileContent = new StringBuilder();
            for (LogEntry entry : entries) {
                fileContent.append(objectMapper.writeValueAsString(entry)).append(System.lineSeparator());
            }
            Files.writeString(Path.of(logFilePath), fileContent.toString(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            log.error("Failed to rewrite truncated log to disk", e);
        }
    }

    public LogEntry getEntry(long index) {
        if (index >= 0 && index < entries.size()) {
            return entries.get((int) index);
        }
        return null;
    }

    public long getLastLogIndex() {
        return entries.isEmpty() ? -1 : entries.size() - 1; // 0-based indexing makes size-1 the last index
    }

    public long getLastLogTerm() {
        if (entries.isEmpty()) return 0;
        return entries.get(entries.size() - 1).getTerm();
    }

    public CopyOnWriteArrayList<LogEntry> getEntries() {
        return entries;
    }
}
