package com.example.ds_project.timesync;

import lombok.extern.slf4j.Slf4j;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Buffers out-of-order log entries and reorders them by correctedTimestamp.
 * 
 * Phase 4: Handles payments that arrive out-of-sequence
 * Example: Receive P3(ts=1100), P1(ts=1000), P2(ts=1050)
 *          Buffer reorders to: P1(ts=1000), P2(ts=1050), P3(ts=1100)
 * 
 * Strategy: Sliding window with timeout
 * - Collect entries in a buffer
 * - When window expires or buffer full, reorder and flush
 * - Entries outside window are immediately flushed
 * 
 * Generic Type Parameter: T must implement Timestamped interface
 */
@Slf4j
public class LogReorderBuffer<T extends Timestamped> {
    
    private final SortedSet<T> buffer;  // Sorted by correctedTimestamp
    private final long windowSizeMs;    // Time window for buffering (e.g., 500ms)
    private final int maxBufferSize;    // Max entries to hold
    private final String bufferId;      // For logging: e.g., "node1-reorder-buffer"
    
    private long lastFlushTimeMs = System.currentTimeMillis();
    private long entriesBuffered = 0;
    private long entriesFlushed = 0;
    private long entriesReordered = 0;  // Count of entries that needed reordering
    
    /**
     * Create a reorder buffer with given window size and capacity.
     * 
     * @param bufferId Identifier for this buffer (for logging)
     * @param windowSizeMs Time window to collect entries before flushing (e.g., 500ms)
     * @param maxBufferSize Maximum entries to hold in buffer (e.g., 100)
     */
    public LogReorderBuffer(String bufferId, long windowSizeMs, int maxBufferSize) {
        this.bufferId = bufferId;
        this.windowSizeMs = windowSizeMs;
        this.maxBufferSize = maxBufferSize;
        
        // Comparator: Sort by correctedTimestamp, then by identity for stable sort
        this.buffer = new TreeSet<>((a, b) -> {
            int cmp = Long.compare(a.getCorrectedTimestamp(), b.getCorrectedTimestamp());
            if (cmp != 0) {
                return cmp;
            }
            return Integer.compare(System.identityHashCode(a), System.identityHashCode(b));
        });
        
        log.debug("[{}] Initialized with windowSize={}ms, maxBufferSize={}",
                bufferId, windowSizeMs, maxBufferSize);
    }
    
    /**
     * Add an entry to the buffer.
     * May trigger auto-flush if window expires or buffer exceeds capacity.
     * 
     * @param entry The entry to buffer and potentially reorder
     * @return List of entries ready to be flushed (reordered), or empty if still buffering
     */
    public synchronized List<T> add(T entry) {
        buffer.add(entry);
        entriesBuffered++;
        
        long now = System.currentTimeMillis();
        long timeSinceLastFlush = now - lastFlushTimeMs;
        
        List<T> toFlush = new ArrayList<>();
        
        // Check if we should flush based on window timeout or buffer capacity
        if (timeSinceLastFlush >= windowSizeMs || buffer.size() >= maxBufferSize) {
            toFlush = new ArrayList<>(buffer);
            buffer.clear();
            lastFlushTimeMs = now;
            entriesFlushed += toFlush.size();
            
            // Count how many needed reordering (entries not first-received)
            if (toFlush.size() > 1) {
                entriesReordered += toFlush.size() - 1;
            }
            
            log.debug("[{}] Flushing {} entries (buffered={}s, bufferSize={})",
                    bufferId, toFlush.size(), timeSinceLastFlush >= windowSizeMs ? "timeout" : "full", buffer.size());
        }
        
        return toFlush;
    }
    
    /**
     * Force flush all buffered entries immediately.
     * Called on graceful shutdown or when demanding immediate output.
     * 
     * @return All buffered entries, sorted by correctedTimestamp
     */
    public synchronized List<T> flush() {
        List<T> result = new ArrayList<>(buffer);
        if (!result.isEmpty()) {
            buffer.clear();
            lastFlushTimeMs = System.currentTimeMillis();
            entriesFlushed += result.size();
            if (result.size() > 1) {
                entriesReordered += result.size() - 1;
            }
            log.debug("[{}] Force flush: {} entries", bufferId, result.size());
        }
        return result;
    }
    
    /**
     * Get current buffer statistics for monitoring.
     * 
     * @return Map with: size, windowSizeMs, buffered, flushed, reordered, reorderRatio
     */
    public synchronized Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("bufferId", bufferId);
        stats.put("currentBufferSize", buffer.size());
        stats.put("windowSizeMs", windowSizeMs);
        stats.put("maxBufferSize", maxBufferSize);
        stats.put("totalBuffered", entriesBuffered);
        stats.put("totalFlushed", entriesFlushed);
        stats.put("totalReordered", entriesReordered);
        stats.put("reorderRatio", entriesBuffered > 0 ? 
                String.format("%.1f%%", (100.0 * entriesReordered) / entriesBuffered) : "0%");
        
        return stats;
    }
    
    /**
     * Get buffer utilization (0.0 to 1.0).
     * 
     * @return Ratio of current buffer size to max buffer size
     */
    public synchronized double getUtilization() {
        return (double) buffer.size() / maxBufferSize;
    }
    
    /**
     * Reset statistics (for testing).
     */
    public synchronized void resetStatistics() {
        entriesBuffered = 0;
        entriesFlushed = 0;
        entriesReordered = 0;
    }
    
    /**
     * Check if buffer has any entries pending.
     * 
     * @return true if buffer contains entries, false if empty
     */
    public synchronized boolean hasPendingEntries() {
        return !buffer.isEmpty();
    }
    
    /**
     * Get current buffer size.
     * 
     * @return Number of entries currently in the buffer
     */
    public synchronized int getBufferSize() {
        return buffer.size();
    }
}
