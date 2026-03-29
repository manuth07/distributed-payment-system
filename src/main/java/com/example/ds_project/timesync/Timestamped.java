package com.example.ds_project.timesync;

/**
 * Marker interface for entries that can be buffered and reordered.
 * 
 * Phase 4: Any entry type must implement this to work with LogReorderBuffer<T>.
 * The interface provides a corrected timestamp for sorting out-of-order entries.
 */
public interface Timestamped {
    /**
     * Get the corrected timestamp for this entry.
     * Used by LogReorderBuffer to sort entries in causally-correct order.
     * 
     * @return Timestamp in milliseconds (corrected for clock skew by Phase 3a/3b)
     */
    long getCorrectedTimestamp();
}
