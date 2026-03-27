# Phase 2: Clock Skew Detection & Reporting - Implementation Summary

## Objective
Monitor clock skew across the cluster, detect anomalies, track history, and expose metrics for analysis.

## Completed Implementation

### 1. Data Model (SkewMeasurement)
**SkewMeasurement.java** - Captures a point-in-time snapshot:
- `timestamp` - When measurement was taken (epoch ms)
- `clusterSkew` - Current skew: max(offsets) - min(offsets)
- `severity` - "OK", "ALERT" (≥100ms), "WARNING" (≥500ms), "CRITICAL" (≥1000ms)
- `numNodes` - Node count at measurement time
- `nodeOffsets` - Map of all node IDs → their offsets (ms)
- `affectedNodes` - List of nodes with min/max offsets
- `maxOffsetNode` - Node with highest positive offset (fastest clock)
- `minOffsetNode` - Node with highest negative offset (slowest clock)

**Nested class**: `NodeOffsetInfo` - Simple pair of (nodeId, offset)

### 2. Core Service (ClockSkewMonitor)
**ClockSkewMonitor.java** - Tracks and monitors skew:

**Key Methods:**
- `measureClusterSkew()` - Main measurement routine
  - Queries ClockSynchronizationService for all node offsets
  - Calculates current skew
  - Determines severity based on thresholds
  - Identifies affected nodes (min/max offset nodes)
  - Persists to JSONL file
  - Logs warnings if severity > "OK"
  - Returns SkewMeasurement
  
- `getLatestMeasurement()` - Returns most recent measurement (cached in memory)
- `getSkewHistory(hours)` - Queries JSONL file for measurements in last N hours
- `getSkewStatistics(hours)` - Derives summary stats (min, max, average skew) from history

**Severity Thresholds:**
| Severity | Threshold |
|----------|-----------|
| OK | < 100ms (default) |
| ALERT | ≥ 100ms (configurable via `timesync.skew-threshold`) |
| WARNING | ≥ 500ms |
| CRITICAL | ≥ 1000ms |

**Persistence:**
- Measurements appended to `./data/clock-skew-history.jsonl` (one JSON object per line)
- Latest in-memory cached for fast retrieval
- Can be replayed for historical analysis

### 3. Monitoring Scheduler (SkewMonitoringScheduler)
**SkewMonitoringScheduler.java** - Periodic measurement execution:
- Runs every 10 seconds (configurable via `timesync.skew-monitoring-interval`)
- Calls `clockSkewMonitor.measureClusterSkew()` on each cycle
- Handles exceptions gracefully

### 4. REST Controller (TimeSyncMetricsController)
Exposes monitoring endpoints under `/timesync` prefix:

**Endpoints:**

1. **GET /timesync/skew-report** (New monitoring endpoint)
   - Returns latest SkewMeasurement
   - Includes current skew, affected nodes, severity
   - Response example:
   ```json
   {
     "timestamp": 1648374600000,
     "clusterSkew": 75,
     "severity": "OK",
     "numNodes": 5,
     "nodeOffsets": {
       "node-8081": 0,
       "node-8082": 25,
       "node-8083": -10,
       "node-8084": 15,
       "node-8085": -15
     },
     "affectedNodes": ["node-8082", "node-8085"],
     "maxOffsetNode": { "nodeId": "node-8082", "offset": 25 },
     "minOffsetNode": { "nodeId": "node-8085", "offset": -15 }
   }
   ```

2. **GET /timesync/skew-history?hours=N** (NEW - Phase 2)
   - Retrieves all measurements from last N hours (default 1, max 168)
   - Returns array of SkewMeasurement objects
   - Useful for trend analysis, graphing skew over time

3. **GET /timesync/skew-statistics?hours=N** (NEW - Phase 2)
   - Returns aggregate statistics for the period
   - Response:
   ```json
   {
     "minSkew": 5,
     "maxSkew": 150,
     "averageSkew": 52,
     "measurementCount": 73
   }
   ```

### 5. Spring Configuration Update
**TimeSyncConfig.java** - Added bean:
```java
@Bean
public ClockSkewMonitor clockSkewMonitor() {
    log.info("Initializing ClockSkewMonitor for {}", nodeId);
    return new ClockSkewMonitor();
}
```

### 6. Configuration Properties
**application.properties** - New property:
```properties
timesync.skew-monitoring-interval=10000  # Measurement every 10 seconds
```

## How It Works

### Measurement Cycle (Every 10 seconds)
```
SkewMonitoringScheduler.monitorSkew()
  ↓
  ClockSkewMonitor.measureClusterSkew()
    ↓
    ClockSynchronizationService.calculateClusterSkew()  [already exists from Phase 1]
    ↓
    Extract all nodeOffsets from ClockMetadata
    ↓
    Calculate: clusterSkew = max(offsets) - min(offsets)
    ↓
    Determine severity based on thresholds
    ↓
    Identify min/max offset nodes
    ↓
    Create SkewMeasurement object
    ↓
    [If severity != "OK"] Log warning with affected nodes
    ↓
    persistMeasurement() → append to clock-skew-history.jsonl
    ↓
    cache in lastMeasurement
    ↓
    return SkewMeasurement
```

### File Structure
```
./data/
├── node-8081/
│   ├── clock-metadata.json      (Phase 1 - per-node offset)
│   
├── node-8082/
│   ├── clock-metadata.json
│
├── clock-skew-history.jsonl     (Phase 2 - cluster-wide skew timeline)
```

Example JSONL content (clock-skew-history.jsonl):
```
{"timestamp":1648374590000,"clusterSkew":85,"severity":"OK","numNodes":5,...}
{"timestamp":1648374600000,"clusterSkew":95,"severity":"OK","numNodes":5,...}
{"timestamp":1648374610000,"clusterSkew":230,"severity":"CRITICAL","numNodes":5,...}
```

## Integration with Phase 1

- Uses `ClockSynchronizationService.calculateClusterSkew()` → already provided offsets
- Uses `ClockSynchronizationService.getAllNodeMetadata()` → retrieves all ClockMetadata
- No changes needed to Phase 1 code; builds cleanly on top

## Verification Checklist

- [ ] **Code Compiles**: `mvn clean compile -DskipTests` succeeds
- [ ] **Monitoring Runs**: Logs show "Skew measurement: XXms" every 10 seconds
- [ ] **Severity Detection**: Trigger high skew (via simulateSkew), verify "CRITICAL" severity logged
- [ ] **File Persistence**: `./data/clock-skew-history.jsonl` created and grows
- [ ] **REST Endpoints**:
  - [ ] GET /timesync/skew-report returns latest measurement
  - [ ] GET /timesync/skew-history?hours=1 returns array of past measurements
  - [ ] GET /timesync/skew-statistics?hours=1 returns {minSkew, maxSkew, averageSkew, count}
- [ ] **Historical Analysis**: Parse JSONL file, verify JSON parsing works
- [ ] **Threshold Customization**: Change `timesync.skew-threshold` to 50ms, verify alerts trigger earlier

## Files Created/Modified

### Created:
1. **SkewMeasurement.java** - Data model
2. **ClockSkewMonitor.java** - Core monitoring service
3. **SkewMonitoringScheduler.java** - Periodic scheduler
4. **TimeSyncMetricsController.java** - REST endpoints

### Modified:
1. **TimeSyncConfig.java** - Added `@Bean clockSkewMonitor()`
2. **application.properties** - Added `timesync.skew-monitoring-interval=10000`

## Key Features

✅ **Real-time Severity Classification** - Automatic detection of ALERT/WARNING/CRITICAL states
✅ **Historical Tracking** - All measurements persisted to JSONL for analysis
✅ **Efficient Queries** - Load last N hours from file (line-delimited JSON for streaming)
✅ **Statistics Aggregation** - Quick min/max/avg calculation from history
✅ **Affected Node Tracking** - Identifies which nodes have drift issues
✅ **Configurable Thresholds** - Customize when alerts trigger
✅ **Seamless Integration** - Builds on Phase 1 without disruption

## Next Steps

**Phase 3: Payment Timestamp Integration** - Extend Payment model with `correctedTimestamp` field and modify PaymentService to apply clock offsets before saving payments.

---

**Architecture Diagram:**
```
                    ┌──────────────────────────────┐
                    │ SkewMonitoringScheduler      │
                    │ (Every 10 seconds)           │
                    └────────────┬─────────────────┘
                                 │
                    ┌────────────▼─────────────────┐
                    │ ClockSkewMonitor             │
                    │ - measureClusterSkew()       │
                    │ - getSkewHistory()           │
                    │ - getSkewStatistics()        │
                    └────────┬──────────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
    ┌────▼────┐    ┌────────▼──────┐   ┌───────▼────────┐
    │ClockSync │    │FS Persistence │   │ In-Memory Cache│
    │Service   │    │(JSONL file)    │   │(lastMeasure)   │
    └──────────┘    └────────────────┘   └────────────────┘
         │
         │ calculateClusterSkew()
         │
    ┌────▼──────────────┐
    │ All ClockMetadata │
    │ (Phase 1)         │
    └───────────────────┘
         │
    ┌────▼──────────────┐
    │ REST Endpoints    │
    │ - /timesync/      │
    │   skew-report     │
    │ - /timesync/      │
    │   skew-history    │
    │ - /timesync/      │
    │   skew-statistics │
    └───────────────────┘
```

