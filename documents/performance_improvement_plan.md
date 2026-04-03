# Performance and Observability Improvements - Member 2

This plan outlines safe, small-scale improvements to the data replication and consistency layer to optimize performance and improve debugging without touching core Raft election or heartbeat logic.

## Proposed Changes

### 1. Optimize Deduplication Performance
Currently, the Leader performs a linear scan ($O(N)$) of the Raft log for every payment consumed from Kafka. As the system runs, this becomes a performance bottleneck.

#### [MODIFY] [RaftLog.java](file:///c:/Users/kavee/Desktop/SLIIT/Y2S2/Y2S2/DS/project/distributed-payment-system/src/main/java/com/example/ds_project/raft/RaftLog.java)
- Add a `Set<String> paymentIdIndex` to provide $O(1)$ lookups.
- Update the set in `appendEntry` and any methods that modify the log.
- Add a method `containsPayment(String paymentId)`.

### 2. Follower Efficiency in Kafka Consumption
Followers currently parse and log every Kafka message only to drop it. In a high-throughput environment, this wastes CPU and fills logs with "Ignoring" messages.

#### [MODIFY] [KafkaConsumerService.java](file:///c:/Users/kavee/Desktop/SLIIT/Y2S2/Y2S2/DS/project/distributed-payment-system/src/main/java/com/example/ds_project/kafka/KafkaConsumerService.java)
- Move the `raftNode.getState() == LEADER` check to the very top of the `consume` method.
- Update `alreadyInLog` to use the new $O(1)$ check from `RaftLog`.

### 3. Enhanced Consistency Observability
Add metrics to help debug replication and application lag.

#### [MODIFY] [PaymentController.java](file:///c:/Users/kavee/Desktop/SLIIT/Y2S2/Y2S2/DS/project/distributed-payment-system/src/main/java/com/example/ds_project/controller/PaymentController.java)
- In `/cluster-status`, add:
    - `applicationLag`: `commitIndex - lastApplied`.
    - `replicationLag`: For each peer, `lastLogIndex - matchIndex`.

---

## Analysis Categories (For Report)

### Already Effectively Done
- **Multi-Level Deduplication**: Filtering at ingestion (Kafka), replication (Raft Log), and persistence (Database).
- **Asynchronous Ingestion**: Decoupling client response from consensus via Kafka buffers.
- **Quorum-Safe Persistence**: Ensuring majority agreement before state machine application.

### Improvements to Mention (But Not Implement)
- **Persistent Bloom Filters**: For deduplication across restarts without scanning giant logs.
- **Log Compaction/Snapshotting**: To prevent the Raft log from growing indefinitely.
- **Dynamic Kafka Consumer Management**: Pausing the consumer on Followers to eliminate all Kafka-side overhead on non-leaders.

## Verification Plan

### Automated Tests
- No new automated tests, but verify existing ones pass.

### Manual Verification
- **Performance**: Monitor logs for decreased "Ignoring" spam on follower nodes.
- **Observability**: Verify new `lag` metrics show up in the `/payments/cluster-status` endpoint.
- **Correctness**: Submit duplicate payments via Postman and verify they are still correctly de-duplicated with $O(1)$ logic.
