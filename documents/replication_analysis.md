# Data Replication and Consistency Analysis: Distributed Payment System

This report analyzes the impact of data replication and consistency mechanisms on the performance, reliability, and accuracy of the Distributed Payment System, specifically focusing on Member 2's responsibilities: **Data Replication and Consistency**.

---

## 1. Write Latency
The system employs an asynchronous ingestion pipeline followed by a synchronous consensus protocol (Raft), which creates a multi-layered latency profile:

- **Ingestion Latency**: The `PaymentController` returns a `200 OK` response as soon as the payment is published to the **Kafka** topic. This provides low initial latency for the client.
- **Consensus Latency**: Unlike a standard database write, a payment is not "final" until the **Raft Leader** consumes the event, appends it to its local log, and successfully replicates it to a **quorum (majority)** of followers.
- **Network Hops**: Each write requires:
    1. Node $\rightarrow$ Kafka (Publish)
    2. Kafka $\rightarrow$ Leader (Consume)
    3. Leader $\rightarrow$ Followers (AppendEntries RPC)
    4. Followers $\rightarrow$ Leader (Response)
- **Impact**: While the user "sees" success quickly, the internal state only reflects the change after these network round-trips. In a 5-node cluster, 3 nodes must acknowledge the entry before the `commitIndex` advances.

## 2. Read Visibility Delay
There is a distinct "visibility gap" between when a payment is accepted and when it appears in `GET /payments`:

- **Asynchronous Commit Pipeline**: Since the controller returns immediately after Kafka publication, there is a period where a payment exists in Kafka but not yet in the Raft state machine.
- **State Machine Application**: The `PaymentStateMachine` poll-interval (currently `200ms` in `applyCommittedEntries`) adds a deterministic delay. Even if Raft reached consensus in 10ms, the payment may not be saved to the database for another 200ms.
- **Sequential Application**: Log entries must be applied in strict index order. If a large batch of payments enters the system, a particular payment might wait behind others before being persisted to the repository.

## 3. Storage Overhead
Replication ensures high availability but significantly increases storage requirements:

- **Log Duplication**: Every node in the cluster maintains a complete copy of the `RaftLog`. In this system, each `LogEntry` contains the full JSON payload of the payment.
- **State Machine Redundancy**: Each node persists committed payments to its local `PaymentRepository` (e.g., an H2 database). 
- **Total Overhead**: For a cluster of $N$ nodes, the system effectively stores $2N$ copies of the data (one in the Raft log and one in the database repository on each of the $N$ nodes).
- **Metadata Cost**: Each entry also stores `term`, `index`, `timestamp`, and `paymentId`, adding small but persistent overhead per transaction.

## 4. Consistency Guarantees
The system prioritizes **Strong Consistency** (specifically Linearizability) for committed data via the Raft protocol:

- **Leader-Based Commit**: Only the current Leader can append entries. This prevents split-brain scenarios and ensures a single global order of transactions.
- **Quorum Safety**: An entry is only considered committed if it is stored on a majority of nodes. This ensures that even if $(N-1)/2$ nodes fail, the system retains all committed payments.
- **State Machine Uniformity**: Because every node applies the same log in the same order, every node's `PaymentRepository` will eventually be identical (Eventual Consistency for followers, Strong Consistency for the cluster as a whole).

## 5. Deduplication: Performance vs. Correctness
Deduplication is critical because Kafka guarantees "at-least-once" delivery, which can lead to duplicate events.

- **Multi-Level Filtering**:
    1. **In-Memory Cache**: `KafkaConsumerService` uses a `Set<UUID>` (`processedPayments`) to filter obvious duplicates quickly.
    2. **Log Scanning**: The Leader scans the existing Raft log for the `paymentId` before appending a new entry in `KafkaConsumerService`.
    3. **Repository Check**: The `PaymentStateMachine` performs a final `findById` check in the database before saving.
- **Performance Trade-off**: Scanning the Raft log on every write (Linear search in `KafkaConsumerService`) increases CPU usage and write latency as the log grows. However, this is necessary to ensure the "Correctness" requirement of Member 2.

## 6. Trade-offs Summarized
The design choices reflect a "Safety-First" approach:

| Feature | Choice | Resulting Trade-off |
| :--- | :--- | :--- |
| **Commit Strategy** | Raft Quorum | **Benefit**: Data is never lost after commit. **Cost**: Higher latency than master-slave replication. |
| **Ingestion** | Kafka Buffer | **Benefit**: High throughput ingestion. **Cost**: Read-after-write lag (visibility delay). |
| **Data Flow** | Leader-Only Ingress | **Benefit**: Simplified concurrency. **Cost**: The Leader node is a potential bottleneck. |

### Conclusion
The Distributed Payment System leverages Kafka for high-availability ingestion and Raft for rock-solid consistency. This architecture ensures that while there is a slight delay before a payment is visible to readers, once it *is* visible, it is guaranteed to be durable and consistent across the entire cluster.
