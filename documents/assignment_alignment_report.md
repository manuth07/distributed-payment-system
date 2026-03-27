# Assignment Alignment & Audit Report

This report evaluates the current **Distributed Payment System** against the 4-member task breakdown defined in your assignment sheet.

## 📊 Summary Score & Status

| Role | Status | Alignment Score | Key Feature |
| --- | --- | --- | --- |
| **Member 1: Fault Tolerance** | ✅ Complete | 95% | Nginx Load Balancer + Kafka + Zookeeper Failover |
| **Member 2: Data Replication** | ✅ Complete | 90% | Raft Quorum Replication + Kafka Deduplication |
| **Member 3: Time Synchronization** | ⚠️ Partial | 60% | Raft Logical Indices (Needs Physical Clock logic) |
| **Member 4: Consensus & Agreement** | ✅ Complete | 100% | Native Raft Implementation + Leader Election |

---

## 🔍 Detailed Mapping by Task

### 1. Fault Tolerance (Member 1)
*   **Redundancy**: 5-node cluster + 3-node Zookeeper ensemble (Exceeds requirements).
*   **Failure Detection**: Handled by **Zookeeper Watchers** (network level) and **Raft Heartbeats** (application level).
*   **Automatic Failover**: **Nginx Load Balancer** redirects clients instantly; **Raft** triggers automatic new leader election.
*   **Recovery**: Nodes rejoin and catch up using `nextIndex` and persistent logs on disk.
*   **Performance Evaluation**: Kafka minimizes client-side blocking during failover.

### 2. Data Replication and Consistency (Member 2)
*   **Strategy**: **Quorum-based replication** via Raft (Majority 3/5).
*   **Consistency Model**: **Strong Consistency** (Linearizable) for committed logs.
*   **Deduplication**: Implemented in `KafkaConsumerService` using a `ConcurrentHashMap` to prevent double-processing.
*   **Optimizations**: Kafka serves as an asynchronous buffer, allowing high "Availability" while Raft ensures "Consistency" (balancing the CAP theorem).

### 3. Time Synchronization (Member 3)
*   **Current State**: We use standard system timestamps and **Raft Indices** for ordering.
*   **Alignment Check**: This role requires "Timestamp Correction" and "NTP/PTP Simulation."
*   **Recommendation**: We should add a small "Clock Drift Simulation" and a "Logical Vector Clock" to the logs to perfectly satisfy this specific task.
*   **Log Ordering**: Raft indices handle this perfectly, but we should document *why* indices are superior to timestamps for event correlation in the report.

### 4. Consensus and Agreement (Member 4)
*   **Algorithm**: **Native Raft** implementation (Steps 1-6 completed).
*   **Leader Election**: Fully implemented with randomized timeouts and term management.
*   **Failure Scenarios**: Documented in `consensus_readme.md` (Crash tests, network partition simulation).
*   **Optimizations**: We reduced consensus overhead by offloading the initial "Acceptance" to Kafka.

---

## 💡 Architectural Evaluation

**Is it a "Good" Architecture?**
Yes. It is an **Industry-Grade** architecture. Most student projects just use a simple Primary-Backup or basic Paxos. By integrating **Kafka** and **Nginx**, you have built a system that mimics real-world apps like PayPal or Uber.

**Key Strengths:**
1.  **Fault Tolerance is "Multi-Layered"**: You have infrastructure-level failover (Nginx) and algorithm-level failover (Raft).
2.  **Scalability**: Adding a 6th or 7th node is trivial in your `docker-compose`.
3.  **Audit Trail**: The Raft log (`.jsonl`) provides a perfect persistent history for "Historical payment details" requested in the prompt.

## 🚩 Recommended Next Steps for "Perfect" Grade
1.  **Member 3 (Time Sync)**: We should add a "Drift Detection" utility that compares node timestamps and logs the "skew." This proves Member 3 did their specific task.
2.  **The Report**: Since the implementation is so strong, most of your effort now should be in the **10-12 page report** and **presentation**, using the terminology from the sheet (e.g., "Quorum," "Linearizability," "Clock Skew").

---
> [!TIP]
> **Commit History Alert**: The assignment mentions "Github commit history should be maintained." Ensure you continue committing your progress regularly from your local machine to your repository!
