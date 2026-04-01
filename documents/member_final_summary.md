Member 2: Data Replication & Consistency Summary
1. Replication Strategy
Leader-Based Replication: The system uses a pull-push hybrid model. All nodes ingest events from a shared Kafka topic, but only the Raft Leader is authorized to append these to the replicated index.
Raft AppendEntries: The Leader pushes log entries to Followers synchronously via RPC to reach a majority quorum (3/5 nodes) before any payment is considered final.
2. Consistency Model
Strong Consistency (Linearizability): Once a payment is committed by a quorum, it is guaranteed to be visible to all subsequent reads in the same global order.
Sequential Application: The PaymentStateMachine ensures that logs are applied strictly in index order, preventing out-of-order state transitions.
3. Deduplication Mechanism
Triple-Layer Defense:
Ingestion Layer: KafkaConsumerService uses an in-memory HashSet of unique paymentIds to filter local duplicates.
Consensus Layer: The Raft Leader scans the log before appending to ensure no duplicate IDs enter the consensus round.
Persistence Layer: The H2/SQL repository uses findById as a final check before saving to the permanent ledger.
4. Latency Impact
Write Latency: Decoupled. Clients receive an immediate acknowledgment via Kafka, but the "internal" latency includes the network round-trip for Raft replication.
Read Visibility Delay: There is a deterministic "visibility gap" of up to 200ms (the state machine polling interval) plus the time taken for quorum consensus.
5. Storage Overhead
$2N$ Redundancy: Every node stores the full Raft Log (serialized JSON entries) plus the Applied State Machine (Database records). This ensures that any node can become a leader with zero data loss.
6. Performance vs. Consistency Trade-offs
Safety Over Speed: We prioritized Safety (Consistency) over absolute low latency. By waiting for a quorum acknowledge, we prevent "double-spending" or lost updates, which is critical for a payment system, even if it adds milliseconds to the internal commit time.
7. Completed by Member 2
Kafka-Raft Integration: Fixed consumer group isolation so all nodes receive events but only one acts.
State Machine Pipeline: Implemented the PaymentStateMachine to reliably bridge the Raft log and the Database repository.
Deduplication Logic: Secured the system against Kafka's at-least-once delivery duplicates.
Consistency Observability: Added metrics to track commit indices and replication lag across the cluster.
8. Blocked by Member 4 (Consensus Layer)
Leader Availability: If the Leader fails, Member 2's pipeline pauses until Member 4's Election Logic completes a new term.
Log Conflict Resolution: The logic for overwriting uncommitted logs on followers during a leadership change resides in the core Raft implementation (Member 4).
Heartbeat Reliability: The actual network transport and timeout management for AppendEntries is part of the consensus heartbeat infrastructure.