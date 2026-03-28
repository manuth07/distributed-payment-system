# Testing the Distributed Payment System with Docker & Postman

This guide explains how to test the new **Kafka-backed, Load-Balanced, Raft-coordinated** distributed payment system.

## 1. Starting the Entire Cluster
We now use Docker Compose to manage 10 connected services: 3 ZooKeepers, 1 Kafka broker, 1 Nginx Load Balancer, and 5 Payment Nodes.

```bash
# Clean and compile your Java code first
mvn clean package -DskipTests

# Start everything globally
docker-compose up -d --build
```

## 2. The New Architecture
- **Load Balancer (Nginx)**: Listens on port `8080`. It automatically distributes requests to the 5 nodes.
- **Kafka**: Acts as an asynchronous buffer. Payments are accepted immediately and finalized later.
- **Consensus**: Only the Raft Leader processes the messages from Kafka into the permanent ledger.

## 3. Testing Payment Creation (Asynchronous)
Instead of hitting a specific node, you now hit the central API Gateway.

- **Method**: `POST`
- **URL**: `http://localhost:8080/payments?amount=250`
- **Response**:
  ```json
  {
      "paymentId": "550e8400-e29b-41d4-a716-446655440000",
      "amount": 250,
      "timestamp": 1711584000000,
      "raftStatus": "PENDING",
      "raftLeaderNodeId": "node-8082",
      "raftLeaderUrl": "http://node2:8082",
      "replicatedToNodes": 3,
      "quorumRequired": 3,
      "consensusReached": true,
      "raftTerm": 2,
      "logIndex": 5,
      "kafkaTopic": "payments",
      "kafkaConsumerGroup": "ds-payment-group",
      "receivingNode": "node-8081"
  }
  ```
> [!NOTE]
> `raftStatus` is `PENDING` immediately. Once the Raft state machine commits the entry (usually within 200ms), the payment appears in `GET /payments` as `SUCCESS`.

## 4. Verifying Kafka Flow
To confirm your payment is actually flowing through Kafka, check the logs of any node:
```bash
docker-compose logs -f node1 | findstr "Kafka\|Consumed\|Appended\|Consensus"
```
You should see a sequence like:
```
Published payment 550e8400... to Kafka topic 'payments'   ← Producer sent it
Consumed payment 550e8400... off Kafka stream.            ← Consumer received it
I am LEADER. Packaging 550e8400... into Raft Log          ← Leader appended to log
Appended log entry 6 for payment 550e8400...              ← Log index
Consensus reached on index 6. Updating commitIndex.       ← Quorum achieved
State Machine: Applied payment 550e8400... (Index: 6)     ← Saved to DB
```

## 4. Checking Raft Consensus Status
Monitor which node is the leader and the state of their commit logs:
- **URL**: `http://localhost:8080/raft/status`
- **What to look for**: The `commitIndex` should be identical across all healthy nodes.

## 5. Testing Fault Tolerance (The "Crash Test")
This is where the Kafka + Load Balancer architecture shines:

1. **Submit a payment** via the Load Balancer (`8080`).
2. **Abruptly kill a node** (even the leader!): `docker kill node1`
3. **Submit another payment** via the Load Balancer (`8080`).
4. **Observation**: The second payment will **NOT** fail. Nginx will route it to an alive node, and Kafka will hold the message until the cluster elects a new leader and resumes processing.

## 6. Verifying Finalized Ledger
To see the actual processed payments that passed consensus:
- **Method**: `GET`
- **URL**: `http://localhost:8080/payments`
