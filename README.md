# Distributed Payment Consensus System (Raft & Kafka)

This repository contains a prototype for a simplified **Fault-Tolerant Distributed Payment Processing System**. It uses **Apache Kafka** for asynchronous event-driven communication and a **Native Raft Implementation** for strong data consistency and leader-based coordination across a 5-node cluster.

## 🏗️ System Architecture
- **Nginx (Load Balancer)**: Central entry point on port `8080`.
- **Payment Service (Spring Boot)**: 5 nodes listening for payments, coordinating via Raft.
- **Apache Kafka**: Persistent event bus for payment events.
- **Apache ZooKeeper**: Distributed coordination for the Kafka cluster.

---

## 🚀 How to Run (Getting Started)

Follow these steps to get the entire 10-container system running on your local machine:

### 1. Prerequisites
- **Java 17** (The project is locked to JDK 17).
- **Maven 3.x**.
- **Docker Desktop** (Make sure it is running).

### 2. Build the Application
Open your terminal in the root directory and run:
```bash
mvn clean package -DskipTests
```

### 3. Launch the Cluster
Use Docker Compose to start all 10 services (Zookeeper, Kafka, Nginx, and 5 Nodes):
```bash
docker-compose up -d --build
```
*Wait ~1 minute for all nodes to start and elect a Raft leader.*

---

## 🧪 Testing the Pipeline

### Simple Postman Check
- **Endpoint**: `POST http://localhost:8080/payments?amount=500`
- **What happens**: The Load Balancer hits a node, which drops the payment into Kafka. You'll get an immediate `PENDING` response with cluster metadata (Leader, Quorum reached, etc.).

### Verify Consistency
- **Snapshot Status**: `GET http://localhost:8080/payments/cluster-status` (See Raft log size and commit index).
- **Finalized Ledger**: `GET http://localhost:8080/payments` (See all payments that successfully reached consensus).

---

## 📂 Project Documentation
Detailed guides for each component can be found in the `documents/` folder:
- **`consensus_readme.md`**: Deep dive into testing Raft and Kafka flow.
- **`docker_commands_readme.md`**: Cheat sheet for terminal commands.
- **`assignment_alignment_report.md`**: Audit of the code against the group assignment tasks.

---

## ⚖️ License
This project is part of a Distributed Systems group assignment.
