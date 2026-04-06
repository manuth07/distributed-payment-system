# Distributed Payment Consensus System (Raft & Kafka)

This repository contains a prototype for a simplified **Fault-Tolerant Distributed Payment Processing System**. It uses **Apache Kafka** for asynchronous event-driven communication and a **Native Raft Implementation** for strong data consistency and leader-based coordination across a 5-node cluster.

## 🏗️ System Architecture
- **Nginx (Load Balancer)**: Central entry point on port `8080`.
- **Payment Service (Spring Boot)**: 5 nodes listening for payments, coordinating via Raft.
- **Apache Kafka**: Persistent event bus for payment events.
- **Apache ZooKeeper**: Distributed coordination for the Kafka cluster.

---

## 🚀 How to Run (Getting Started)

### Quick Start (Recommended)
```bash
# Clean and rebuild
mvn clean compile

# Start all services (includes Docker build)
docker-compose up -d --build

# Wait 15-20 seconds, then open dashboard in browser
# http://localhost:8080/dashboard.html
```

### Prerequisites
- **Java 17** (locked to JDK 17 via pom.xml)
- **Maven 3.6+**
- **Docker Desktop** (running)

### Step-by-Step Setup

**1. Build the Application**
```bash
mvn clean compile
```

**2. Build & Launch Cluster (10 containers)**
```bash
docker-compose up -d --build
```
Wait ~15-20 seconds for all services to start and leader election complete.

**3. Verify Startup**
```bash
# Check all containers running
docker-compose ps

# Should show: 5 nodes, 3 ZooKeepers, Kafka, nginx lb
```

**4. Access the System**

| Component | URL | Purpose |
|-----------|-----|---------|
| **Dashboard** | http://localhost:8080/dashboard.html | Interactive control panel |
| **Direct Node** | http://localhost:8081/dashboard.html | Direct access to node1 |
| **Swagger UI** | http://localhost:8080/swagger-ui.html | Full API documentation |
| **Health Check** | http://localhost:8080/actuator/health | System status |

---

## 📊 Dashboard Features

**Real-time Monitoring:**
- 📊 Cluster status (current node, Raft state, term, quorum)
- 🔗 Peer replication tracking (nextIndex, matchIndex)
- ❤️ Leader/follower state visualization

**Interactive Controls:**
- 💳 Submit test payments immediately
- ⚙️ Kill/restart individual nodes
- 🧪 Fault tolerance demo walkthrough
- ⚡ Stress test (10 rapid payments)
- 📈 Raft consensus details (term, state, commit index)
- ⏱️ Time synchronization metrics

**API Exploration:**
- View Swagger docs with all endpoints
- Check system health status
- Test stress scenarios

---

## 🧪 Testing & Demos

### Option 1: Web Dashboard (Easiest)
1. Open http://localhost:8080/dashboard.html
2. Submit a payment → See it replicated live
3. Click a Node button → Kill the leader
4. Watch new leader elected (term increases)
5. Submit another payment → Still works!
6. Click "Get Raft State" → See new leader info
7. Run stress test → 10 concurrent payments

### Option 2: API Direct Testing
```bash
# Submit a payment
curl -X POST "http://localhost:8080/payments?amount=100&userId=user1"

# Get cluster status
curl "http://localhost:8080/payments/cluster-status"

# See all payments
curl "http://localhost:8080/payments"

# Get Raft status
curl "http://localhost:8080/raft/status"
```

### Manual Node Testing
```bash
# Kill a node
docker stop node1

# View logs
docker logs node1

# Restart it
docker start node1
```

---

## 🧹 Clean Up & Reset

**Stop the cluster:**
```bash
docker-compose down
```

**Full reset (delete all state/payments):**
```bash
# Remove persistent data
Remove-Item -Path data\node* -Recurse -Force

# Restart
docker-compose up -d --build
```

---

## 📂 Project Documentation
Detailed guides for each component can be found in the `documents/` folder:
- **`consensus_readme.md`**: Deep dive into testing Raft and Kafka flow.
- **`docker_commands_readme.md`**: Cheat sheet for terminal commands.
- **`assignment_alignment_report.md`**: Audit of the code against the group assignment tasks.

---

## ⚖️ License
This project is part of a Distributed Systems group assignment.
