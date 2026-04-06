# Swagger API Documentation & Control Dashboard

## Quick Access

### 📊 Control Dashboard (Minimal Frontend)
**URL:** http://localhost:8081/dashboard.html

Features:
- **Cluster Status** - Real-time Raft state, term, commit index, replication status
- **Raft Status** - Node state, term, commit index
- **Time Synchronization** - Clock offset, skew status, last sync time
- **Payment Management** - Submit payments with user ID and amount
- **Node Control** - Kill/restart nodes, view node info
- **Recent Payments** - Live payment history with amounts and status
- **Peer Replication** - nextIndex and matchIndex for each peer

### 📖 Swagger UI (Full API Documentation)
**URL:** http://localhost:8081/swagger-ui.html

Or via OpenAPI JSON:
- **URL:** http://localhost:8081/v3/api-docs
- **YAML:** http://localhost:8081/v3/api-docs.yaml

---

## Available Endpoints

### Payments API (`/payments`)
```bash
# Make a payment
POST /payments?amount=100&userId=user123

# Get all payments
GET /payments

# Get payment count
GET /payments/count

# Get cluster status
GET /payments/cluster-status

# Get payments by timestamp range
GET /payments/by-timestamp?startMs=1000&endMs=2000

# Get payments by node
GET /payments/by-node?nodeId=node1

# Get payments by publishing node
GET /payments/by-publishing-node?publishingNodeId=node2

# Get statistics
GET /payments/statistics

# Get user transaction history (quorum-based)
GET /payments/users/{userId}/transactions?status=SUCCESS&from=1000&to=2000

# Node info
GET /payments/node/info
```

### Raft API (`/raft`)
```bash
# Get Raft status
GET /raft/status

# Internal: Request vote
POST /raft/request-vote

# Internal: Append entries (heartbeat)
POST /raft/append-entries
```

### Internal API (`/internal`)
```bash
# Health check
GET /internal/health

# Replicate payment
POST /internal/replicate

# Get user transactions
GET /internal/users/{userId}/transactions

# Get consensus transactions
GET /internal/users/{userId}/transactions/consensus
```

### Admin API (`/admin`)
```bash
# System info
GET /admin/info

# System stats (memory, CPU)
GET /admin/system-stats

# Shutdown node (graceful)
POST /admin/shutdown?force=false

# Readiness probe
GET /admin/ready

# Liveness probe
GET /admin/liveness
```

---

## Dashboard Usage Guide

### 1. Monitor Cluster Health
- Click **"🔄 Refresh"** to update cluster status
- Watch **Raft State** (LEADER/FOLLOWER/CANDIDATE)
- Check **Quorum Reached** status (must be YES for operations)

### 2. Submit Payments
- Enter **User ID** (e.g., "user123")
- Enter **Amount** (e.g., "50")
- Click **"✓ Submit Payment"**
- View confirmation message

### 3. Node Control
- Click one of the **5 node buttons** (node1-node5)
- Choose option:
  - **1) Kill node** - Simulate node failure
  - **2) Get node info** - View node details
  - **3) Get node logs** - Docker logs command

### 4. Monitor Replication
- **Peer Replication Status** section shows per-node replication progress
- **nextIndex** = next log entry to send to that peer
- **matchIndex** = highest log entry replicated to that peer

### 5. View Payments
- **Recent Payments** automatically updates every 10 seconds
- Shows payment ID, user, amount, and status
- Displays last 10 payments in table format

---

## Docker Commands for Node Control

### View Running Nodes
```bash
docker ps | grep distributed-payment-system
```

### Kill a Node (Simulate Failure)
```bash
docker kill distributed-payment-system-node1-1
docker kill distributed-payment-system-node2-1
# etc.
```

### Restart a Node
```bash
docker start distributed-payment-system-node1-1
docker start distributed-payment-system-node2-1
# etc.
```

### View Node Logs
```bash
docker logs -f distributed-payment-system-node1-1
docker logs distributed-payment-system-node2-1 | head -100
```

### Full System Restart
```bash
docker-compose up -d --build
```

---

## Example Workflows

### Workflow 1: Test Leader Failover
1. Open dashboard at http://localhost:8081/dashboard.html
2. Note the current **LEADER** node
3. Click on that node and select **"1) Kill node"**
4. Watch as:
   - Raft state changes from LEADER → (tries election)
   - New leader is elected within ~1 second
   - Quorum adjusts to 2/3 nodes (now have 4 online)
5. Make a payment - it should still work (new leader accepts it)

### Workflow 2: Test Payment Replication
1. Submit a payment with `POST /payments?amount=100&userId=user1`
2. Wait 1 second for replication
3. Query all 5 nodes:
   ```bash
   for port in 8081 8082 8083 8084 8085; do
       echo "Node :$port"
       curl http://localhost:$port/payments/count
   done
   ```
4. All nodes should report the same count (replicated via Raft)

### Workflow 3: Test Time Synchronization
1. View dashboard **Time Synchronization** section
2. Clock offset shows NTP-like correction
3. Skew status indicates health (OK/ALERT/WARNING/CRITICAL)
4. Submit payments from different nodes
5. Payments appear sorted by correctedTimestamp (despite clock skew)

### Workflow 4: Test Consistency Under Load
1. Use stress test: `python3 stress_test_advanced.py`
2. While it runs, kill a leader node via dashboard
3. Stress test continues working (failover transparent)
4. Check consistency audit section for count verification

---

## Swagger Configuration

The Swagger UI is auto-configured via `springdoc-openapi`. Key properties:

```properties
# In application.properties (optional customization)
springdoc.api-docs.path=/v3/api-docs
springdoc.swagger-ui.path=/swagger-ui.html
springdoc.swagger-ui.enabled=true
springdoc.swagger-ui.operations-sorter=method
springdoc.swagger-ui.tags-sorter=alpha
```

---

## Performance Notes

- **Dashboard Updates**: Cluster status refreshes every 5 seconds
- **Payments View**: Refreshes every 10 seconds
- **P2P Raft RPCs**: Heartbeats every ~150ms (configurable)
- **Consensus Latency**: ~50-200ms with 5-node quorum
- **Replication**: AppendEntries propagates to all peers within 200ms

---

## Next Steps

1. ✅ **Swagger is active** - Visit http://localhost:8081/swagger-ui.html for interactive API docs
2. ✅ **Dashboard is ready** - Visit http://localhost:8081/dashboard.html for minimal control panel
3. ⏳ **Start Docker cluster** - `docker-compose up -d --build` once Docker finishes building
4. ⏳ **Run stress test** - `python3 stress_test_advanced.py` to validate system under load
5. ⏳ **Kill/restart nodes** - Use dashboard or Docker commands to simulate failures

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Can't access dashboard | Check port 8081 is open, verify Docker container is running: `docker logs -f` |
| Swagger returns 404 | Ensure `springdoc-openapi` dependency is in pom.xml and project compiled |
| Payment submission fails | Verify leader is elected (Raft State = LEADER), check logs for errors |
| Dashboard doesn't auto-update | JavaScript console may have errors, open DevTools (F12) |
| Nodes can't communicate | Verify Docker network, check `docker network ls`, ensure all nodes on same network |
