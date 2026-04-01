# Automated Stress Testing & Chaos Engineering

To fulfill the group homework's requirement to evaluate and test the system under high loads and failure scenarios (Network partitions, Node crashes), I have provided an automated Python script.

This script tests:
1. **Concurrency**: Sending 100 simultaneous requests via multi-threading.
2. **Failure Detection**: Automating the process of identifying and killing the active Raft Leader.
3. **Failover Availability**: Running a massive bulk of requests while the cluster is degraded to ensure Nginx and Kafka hold traffic safely.
4. **Log Replication Consistency**: Starting the dead node back up and tracking whether it successfully copies all missing logs when rejoining the consensus loop.

---

## 🚀 Prerequisites

1. Ensure the 10-container system is completely up and running.
   ```bash
   docker-compose up -d --build
   ```
2. You need Python installed on your Windows machine. (Usually comes installed, or download from Python.org).
3. Ensure you have the `requests` library installed globally or in your environment.
   Open your PowerShell and run:
   ```bash
   pip install requests
   ```

---

## 🕹️ Run the Chaos Test

Navigate to your project root `E:\DS_project` and run the script:

```bash
python stress_test.py
```

### What You Will See in the Terminal:
1. **Baseline Load**: It hits `http://localhost:8080/payments` rapidly. (Nginx routes traffic evenly. Kafka absorbs the load).
2. **The Assasination**: It queries `/cluster-status`, parses who the Raft Leader is, and runs a `docker kill` command targeting that specific container.
3. **The Panic Load**: While the cluster detects the crash and triggers a new Election Term, it fires *another* 100 requests. 
   > Note: A few connections may briefly "Error" natively through Nginx while it drops the dead connection, but the system overall survives.
4. **The Resurrection**: It runs a `docker start` on the killed Leader container.
5. **The Final Ledger Audit**: After 10 seconds, it queries all 5 nodes on their unique ports (`8081` to `8085`). You will physically see the recovered node pull the missing logs from the new Leader and eventually report the **exact same ledger size** as the rest of the cluster.

---

## 📊 Mapping Results to Your Assignment

*   **Member 1 (Fault Tolerance)**: You can show your professor the console output proving Nginx/Kafka successfully rerouted payments during "PHASE 3".
*   **Member 2 & 4 (Replication & Consensus)**: The console output for "PHASE 5" proves the recovered node synchronized the correct logs from the Raft quorum.
