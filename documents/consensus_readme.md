# Testing the Distributed Payment System with Postman

This guide explains how to test the hybrid Raft + ZooKeeper distributed payment system locally using Postman. The cluster consists of 5 payment nodes and 3 ZooKeeper instances.

## 1. Starting the Cluster
Before testing, ensure you have the ZooKeeper ensemble running locally (ports 2181, 2182, 2183).

Launch all 5 payment instances, overriding the `SERVER_PORT` for each:
```bash
# Start all 5 nodes manually via Bash or PowerShell:
java -jar target/DS_project-0.0.1-SNAPSHOT.jar --SERVER_PORT=8081
java -jar target/DS_project-0.0.1-SNAPSHOT.jar --SERVER_PORT=8082
java -jar target/DS_project-0.0.1-SNAPSHOT.jar --SERVER_PORT=8083
java -jar target/DS_project-0.0.1-SNAPSHOT.jar --SERVER_PORT=8084
java -jar target/DS_project-0.0.1-SNAPSHOT.jar --SERVER_PORT=8085
```

## 2. Testing Payment Creation
The Leader node will process the payment directly, or any Follower will automatically forward the transaction to the Leader.

- **Method**: `POST`
- **URL**: `http://localhost:8081/payments?amount=300` (or any port 8081-8085)
- **Response**: You should receive a `200 OK` with the created payment JSON:
  ```json
  {
      "id": "ea5552bd-d5d3-484e-bcce-82a67a05629c",
      "nodeId": "node-8081",
      "amount": 300,
      "status": "SUCCESS"
  }
  ```

## 3. Testing High Availability (Failover)
To test the failover and Raft log replication explicitly:
1. Send a POST request to a follower (e.g., `8085`) and ensure it succeeds (it routes to the leader, e.g., `8081`).
2. Forcefully kill the leader process (`8081`).
3. Immediately send the same POST request to the follower (`8085`).
4. The system will hold the request briefly. Once the new leader is elected by the remaining 4 nodes, the request will succeed and return the JSON response seamlessly without throwing a 500 error.

## 4. Testing Payment Retrieval
To retrieve all committed payments from the cluster via any node:
- **Method**: `GET`
- **URL**: `http://localhost:8081/payments` (or any valid node URL)
