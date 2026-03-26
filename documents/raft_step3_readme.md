# Raft Step 3 & Docker Automation Setup

This document explains the Docker orchestration introduced to manage the 5-node cluster, and how to test the newly implemented Raft **RequestVote RPC** (Step 3).

## 1. What is the Docker Setup?
To prevent ZooKeeper `ConnectionLossException` errors and to cleanly run 8 processes (3 ZK + 5 Payment Nodes), we introduced two files:
- **`Dockerfile`**: A blueprint that takes your compiled Spring Boot application (`.jar` file) and packages it into a highly isolated, runnable virtual Linux environment (a container). 
- **`docker-compose.yml`**: An orchestrator that reads the `Dockerfile` and elegantly spins up 5 identical Payment Nodes and 3 independent ZooKeeper instances. It wires them together into their own private virtual network and injects their individual configuration variables (like server port and ZK URLs) so you never have to manually run 8 console windows again!

**To launch the entire 8-node system:**
```bash
mvn clean package -DskipTests
docker-compose up --build
```

## 2. What was built in Step 3?
We implemented Phase 3 of the Raft native algorithm:
- `RequestVoteRequest` & `RequestVoteResponse` models.
- `RaftController`: Exposes `POST /raft/request-vote`.
- `RaftRpcService`: Implements strict Raft paper §5.1, §5.2 rules. It grants a vote *only* if the incoming candidate term is higher/equal to the current node's term AND the candidate's log is at least as up-to-date.

## 3. How to Test RequestVote via Postman
You can test the internal Raft voting mechanism manually right now, even before the automated Leader logic is built!

**Target**: `http://localhost:8081/raft/request-vote`
**Method**: `POST`
**Headers**: `Content-Type: application/json`

### Scenario A: Successful Vote Request
Send a request representing a candidate with a highly updated log (high term and index):
```json
{
    "term": 5,
    "candidateId": "node-8085",
    "lastLogIndex": 100,
    "lastLogTerm": 5
}
```
**Expected Response**:
Because Node 1 starts at Term 0 and Index -1, it will gladly grant its vote to Candidate 8085. Notice the node's state transition logged in your console!
```json
{
    "term": 5,
    "voteGranted": true
}
```

### Scenario B: Rejected Vote Request (Outdated Log)
Send a request where the candidate's term or log is too old:
```json
{
    "term": 1,
    "candidateId": "node-8082",
    "lastLogIndex": -1,
    "lastLogTerm": 1
}
```
**Expected Response**:
Because Node 1's `currentTerm` was just upgraded to 5 (from Scenario A), it will strictly reject this outdated term 1 vote:
```json
{
    "term": 5,
    "voteGranted": false
}
```
