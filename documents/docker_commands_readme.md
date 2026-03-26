# Docker Cluster Testing Commands

This file serves as a quick reference for the commands needed to test the 8-node cluster (3 ZooKeepers + 5 Payment Nodes) locally using Docker.

## 1. Complete Rebuild & Start
Whenever you change Java code, you must recompile the `.jar` file and trigger Docker to rebuild its containers so your new code takes effect.
```bash
# 1. Compile the new code into the target folder
mvn clean package -DskipTests

# 2. Start the entire 8-node cluster in the background
docker-compose up -d --build
```

## 2. Watching the Live Logs
To watch the console output of all 8 nodes simultaneously (extremely useful for monitoring elections):
```bash
docker-compose logs -f
```
*(Press `Ctrl+C` to stop watching the logs. This does not stop the servers.)*

## 3. Testing Server Failures & Recovery
You can simulate different types of server crashes directly from your terminal. Open a **new** terminal window to run these while the cluster is running in the background:

**To abruptly crash a server (simulates a power loss):**
```bash
docker kill node1
```
*(You can replace `node1` with any node from `node1` to `node5`, or even `zookeeper1`)*

**To gracefully shut down a server:**
```bash
docker stop node1
```

**To bring a dead/crashed node back online:**
```bash
docker start node1
```

## 4. Shutting Everything Down
When you are done testing for the day, you can instantly tear down all 8 nodes and their networks with a single command:
```bash
docker-compose down
```
