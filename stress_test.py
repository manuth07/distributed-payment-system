import requests
import concurrent.futures
import time
import subprocess
import random
import os

# Configuration
LB_URL = "http://localhost:8080/payments"
NODES = {"node1": 8081, "node2": 8082, "node3": 8083, "node4": 8084, "node5": 8085}
API_URL = LB_URL # Default to Load Balancer

def print_header(text):
    print(f"\n{'='*60}\n{text}\n{'='*60}")

def check_connection(url, timeout=5):
    try:
        # Try both the ready probe and the actual payments endpoint
        test_paths = [
            url.replace("/payments", "") + "/admin/ready",
            url # fallback
        ]
        for path in test_paths:
            try:
                response = requests.get(path, timeout=timeout)
                if response.status_code == 200:
                    return True
            except:
                continue
        return False
    except:
        return False

def send_payment(payment_id):
    amount = round(random.uniform(10.0, 1000.0), 2)
    user_id = f"stress-user-{random.randint(1, 1000)}"
    try:
        # Submit to current API_URL
        response = requests.post(f"{API_URL}?amount={amount}&userId={user_id}", timeout=25)
        if response.status_code == 200:
            return True
        return False
    except Exception:
        return False

def stress_test_payments(count):
    print(f"Targeting API at: {API_URL}")
    print(f"Sending {count} concurrent payment requests...")
    start_time = time.time()
    success = 0
    errors = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(send_payment, i) for i in range(count)]
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                success += 1
            else:
                errors += 1
                
    duration = time.time() - start_time
    print(f"⏱️  Completed in {duration:.2f} seconds.")
    print(f"✅  Success: {success} | ❌ Errors: {errors}")
    if duration > 0:
        print(f"⚡  Throughput: {count/duration:.2f} req/sec")
    return success

def get_leader():
    for node_name, port in NODES.items():
        try:
            res = requests.get(f"http://localhost:{port}/payments/cluster-status", timeout=2).json()
            if res.get("raftState") == "LEADER":
                return node_name
        except Exception:
            pass
    return None

def get_pid_by_port(port):
    try:
        output = subprocess.check_output(f"netstat -ano | findstr :{port}", shell=True).decode()
        for line in output.strip().split('\n'):
            if 'LISTENING' in line:
                return line.strip().split()[-1]
    except:
        return None
    return None

def kill_node(node_name):
    port = NODES.get(node_name)
    print(f"💥 Simulating Crash: Killing {node_name} (Port {port})...")
    
    # Try Docker kill
    subprocess.run(["docker", "kill", node_name], capture_output=True)
    
    # Verify if it actually died, if not, try local PID kill (for JAR setup)
    time.sleep(1)
    if check_connection(f"http://localhost:{port}/payments"):
        print(f"⚠️  Docker kill failed or not in Docker. Trying taskkill on port {port}...")
        pid = get_pid_by_port(port)
        if pid:
            subprocess.run(["taskkill", "/F", "/PID", pid], capture_output=True)
            print(f"✅ Killed local process PID {pid}")
    else:
        print(f"✅ {node_name} container killed via Docker")

    print("⏳ Waiting 10 seconds for Raft cluster to detect failure and elect new leader...")
    time.sleep(10)

def start_node(node_name):
    print(f"🏥 Simulating Recovery: Starting {node_name}...")
    # Try Docker start
    subprocess.run(["docker", "start", node_name], capture_output=True)
    print("⏳ Waiting 15 seconds for node to boot and rejoin cluster...")
    time.sleep(15)

def verify_ledgers():
    print("📊 Fetching Ledger Sizes across all nodes...")
    total = 0
    counts = []
    for node, port in NODES.items():
        try:
            res = requests.get(f"http://localhost:{port}/payments", timeout=3)
            if res.status_code == 200:
                ledger = res.json()
                count = len(ledger)
                total += count
                counts.append(count)
                print(f"   🟢 {node} (Port {port}): {count} payments in ledger")
            else:
                print(f"   🔴 {node} (Port {port}): Responded with status {res.status_code}")
        except:
            print(f"   🔴 {node} (Port {port}): Unreachable / DOWN")
    
    avg = total / len(counts) if counts else 0
    return int(avg) 

def check_raft_leader():
    print("👑 Checking Raft Leader Status...")
    leader_found = False
    for node, port in NODES.items():
        try:
            res = requests.get(f"http://localhost:{port}/payments/cluster-status", timeout=2).json()
            state = res.get("raftState", "?")
            term = res.get("raftTerm", "?")
            marker = " ← LEADER 👑" if state == "LEADER" else ""
            if state == "LEADER": leader_found = True
            print(f"   {node}: state={state}, term={term}{marker}")
        except:
            print(f"   {node}: Unreachable / DOWN")
    if not leader_found:
        print("   ⚠️  WARNING: NO LEADER DETECTED!")
    return leader_found

def reset_cluster_data():
    print_header("🧹 CLEANING UP PREVIOUS TEST DATA")
    for node, port in NODES.items():
        try:
            print(f"   Resetting {node} (Port {port})...")
            res = requests.post(f"http://localhost:{port}/admin/reset-all", timeout=5)
            if res.status_code == 200:
                print(f"   ✅ {node} reset successfully.")
        except Exception:
            pass
    print("⏳ Waiting 2 seconds for cluster to stabilize...")
    time.sleep(2)

if __name__ == "__main__":
    print_header("🚀 DISTRIBUTED PAYMENT SYSTEM - STRESS & CHAOS TEST")
    
    # Pre-flight check with retries
    max_retries = 3
    connected = False
    for attempt in range(1, max_retries + 1):
        print(f"🔍 Connection attempt {attempt}/{max_retries}...")
        if check_connection(LB_URL):
            print("✅ Load Balancer (Port 8080) is UP.")
            connected = True
            break
        for name, port in NODES.items():
            if check_connection(f"http://localhost:{port}/payments"):
                API_URL = f"http://localhost:{port}/payments"
                print(f"✅ Found alive node: {name} on port {port}. Using it as entry point.")
                connected = True
                break
        if connected: break
        if attempt < max_retries:
            print("⏳ Cluster not fully ready yet. Waiting 10 seconds...")
            time.sleep(10)

    if not connected:
        print("❌ ERROR: No nodes are reachable. Please ensure Docker is running and ports are mapped.")
        exit(1)

    # 1. Reset data for a clean test run
    reset_cluster_data()
    initial_count = verify_ledgers()

    # 2. PHASE 1: Baseline Stress Test
    print_header("PHASE 1: Baseline Stress Test (All Nodes Alive)")
    success = stress_test_payments(100)
    print("⏳ Waiting 5 seconds for consensus...")
    time.sleep(5)
    current_count = verify_ledgers()
    check_raft_leader()
    print(f"\n📊 Results: {success} requests succeeded, {current_count - initial_count} payments replicated.")

    # 3. PHASE 2: Kill Leader
    print_header("PHASE 2: Leader Crash & Failover Test")
    leader = get_leader()
    if leader:
        print(f"👑 Current Raft Leader: {leader}")
        kill_node(leader)
    else:
        print("⚠️ No leader found. Skipping kill phase.")
    check_raft_leader()

    # 4. PHASE 3: Stress During Failover
    print_header("PHASE 3: Stress Test During Failover (4 nodes)")
    success_failover = stress_test_payments(50)
    print("⏳ Waiting 5 seconds for consensus...")
    time.sleep(5)
    current_count_after_failover = verify_ledgers()
    print(f"\n📊 Results: {success_failover} successes, {current_count_after_failover - current_count} payments stored.")

    # 5. PHASE 4: Recovery
    print_header("PHASE 4: Node Recovery & Catch-up")
    if leader:
        start_node(leader)
    
    # 6. PHASE 5: Final Check
    print_header("PHASE 5: Final Validation")
    verify_ledgers()
    check_raft_leader()

    # 7. PHASE 6: Clock Skew Test
    print_header("PHASE 6: Clock Skew Simulation & Detection")
    skew_node = "node2"
    port = NODES[skew_node]
    skew_amount = 10000  # 10 seconds ahead
    print(f"⏰ Injecting {skew_amount}ms clock skew into {skew_node} (Port {port})...")
    try:
        res = requests.post(f"http://localhost:{port}/admin/simulate-skew?skewMs={skew_amount}", timeout=5)
        if res.status_code == 200:
            print(f"✅ Skew injected successfully.")
            print("⏳ Waiting 10 seconds for TimeSync service to process the skew...")
            time.sleep(10)
            status = requests.get(f"http://localhost:{port}/timesync/status", timeout=5).json()
            print(f"📊 {skew_node} Reported Offset: {status.get('clockOffset')}ms")
            print(f"📊 Cluster Max Skew Detected: {status.get('estimatedSkew')}ms")
            if abs(status.get('clockOffset', 0) + skew_amount) < 2000:
                print("✅ SUCCESS: System detected the skew and calculated a compensating offset!")
            else:
                print("⚠️  WAIT: Sync in progress. Check the dashboard 'Offset' column for node 2.")
    except Exception as e:
        print(f"❌ Failed to test clock skew: {e}")

    print_header("🏁 TEST SUITE COMPLETE")