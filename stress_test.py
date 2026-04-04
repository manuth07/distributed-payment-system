import requests
import concurrent.futures
import time
import subprocess
import random

API_URL = "http://localhost:8080/payments"
STATUS_URL = "http://localhost:8080/payments/cluster-status"
NODES = {"node1": 8081, "node2": 8082, "node3": 8083, "node4": 8084, "node5": 8085}

def print_header(text):
    print(f"\n{'='*60}\n{text}\n{'='*60}")

def send_payment(payment_id):
    amount = random.randint(10, 1000)
    try:
        response = requests.post(f"{API_URL}?amount={amount}", timeout=5)
        if response.status_code == 200:
            return True
        return False
    except Exception as e:
        return False

def stress_test_payments(count):
    print(f"Sending {count} concurrent payment requests via Load Balancer...")
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
    print(f"⚡  Throughput: {count/duration:.2f} req/sec")
    return success

def get_leader():
    for target_node, port in NODES.items():
        try:
            res = requests.get(f"http://localhost:{port}/payments/cluster-status", timeout=2).json()
            if res.get("isLeader", False):
                return target_node
        except Exception:
            pass
    return None

def kill_node(node_name):
    print(f"💥 Simulating Crash: Killing {node_name} container...")
    subprocess.run(["docker", "kill", node_name], capture_output=True)
    print("⏳ Waiting 5 seconds for Raft cluster to detect failure and elect new leader...")
    time.sleep(5)

def start_node(node_name):
    print(f"🏥 Simulating Recovery: Starting {node_name} container...")
    subprocess.run(["docker", "start", node_name], capture_output=True)
    print("⏳ Waiting 15 seconds for node to boot and rejoin Kafka consumer group...")
    time.sleep(15)

def verify_ledgers():
    print("📊 Fetching Ledger Sizes across all nodes...")
    total = 0
    for node, port in NODES.items():
        try:
            res = requests.get(f"http://localhost:{port}/payments/count", timeout=3)
            if res.status_code == 200:
                count = int(res.text)
                total += count
                print(f"   🟢 {node} (Port {port}): {count} payments in ledger")
            else:
                print(f"   🔴 {node} (Port {port}): Responded with status {res.status_code}")
        except:
            print(f"   🔴 {node} (Port {port}): Unreachable / DOWN")
    print(f"   📦 TOTAL payments across cluster: {total}")
    return total

def check_raft_leader():
    print("👑 Checking Raft Leader Status...")
    for node, port in NODES.items():
        try:
            res = requests.get(f"http://localhost:{port}/payments/cluster-status", timeout=2).json()
            state = res.get("raftState", "?")
            term = res.get("raftTerm", "?")
            is_leader = res.get("isLeader", False)
            marker = " ← LEADER 👑" if is_leader else ""
            print(f"   {node}: state={state}, term={term}{marker}")
        except:
            print(f"   {node}: Unreachable / DOWN")

if __name__ == "__main__":
    print_header("🚀 DISTRIBUTED PAYMENT SYSTEM - STRESS & CHAOS TEST")
    
    # ─── PHASE 1: Baseline ────────────────────────────────────
    print_header("PHASE 1: Baseline Stress Test (All Nodes Alive)")
    sent = stress_test_payments(100)
    
    # Wait for Kafka consumers to process all messages
    print("⏳ Waiting 10 seconds for Kafka consumers to finish processing...")
    time.sleep(10)
    
    total = verify_ledgers()
    check_raft_leader()
    
    if total == 0:
        print("\n🚨 CRITICAL: No payments stored! Pipeline is broken.")
        print("   Check docker logs: docker-compose logs --tail=50 node1")
        exit(1)
    elif total < sent:
        print(f"\n⚠️ WARNING: Only {total}/{sent} payments stored. Some may still be processing.")
    else:
        print(f"\n✅ PERFECT: All {total}/{sent} payments distributed and stored!")

    # ─── PHASE 2: Kill Leader ─────────────────────────────────
    print_header("PHASE 2: Leader Crash & Failover Test")
    leader = get_leader()
    if leader:
        print(f"👑 Current Raft Leader identified as: {leader}")
        kill_node(leader)
    else:
        print("⚠️ Could not identify leader. Randomly killing node3.")
        kill_node("node3")
        leader = "node3"
        
    check_raft_leader()

    # ─── PHASE 3: Stress During Failover ──────────────────────
    print_header("PHASE 3: Stress Test During Failover (4 nodes)")
    stress_test_payments(50)
    
    print("⏳ Waiting 10 seconds for processing...")
    time.sleep(10)
    verify_ledgers()
    check_raft_leader()

    # ─── PHASE 4: Recovery ────────────────────────────────────
    print_header("PHASE 4: Node Recovery & State Catch-up")
    start_node(leader)
    
    # ─── PHASE 5: Final Check ─────────────────────────────────
    print_header("PHASE 5: Final Validation")
    verify_ledgers()
    check_raft_leader()
    
    print_header("🏁 TEST SUITE COMPLETE")
