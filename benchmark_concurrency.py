import os
import time
import random
import json
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

TARGET_URI = os.getenv("NEO4J_URI")
TARGET_USER = os.getenv("NEO4J_USER", "neo4j")
TARGET_PASSWORD = os.getenv("NEO4J_PASSWORD")
PLATFORM_NAME = "Neo4j-Aura-Free"

CONCURRENCY_LEVELS = [1, 10, 40]
TOTAL_OPERATIONS = 200  # Operations per concurrency level
READ_RATIO = 0.80       # 80% Reads, 20% Writes

def worker_task(driver, op_id):
    """Executes a single read or write transaction."""
    is_read = random.random() < READ_RATIO
    start = time.perf_counter()
    
    with driver.session() as session:
        if is_read:
            # 80% Read: 1-hop traversal
            node_id = random.randint(0, 19999)
            res = session.run("MATCH (a:User {id: $id})-[:FOLLOWS]->(b) RETURN b.id LIMIT 10", {"id": node_id})
            _ = list(res)
        else:
            # 20% Write: Insert temporary interaction
            u1 = random.randint(0, 19999)
            u2 = random.randint(20000, 25000)
            session.run("""
            MERGE (a:User {id: $u1})
            MERGE (b:User {id: $u2})
            CREATE (a)-[:INTERACTED {ts: timestamp()}]->(b)
            """, {"u1": u1, "u2": u2})
            
    return (time.perf_counter() - start) * 1000.0

def run_concurrency_sweep():
    print(f"Connecting to {PLATFORM_NAME} at {TARGET_URI}...")
    driver = GraphDatabase.driver(
        TARGET_URI, 
        auth=(TARGET_USER, TARGET_PASSWORD),
        max_connection_pool_size=50
    )

    concurrency_results = {}

    print("\n" + "=" * 65)
    print(f"      {PLATFORM_NAME.upper()} CONCURRENCY SWEEP (80% Read / 20% Write)")
    print("=" * 65)
    print(f"{'Concurrency':<14} | {'QPS (Throughput)':<18} | {'p50 (ms)':<12} | {'p95 (ms)':<10}")
    print("-" * 65)

    for workers in CONCURRENCY_LEVELS:
        latencies = []
        start_time = time.perf_counter()

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(worker_task, driver, i) for i in range(TOTAL_OPERATIONS)]
            for f in as_completed(futures):
                try:
                    latencies.append(f.result())
                except Exception:
                    pass

        total_wall_clock = time.perf_counter() - start_time
        successful_ops = len(latencies)
        qps = successful_ops / total_wall_clock if total_wall_clock > 0 else 0
        p50 = float(np.percentile(latencies, 50)) if latencies else 0.0
        p95 = float(np.percentile(latencies, 95)) if latencies else 0.0

        concurrency_results[f"{workers}_workers"] = {
            "workers": workers,
            "qps": round(qps, 2),
            "p50_ms": round(p50, 2),
            "p95_ms": round(p95, 2),
            "total_ops": successful_ops
        }

        print(f"{workers:<14} | {qps:<18.2f} | {p50:<12.2f} | {p95:<10.2f}")

    print("=" * 65)
    driver.close()

    os.makedirs("results", exist_ok=True)
    out_file = f"results/{PLATFORM_NAME.lower()}_concurrency.json"
    with open(out_file, "w") as f:
        json.dump(concurrency_results, f, indent=2)
    print(f"\nConcurrency results saved to '{out_file}'")

if __name__ == "__main__":
    run_concurrency_sweep()