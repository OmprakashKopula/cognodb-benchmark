import os
import time
import random
import json
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

CONCURRENCY_LEVELS = [1, 10, 40]
TOTAL_OPERATIONS = 200
READ_RATIO = 0.80

def worker_task(driver, op_id):
    is_read = random.random() < READ_RATIO
    start = time.perf_counter()
    
    with driver.session() as session:
        if is_read:
            node_id = random.randint(0, 19999)
            res = session.run("MATCH (a:User {id: $id})-[:FOLLOWS]->(b) RETURN b.id LIMIT 10", {"id": node_id})
            _ = list(res)
        else:
            u1 = random.randint(0, 19999)
            u2 = random.randint(20000, 25000)
            session.run("""
            MERGE (a:User {id: $u1})
            MERGE (b:User {id: $u2})
            CREATE (a)-[:INTERACTED {ts: timestamp()}]->(b)
            """, {"u1": u1, "u2": u2})
            
    return (time.perf_counter() - start) * 1000.0

def run_concurrency(platform_name, uri, user, password):
    print("\n" + "="*65)
    print(f"      {platform_name.upper()} CONCURRENCY SWEEP (80% Read / 20% Write)")
    print("="*65)
    print(f"{'Concurrency':<14} | {'QPS (Throughput)':<18} | {'p50 (ms)':<12} | {'p95 (ms)':<10}")
    print("-" * 65)

    driver = GraphDatabase.driver(uri, auth=(user, password), max_connection_pool_size=50)
    concurrency_results = {}

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

    print("="*65)
    driver.close()

    os.makedirs("results", exist_ok=True)
    filename = f"results/{platform_name.lower()}_concurrency.json"
    with open(filename, "w") as f:
        json.dump(concurrency_results, f, indent=2)
    print(f"Saved concurrency metrics to '{filename}'")

if __name__ == "__main__":
    run_concurrency(
        platform_name="Neo4j-Aura-Free",
        uri=os.getenv("NEO4J_URI"),
        user=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD")
    )