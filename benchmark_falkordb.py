import os
import time
import random
import json
import numpy as np
import pandas as pd
from falkordb import FalkorDB
from dotenv import load_dotenv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

FALKORDB_HOST = os.getenv("FALKORDB_HOST")
FALKORDB_PORT = os.getenv("FALKORDB_PORT", "6379")
FALKORDB_USERNAME = os.getenv("FALKORDB_USERNAME", "default")
FALKORDB_PASSWORD = os.getenv("FALKORDB_PASSWORD")

CSV_FILE = "data/edges.csv"
BATCH_SIZE = 500  # Safe batch size for FalkorDB Free Tier
ITERATIONS = 100
CONCURRENCY_LEVELS = [1, 10, 40]
TOTAL_CONCURRENT_OPS = 200

def get_client():
    url = f"falkor://{FALKORDB_USERNAME}:{FALKORDB_PASSWORD}@{FALKORDB_HOST}:{FALKORDB_PORT}"
    return FalkorDB.from_url(url, socket_timeout=60.0, socket_connect_timeout=15.0)

def run_falkordb_benchmarks():
    print("\n" + "=" * 65)
    print("           STARTING BENCHMARK: FALKORDB-CLOUD")
    print("=" * 65)
    
    client = get_client()
    g = client.select_graph("benchmark_graph")
    
    # 1. Clean previous state
    try:
        g.delete()
        g = client.select_graph("benchmark_graph")
    except Exception:
        pass

    # 2. Ingestion
    print("\n[Phase 1/3] Creating Index & Ingesting Data...")
    try:
        g.query("CREATE INDEX FOR (u:User) ON (u.id)")
    except Exception:
        pass

    df = pd.read_csv(CSV_FILE)
    total_edges = len(df)
    unique_nodes = len(set(df["src"]).union(set(df["dst"])))
    records = df.to_dict(orient="records")

    start_load = time.perf_counter()
    query = """
    UNWIND $batch AS row
    MERGE (a:User {id: row.src})
    MERGE (b:User {id: row.dst})
    MERGE (a)-[:FOLLOWS]->(b)
    """

    for i in tqdm(range(0, total_edges, BATCH_SIZE), desc="Ingesting into FalkorDB"):
        batch = records[i : i + BATCH_SIZE]
        # Re-try on transient disconnects
        for attempt in range(3):
            try:
                g.query(query, {"batch": batch})
                break
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(1)
                client = get_client()
                g = client.select_graph("benchmark_graph")

    total_load_time = time.perf_counter() - start_load
    edges_per_sec = total_edges / total_load_time
    nodes_per_sec = unique_nodes / total_load_time

    print(f"\nIngest Completed: {total_edges:,} edges in {total_load_time:.2f}s ({edges_per_sec:.2f} edges/sec)")

    # 3. Latency Workloads
    print("\n[Phase 2/3] Running Latency Workloads (100 iterations)...")
    random.seed(42)
    sample_nodes = random.sample(range(0, 20000), ITERATIONS)
    metrics = {"point_lookup": [], "hop_1": [], "hop_2": [], "hop_3": [], "aggregation": []}

    # Warmup
    for nid in sample_nodes[:10]:
        g.query(f"MATCH (n:User {{id: {nid}}}) RETURN n")

    # Point Lookup
    for nid in sample_nodes:
        t0 = time.perf_counter()
        g.query(f"MATCH (n:User {{id: {nid}}}) RETURN n")
        metrics["point_lookup"].append((time.perf_counter() - t0) * 1000.0)

    # 1-Hop
    for nid in sample_nodes:
        t0 = time.perf_counter()
        g.query(f"MATCH (a:User {{id: {nid}}})-[:FOLLOWS]->(b:User) RETURN b.id")
        metrics["hop_1"].append((time.perf_counter() - t0) * 1000.0)

    # 2-Hop
    for nid in sample_nodes:
        t0 = time.perf_counter()
        g.query(f"MATCH (a:User {{id: {nid}}})-[:FOLLOWS]->()-[:FOLLOWS]->(c:User) RETURN count(c)")
        metrics["hop_2"].append((time.perf_counter() - t0) * 1000.0)

    # 3-Hop
    for nid in sample_nodes:
        t0 = time.perf_counter()
        g.query(f"MATCH (a:User {{id: {nid}}})-[:FOLLOWS*3]->(d:User) RETURN count(d)")
        metrics["hop_3"].append((time.perf_counter() - t0) * 1000.0)

    # Aggregation
    for _ in range(ITERATIONS):
        t0 = time.perf_counter()
        g.query("MATCH (n:User)-[r:FOLLOWS]->() RETURN n.id, count(r) AS degree ORDER BY degree DESC LIMIT 10")
        metrics["aggregation"].append((time.perf_counter() - t0) * 1000.0)

    latency_summary = {}
    print("\n" + "-" * 65)
    print(f"{'Workload':<20} | {'p50 (ms)':<10} | {'p95 (ms)':<10} | {'Avg (ms)':<10}")
    print("-" * 65)
    for w, lats in metrics.items():
        p50 = round(float(np.percentile(lats, 50)), 2)
        p95 = round(float(np.percentile(lats, 95)), 2)
        avg = round(float(np.mean(lats)), 2)
        latency_summary[w] = {"p50": p50, "p95": p95, "avg": avg}
        print(f"{w:<20} | {p50:<10.2f} | {p95:<10.2f} | {avg:<10.2f}")

    # 4. Concurrency Sweep
    print("\n[Phase 3/3] Running Mixed Concurrency Sweep (80% Read / 20% Write)...")
    concurrency_summary = {}
    
    def worker_task(op_id):
        client_thread = get_client()
        g_thread = client_thread.select_graph("benchmark_graph")
        is_read = random.random() < 0.80
        t0 = time.perf_counter()
        if is_read:
            nid = random.randint(0, 19999)
            g_thread.query(f"MATCH (a:User {{id: {nid}}})-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 10")
        else:
            u1, u2 = random.randint(0, 19999), random.randint(20000, 25000)
            g_thread.query(f"MERGE (a:User {{id: {u1}}}) MERGE (b:User {{id: {u2}}}) CREATE (a)-[:INTERACTED]->(b)")
        return (time.perf_counter() - t0) * 1000.0

    for workers in CONCURRENCY_LEVELS:
        latencies = []
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(worker_task, i) for i in range(TOTAL_CONCURRENT_OPS)]
            for f in as_completed(futures):
                try:
                    latencies.append(f.result())
                except Exception:
                    pass
        wall_time = time.perf_counter() - t0
        qps = len(latencies) / wall_time if wall_time > 0 else 0
        p50 = float(np.percentile(latencies, 50)) if latencies else 0.0
        p95 = float(np.percentile(latencies, 95)) if latencies else 0.0
        concurrency_summary[f"{workers}_workers"] = {
            "workers": workers, 
            "qps": round(qps, 2), 
            "p50_ms": round(p50, 2), 
            "p95_ms": round(p95, 2)
        }
        print(f"Workers: {workers:<2} | QPS: {qps:<6.2f} | p50: {p50:<6.2f} ms | p95: {p95:<6.2f} ms")

    # 5. Save Summary
    final_output = {
        "platform": "FalkorDB-Cloud",
        "ingestion": {
            "wall_clock_sec": round(total_load_time, 2),
            "edges_per_sec": round(edges_per_sec, 2),
            "nodes_per_sec": round(nodes_per_sec, 2)
        },
        "latencies": latency_summary,
        "concurrency": concurrency_summary
    }
    os.makedirs("results", exist_ok=True)
    with open("results/falkordb-cloud_results.json", "w") as f:
        json.dump(final_output, f, indent=2)
    print("\nResults saved to 'results/falkordb-cloud_results.json'")

if __name__ == "__main__":
    run_falkordb_benchmarks()