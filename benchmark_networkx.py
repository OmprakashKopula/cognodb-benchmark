import os
import time
import random
import json
import numpy as np
import pandas as pd
import networkx as nx
from concurrent.futures import ThreadPoolExecutor, as_completed

CSV_FILE = "data/edges.csv"
ITERATIONS = 100
CONCURRENCY_LEVELS = [1, 10, 40]
TOTAL_CONCURRENT_OPS = 200

def run_networkx_benchmark():
    print("\n" + "="*65)
    print("       STARTING BENCHMARK: NETWORKX (IN-MEMORY BASELINE)")
    print("="*65)

    df = pd.read_csv(CSV_FILE)
    total_edges = len(df)
    unique_nodes = len(set(df["src"]).union(set(df["dst"])))

    # 1. Ingestion
    print("\n[Phase 1/3] Loading Graph into In-Memory NetworkX Structure...")
    start_load = time.perf_counter()
    G = nx.DiGraph()
    for row in df.itertuples(index=False):
        G.add_edge(row.src, row.dst)
    total_load_time = time.perf_counter() - start_load
    edges_per_sec = total_edges / total_load_time
    nodes_per_sec = unique_nodes / total_load_time
    print(f"Ingest Completed: {total_edges:,} edges in {total_load_time:.2f}s ({edges_per_sec:.2f} edges/sec)")

    # 2. Latency Workloads
    print("\n[Phase 2/3] Running Latency Workloads (100 iterations)...")
    random.seed(42)
    sample_nodes = random.sample(list(G.nodes()), ITERATIONS)
    metrics = {"point_lookup": [], "hop_1": [], "hop_2": [], "hop_3": [], "aggregation": []}

    # Point Lookup
    for nid in sample_nodes:
        t0 = time.perf_counter()
        _ = G.has_node(nid)
        metrics["point_lookup"].append((time.perf_counter() - t0) * 1000.0)

    # 1-Hop
    for nid in sample_nodes:
        t0 = time.perf_counter()
        _ = list(G.successors(nid))
        metrics["hop_1"].append((time.perf_counter() - t0) * 1000.0)

    # 2-Hop
    for nid in sample_nodes:
        t0 = time.perf_counter()
        hops2 = {nbr2 for nbr1 in G.successors(nid) for nbr2 in G.successors(nbr1)}
        _ = len(hops2)
        metrics["hop_2"].append((time.perf_counter() - t0) * 1000.0)

    # 3-Hop
    for nid in sample_nodes:
        t0 = time.perf_counter()
        hops3 = {nbr3 for nbr1 in G.successors(nid) for nbr2 in G.successors(nbr1) for nbr3 in G.successors(nbr2)}
        _ = len(hops3)
        metrics["hop_3"].append((time.perf_counter() - t0) * 1000.0)

    # Aggregation (Top 10 out-degree nodes)
    for _ in range(ITERATIONS):
        t0 = time.perf_counter()
        _ = sorted(G.out_degree(), key=lambda x: x[1], reverse=True)[:10]
        metrics["aggregation"].append((time.perf_counter() - t0) * 1000.0)

    latency_summary = {}
    print("\n" + "-"*65)
    print(f"{'Workload':<20} | {'p50 (ms)':<10} | {'p95 (ms)':<10} | {'Avg (ms)':<10}")
    print("-"*65)
    for w, lats in metrics.items():
        p50 = round(float(np.percentile(lats, 50)), 4)
        p95 = round(float(np.percentile(lats, 95)), 4)
        avg = round(float(np.mean(lats)), 4)
        latency_summary[w] = {"p50": p50, "p95": p95, "avg": avg}
        print(f"{w:<20} | {p50:<10.4f} | {p95:<10.4f} | {avg:<10.4f}")

    # 3. Concurrency
    print("\n[Phase 3/3] Running Concurrency Sweep...")
    concurrency_summary = {}
    
    def worker_task(i):
        is_read = random.random() < 0.80
        t0 = time.perf_counter()
        if is_read:
            nid = random.choice(sample_nodes)
            _ = list(G.successors(nid))
        else:
            u1, u2 = random.randint(0, 19999), random.randint(20000, 25000)
            G.add_edge(u1, u2)
        return (time.perf_counter() - t0) * 1000.0

    for workers in CONCURRENCY_LEVELS:
        latencies = []
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(worker_task, i) for i in range(TOTAL_CONCURRENT_OPS)]
            for f in as_completed(futures):
                latencies.append(f.result())
        wall_time = time.perf_counter() - t0
        qps = len(latencies) / wall_time if wall_time > 0 else 0
        p50 = float(np.percentile(latencies, 50))
        p95 = float(np.percentile(latencies, 95))
        concurrency_summary[f"{workers}_workers"] = {"workers": workers, "qps": round(qps, 2), "p50_ms": round(p50, 4), "p95_ms": round(p95, 4)}
        print(f"Workers: {workers:<2} | QPS: {qps:<10.2f} | p50: {p50:<8.4f} ms | p95: {p95:<8.4f} ms")

    final_output = {
        "platform": "NetworkX-InMemory",
        "ingestion": {"wall_clock_sec": round(total_load_time, 2), "edges_per_sec": round(edges_per_sec, 2), "nodes_per_sec": round(nodes_per_sec, 2)},
        "latencies": latency_summary,
        "concurrency": concurrency_summary
    }
    os.makedirs("results", exist_ok=True)
    with open("results/networkx-inmemory_results.json", "w") as f:
        json.dump(final_output, f, indent=2)
    print("\nResults saved to 'results/networkx-inmemory_results.json'")

if __name__ == "__main__":
    run_networkx_benchmark()