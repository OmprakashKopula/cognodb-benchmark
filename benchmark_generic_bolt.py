import os
import time
import random
import json
import numpy as np
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

CSV_FILE = "data/edges.csv"
BATCH_SIZE = 2500
ITERATIONS = 100

def run_platform_benchmark(platform_name, uri, user, password):
    print("\n" + "=" * 60)
    print(f"      STARTING BENCHMARK: {platform_name.upper()}")
    print("=" * 60)
    print(f"Connecting to: {uri} ...")
    
    driver = GraphDatabase.driver(uri, auth=(user, password))
    df = pd.read_csv(CSV_FILE)
    total_edges = len(df)
    unique_nodes = len(set(df["src"]).union(set(df["dst"])))
    records = df.to_dict(orient="records")

    results = {"platform": platform_name}

    # ---------------- 1. INGESTION ----------------
    print("\n[Phase 1/2] Index Creation & Data Ingestion...")
    with driver.session() as session:
        try:
            session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.id IS UNIQUE;")
        except Exception:
            try:
                session.run("CREATE INDEX ON :User(id);")
            except Exception:
                pass

        start_load = time.perf_counter()
        query = """
        UNWIND $batch AS row
        MERGE (a:User {id: row.src})
        MERGE (b:User {id: row.dst})
        MERGE (a)-[:FOLLOWS]->(b)
        """
        for i in tqdm(range(0, total_edges, BATCH_SIZE), desc=f"Ingesting into {platform_name}"):
            batch = records[i : i + BATCH_SIZE]
            session.run(query, {"batch": batch})

        total_load_time = time.perf_counter() - start_load
        edges_per_sec = total_edges / total_load_time
        nodes_per_sec = unique_nodes / total_load_time

        results["ingestion"] = {
            "wall_clock_sec": round(total_load_time, 2),
            "edges_per_sec": round(edges_per_sec, 2),
            "nodes_per_sec": round(nodes_per_sec, 2)
        }

    # ---------------- 2. LATENCY WORKLOADS ----------------
    print("\n[Phase 2/2] Query Latency & Traversal Workloads (100 iterations)...")
    random.seed(42)
    sample_nodes = random.sample(range(0, 20000), ITERATIONS)
    metrics = {"point_lookup": [], "hop_1": [], "hop_2": [], "hop_3": [], "aggregation": []}

    def time_query(session, query, params=None):
        t0 = time.perf_counter()
        res = session.run(query, params or {})
        _ = list(res)
        return (time.perf_counter() - t0) * 1000.0

    with driver.session() as session:
        # Warmup
        for nid in sample_nodes[:10]:
            time_query(session, "MATCH (n:User {id: $id}) RETURN n", {"id": int(nid)})

        # Point lookup
        for nid in sample_nodes:
            metrics["point_lookup"].append(time_query(session, "MATCH (n:User {id: $id}) RETURN n", {"id": int(nid)}))

        # 1-hop
        for nid in sample_nodes:
            metrics["hop_1"].append(time_query(session, "MATCH (a:User {id: $id})-[:FOLLOWS]->(b) RETURN b.id", {"id": int(nid)}))

        # 2-hop
        for nid in sample_nodes:
            metrics["hop_2"].append(time_query(session, "MATCH (a:User {id: $id})-[:FOLLOWS]->()-[:FOLLOWS]->(c) RETURN count(c)", {"id": int(nid)}))

        # 3-hop
        for nid in sample_nodes:
            metrics["hop_3"].append(time_query(session, "MATCH (a:User {id: $id})-[:FOLLOWS*3]->(d) RETURN count(d)", {"id": int(nid)}))

        # Aggregation
        for _ in range(ITERATIONS):
            query = "MATCH (n:User)-[r:FOLLOWS]->() RETURN n.id, count(r) AS degree ORDER BY degree DESC LIMIT 10"
            metrics["aggregation"].append(time_query(session, query))

    results["latencies"] = {}
    print("\n" + "-" * 60)
    print(f"{'Workload':<20} | {'p50 (ms)':<10} | {'p95 (ms)':<10} | {'Avg (ms)':<10}")
    print("-" * 60)
    for w, lats in metrics.items():
        p50 = round(float(np.percentile(lats, 50)), 2)
        p95 = round(float(np.percentile(lats, 95)), 2)
        avg = round(float(np.mean(lats)), 2)
        results["latencies"][w] = {"p50": p50, "p95": p95, "avg": avg}
        print(f"{w:<20} | {p50:<10.2f} | {p95:<10.2f} | {avg:<10.2f}")

    driver.close()

    os.makedirs("results", exist_ok=True)
    filename = f"results/{platform_name.lower()}_results.json"
    with open(filename, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved results to {filename}")

if __name__ == "__main__":
    run_platform_benchmark(
        platform_name="Neo4j-Aura-Free",
        uri=os.getenv("NEO4J_URI"),
        user=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD")
    )