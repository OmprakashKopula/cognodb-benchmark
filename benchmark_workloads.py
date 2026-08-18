import os
import time
import random
import json
import numpy as np
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

COGNODB_URI = os.getenv("COGNODB_URI")
COGNODB_USER = os.getenv("COGNODB_USER", "cognodb")
COGNODB_PASSWORD = os.getenv("COGNODB_PASSWORD")

# Number of iterations for stable percentiles
ITERATIONS = 100

def time_query(session, query, params=None):
    start = time.perf_counter()
    result = session.run(query, params or {})
    _ = list(result)  # Exhaust the stream completely
    return (time.perf_counter() - start) * 1000.0  # Return latency in ms

def run_benchmarks():
    print(f"Connecting to CognoDB at {COGNODB_URI}...")
    driver = GraphDatabase.driver(COGNODB_URI, auth=(COGNODB_USER, COGNODB_PASSWORD))

    # Pick 100 random node IDs from the loaded range (0 to 19999)
    random.seed(42)
    sample_nodes = random.sample(range(0, 20000), ITERATIONS)

    metrics = {
        "point_lookup": [],
        "hop_1": [],
        "hop_2": [],
        "hop_3": [],
        "aggregation": []
    }

    with driver.session() as session:
        # 1. Warm-Up Phase (Discard cold-start execution)
        print("\n[1/5] Warming up engine caches (15 iterations)...")
        for nid in sample_nodes[:15]:
            time_query(session, "MATCH (n:User {id: $id}) RETURN n", {"id": int(nid)})
            time_query(session, "MATCH (a:User {id: $id})-[:FOLLOWS]->(b) RETURN b.id", {"id": int(nid)})

        # 2. Point Lookup (Indexed property lookup)
        print("[2/5] Running Point Lookup workload (100 iterations)...")
        for nid in sample_nodes:
            lat = time_query(session, "MATCH (n:User {id: $id}) RETURN n", {"id": int(nid)})
            metrics["point_lookup"].append(lat)

        # 3. 1-Hop Traversal
        print("[3/5] Running 1-Hop Traversal workload (100 iterations)...")
        for nid in sample_nodes:
            lat = time_query(session, "MATCH (a:User {id: $id})-[:FOLLOWS]->(b) RETURN b.id", {"id": int(nid)})
            metrics["hop_1"].append(lat)

        # 4. 2-Hop Traversal
        print("[4/5] Running 2-Hop Traversal workload (100 iterations)...")
        for nid in sample_nodes:
            lat = time_query(session, "MATCH (a:User {id: $id})-[:FOLLOWS]->()-[:FOLLOWS]->(c) RETURN count(c)", {"id": int(nid)})
            metrics["hop_2"].append(lat)

        # 5. 3-Hop Traversal
        print("[5/5] Running 3-Hop Traversal workload (100 iterations)...")
        for nid in sample_nodes:
            lat = time_query(session, "MATCH (a:User {id: $id})-[:FOLLOWS*3]->(d) RETURN count(d)", {"id": int(nid)})
            metrics["hop_3"].append(lat)

        # 6. Aggregation Query (Group-by / Top-10 degree)
        print("[6/6] Running Aggregation workload (100 iterations)...")
        for _ in range(ITERATIONS):
            query = "MATCH (n:User)-[r:FOLLOWS]->() RETURN n.id, count(r) AS degree ORDER BY degree DESC LIMIT 10"
            lat = time_query(session, query)
            metrics["aggregation"].append(lat)

    driver.close()

    # Calculate p50 (median), p95, and average latency
    summary = {}
    print("\n" + "="*65)
    print("           COGNODB QUERY LATENCY BENCHMARK RESULTS")
    print("="*65)
    print(f"{'Workload':<22} | {'p50 (ms)':<12} | {'p95 (ms)':<12} | {'Avg (ms)':<10}")
    print("-" * 65)

    for workload, lats in metrics.items():
        p50 = float(np.percentile(lats, 50))
        p95 = float(np.percentile(lats, 95))
        avg = float(np.mean(lats))
        summary[workload] = {"p50": round(p50, 2), "p95": round(p95, 2), "avg": round(avg, 2)}
        print(f"{workload:<22} | {p50:<12.2f} | {p95:<12.2f} | {avg:<10.2f}")

    print("="*65)

    os.makedirs("results", exist_ok=True)
    with open("results/cognodb_latencies.json", "w") as f:
        json.dump(summary, f, indent=2)
    print("\nResults automatically saved to 'results/cognodb_latencies.json'")

if __name__ == "__main__":
    run_benchmarks()