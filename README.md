# Graph Database Performance Benchmark: CognoDB Cloud vs. Modern Graph Engines

**Author:** Omprakash Kopula Kumar  
**Dataset:** Social Network Graph (119,957 Edges, 20,000 Nodes)  
**Evaluated Platforms:** CognoDB Cloud, Memgraph Cloud, FalkorDB Cloud, Neo4j AuraDB Free, NetworkX (In-Memory Baseline)

---

## 1. Executive Summary

This benchmark rigorously evaluates **CognoDB Cloud** against three production cloud graph databases (**Memgraph Cloud**, **FalkorDB Cloud**, **Neo4j AuraDB Free**) and an algorithmic in-memory reference (**NetworkX**). The benchmark workload executes under identical hardware, network, and dataset conditions to evaluate:
1. **Bulk Ingestion Throughput** ($119,957$ relationships).
2. **Read Traversal & Analytical Latencies** ($100$ iterations each: Point Lookup, 1-Hop, 2-Hop, 3-Hop, Degree Aggregation).
3. **Multi-Threaded Mixed Concurrency** ($80\%$ Read / $20\%$ Write transactions at $1$, $10$, and $40$ concurrent clients).

---

## 2. Experimental Setup & Methodology

* **Dataset:** Directed social network follower graph comprising **119,957 edges** and **20,000 vertices**.
* **Client Environment:** Python 3.14 runner executing parallelized Cypher queries via official Bolt / Redis / native protocol drivers.
* **Warmup Protocol:** 10 unmeasured warmup queries executed prior to every workload to ensure JIT/query cache stabilization.
* **Isolation:** Each database instance was initialized from a clean state with explicit node index creation (`:User(id)`) prior to ingestion.

---

## 3. Benchmark Results & Key Findings

### Ingestion Throughput Comparison

| Platform | Total Edges | Wall-Clock Time (s) | Ingestion Rate (Edges/sec) | Node Indexing |
| :--- | :--- | :--- | :--- | :--- |
| **NetworkX (In-Memory)** | 119,957 | **0.31 s** | **382,775.07** | Native dict |
| **FalkorDB Cloud** | 119,957 | **19.96 s** | **6,008.42** | Range index |
| **CognoDB Cloud** | 119,957 | **18m 42s** | **106.85** | Schema constraint |
| **Memgraph Cloud** | 119,957 | **19m 20s** | **103.41** | In-memory index |
| **Neo4j Aura Free** | 119,957 | **19m 34s** | **102.18** | Unique constraint |

---

### Latency Matrix (p50 / p95 in Milliseconds)

| Workload | CognoDB Cloud | Memgraph Cloud | FalkorDB Cloud | Neo4j Aura Free | NetworkX (Local) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Point Lookup** | 102.14 / 140.21 | 204.69 / 229.40 | **61.03 / 105.48** | 102.06 / 138.99 | 0.0003 / 0.0005 |
| **1-Hop Traversal** | 101.85 / 136.50 | 204.23 / 248.70 | **33.73 / 103.39** | 101.98 / 148.25 | 0.0011 / 0.0019 |
| **2-Hop Traversal** | 102.40 / 142.10 | 204.81 / 213.56 | **102.31 / 132.09** | 101.88 / 127.85 | 0.0023 / 0.0248 |
| **3-Hop Traversal** | 103.12 / 148.90 | 204.84 / 236.72 | **102.36 / 106.15** | 102.23 / 153.73 | 0.0048 / 0.1890 |
| **Aggregation (Top-10)**| 104.50 / 152.30 | 249.58 / 274.90 | 204.67 / 247.74 | **102.45 / 131.02** | 8.3301 / 10.8061 |

---

### Concurrency Scaling (80% Read / 20% Write)

| Concurrency Level | CognoDB Cloud (QPS) | Memgraph Cloud (QPS) | FalkorDB Cloud (QPS) | Neo4j Aura Free (QPS) | NetworkX (QPS) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1 Worker** | 4.85 | 4.79 | 0.68 | 4.81 | **69,177.82** |
| **10 Workers** | 38.92 | **41.05** | 2.11 | 39.40 | **34,717.40** |
| **40 Workers** | 76.50 | **84.87** | 2.17 | 78.10 | **14,631.54** |

---

## 4. Deep-Dive Architectural Analysis

### 1. Ingestion Performance & Bottlenecks
* **FalkorDB** demonstrated orders-of-magnitude faster ingestion ($6,008.42\text{ edges/sec}$) due to GraphBLAS sparse-matrix representation and Redis protocol pipelining, requiring only 19.96 seconds to ingest all 119k edges.
* **CognoDB, Memgraph, and Neo4j** exhibited consistent ingestion rates (~$102\text{--}106\text{ edges/sec}$), reflecting round-trip network transaction boundaries across TLS-encrypted Bolt sessions for batched `UNWIND` Cypher operations.

### 2. Multi-Hop Traversal Latency
* **FalkorDB** achieved the lowest p50 latencies on shallow traversals ($33.73\text{ ms}$ for 1-hop), benefiting from adjacency matrix multiplication.
* **CognoDB Cloud** and **Neo4j Aura** maintained tight, predictable p50 latencies across deep multi-hop lookups ($102\text{--}103\text{ ms}$ up to 3-hop depth), showing effective pointer-chasing traversal paths.
* **Memgraph Cloud** suffered an elevated baseline round-trip overhead ($~204\text{ ms}$ p50), driven by geographic network distance to the trial cluster.

### 3. Concurrency & Throughput Scaling
* **Memgraph Cloud** achieved the highest multi-client throughput at 40 concurrent workers ($84.87\text{ QPS}$), closely followed by **CognoDB Cloud** ($76.50\text{ QPS}$) and **Neo4j Aura** ($78.10\text{ QPS}$).
* **FalkorDB** plateaued at ~$2.17\text{ QPS}$ under concurrent write contention due to global single-threaded Redis graph lock boundaries during mixed `MERGE` transactions.

---

## 5. Visual Artifacts

The generated comparison charts are located in the `figures/` directory:
* `figures/ingestion_throughput_comparison.png` — Ingestion rate across all evaluated platforms (log scale).
* `figures/traversal_latency_p50.png` — Multi-hop traversal p50 latency matrix.

---

## 6. How to Reproduce

```bash
# 1. Clone repository & install dependencies
pip install -r requirements.txt

# 2. Configure environment credentials in .env
# COGNODB_URI, MEMGRAPH_URI, FALKORDB_HOST, NEO4J_URI

# 3. Execute platform benchmarks
python benchmark_cognodb.py
python benchmark_generic_bolt.py
python benchmark_falkordb.py
python benchmark_networkx.py

# 4. Generate visual comparison charts
python generate_report_visuals.py