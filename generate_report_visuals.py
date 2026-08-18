import os
import glob
import json
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

os.makedirs("figures", exist_ok=True)

# 1. Ingestion Throughput Comparison
ingestion_data = {}
latency_data = {}
concurrency_data = {}

# Load all results
for path in glob.glob("results/*_results.json"):
    with open(path) as f:
        data = json.load(f)
        pname = data.get("platform")
        if not pname:
            continue
        if "ingestion" in data:
            ingestion_data[pname] = data["ingestion"]["edges_per_sec"]
        if "latencies" in data:
            latency_data[pname] = data["latencies"]
        if "concurrency" in data:
            concurrency_data[pname] = data["concurrency"]

# Add standalone concurrency files if any
for path in glob.glob("results/*_concurrency.json"):
    base = os.path.basename(path).replace("_concurrency.json", "").title()
    with open(path) as f:
        data = json.load(f)
        if base not in concurrency_data:
            concurrency_data[base] = data

# Chart 1: Ingestion Throughput
plt.figure(figsize=(10, 5))
platforms = list(ingestion_data.keys())
rates = [ingestion_data[p] for p in platforms]
bars = plt.bar(platforms, rates, color=['#2b5c8f', '#4682b4', '#e67e22', '#27ae60', '#9b59b6'][:len(platforms)])
plt.yscale('log')
plt.ylabel('Edges / Second (Log Scale)')
plt.title('Graph Ingestion Throughput Comparison (119,957 Edges)')
plt.grid(axis='y', linestyle='--', alpha=0.7)
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2.0, yval * 1.15, f'{yval:,.1f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
plt.tight_layout()
plt.savefig("figures/ingestion_throughput_comparison.png", dpi=300)
plt.close()

# Chart 2: Traversal p50 Latency (1-hop, 2-hop, 3-hop)
workloads = ["hop_1", "hop_2", "hop_3"]
wl_labels = ["1-Hop", "2-Hop", "3-Hop"]

plt.figure(figsize=(10, 6))
bar_width = 0.18
x = np.arange(len(wl_labels))

cloud_platforms = [p for p in latency_data.keys() if "NetworkX" not in p]
for idx, p in enumerate(cloud_platforms):
    p50_vals = [latency_data[p].get(w, {}).get("p50", 0) for w in workloads]
    plt.bar(x + idx * bar_width, p50_vals, width=bar_width, label=p)

plt.xlabel('Traversal Workload')
plt.ylabel('p50 Latency (ms)')
plt.title('Multi-Hop Traversal Latency (p50) Across Cloud Graph Databases')
plt.xticks(x + bar_width * (len(cloud_platforms) - 1) / 2, wl_labels)
plt.legend()
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig("figures/traversal_latency_p50.png", dpi=300)
plt.close()

print("Comparison charts successfully created in './figures/' directory.")