import os
import pandas as pd
import networkx as nx

def generate_dataset(output_path="data/edges.csv", num_nodes=20000, num_edges=120000):
    os.makedirs("data", exist_ok=True)
    
    print("Generating power-law social graph (SNAP-style)...")
    # Generates a scale-free network graph
    G = nx.powerlaw_cluster_graph(n=num_nodes, m=6, p=0.05, seed=42)
    
    edges = list(G.edges())[:num_edges]
    df = pd.DataFrame(edges, columns=["src", "dst"])
    
    df.to_csv(output_path, index=False)
    
    unique_nodes = len(set(df["src"]).union(set(df["dst"])))
    print(f"\nDataset created successfully:")
    print(f"  - File path: {output_path}")
    print(f"  - Nodes: {unique_nodes}")
    print(f"  - Relationships (Edges): {len(df)}")

if __name__ == "__main__":
    generate_dataset()