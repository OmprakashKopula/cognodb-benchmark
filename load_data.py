import os
import time
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

COGNODB_URI = os.getenv("COGNODB_URI")
COGNODB_USER = os.getenv("COGNODB_USER", "cognodb")
COGNODB_PASSWORD = os.getenv("COGNODB_PASSWORD")

CSV_FILE = "data/edges.csv"
BATCH_SIZE = 2500  # Safe batch size for 256MB RAM free tier

def load_data():
    if not os.path.exists(CSV_FILE):
        print(f"Error: {CSV_FILE} not found. Please run prepare_data.py first.")
        return

    print("Reading dataset...")
    df = pd.read_csv(CSV_FILE)
    total_edges = len(df)
    unique_nodes = len(set(df["src"]).union(set(df["dst"])))
    records = df.to_dict(orient="records")

    print(f"Connecting to CognoDB at {COGNODB_URI}...")
    driver = GraphDatabase.driver(COGNODB_URI, auth=(COGNODB_USER, COGNODB_PASSWORD))

    with driver.session() as session:
        # Step A: Clean slate & Create Index
        print("Creating index on :User(id)...")
        try:
            session.run("CREATE CONSTRAINT FOR (u:User) REQUIRE u.id IS UNIQUE;")
        except Exception:
            try:
                session.run("CREATE INDEX user_id_idx FOR (n:User) ON (n.id);")
            except Exception as e:
                print(f"Note on index creation: {e}")

        # Step B: Batch Ingestion
        print(f"Starting ingestion of {total_edges} relationships in batches of {BATCH_SIZE}...")
        start_time = time.perf_counter()

        query = """
        UNWIND $batch AS row
        MERGE (a:User {id: row.src})
        MERGE (b:User {id: row.dst})
        MERGE (a)-[:FOLLOWS]->(b)
        """

        for i in tqdm(range(0, total_edges, BATCH_SIZE), desc="Ingesting Batches"):
            batch = records[i : i + BATCH_SIZE]
            session.run(query, {"batch": batch})

        total_wall_clock = time.perf_counter() - start_time
        edges_per_sec = total_edges / total_wall_clock
        nodes_per_sec = unique_nodes / total_wall_clock

    driver.close()

    print("\n" + "="*45)
    print("        DATA INGESTION COMPLETE")
    print("="*45)
    print(f"Total Nodes Loaded      : {unique_nodes:,}")
    print(f"Total Edges Loaded      : {total_edges:,}")
    print(f"Total Wall-Clock Time   : {total_wall_clock:.2f} seconds")
    print(f"Ingest Throughput (Edges): {edges_per_sec:.2f} relationships/sec")
    print(f"Ingest Throughput (Nodes): {nodes_per_sec:.2f} nodes/sec")
    print("="*45)
    print("\n* Save these numbers! You will put them in your README results matrix.")

if __name__ == "__main__":
    load_data()