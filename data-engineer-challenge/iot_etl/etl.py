import os
import json
import pandas as pd
import duckdb
from time import sleep
from datetime import datetime

DATA_PATH = os.getenv("DATA_PATH", "/data")
DB_PATH = os.getenv("DB_PATH", "/data/iot_data.duckdb")

def ensure_tables(con):
    """Create main and tracking tables if they don't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS iot_data AS SELECT * FROM (SELECT NULL::VARCHAR AS file_name) WHERE 0;
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            file_name VARCHAR PRIMARY KEY,
            processed_at TIMESTAMP
        );
    """)

def get_unprocessed_files(con):
    """List JSON files that haven’t been loaded yet."""
    all_files = [f for f in os.listdir(DATA_PATH) if f.endswith(".json")]
    if not all_files:
        return []

    result = con.execute("SELECT file_name FROM processed_files").fetchdf()
    processed = set(result["file_name"].tolist())
    new_files = [f for f in all_files if f not in processed]
    return new_files

def load_json_to_df(file_path):
    """Load one JSON file into a DataFrame."""
    try:
        with open(file_path) as file:
            data = json.load(file)
            # Handle either list of dicts or single dict
            if isinstance(data, dict):
                return pd.DataFrame([data])
            elif isinstance(data, list):
                return pd.DataFrame(data)
            else:
                return pd.DataFrame()
    except Exception as e:
        print(f"[Query] Skipping {file_path}: {e}")
        return pd.DataFrame()

def load_json_files_to_duckdb():
    con = duckdb.connect(DB_PATH)
    ensure_tables(con)

    new_files = get_unprocessed_files(con)
    if not new_files:
        print("[Query] No new JSON files to load.")
        con.close()
        return

    for f in sorted(new_files):
        file_path = os.path.join(DATA_PATH, f)
        df = load_json_to_df(file_path)
        if df.empty:
            print(f"[Query] No data found in {f}, skipping.")
            continue

        # Add file name column for tracking
        df["file_name"] = f

        # Create iot_data table if it doesn't exist (based on first schema)
        con.execute("""
            CREATE TABLE IF NOT EXISTS iot_data AS SELECT * FROM df LIMIT 0;
        """)

        # Insert new records
        con.register("df", df)
        con.execute("INSERT INTO iot_data SELECT * FROM df")
        con.unregister("df")

        # Mark as processed
        con.execute(
            "INSERT INTO processed_files (file_name, processed_at) VALUES (?, ?)",
            (f, datetime.utcnow())
        )

        print(f"[Query] Loaded {len(df)} records from {f}")

    con.close()

def run_queries():
    con = duckdb.connect(DB_PATH)
    print("\n[Query] Example queries:")
    print(con.execute("SELECT COUNT(*) AS total_records FROM iot_data").fetchdf())
    # Only run avg if temperature column exists
    cols = [c[0] for c in con.execute("DESCRIBE iot_data").fetchall()]
    if "temperature" in cols and "sensor_id" in cols:
        print(con.execute("""
            SELECT sensor_id, AVG(temperature) AS avg_temp
            FROM iot_data
            GROUP BY sensor_id
            ORDER BY avg_temp DESC
        """).fetchdf())
    con.close()

def main():
    print("[Query] Starting BI data service...")
    while True:
        load_json_files_to_duckdb()
        run_queries()
        sleep(30)  # refresh every 30 seconds

if __name__ == "__main__":
    main()

