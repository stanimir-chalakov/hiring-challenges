import os
import json
import pandas as pd
import duckdb
from time import sleep
from datetime import datetime, timedelta
from utils.logger import setup_logger, colorize_message

logger = setup_logger("IoTDataLoader")


class IoTDataLoader:
    def __init__(self, data_path="/data", db_path="/data/duckdb/iot_data.duckdb", refresh_interval=120, buffer_minutes=2):
        self.data_path = os.getenv("DATA_PATH", data_path)
        self.db_path = os.getenv("DB_PATH", db_path)
        self.refresh_interval = refresh_interval
        self.buffer_minutes = buffer_minutes
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        # Connect to DuckDB
        self.dbcon = duckdb.connect(self.db_path)
        self.ensure_tables()

    def ensure_tables(self):
        """Create main and tracking tables if they don't exist."""
        self.dbcon.execute("""
            CREATE TABLE IF NOT EXISTS iot_data_bronze (
                sensor_id VARCHAR,
                timestamp TIMESTAMP,
                value DOUBLE,
                file_name VARCHAR
            );
        """)
        self.dbcon.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                file_name VARCHAR PRIMARY KEY,
                processed_at TIMESTAMP
            );
        """)

    def get_unprocessed_files(self):
        """Return JSON files that haven’t been loaded yet and are older than buffer time."""
        all_files = [f for f in os.listdir(self.data_path) if f.endswith(".json")]
        if not all_files:
            return []

        result = self.dbcon.execute("SELECT file_name FROM processed_files").fetchdf()
        processed = set(result["file_name"].tolist())

        eligible_files = []
        now = datetime.utcnow()
        buffer_delta = timedelta(minutes=self.buffer_minutes)

        for f in all_files:
            if f in processed:
                continue
            file_path = os.path.join(self.data_path, f)
            mtime = datetime.utcfromtimestamp(os.path.getmtime(file_path))
            if mtime <= now - buffer_delta:  # only pick files older than buffer
                eligible_files.append(f)

        return sorted(eligible_files)

    def load_json_file(self, file_path):
        """Load one line-delimited JSON file into a DataFrame."""
        records = []
        try:
            with open(file_path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    data = json.loads(line)
                    ts= data.get("dt")
                    if isinstance(ts, dict) and "micros" in ts:
                        ts = datetime.utcfromtimestamp(int(ts["micros"]) / 1e6)
                    records.append({
                        "sensor_id": data.get("id"),
                        "timestamp": ts,
                        "value": data.get("value")
                    })
            if records:
                df = pd.DataFrame(records)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(colorize_message(f"[IoTDataLoader] Skipping {file_path}: {e}"))
            return pd.DataFrame()

    def load_new_files(self):
        """Load all eligible, unprocessed JSON files into DuckDB."""
        new_files = self.get_unprocessed_files()
        if not new_files:
            return

        for f in new_files:
            file_path = os.path.join(self.data_path, f)
            df = self.load_json_file(file_path)
            if df.empty:
                continue

            df["file_name"] = f
            self.dbcon.register("df", df)
            self.dbcon.execute("INSERT INTO iot_data_bronze SELECT * FROM df")
            self.dbcon.unregister("df")

            self.dbcon.execute(
                "INSERT INTO processed_files (file_name, processed_at) VALUES (?, ?)",
                (f, datetime.utcnow().isoformat())
            )
            logger.info(colorize_message(f"[IoTDataLoader] Loaded {len(df)} records from {f}"))

    def transform_silver(self):
        """Incrementally load new bronze records into silver."""
        # Make sure silver table exists
        self.dbcon.execute("""
            CREATE TABLE IF NOT EXISTS iot_data_silver (
                sensor_id VARCHAR,
                timestamp TIMESTAMP,
                value DOUBLE
            );
        """)

        self.dbcon.execute("""
            CREATE TABLE IF NOT EXISTS silver_processed_files (
    			file_name VARCHAR PRIMARY KEY,
    			processed_at TIMESTAMP
				);
        """)

        # Insert only records not already in silver
        self.dbcon.execute("""
            INSERT INTO iot_data_silver (sensor_id, timestamp, value)
            SELECT
                b.sensor_id,
                strftime(b.timestamp, '%Y-%m-%d %H:%M:%S.%f') AS timestamp,
                b.value
            FROM iot_data_bronze b
            LEFT JOIN silver_processed_files spf
            	ON b.file_name = spf.file_name
            WHERE spf.file_name IS NULL
            	AND b.sensor_id IS NOT NULL
    			AND b.timestamp IS NOT NULL
    			AND b.value IS NOT NULL
            	AND b.value >= 0;
        """)

        self.dbcon.execute("""
    		INSERT INTO silver_processed_files (file_name, processed_at)
    		SELECT DISTINCT b.file_name, CURRENT_TIMESTAMP
    		FROM iot_data_bronze b
    		LEFT JOIN silver_processed_files spf
       		ON b.file_name = spf.file_name
    		WHERE spf.file_name IS NULL;
		""")

        logger.info(colorize_message("[IoTDataLoader] Silver table incrementally updated."))


    def transform_gold(self):
        """Resample IoT data to 1-minute mean values."""
        self.dbcon.execute("""
        CREATE TABLE IF NOT EXISTS iot_data_gold (
            sensor_id VARCHAR,
            minute_window TIMESTAMP,
            avg_temperature DOUBLE,
            records_per_minute INT
        );
    """)

        self.dbcon.execute("""
        CREATE TABLE IF NOT EXISTS gold_processed_minutes (
            sensor_id VARCHAR,
            minute_window TIMESTAMP,
            PRIMARY KEY(sensor_id, minute_window)
        );
    """)

        self.dbcon.execute("""
        	INSERT INTO iot_data_gold (sensor_id, minute_window, avg_temperature, records_per_minute)
			SELECT
    			sensor_id,
    			minute_window,
    			AVG(value) AS avg_temperature,
    			COUNT(*) AS records_per_minute
			FROM (
    			SELECT
        			s.sensor_id,
        			date_trunc('minute', s.timestamp) AS minute_window,
        			s.value
    			FROM iot_data_silver s
    			LEFT JOIN gold_processed_minutes gpm
        			ON s.sensor_id = gpm.sensor_id
       				AND date_trunc('minute', s.timestamp) = gpm.minute_window
    			WHERE gpm.sensor_id IS NULL
			) AS sub
			GROUP BY sensor_id, minute_window;
    """)
        logger.info(colorize_message("[IoTDataLoader] Gold table (1-minute resample) refreshed.", color="yellow"))

        self.dbcon.execute("""
        	INSERT INTO gold_processed_minutes (sensor_id, minute_window)
        	SELECT DISTINCT s.sensor_id, date_trunc('minute', s.timestamp)
        	FROM iot_data_silver s
        	LEFT JOIN gold_processed_minutes gpm
            	ON s.sensor_id = gpm.sensor_id
            	AND date_trunc('minute', s.timestamp) = gpm.minute_window
        	WHERE gpm.sensor_id IS NULL;
 		""")

        self.dbcon.execute("""
        	CREATE OR REPLACE VIEW vw_iot_data_gold_rs AS
			SELECT
  				sensor_id,
  				strftime(minute_window, '%Y-%m-%d %H:%M:%S.%f') AS minute_window,
  				avg_temperature,
  				records_per_minute
			FROM iot_data_gold
            ORDER BY 2,1;
    		""")
        logger.info(colorize_message("[IoTDataLoader] Gold view created.", color="cyan"))


    def start(self):
        """Start incremental loading service."""
        logger.info(colorize_message("[IoTDataLoader] Starting data loader...", color="green"))
        try:
            while True:
                self.load_new_files()
                self.transform_silver()
                self.transform_gold()
                self.dbcon.execute("CHECKPOINT;")
                sleep(self.refresh_interval)
                logger.debug(colorize_message(f"[IoTDataLoader] Sleeping {self.refresh_interval} seconds...", color="cyan"))
        except KeyboardInterrupt:
            logger.warning(colorize_message("[IoTDataLoader] Stopped by user.", color="yellow"))
        finally:
            self.dbcon.close()


if __name__ == "__main__":
    loader = IoTDataLoader()
    loader.start()