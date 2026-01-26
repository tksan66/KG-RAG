import psycopg
from dotenv import load_dotenv
import os
import pandas as pd

PORT=os.getenv("POSTGRES_PORT")
DBNAME=os.getenv("POSTGRES_DBNAME")
USER=os.getenv("POSTGRES_USER")
PASSWORD=os.getenv("POSTGRES_PASSWORD")

conninfo = f"host=127.0.0.1 port={PORT} dbname={DBNAME} user={USER} password={PASSWORD}"

def fetch_timeseries(sensor_key: str, start_ts: str, end_ts: str) -> pd.DataFrame:
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts, value
                FROM sensor_readings
                WHERE sensor_id = %s
                  AND ts >= %s::timestamptz
                  AND ts <= %s::timestamptz
                ORDER BY ts
                """,
                (sensor_key, start_ts, end_ts),
            )
            rows = cur.fetchall()

    return pd.DataFrame(rows, columns=["ts", "value"])