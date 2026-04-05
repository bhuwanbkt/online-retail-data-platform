import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    sslmode="require",
)

query = """
SELECT event_id, event_type, order_id, total_amount, event_timestamp
FROM raw_ecommerce_events
ORDER BY ingestion_timestamp DESC
LIMIT 10;
"""

with conn:
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

        print("Latest raw events:")
        for row in rows:
            print(row)

conn.close()