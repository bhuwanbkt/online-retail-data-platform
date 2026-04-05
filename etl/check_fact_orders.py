import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        sslmode="require",
    )


QUERY = """
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    total_amount,
    payment_status,
    order_status,
    order_created_at
FROM fact_orders
ORDER BY order_created_at DESC
LIMIT 10;
"""


def main():
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(QUERY)
            rows = cur.fetchall()

            print("Latest rows in fact_orders:")
            for row in rows:
                print(row)
    finally:
        conn.close()


if __name__ == "__main__":
    main()