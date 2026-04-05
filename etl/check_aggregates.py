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


def print_rows(conn, table_name, order_by="1", limit=10):
    query = f"SELECT * FROM {table_name} ORDER BY {order_by} DESC LIMIT {limit};"

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

        print(f"\nData from {table_name}:")
        for row in rows:
            print(row)


def main():
    conn = get_connection()

    try:
        print_rows(conn, "agg_daily_sales", "sales_date")
        print_rows(conn, "agg_hourly_orders", "sales_hour")
        print_rows(conn, "agg_top_products", "total_revenue")
    finally:
        conn.close()


if __name__ == "__main__":
    main()