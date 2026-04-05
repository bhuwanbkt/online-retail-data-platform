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


DELETE_AND_INSERT_DAILY_SQL = """
TRUNCATE TABLE agg_daily_sales;

INSERT INTO agg_daily_sales (
    sales_date,
    total_orders,
    total_revenue,
    total_failed_payments,
    avg_order_value
)
SELECT
    DATE(order_created_at) AS sales_date,
    COUNT(*) AS total_orders,
    COALESCE(SUM(total_amount), 0) AS total_revenue,
    COUNT(*) FILTER (WHERE payment_status = 'failed') AS total_failed_payments,
    COALESCE(AVG(total_amount), 0) AS avg_order_value
FROM fact_orders
GROUP BY DATE(order_created_at)
ORDER BY sales_date;
"""

DELETE_AND_INSERT_HOURLY_SQL = """
TRUNCATE TABLE agg_hourly_orders;

INSERT INTO agg_hourly_orders (
    sales_hour,
    total_orders,
    total_revenue
)
SELECT
    DATE_TRUNC('hour', order_created_at) AS sales_hour,
    COUNT(*) AS total_orders,
    COALESCE(SUM(total_amount), 0) AS total_revenue
FROM fact_orders
GROUP BY DATE_TRUNC('hour', order_created_at)
ORDER BY sales_hour;
"""

DELETE_AND_INSERT_TOP_PRODUCTS_SQL = """
TRUNCATE TABLE agg_top_products;

INSERT INTO agg_top_products (
    snapshot_date,
    product_id,
    product_name,
    category,
    total_quantity,
    total_revenue
)
SELECT
    CURRENT_DATE AS snapshot_date,
    f.product_id,
    p.product_name,
    p.category,
    COALESCE(SUM(f.quantity), 0) AS total_quantity,
    COALESCE(SUM(f.total_amount), 0) AS total_revenue
FROM fact_orders f
JOIN dim_products p
    ON f.product_id = p.product_id
GROUP BY
    CURRENT_DATE,
    f.product_id,
    p.product_name,
    p.category
ORDER BY total_revenue DESC;
"""


def main():
    conn = get_connection()
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute(DELETE_AND_INSERT_DAILY_SQL)
            print("agg_daily_sales refreshed.")

            cur.execute(DELETE_AND_INSERT_HOURLY_SQL)
            print("agg_hourly_orders refreshed.")

            cur.execute(DELETE_AND_INSERT_TOP_PRODUCTS_SQL)
            print("agg_top_products refreshed.")

        print("All aggregate tables refreshed successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()