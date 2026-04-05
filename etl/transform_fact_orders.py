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


TRANSFORM_SQL = """
INSERT INTO fact_orders (
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    payment_method,
    payment_status,
    order_status,
    order_created_at,
    order_updated_at
)
SELECT
    r.order_id,
    r.customer_id,
    r.product_id,
    r.quantity,
    r.unit_price,
    r.total_amount,
    r.payment_method,
    r.payment_status,
    r.order_status,
    r.event_timestamp AS order_created_at,
    r.event_timestamp AS order_updated_at
FROM raw_ecommerce_events r
WHERE r.event_type = 'order_created'
ON CONFLICT (order_id)
DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    product_id = EXCLUDED.product_id,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    total_amount = EXCLUDED.total_amount,
    payment_method = EXCLUDED.payment_method,
    payment_status = EXCLUDED.payment_status,
    order_status = EXCLUDED.order_status,
    order_updated_at = EXCLUDED.order_updated_at;
"""


def main():
    conn = get_connection()
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute(TRANSFORM_SQL)
            print("fact_orders transformed successfully from raw_ecommerce_events.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()