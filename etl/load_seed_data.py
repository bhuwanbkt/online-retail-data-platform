import os
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

CUSTOMERS_CSV = BASE_DIR / "data" / "customers.csv"
PRODUCTS_CSV = BASE_DIR / "data" / "products.csv"


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        sslmode="require",
    )


def load_customers(conn):
    customers_df = pd.read_csv(CUSTOMERS_CSV)

    insert_sql = """
    INSERT INTO dim_customers (
        customer_id,
        customer_name,
        city,
        state,
        segment
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (customer_id)
    DO UPDATE SET
        customer_name = EXCLUDED.customer_name,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        segment = EXCLUDED.segment;
    """

    with conn.cursor() as cur:
        for _, row in customers_df.iterrows():
            cur.execute(
                insert_sql,
                (
                    int(row["customer_id"]),
                    row["customer_name"],
                    row["city"],
                    row["state"],
                    row["segment"],
                ),
            )

    print(f"Loaded {len(customers_df)} customers into dim_customers.")


def load_products(conn):
    products_df = pd.read_csv(PRODUCTS_CSV)

    insert_sql = """
    INSERT INTO dim_products (
        product_id,
        product_name,
        category,
        unit_price
    )
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (product_id)
    DO UPDATE SET
        product_name = EXCLUDED.product_name,
        category = EXCLUDED.category,
        unit_price = EXCLUDED.unit_price;
    """

    with conn.cursor() as cur:
        for _, row in products_df.iterrows():
            cur.execute(
                insert_sql,
                (
                    int(row["product_id"]),
                    row["product_name"],
                    row["category"],
                    float(row["price"]),
                ),
            )

    print(f"Loaded {len(products_df)} products into dim_products.")


def main():
    conn = get_connection()
    conn.autocommit = True

    try:
        load_customers(conn)
        load_products(conn)
        print("Seed data load complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()