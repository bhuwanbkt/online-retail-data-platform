import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

SQL_FILES = [
    "sql/01_create_raw_tables.sql",
    "sql/02_create_dimension_tables.sql",
    "sql/03_create_fact_tables.sql",
    "sql/04_create_aggregate_tables.sql",
]


def main():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        sslmode="require",
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            for file_path in SQL_FILES:
                full_path = BASE_DIR / file_path

                if not full_path.exists():
                    raise FileNotFoundError(f"SQL file not found: {full_path}")

                sql = full_path.read_text(encoding="utf-8")
                cur.execute(sql)
                print(f"Executed: {file_path}")

        print("All tables created successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()