import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    sslmode="require",
)

cur = conn.cursor()
cur.execute("SELECT version();")
version = cur.fetchone()

print("Connected successfully!")
print("PostgreSQL version:", version[0])

cur.close()
conn.close()