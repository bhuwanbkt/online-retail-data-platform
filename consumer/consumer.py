import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=f"{os.getenv('KAFKA_HOST')}:{os.getenv('KAFKA_PORT')}",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=os.getenv("KAFKA_USERNAME"),
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
    ssl_cafile=os.getenv("KAFKA_CA_PATH"),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="retail-consumer-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    api_version=(2, 5, 0),
)

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    sslmode="require",
)
conn.autocommit = True

INSERT_SQL = """
INSERT INTO raw_ecommerce_events (
    event_id,
    event_type,
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    payment_method,
    payment_status,
    order_status,
    customer_city,
    customer_state,
    event_timestamp,
    raw_payload
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING;
"""

if __name__ == "__main__":
    print("Consumer started. Listening for messages...")

    try:
        with conn.cursor() as cur:
            for message in consumer:
                e = message.value

                cur.execute(
                    INSERT_SQL,
                    (
                        e.get("event_id"),
                        e.get("event_type"),
                        e.get("order_id"),
                        e.get("customer_id"),
                        e.get("product_id"),
                        e.get("quantity"),
                        e.get("unit_price"),
                        e.get("total_amount"),
                        e.get("payment_method"),
                        e.get("payment_status"),
                        e.get("order_status"),
                        e.get("customer_city"),
                        e.get("customer_state"),
                        e.get("event_timestamp"),
                        json.dumps(e),
                    ),
                )

                print("inserted:", e.get("event_id"))
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()
        conn.close()