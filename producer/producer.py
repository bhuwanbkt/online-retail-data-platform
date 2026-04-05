import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

customers = pd.read_csv(BASE_DIR / "data" / "customers.csv").to_dict(orient="records")
products = pd.read_csv(BASE_DIR / "data" / "products.csv").to_dict(orient="records")

TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")

producer = KafkaProducer(
    bootstrap_servers=f"{os.getenv('KAFKA_HOST')}:{os.getenv('KAFKA_PORT')}",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=os.getenv("KAFKA_USERNAME"),
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
    ssl_cafile=os.getenv("KAFKA_CA_PATH"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(2, 5, 0),
)


def create_event() -> dict:
    customer = random.choice(customers)
    product = random.choice(products)
    quantity = random.randint(1, 3)
    unit_price = float(product["price"])
    total_amount = round(quantity * unit_price, 2)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "order_created",
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "customer_id": int(customer["customer_id"]),
        "product_id": int(product["product_id"]),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "payment_method": random.choice(["card", "paypal", "apple_pay"]),
        "payment_status": "pending",
        "order_status": "created",
        "customer_city": customer["city"],
        "customer_state": customer["state"],
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    print("Producer started...")
    try:
        while True:
            event = create_event()
            producer.send(TOPIC, value=event)
            producer.flush()
            print("sent:", event)
            time.sleep(5)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()