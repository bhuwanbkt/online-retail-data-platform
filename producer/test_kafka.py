import os
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

producer = KafkaProducer(
    bootstrap_servers=f"{os.getenv('KAFKA_HOST')}:{os.getenv('KAFKA_PORT')}",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=os.getenv("KAFKA_USERNAME"),
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
    ssl_cafile=os.getenv("KAFKA_CA_PATH"),
    api_version=(2, 5, 0),
)

print("Kafka connected successfully!")
producer.close()