import os
import certifi
from dotenv import load_dotenv

load_dotenv()

TOPIC = os.getenv("KAFKA_TOPIC", "etl.records")


def kafka_conf() -> dict:
    """Configuración del consumidor Kafka (SASL_SSL + SCRAM-SHA-512)."""
    return {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": os.environ["KAFKA_USERNAME"],
        "sasl.password": os.environ["KAFKA_PASSWORD"],
        "ssl.ca.location": certifi.where(),
        "group.id": os.environ["KAFKA_GROUP_ID"],
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }