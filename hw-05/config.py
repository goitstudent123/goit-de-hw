import json
import os

from dotenv import load_dotenv

load_dotenv()

TOPIC_PREFIX = os.getenv("TOPIC_PREFIX") or "anton_husiev"
BUILDING_SENSORS = f"{TOPIC_PREFIX}_building_sensors"
TEMPERATURE_ALERTS = f"{TOPIC_PREFIX}_temperature_alerts"
HUMIDITY_ALERTS = f"{TOPIC_PREFIX}_humidity_alerts"


def kafka_config():
    config = {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS") or "localhost:19092"
    }
    if os.getenv("KAFKA_USERNAME"):
        config.update(
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            sasl_plain_username=os.getenv("KAFKA_USERNAME"),
            sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
        )
    return config


def serialize(value):
    return json.dumps(value).encode("utf-8")


def deserialize(data):
    return json.loads(data.decode("utf-8"))
