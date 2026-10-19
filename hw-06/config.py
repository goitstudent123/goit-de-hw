import json
import os

from dotenv import load_dotenv

load_dotenv()

TOPIC_PREFIX = os.getenv("TOPIC_PREFIX") or "anton_husiev"
BUILDING_SENSORS = f"{TOPIC_PREFIX}_building_sensors"
ALERTS = f"{TOPIC_PREFIX}_alerts"

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or "localhost:29092"
USERNAME = os.getenv("KAFKA_USERNAME")
PASSWORD = os.getenv("KAFKA_PASSWORD")
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")


def kafka_config():
    config = {"bootstrap_servers": BOOTSTRAP_SERVERS}
    if USERNAME:
        config.update(
            security_protocol=SECURITY_PROTOCOL,
            sasl_mechanism=SASL_MECHANISM,
            sasl_plain_username=USERNAME,
            sasl_plain_password=PASSWORD,
        )
    return config


def spark_kafka_options():
    options = {"kafka.bootstrap.servers": BOOTSTRAP_SERVERS}
    if USERNAME:
        options.update(
            {
                "kafka.security.protocol": SECURITY_PROTOCOL,
                "kafka.sasl.mechanism": SASL_MECHANISM,
                "kafka.sasl.jaas.config": (
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    f'username="{USERNAME}" password="{PASSWORD}";'
                ),
            }
        )
    return options


def serialize(value):
    return json.dumps(value).encode("utf-8")


def deserialize(data):
    return json.loads(data.decode("utf-8"))
