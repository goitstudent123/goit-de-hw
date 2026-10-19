import os
import subprocess
import sys
import uuid

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable

from alerts_processor import check_reading
from alerts_reader import format_alert
from config import (
    HUMIDITY_ALERTS,
    TEMPERATURE_ALERTS,
    deserialize,
    kafka_config,
    serialize,
)
from sensor import make_reading


def reading(temperature, humidity):
    return {
        "sensor_id": 1234,
        "timestamp": "2026-10-19T10:00:00+00:00",
        "temperature": temperature,
        "humidity": humidity,
    }


def test_reading_has_id_time_and_values_in_range():
    for _ in range(1000):
        r = make_reading(42)
        assert r["sensor_id"] == 42
        assert r["timestamp"]
        assert 25 <= r["temperature"] <= 45
        assert 15 <= r["humidity"] <= 85


def test_normal_reading_gives_no_alerts():
    assert check_reading(reading(40, 80)) == []
    assert check_reading(reading(25, 20)) == []


def test_temperature_above_40():
    [(topic, alert)] = check_reading(reading(40.1, 50))
    assert topic == TEMPERATURE_ALERTS
    assert alert["sensor_id"] == 1234
    assert alert["temperature"] == 40.1
    assert alert["humidity"] == 50
    assert alert["timestamp"] == "2026-10-19T10:00:00+00:00"
    assert "above 40" in alert["message"]


@pytest.mark.parametrize("humidity, text", [(80.1, "above 80"), (19.9, "below 20")])
def test_humidity_out_of_range(humidity, text):
    [(topic, alert)] = check_reading(reading(30, humidity))
    assert topic == HUMIDITY_ALERTS
    assert alert["humidity"] == humidity
    assert text in alert["message"]


def test_both_alerts_for_one_reading():
    topics = [topic for topic, _ in check_reading(reading(44, 84))]
    assert topics == [TEMPERATURE_ALERTS, HUMIDITY_ALERTS]


def test_format_alert():
    [(_, alert)] = check_reading(reading(41, 50))
    assert format_alert(alert) == (
        "2026-10-19T10:00:00+00:00 sensor 1234: Temperature 41 is above 40 "
        "(temperature 41, humidity 50)"
    )


def broker_available():
    try:
        KafkaAdminClient(**kafka_config()).close()
        return True
    except NoBrokersAvailable:
        return False


needs_kafka = pytest.mark.skipif(not broker_available(), reason="Kafka is not running")


@pytest.fixture
def prefix():
    # separate topics per test, so the real ones stay clean
    prefix = f"test_{uuid.uuid4().hex[:8]}"
    yield prefix
    admin = KafkaAdminClient(**kafka_config())
    ours = [t for t in admin.list_topics() if t.startswith(prefix)]
    if ours:
        admin.delete_topics(ours)
    admin.close()


def run(script, prefix, *args):
    env = {**os.environ, "TOPIC_PREFIX": prefix}
    result = subprocess.run(
        [sys.executable, script, *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return result.stdout


def read_topic(topic):
    consumer = KafkaConsumer(
        topic,
        **kafka_config(),
        auto_offset_reset="earliest",
        value_deserializer=deserialize,
        consumer_timeout_ms=5000,
    )
    values = [record.value for record in consumer]
    consumer.close()
    return values


@needs_kafka
def test_create_topics(prefix):
    output = run("create_topics.py", prefix)
    assert f"{prefix}_building_sensors" in output
    assert f"{prefix}_temperature_alerts" in output
    assert f"{prefix}_humidity_alerts" in output

    output = run("create_topics.py", prefix)
    assert output.count("already exists") == 3


@needs_kafka
def test_sensor_sends_readings_with_one_id(prefix):
    run("create_topics.py", prefix)
    run("sensor.py", prefix, "--count", "5", "--interval", "0")

    readings = read_topic(f"{prefix}_building_sensors")
    assert len(readings) == 5
    assert len({r["sensor_id"] for r in readings}) == 1


@needs_kafka
def test_alerts_go_through_the_pipeline(prefix):
    run("create_topics.py", prefix)
    producer = KafkaProducer(**kafka_config(), value_serializer=serialize)
    for r in [reading(30, 50), reading(42, 50), reading(30, 17), reading(44, 84)]:
        producer.send(f"{prefix}_building_sensors", r)
    producer.flush()
    producer.close()

    run("alerts_processor.py", prefix, "--idle", "10")

    temperature = read_topic(f"{prefix}_temperature_alerts")
    humidity = read_topic(f"{prefix}_humidity_alerts")
    assert sorted(a["temperature"] for a in temperature) == [42, 44]
    assert sorted(a["humidity"] for a in humidity) == [17, 84]

    output = run("alerts_reader.py", prefix, "--idle", "10")
    alerts = [line for line in output.splitlines() if " sensor 1234: " in line]
    assert len(alerts) == 4
    assert any("Temperature 42 is above 40" in line for line in alerts)
    assert any("Humidity 17 is below 20" in line for line in alerts)
