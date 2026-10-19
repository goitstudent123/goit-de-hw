import json
import os
import re
import subprocess
import sys
import uuid
from datetime import datetime, timezone

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
from pyspark.sql import SparkSession

from alerts_processor import (
    average_by_window,
    find_alerts,
    load_conditions,
    parse_readings,
    to_kafka_messages,
)
from config import deserialize, kafka_config, serialize
from sensor import HUMIDITY_RANGE, TEMPERATURE_RANGE, drift, make_reading

os.environ["PYSPARK_PYTHON"] = sys.executable


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test_alerts")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def at(time):
    return datetime.fromisoformat(f"2026-10-19T{time}+00:00")


def utc(value):
    # collect() gives naive datetimes in the local time zone
    return value.astimezone(timezone.utc)


def reading_json(temperature, humidity, time, sensor_id=1234):
    return serialize(
        {
            "id": sensor_id,
            "temperature": temperature,
            "humidity": humidity,
            "timestamp": f"2026-10-19T{time}.000+00:00",
        }
    )


def readings_df(spark, rows):
    raw = spark.createDataFrame([(reading_json(*row),) for row in rows], ["value"])
    return parse_readings(raw)


def averages_df(spark, t_avg, h_avg):
    return spark.createDataFrame(
        [((at("10:00:00"), at("10:01:00")), float(t_avg), float(h_avg))],
        "window struct<start: timestamp, end: timestamp>, t_avg double, h_avg double",
    )


def test_reading_has_id_temperature_humidity_timestamp():
    reading = make_reading(42, 30.5, 55.0)
    assert list(reading) == ["id", "temperature", "humidity", "timestamp"]
    assert reading["id"] == 42
    assert reading["temperature"] == 30.5
    assert reading["humidity"] == 55.0
    assert datetime.fromisoformat(reading["timestamp"]).tzinfo is not None


def test_drift_stays_in_range():
    temperature = 25
    for _ in range(1000):
        new = drift(temperature, TEMPERATURE_RANGE, 1)
        assert TEMPERATURE_RANGE[0] <= new <= TEMPERATURE_RANGE[1]
        assert abs(new - temperature) <= 1.05
        temperature = new
    assert drift(85, HUMIDITY_RANGE, 3) <= 85


def test_conditions_are_read_from_file(spark):
    rows = load_conditions(spark).orderBy("id").collect()
    assert [(r.code, r.message) for r in rows] == [
        ("101", "It's too dry"),
        ("102", "It's too wet"),
        ("103", "It's too cold"),
        ("104", "It's too hot"),
    ]
    assert (rows[0].humidity_min, rows[0].humidity_max) == (0, 40)
    assert (rows[0].temperature_min, rows[0].temperature_max) == (-999, -999)


def test_parse_readings(spark):
    [row] = readings_df(spark, [(31.5, 47.2, "10:00:10")]).collect()
    assert row.id == 1234
    assert row.temperature == 31.5
    assert row.humidity == 47.2
    assert utc(row.timestamp) == at("10:00:10")


def test_sliding_window_is_one_minute_every_30_seconds(spark):
    readings = readings_df(spark, [(20, 50, "10:00:10"), (40, 70, "10:00:40")])
    rows = average_by_window(readings).orderBy("window").collect()
    result = [
        (
            utc(r.window.start).strftime("%H:%M:%S"),
            utc(r.window.end).strftime("%H:%M:%S"),
            r.t_avg,
            r.h_avg,
        )
        for r in rows
    ]
    assert result == [
        ("09:59:30", "10:00:30", 20, 50),
        ("10:00:00", "10:01:00", 30, 60),
        ("10:00:30", "10:01:30", 40, 70),
    ]


@pytest.mark.parametrize(
    "t_avg, h_avg, codes",
    [
        (35, 50, []),
        (45, 50, ["104"]),
        (25, 50, ["103"]),
        (35, 30, ["101"]),
        (35, 70, ["102"]),
        (45, 30, ["101", "104"]),
        (25, 70, ["102", "103"]),
    ],
)
def test_find_alerts(spark, t_avg, h_avg, codes):
    alerts = find_alerts(averages_df(spark, t_avg, h_avg), load_conditions(spark))
    assert sorted(r.code for r in alerts.collect()) == codes


def test_kafka_message_looks_like_example(spark):
    alerts = find_alerts(averages_df(spark, 56.6, 58.1), load_conditions(spark))
    [row] = to_kafka_messages(alerts).collect()
    assert row.key == "104"
    message = json.loads(row.value)
    assert list(message) == ["window", "t_avg", "h_avg", "code", "message", "timestamp"]
    assert message["window"] == {
        "start": "2026-10-19T10:00:00.000Z",
        "end": "2026-10-19T10:01:00.000Z",
    }
    assert message["t_avg"] == 56.6
    assert message["h_avg"] == 58.1
    assert message["code"] == "104"
    assert message["message"] == "It's too hot"
    assert re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(\.\d+)?", message["timestamp"])


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
        timeout=300,
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
def test_sensor_sends_readings_with_one_id(prefix):
    run("create_topics.py", prefix)
    run("sensor.py", prefix, "--count", "5", "--interval", "0")

    readings = read_topic(f"{prefix}_building_sensors")
    assert len(readings) == 5
    assert len({r["id"] for r in readings}) == 1
    assert all(
        list(r) == ["id", "temperature", "humidity", "timestamp"] for r in readings
    )


@needs_kafka
def test_alerts_go_through_kafka(prefix):
    run("create_topics.py", prefix)
    producer = KafkaProducer(**kafka_config())
    for second in range(5, 60, 10):
        producer.send(
            f"{prefix}_building_sensors", reading_json(50, 50, f"10:00:{second:02}")
        )
    # moves the watermark past the first windows so they get closed
    producer.send(f"{prefix}_building_sensors", reading_json(35, 50, "10:05:00"))
    producer.flush()
    producer.close()

    run(
        "alerts_processor.py",
        prefix,
        "--starting-offsets",
        "earliest",
        "--timeout",
        "30",
    )

    alerts = read_topic(f"{prefix}_alerts")
    starts = sorted(datetime.fromisoformat(a["window"]["start"]) for a in alerts)
    assert starts == [at("09:59:30"), at("10:00:00"), at("10:00:30")]
    assert {(a["code"], a["message"], a["t_avg"], a["h_avg"]) for a in alerts} == {
        ("104", "It's too hot", 50, 50)
    }
