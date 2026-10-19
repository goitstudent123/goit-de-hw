import argparse

from kafka import KafkaConsumer, KafkaProducer

from config import (
    BUILDING_SENSORS,
    HUMIDITY_ALERTS,
    TEMPERATURE_ALERTS,
    TOPIC_PREFIX,
    deserialize,
    kafka_config,
    serialize,
)

MAX_TEMPERATURE = 40
MIN_HUMIDITY = 20
MAX_HUMIDITY = 80


def make_alert(reading, message):
    return {
        "sensor_id": reading["sensor_id"],
        "timestamp": reading["timestamp"],
        "temperature": reading["temperature"],
        "humidity": reading["humidity"],
        "message": message,
    }


def check_reading(reading):
    """Returns a list of (topic, alert) pairs, empty when the reading is fine."""
    alerts = []
    if reading["temperature"] > MAX_TEMPERATURE:
        message = f"Temperature {reading['temperature']} is above {MAX_TEMPERATURE}"
        alerts.append((TEMPERATURE_ALERTS, make_alert(reading, message)))
    if reading["humidity"] > MAX_HUMIDITY:
        message = f"Humidity {reading['humidity']} is above {MAX_HUMIDITY}"
        alerts.append((HUMIDITY_ALERTS, make_alert(reading, message)))
    elif reading["humidity"] < MIN_HUMIDITY:
        message = f"Humidity {reading['humidity']} is below {MIN_HUMIDITY}"
        alerts.append((HUMIDITY_ALERTS, make_alert(reading, message)))
    return alerts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--idle", type=float, default=0, help="stop after N seconds without messages"
    )
    args = parser.parse_args()

    consumer = KafkaConsumer(
        BUILDING_SENSORS,
        **kafka_config(),
        group_id=f"{TOPIC_PREFIX}_alerts_processor",
        auto_offset_reset="earliest",
        value_deserializer=deserialize,
        consumer_timeout_ms=int(args.idle * 1000) if args.idle else float("inf"),
    )
    producer = KafkaProducer(
        **kafka_config(), value_serializer=serialize, key_serializer=str.encode
    )
    print(f"[processor] reading {BUILDING_SENSORS}", flush=True)
    try:
        for record in consumer:
            reading = record.value
            alerts = check_reading(reading)
            result = f"{len(alerts)} alert(s)" if alerts else "ok"
            print(
                f"[processor] got sensor {reading['sensor_id']}: "
                f"temperature {reading['temperature']}, humidity {reading['humidity']}"
                f" -> {result}",
                flush=True,
            )
            for topic, alert in alerts:
                # wait for the broker to confirm, so the log shows where the alert landed
                meta = producer.send(
                    topic, key=str(alert["sensor_id"]), value=alert
                ).get(timeout=10)
                print(
                    f"[processor]   sent to {topic} "
                    f"(partition {meta.partition}, offset {meta.offset}): {alert['message']}",
                    flush=True,
                )
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
