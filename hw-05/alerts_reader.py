import argparse

from kafka import KafkaConsumer

from config import (
    HUMIDITY_ALERTS,
    TEMPERATURE_ALERTS,
    TOPIC_PREFIX,
    deserialize,
    kafka_config,
)


def format_alert(alert):
    return (
        f"{alert['timestamp']} sensor {alert['sensor_id']}: {alert['message']} "
        f"(temperature {alert['temperature']}, humidity {alert['humidity']})"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--idle", type=float, default=0, help="stop after N seconds without messages"
    )
    args = parser.parse_args()

    consumer = KafkaConsumer(
        TEMPERATURE_ALERTS,
        HUMIDITY_ALERTS,
        **kafka_config(),
        group_id=f"{TOPIC_PREFIX}_alerts_reader",
        auto_offset_reset="earliest",
        value_deserializer=deserialize,
        consumer_timeout_ms=int(args.idle * 1000) if args.idle else float("inf"),
    )
    print(f"[alerts] reading {TEMPERATURE_ALERTS}, {HUMIDITY_ALERTS}", flush=True)
    try:
        for record in consumer:
            print(f"[alerts] {format_alert(record.value)}", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
