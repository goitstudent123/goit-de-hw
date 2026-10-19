import json

from kafka import KafkaConsumer

from config import ALERTS, TOPIC_PREFIX, deserialize, kafka_config


def main():
    consumer = KafkaConsumer(
        ALERTS,
        **kafka_config(),
        group_id=f"{TOPIC_PREFIX}_alerts_reader",
        auto_offset_reset="earliest",
        value_deserializer=deserialize,
    )
    print(f"[alerts] reading {ALERTS}", flush=True)
    try:
        for record in consumer:
            print(f"[alerts] {json.dumps(record.value)}", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
