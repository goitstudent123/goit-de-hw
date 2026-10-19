import argparse
import json

from kafka import KafkaConsumer

from config import OUTPUT_TOPIC, kafka_config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=20, help="messages to print")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        OUTPUT_TOPIC,
        **kafka_config(),
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=lambda data: json.loads(data.decode("utf-8")),
    )
    print(f"[reader] first {args.count} messages of {OUTPUT_TOPIC}", flush=True)
    try:
        for i, record in enumerate(consumer):
            if i == args.count:
                break
            print(json.dumps(record.value), flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
