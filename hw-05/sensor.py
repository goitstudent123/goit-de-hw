import argparse
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from config import BUILDING_SENSORS, kafka_config, serialize


def make_reading(sensor_id):
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "temperature": round(random.uniform(25, 45), 1),
        "humidity": round(random.uniform(15, 85), 1),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=0, help="0 means run forever")
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()

    # one run is one sensor, so the id stays the same until restart
    sensor_id = random.randint(1000, 9999)
    producer = KafkaProducer(
        **kafka_config(), value_serializer=serialize, key_serializer=str.encode
    )
    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            reading = make_reading(sensor_id)
            producer.send(BUILDING_SENSORS, key=str(sensor_id), value=reading)
            sent += 1
            print(f"[sensor {sensor_id}] sent {reading}", flush=True)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
