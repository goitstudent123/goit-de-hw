import argparse
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from config import BUILDING_SENSORS, kafka_config, serialize

TEMPERATURE_RANGE = (25, 45)
HUMIDITY_RANGE = (15, 85)


def make_reading(sensor_id, temperature, humidity):
    return {
        "id": sensor_id,
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


def drift(value, value_range, step):
    low, high = value_range
    return round(min(high, max(low, value + random.uniform(-step, step))), 1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=0, help="0 means run forever")
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()

    # one run is one sensor, so the id stays the same until restart
    sensor_id = random.randint(1000, 9999)
    # values drift instead of jumping around, otherwise the window averages
    # always end up in the middle of the range and never trigger an alert
    temperature = round(random.uniform(*TEMPERATURE_RANGE), 1)
    humidity = round(random.uniform(*HUMIDITY_RANGE), 1)

    producer = KafkaProducer(
        **kafka_config(), value_serializer=serialize, key_serializer=str.encode
    )
    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            reading = make_reading(sensor_id, temperature, humidity)
            producer.send(BUILDING_SENSORS, key=str(sensor_id), value=reading)
            sent += 1
            print(f"[sensor {sensor_id}] sent {reading}", flush=True)
            temperature = drift(temperature, TEMPERATURE_RANGE, 1)
            humidity = drift(humidity, HUMIDITY_RANGE, 3)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
