# Homework 05: Apache Kafka

Simulate IoT sensors with Kafka. Each `sensor.py` run is one sensor with a random id that sends temperature (25-45) and humidity (15-85) to `building_sensors`. `alerts_processor.py` reads them and sends alerts to `temperature_alerts` (above 40) and `humidity_alerts` (above 80 or below 20). `alerts_reader.py` reads both alert topics and prints them.

Topic names get a `TOPIC_PREFIX` (default `anton_husiev`), so the real names are `anton_husiev_building_sensors` and so on. `create_topics.py` creates the three topics.

## How to run

Requires [uv](https://docs.astral.sh/uv/) and Docker.

`./run.sh` starts a local Kafka from `docker-compose.yml` on `localhost:19092`, runs the tests, creates the topics, then runs the processor, the reader and three sensors (10 readings each, one per second) at the same time. The processor and the reader stop after 15-20 seconds with no new messages. Stop the broker with `docker compose down`.

To use another broker, copy `.env.example` to `.env` and fill in the address and credentials; `run.sh` then skips the local one.

Without arguments, every script runs until Ctrl+C. Start `sensor.py` in several terminals to get several sensors.

## Results

Run output: [results/output.txt](results/output.txt), tests: [results/tests.txt](results/tests.txt).

Three sensors (6322, 6735, 8604) sent 30 readings. The processor sent 6 temperature alerts and 6 humidity alerts, and the reader printed all 12. One reading had both values out of range, so 11 readings gave 12 alerts.

### Screenshots

These come from a separate run with two sensors.

1. Topics on the broker: [results/1_topics.png](results/1_topics.png)
2. Two sensors sending to `building_sensors` at the same time: [results/2_sensors.png](results/2_sensors.png)
3. The processor reads the readings and picks the ones out of range: [results/3_processing.png](results/3_processing.png)
4. The alerts in `temperature_alerts` and `humidity_alerts`, read with `kafka-console-consumer.sh`: [results/4_alert_topics.png](results/4_alert_topics.png)
5. The reader prints the alerts: [results/5_alerts_reader.png](results/5_alerts_reader.png)
