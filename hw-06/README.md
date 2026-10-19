# Homework 06: Spark Streaming

IoT sensor alerts with Spark Structured Streaming. `sensor.py` (from hw-05) sends `id`, `temperature`, `humidity` and `timestamp` to `building_sensors`; each run is one sensor with a random id. `alerts_processor.py` reads the stream, finds the average temperature and humidity in a 1 minute sliding window (every 30 seconds, watermark 10 seconds), cross joins the averages with the rules from `alerts_conditions.csv` and writes the matching ones to the `alerts` topic. `alerts_reader.py` prints what lands in `alerts`.

A rule matches when the averages are within its min/max; `-999` means that limit is not used. Sensor values drift a bit on each reading instead of being fully random, otherwise the averages always stay in the middle of the range and nothing gets flagged.

Topic names get a `TOPIC_PREFIX` (default `anton_husiev`), so the real names are `anton_husiev_building_sensors` and `anton_husiev_alerts`.

## How to run

Requires [uv](https://docs.astral.sh/uv/), Docker and Java 17 or 21 (for Spark 4.1).

`./run.sh` starts a local Kafka from `docker-compose.yml` on `localhost:29092`, runs the tests, creates the topics, then runs the processor (3 minutes), the reader and two sensors (150 readings each, one per second) at the same time. The first run also downloads the Spark Kafka connector. Stop the broker with `docker compose down`.

To use another broker, copy `.env.example` to `.env` and fill in the address and credentials; `run.sh` then skips the local one.

Without arguments, every script runs until Ctrl+C. Start `sensor.py` in several terminals to get several sensors.

## Results

Run output: [results/output.txt](results/output.txt), tests: [results/tests.txt](results/tests.txt).

Two sensors (8273, 2189) sent 300 readings. The processor closed 4 windows and printed their averages. Humidity drifted down to 30-37%, so the last three windows got "It's too dry" (101) alerts, and the reader got all three from the `alerts` topic in the same format as in the assignment:

```
{"window": {"start": "2026-10-19T17:22:00.000+03:00", "end": "2026-10-19T17:23:00.000+03:00"}, "t_avg": 35.608510638297865, "h_avg": 36.57553191489361, "code": "101", "message": "It's too dry", "timestamp": "2026-10-19 17:23:11.503"}
```

### Screenshots

These come from a separate run with fresh topics. Sensors 1832 and 1272 started together; temperature was around 39-43 and humidity around 67-70, so the windows got "It's too hot" (104) and "It's too wet" (102) alerts.

1. Two sensors sending to `building_sensors` at the same time: [results/1_sensors.png](results/1_sensors.png)
2. The processor prints the averages of each closed window: [results/2_processing.png](results/2_processing.png)
3. The alerts in the `alerts` topic, read with `kafka-console-consumer.sh`: [results/3_alerts_topic.png](results/3_alerts_topic.png)
4. The reader prints the same alerts: [results/4_alerts_reader.png](results/4_alerts_reader.png)
