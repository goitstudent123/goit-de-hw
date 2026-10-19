# Final project: streaming pipeline and batch data lake

Athlete data for ML features: average height and weight per sport, medal (or no medal), sex and country. Part 1 does it as a Spark Structured Streaming pipeline with MySQL and Kafka, part 2 as a landing/bronze/silver/gold data lake run by an Airflow DAG.

## Part 1: streaming pipeline

Code in [part_1](part_1). Stages are marked with `# Stage N` comments.

- [create_topics.py](part_1/create_topics.py) recreates `anton_athlete_event_results` and `anton_athlete_enriched_agg`, so a rerun starts from empty topics.
- [mysql_to_kafka.py](part_1/mysql_to_kafka.py): stage 3, reads `olympic_dataset.athlete_event_results` from MySQL and writes every row as json to `anton_athlete_event_results`.
- [streaming_pipeline.py](part_1/streaming_pipeline.py): stages 1-6. Reads `olympic_dataset.athlete_bio`, keeps rows with numeric height and weight, reads the results topic as a stream and parses the json into columns, joins by `athlete_id`, averages height and weight per `sport`, `medal`, `sex`, `country_noc` and adds `timestamp`. `forEachBatch` writes every micro-batch to the `anton_athlete_enriched_agg` topic and appends it to `anton.athlete_enriched_agg` in MySQL.
- [kafka_reader.py](part_1/kafka_reader.py) prints the first messages of the output topic.

`country_noc` is taken from the results (the country the athlete competed for). Missing medals become `No medal`. The aggregation runs in `complete` mode, so every batch carries the full table; the stream uses `availableNow`, 100,000 messages per micro-batch, and stops when the topic is read.

## Part 2: batch data lake

Code in [part_2](part_2):

- [landing_to_bronze.py](part_2/landing_to_bronze.py) downloads `athlete_bio.csv` and `athlete_event_results.csv` from the FTP server to `landing/` and saves them as parquet in `bronze/{table}`.
- [bronze_to_silver.py](part_2/bronze_to_silver.py) cleans all text columns (keeps letters, digits, spaces and basic punctuation, squeezes spaces), drops duplicate rows and saves `silver/{table}`.
- [silver_to_gold.py](part_2/silver_to_gold.py) joins the two silver tables by `athlete_id`, drops non-numeric height and weight (empty values, ranges like `80-82`), averages them per `sport`, `medal`, `sex`, `country_noc`, adds `timestamp` and saves `gold/avg_stats`.
- [project_solution.py](part_2/project_solution.py): DAG `anton_project_solution` that runs the three jobs one after another with `spark-submit`.

Each job prints its final DataFrame with `df.show()`. The DAG uses `BashOperator` with `spark-submit --master local[*]` and the job folder as working directory, so the data folders end up next to the scripts. The local Airflow is 2.8.1, the same version as the course server.

## How to run

Requires [uv](https://docs.astral.sh/uv/), Docker and Java 17 or 21 (for Spark 4.1).

`./run.sh` downloads the two csv files to `data/`, starts MySQL 8.4 (loads the csv files into `olympic_dataset`), Kafka and Airflow 2.8.1 from `docker-compose.yml`, and runs the tests. Then it runs part 1 and prints the Kafka messages and the MySQL rows, and triggers the part 2 DAG and prints the task states and the gold table from the task log. The Airflow UI is at http://localhost:8081 (no login). Stop everything with `docker compose down`.

To use the course MySQL and Kafka in part 1, copy `.env.example` to `.env` and fill in the servers and credentials.

## Results

Run output: [results/output.txt](results/output.txt), tests: [results/tests.txt](results/tests.txt).

All 25 tests pass (19 Spark tests, 6 DAG tests inside the Airflow container).

Part 1: 316,834 rows went from MySQL to `anton_athlete_event_results`. The stream read them in 4 micro-batches; each batch went to Kafka and MySQL with 4,517, 8,141, 10,580 and 10,879 groups. After the last batch there are 10,879 groups. The height/weight filter keeps 104,153 of 155,861 athletes: 50,749 have no height and weight, 959 more have a non-numeric value such as the weight `80-82`.

Part 2: all three tasks succeeded. Bronze has 155,861 bio rows and 316,834 result rows; silver keeps all bio rows and 315,626 results (1,208 duplicates dropped). `gold/avg_stats` has 10,879 rows, the same groups as in part 1.

### Screenshots

1. `anton.athlete_enriched_agg` in MySQL (Adminer): [results/1_mysql_table.png](results/1_mysql_table.png)
2. Messages of the `anton_athlete_enriched_agg` topic printed by `kafka_reader.py`: [results/2_kafka_reader.png](results/2_kafka_reader.png)
3. `landing_to_bronze` log with `df.show()` of `athlete_bio`: [results/3_landing_to_bronze_log.png](results/3_landing_to_bronze_log.png)
4. `bronze_to_silver` log with `df.show()` of `athlete_event_results`: [results/4_bronze_to_silver_log.png](results/4_bronze_to_silver_log.png)
5. `silver_to_gold` log with `df.show()` of `avg_stats`: [results/5_silver_to_gold_log.png](results/5_silver_to_gold_log.png)
6. Graph of the finished DAG run: [results/6_dag_graph.png](results/6_dag_graph.png)
