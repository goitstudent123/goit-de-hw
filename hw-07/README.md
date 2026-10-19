# Homework 07: Apache Airflow

A DAG `anton_medals` ([dags/medals_dag.py](dags/medals_dag.py)) that works with MySQL. It creates `anton.hw_dag_results` if it doesn't exist, picks a random medal (Bronze, Silver or Gold) and branches to one of three tasks. That task counts the rows with this medal in `olympic_dataset.athlete_event_results` and saves the count with the medal type and time. Then the DAG sleeps for a while, and a sensor checks that the newest record is not older than 30 seconds.

Tasks:

1. `create_table`: `CREATE TABLE IF NOT EXISTS` with `id`, `medal_type`, `count`, `created_at`.
2. `pick_medal`: `random.choice` of the three medals, the result goes to XCom.
3. `pick_medal_task`: `BranchPythonOperator`, returns `calc_<medal>`; the other two get skipped.
4. `calc_Bronze`, `calc_Silver`, `calc_Gold`: one `INSERT ... SELECT COUNT(*)` each.
5. `generate_delay`: `time.sleep(n)`, runs when one of the calc tasks succeeds (`one_success`). `n` is the `delay` run parameter, 10 seconds by default.
6. `check_for_correctness`: `SqlSensor` with `TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30`. It pokes every 5 seconds and fails after 30 seconds.

`MySqlOperator` is gone from the current MySQL provider, so the SQL tasks use `SQLExecuteQueryOperator` with the `goit_mysql_db` connection. `max_active_runs=1`: all runs write to the same table, so a parallel run could make the sensor pass for another one.

## How to run

Requires [uv](https://docs.astral.sh/uv/) and Docker.

`./run.sh` starts Airflow 3.3 (standalone) and MySQL 8.4 from `docker-compose.yml`, runs the tests, then triggers six DAG runs. Four of them use a 10 second delay and two use 35 seconds, to show the sensor failing. The script waits for the runs to finish and prints the task states and the new records. The Airflow UI is at http://localhost:8080 (no login). Stop everything with `docker compose down`.

The local MySQL gets `olympic_dataset.athlete_bio` and `olympic_dataset.athlete_event_results` from the course CSV files ([athlete_bio.csv](https://ftp.goit.study/neoversity/athlete_bio.csv), [athlete_event_results.csv](https://ftp.goit.study/neoversity/athlete_event_results.csv)). `run.sh` downloads them to `data/` (not committed), and `mysql/init.sql` loads them on the first start of the container (155,861 and 316,834 rows; 15,072 Gold, 14,676 Silver, 14,939 Bronze). After changing the data, recreate MySQL with `docker compose down -v`. To use the course database, copy `.env.example` to `.env` and fill in the server and credentials. The `goit_mysql_db` connection and the tests then use it.

## Results

Run output: [results/output.txt](results/output.txt), tests: [results/tests.txt](results/tests.txt).

All 18 tests pass. Six runs: Silver was picked three times, Gold twice and Bronze once, and each run added one record (Gold 15072, Silver 14676, Bronze 14939). The four runs with a 10 second delay succeeded. In both 35 second runs `check_for_correctness` failed after 30 seconds of poking with `AirflowSensorTimeout`, because the newest record was already older than 30 seconds.

### Screenshots

1. Graph of a successful run: `calc_Gold` ran, the other two were skipped: [results/1_graph_success.png](results/1_graph_success.png)
2. Graph of a 35 second run with the failed sensor: [results/2_graph_failed.png](results/2_graph_failed.png)
3. Grid of all six runs, the two failed ones in red: [results/3_grid.png](results/3_grid.png)
4. Sensor log of a failed run: [results/4_sensor_log.png](results/4_sensor_log.png)
5. Records added by the six runs in `anton.hw_dag_results`: [results/5_table.png](results/5_table.png)
