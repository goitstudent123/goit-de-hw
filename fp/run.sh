#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

[ -d .venv ] || uv venv --python 3.12
uv pip install -q -r requirements.txt

if [ -f .env ]; then
    set -a
    . ./.env
    set +a
fi

# the local MySQL loads these csv files on its first start
mkdir -p data
for table in athlete_bio athlete_event_results; do
    [ -f "data/$table.csv" ] || curl -sfo "data/$table.csv" "https://ftp.goit.study/neoversity/$table.csv"
done

docker compose up -d --build --wait

mkdir -p results
.venv/bin/pytest -v --no-header -p no:cacheprovider test_part_1.py test_part_2.py | tee results/tests.txt
docker compose exec -T airflow pytest -v --no-header -p no:cacheprovider /opt/airflow/test_dag.py | tee -a results/tests.txt

dag=anton_project_solution
airflow() {
    docker compose exec -T airflow airflow "$@"
}
mysql() {
    docker compose exec -T -e MYSQL_PWD="${MYSQL_PASSWORD:-root}" mysql \
        mysql -h "${MYSQL_HOST:-127.0.0.1}" -P "${MYSQL_PORT:-3306}" -u "${MYSQL_USER:-root}" "$@"
}

{
    echo "Part 1: streaming pipeline"
    .venv/bin/python part_1/create_topics.py
    .venv/bin/python part_1/mysql_to_kafka.py
    .venv/bin/python part_1/streaming_pipeline.py
    .venv/bin/python part_1/kafka_reader.py --count 20
    echo
    echo "Last rows in ${MYSQL_OUTPUT_SCHEMA:-anton}.athlete_enriched_agg"
    mysql -t -e "SELECT * FROM ${MYSQL_OUTPUT_SCHEMA:-anton}.athlete_enriched_agg ORDER BY timestamp DESC LIMIT 20"

    echo
    echo "Part 2: batch data lake"
    airflow dags reserialize > /dev/null
    airflow dags unpause "$dag" > /dev/null
    run="manual_$(date +%Y%m%d_%H%M%S)"
    airflow dags trigger "$dag" -r "$run" > /dev/null
    echo "Triggered $run"
    while airflow dags list-runs -d "$dag" -o plain | grep "$run" | grep -Eq 'queued|running'; do
        sleep 10
    done
    airflow tasks states-for-dag-run "$dag" "$run" -o plain
    echo
    echo "gold/avg_stats in the silver_to_gold log"
    docker compose exec -T airflow bash -c \
        "grep -E 'INFO - [|+]' /opt/airflow/logs/dag_id=$dag/run_id=$run/task_id=silver_to_gold/attempt=1.log | sed 's/.*INFO - //'"
} | tee results/output.txt
