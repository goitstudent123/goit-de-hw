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

# mysql/init.sql loads these into the local MySQL on its first start
mkdir -p data
for table in athlete_bio athlete_event_results; do
    [ -f "data/$table.csv" ] || curl -sSfL -o "data/$table.csv" "https://ftp.goit.study/neoversity/$table.csv"
done

docker compose up -d --wait

mkdir -p results
.venv/bin/pytest -v --no-header -p no:cacheprovider | tee results/tests.txt

dag=anton_medals
airflow() {
    docker compose exec -T airflow airflow "$@"
}
mysql() {
    docker compose exec -T -e MYSQL_PWD="${MYSQL_PASSWORD:-root}" mysql \
        mysql -h "${MYSQL_HOST:-127.0.0.1}" -P "${MYSQL_PORT:-3306}" -u "${MYSQL_USER:-root}" "$@"
}

# 35 seconds is longer than the sensor allows, so those runs should fail
delays="10 10 35 10 35 10"

{
    airflow dags reserialize > /dev/null
    airflow dags unpause "$dag" > /dev/null

    prefix=$(date +%Y%m%d_%H%M%S)
    runs=()
    i=1
    for delay in $delays; do
        run="${prefix}_${i}_delay_${delay}"
        airflow dags trigger "$dag" -r "$run" -c "{\"delay\": $delay}" > /dev/null
        runs+=("$run")
        i=$((i + 1))
    done
    echo "Triggered ${#runs[@]} runs with delays: $delays"

    while airflow dags list-runs "$dag" -o json | grep -Eq '"state": "(queued|running)"'; do
        sleep 10
    done

    for run in "${runs[@]}"; do
        echo
        echo "Run $run"
        airflow tasks states-for-dag-run "$dag" "$run" -o plain
    done

    echo
    echo "Last records in anton.hw_dag_results"
    mysql -t -e "SELECT * FROM anton.hw_dag_results ORDER BY id DESC LIMIT ${#runs[@]}"
} | tee results/output.txt
