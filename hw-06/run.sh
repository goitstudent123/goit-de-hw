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
# no external broker in .env, so start the local one
if [ -z "${KAFKA_BOOTSTRAP_SERVERS:-}" ]; then
    docker compose up -d --wait
fi

mkdir -p results
.venv/bin/pytest -v -p no:cacheprovider | tee results/tests.txt

# two sensors for 2.5 minutes, so a few sliding windows get closed while the processor runs
{
    .venv/bin/python create_topics.py
    .venv/bin/python alerts_processor.py --timeout 180 &
    processor=$!
    .venv/bin/python alerts_reader.py &
    reader=$!
    for _ in 1 2; do
        .venv/bin/python sensor.py --count 150 --interval 1 &
    done
    wait "$processor"
    sleep 5
    kill "$reader"
    wait
} | tee results/output.txt
