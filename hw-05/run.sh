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

# three sensors, the processor and the reader at once; consumers stop when there is nothing new to read
{
    .venv/bin/python create_topics.py
    .venv/bin/python alerts_processor.py --idle 15 &
    .venv/bin/python alerts_reader.py --idle 20 &
    for _ in 1 2 3; do
        .venv/bin/python sensor.py --count 10 --interval 1 &
    done
    wait
} | tee results/output.txt
