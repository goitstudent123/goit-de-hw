#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

[ -d .venv ] || uv venv --python 3.12
uv pip install -q -r requirements.txt

mkdir -p results
.venv/bin/pytest -v -p no:cacheprovider | tee results/tests.txt
.venv/bin/python main.py | tee results/output.txt
