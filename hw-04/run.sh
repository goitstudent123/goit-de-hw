#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

[ -d .venv ] || uv venv --python 3.12
uv pip install -q -r requirements.txt

mkdir -p results
.venv/bin/pytest -v -p no:cacheprovider | tee results/tests.txt
# Enter is fed so the parts don't wait; run them by hand to look at Spark UI
for part in part_1 part_2 part_3; do
    echo | .venv/bin/python "$part.py"
    echo
done | tee results/output.txt
