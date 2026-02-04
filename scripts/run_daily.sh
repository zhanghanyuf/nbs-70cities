#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/fetch_data.py
python3 scripts/build_web.py

git add data/processed docs/data.csv docs/chart.csv docs/price.csv data/raw scripts/fetch_data.py scripts/build_web.py
if git diff --cached --quiet; then
  echo "No changes to commit"
  exit 0
fi

git commit -m "Update data $(date +%F)"
git push
