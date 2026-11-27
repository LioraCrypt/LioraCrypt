#!/bin/sh
set -e

echo "Fetching Binance data"
python data/fetch/fetch_crypto.py

echo "Loading data into BigQuery"
python data/load/load_raw.py data/raw/*.jsonl --project lioracrypt

echo "Running dbt"
dbt build --project-dir data/dbt --profiles-dir data/dbt

echo "Execution completed"