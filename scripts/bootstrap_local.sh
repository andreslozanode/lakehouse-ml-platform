#!/usr/bin/env bash
# One-shot local environment bootstrap.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo ">> Creating virtualenv & installing package with extras"
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e ".[dev,spark,ml,serving,delivery,cdc]"
pre-commit install

echo ">> Checking Kaggle credentials"
: "${KAGGLE_USERNAME:?Set KAGGLE_USERNAME (see .env.example)}"
: "${KAGGLE_KEY:?Set KAGGLE_KEY (see .env.example)}"

echo ">> Starting local CDC stack"
docker compose up -d
./cdc/debezium/register.sh

echo ">> Waiting for MLflow Tracking Server"
for _ in $(seq 1 30); do
  curl -fsS http://localhost:5000/health >/dev/null 2>&1 && break
  sleep 2
done

echo ">> Ready. MLflow UI: http://localhost:5000 | Try: make pipeline && make train-classical"
