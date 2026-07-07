#!/usr/bin/env bash
# Idempotent Debezium connector registration: create if absent, update if present.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${CONNECTOR_DIR}/register-postgres.json"
NAME="$(python3 -c "import json;print(json.load(open('${CONFIG_FILE}'))['name'])")"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
for _ in $(seq 1 60); do
  curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1 && break
  sleep 2
done

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors/${NAME}")
if [[ "${STATUS}" == "200" ]]; then
  echo "Connector '${NAME}' exists; updating config."
  python3 -c "import json;print(json.dumps(json.load(open('${CONFIG_FILE}'))['config']))" |
    curl -fsS -X PUT -H "Content-Type: application/json" \
      --data @- "${CONNECT_URL}/connectors/${NAME}/config" >/dev/null
else
  echo "Creating connector '${NAME}'."
  curl -fsS -X POST -H "Content-Type: application/json" \
    --data @"${CONFIG_FILE}" "${CONNECT_URL}/connectors" >/dev/null
fi
echo "Connector state:"
curl -fsS "${CONNECT_URL}/connectors/${NAME}/status"
echo
