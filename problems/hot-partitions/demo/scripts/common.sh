#!/usr/bin/env bash
# Shared helpers for the hot-partitions demo.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose -f "${ROOT}/docker-compose.yml")
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose -f "${ROOT}/docker-compose.yml")
else
  echo "Need Docker Compose (docker compose or docker-compose)." >&2
  exit 1
fi
CONTAINER="${COMPOSE_CONTAINER:-kafka-hot-partitions}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
TOPIC="${TOPIC:-orders}"
PARTITIONS="${PARTITIONS:-6}"

kafka() {
  docker exec -i "${CONTAINER}" "/opt/kafka/bin/$1" --bootstrap-server "${BOOTSTRAP}" "${@:2}"
}

wait_for_broker() {
  echo "Waiting for broker..."
  for _ in $(seq 1 60); do
    if docker exec "${CONTAINER}" /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; then
      echo "Broker is ready."
      return 0
    fi
    sleep 1
  done
  echo "Broker did not become ready in time." >&2
  exit 1
}

ensure_topic() {
  if kafka kafka-topics.sh --list | grep -qx "${TOPIC}"; then
    echo "Topic '${TOPIC}' already exists."
    return 0
  fi
  kafka kafka-topics.sh \
    --create \
    --topic "${TOPIC}" \
    --partitions "${PARTITIONS}" \
    --replication-factor 1
  echo "Created topic '${TOPIC}' with ${PARTITIONS} partitions."
}

print_partition_counts() {
  echo
  echo "End offsets for topic '${TOPIC}' (partition:offset):"
  kafka kafka-get-offsets.sh --topic "${TOPIC}" --time -1 | sort -t: -k2,2n
  echo
  echo "Partition leaders:"
  kafka kafka-topics.sh --describe --topic "${TOPIC}"
}
