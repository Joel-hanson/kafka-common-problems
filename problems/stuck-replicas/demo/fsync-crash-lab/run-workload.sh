#!/usr/bin/env bash
# Drive produces + leadership churn so truncateFromEnd/Start touch LEC often.
# Leave this running, then hard-reset the guest (sysrq-b / reboot -f).
set -euo pipefail

KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
BS="${BS:-127.0.0.1:9092}"
TOPIC="${TOPIC:-fsync-lec}"

echo "Creating topic ${TOPIC} (ignore errors if exists)"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BS}" \
  --create --if-not-exists --topic "${TOPIC}" \
  --partitions 3 --replication-factor 1

echo "Starting continuous produce (Ctrl-C stops produce only)"
(
  while true; do
    seq 1 200 | "${KAFKA_BIN}/kafka-console-producer.sh" \
      --bootstrap-server "${BS}" --topic "${TOPIC}" >/dev/null 2>&1 || true
    sleep 0.2
  done
) &
PRODUCE_PID=$!

cleanup() {
  kill "${PRODUCE_PID}" 2>/dev/null || true
}
trap cleanup EXIT

echo "Churning leadership / forcing replica state transitions"
# Single-broker: delete-records moves log start → truncateFromStart on LEC.
# Also bounce the process is NOT the test — do OS crash from another terminal.
i=0
while true; do
  i=$((i + 1))
  # Advance log-start on partition 0 when there is data (triggers LEC truncateFromStart)
  END="$("${KAFKA_BIN}/kafka-get-offsets.sh" --bootstrap-server "${BS}" \
    --topic "${TOPIC}" --time -1 2>/dev/null | awk -F: '/:0:/ {print $3; exit}')"
  if [[ -n "${END:-}" && "${END}" -gt 50 ]]; then
    CUT=$((END / 2))
    echo "{\"partitions\":[{\"topic\":\"${TOPIC}\",\"partition\":0,\"offset\":${CUT}}],\"version\":1}" \
      > /tmp/delete-records.json
    "${KAFKA_BIN}/kafka-delete-records.sh" --bootstrap-server "${BS}" \
      --offset-json-file /tmp/delete-records.json >/dev/null 2>&1 || true
    echo "$(date -u +%H:%M:%S) delete-records to ${CUT} (iter ${i})"
  fi
  sleep 1
done
