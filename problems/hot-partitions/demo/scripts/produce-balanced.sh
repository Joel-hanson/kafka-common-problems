#!/usr/bin/env bash
# Produce balanced traffic: unique order ids as keys → even partition spread.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

TOTAL="${TOTAL:-10000}"
# Default topic for the balanced run (override with BALANCED_TOPIC=...)
TOPIC="${BALANCED_TOPIC:-orders-balanced}"

wait_for_broker

if ! kafka kafka-topics.sh --list | grep -qx "${TOPIC}"; then
  kafka kafka-topics.sh \
    --create \
    --topic "${TOPIC}" \
    --partitions "${PARTITIONS}" \
    --replication-factor 1
  echo "Created topic '${TOPIC}' with ${PARTITIONS} partitions."
fi

echo "Producing ${TOTAL} records to '${TOPIC}' with unique order-id keys"

{
  for i in $(seq 1 "${TOTAL}"); do
    printf 'order-%s:payload-%s\n' "${i}" "${i}"
  done
} | kafka kafka-console-producer.sh \
  --topic "${TOPIC}" \
  --property parse.key=true \
  --property key.separator=:

echo "Done. Compare with the hot topic."
print_partition_counts
