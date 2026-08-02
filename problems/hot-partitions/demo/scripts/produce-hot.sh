#!/usr/bin/env bash
# Produce skewed traffic: ~90% of records share one tenant key → hot partition.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

TOTAL="${TOTAL:-10000}"
HOT_PCT="${HOT_PCT:-90}"
HOT_KEY="${HOT_KEY:-tenant-vip}"

wait_for_broker
ensure_topic

HOT_COUNT=$((TOTAL * HOT_PCT / 100))
OTHER_COUNT=$((TOTAL - HOT_COUNT))

echo "Producing ${TOTAL} records to '${TOPIC}'"
echo "  ${HOT_COUNT} with key '${HOT_KEY}' (${HOT_PCT}%)"
echo "  ${OTHER_COUNT} with unique tenant keys"

{
  for i in $(seq 1 "${HOT_COUNT}"); do
    printf '%s:order-hot-%s\n' "${HOT_KEY}" "${i}"
  done
  for i in $(seq 1 "${OTHER_COUNT}"); do
    printf 'tenant-%s:order-other-%s\n' "${i}" "${i}"
  done
} | kafka kafka-console-producer.sh \
  --topic "${TOPIC}" \
  --property parse.key=true \
  --property key.separator=:

echo "Done. Inspect skew with: ./scripts/show-partition-sizes.sh"
print_partition_counts
