#!/usr/bin/env bash
# Delete and recreate the demo topic so you can re-run produce steps cleanly.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

wait_for_broker

if kafka kafka-topics.sh --list | grep -qx "${TOPIC}"; then
  kafka kafka-topics.sh --delete --topic "${TOPIC}"
  echo "Deleted '${TOPIC}'. Waiting for deletion to finish..."
  sleep 2
fi

ensure_topic
echo "Topic reset. Run ./scripts/produce-hot.sh or ./scripts/produce-balanced.sh"
