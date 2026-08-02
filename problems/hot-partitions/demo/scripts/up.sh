#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

cd "${ROOT}"
"${COMPOSE[@]}" up -d
wait_for_broker
ensure_topic
echo
echo "Cluster is up. Next:"
echo "  ./scripts/produce-hot.sh       # create skew"
echo "  ./scripts/show-partition-sizes.sh"
