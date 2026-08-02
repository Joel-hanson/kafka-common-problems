#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

wait_for_broker
print_partition_counts

echo "How to read this:"
echo "  - Hot run: one partition's end offset is ~${HOT_PCT:-90}% of total."
echo "  - Balanced run: offsets sit close to each other across partitions."
