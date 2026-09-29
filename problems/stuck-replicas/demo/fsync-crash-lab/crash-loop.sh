#!/usr/bin/env bash
# Run repeated crash trials against the kafka-fsync Multipass VM until a
# torn leader-epoch-checkpoint (PARSE_FAIL) is reproduced.
#
# Each trial:
#   1. Start kafka (if not already running)
#   2. Run run-workload.sh in the background for CHURN_SECS seconds
#   3. Hard-crash the guest kernel via sysrq-b (no page-cache flush)
#   4. Wait for the VM to reboot and SSH to become reachable
#   5. Run inspect-lec.sh — stop the loop if any PARSE_FAIL is found
#   6. Wipe the data dir and re-format so the next trial is clean
#
# Usage (from the host, not inside the VM):
#   bash crash-loop.sh [max-trials]
#
# Examples:
#   bash crash-loop.sh        # runs up to 20 trials
#   bash crash-loop.sh 50     # runs up to 50 trials
#
# Stop early at any time with Ctrl-C. Results land in ./crash-loop-results/.

set -euo pipefail

# ---------------------------------------------------------------------------
# Config — override via environment variables
# ---------------------------------------------------------------------------
VM_NAME="${VM_NAME:-kafka-fsync}"
MAX_TRIALS="${1:-20}"
CHURN_SECS="${CHURN_SECS:-45}"   # seconds of workload before the crash
BOOT_TIMEOUT="${BOOT_TIMEOUT:-120}" # max seconds to wait for VM to come back
RESULTS_DIR="${RESULTS_DIR:-$(cd "$(dirname "$0")" && pwd)/crash-loop-results}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { echo "[$(date -u +%H:%M:%S)] ▶ $*"; }
ok()    { echo "[$(date -u +%H:%M:%S)] ✓ $*"; }
fail()  { echo "[$(date -u +%H:%M:%S)] ✗ $*" >&2; }
die()   { fail "$*"; exit 1; }
sep()   { echo "────────────────────────────────────────────────────────────"; }

# Run a command inside the VM, suppressing multipass noise on stderr
vm() { multipass exec "${VM_NAME}" -- "$@"; }
vm_sudo() { multipass exec "${VM_NAME}" -- sudo "$@"; }

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
command -v multipass >/dev/null 2>&1 || die "'multipass' not found. Run provision-ubuntu-vm.sh first."

multipass info "${VM_NAME}" >/dev/null 2>&1 \
  || die "VM '${VM_NAME}' does not exist. Run provision-ubuntu-vm.sh first."

mkdir -p "${RESULTS_DIR}"

# ---------------------------------------------------------------------------
# Wait for SSH / multipass exec to become available after a reboot
# ---------------------------------------------------------------------------
wait_for_vm() {
  local deadline=$(( $(date +%s) + BOOT_TIMEOUT ))
  info "Waiting for VM to come back (up to ${BOOT_TIMEOUT}s)…"
  while true; do
    if multipass exec "${VM_NAME}" -- true 2>/dev/null; then
      ok "VM is reachable"
      return 0
    fi
    if [[ $(date +%s) -ge ${deadline} ]]; then
      die "VM did not become reachable within ${BOOT_TIMEOUT}s"
    fi
    sleep 3
  done
}

# ---------------------------------------------------------------------------
# Wipe data dir and re-format KRaft storage so each trial starts fresh
# ---------------------------------------------------------------------------
reset_kafka() {
  info "Stopping Kafka and wiping data dir for clean trial…"
  vm_sudo systemctl stop kafka 2>/dev/null || true
  vm_sudo bash -c 'rm -rf /var/lib/kafka/data/*'
  # Re-format with a fresh cluster ID
  local cluster_id
  cluster_id="$(vm_sudo /opt/kafka/bin/kafka-storage.sh random-uuid 2>/dev/null)"
  vm_sudo /opt/kafka/bin/kafka-storage.sh format \
    -t "${cluster_id}" -c /etc/kafka-server.properties \
    >/dev/null 2>&1
  ok "Data dir wiped and re-formatted (cluster_id=${cluster_id})"
}

# ---------------------------------------------------------------------------
# Start Kafka and wait for broker to be ready
# ---------------------------------------------------------------------------
start_kafka() {
  info "Starting Kafka…"
  vm_sudo systemctl start kafka
  local deadline=$(( $(date +%s) + 60 ))
  while true; do
    if vm /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server 127.0.0.1:9092 >/dev/null 2>&1; then
      ok "Broker is up"
      return 0
    fi
    [[ $(date +%s) -lt ${deadline} ]] || die "Broker did not start within 60s"
    sleep 2
  done
}

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
FOUND_TRIAL=0
PASS_COUNT=0   # trials with PARSE_FAIL (bug reproduced)
CLEAN_COUNT=0  # trials with no corruption

sep
info "Starting crash loop: VM=${VM_NAME}, max=${MAX_TRIALS} trials, churn=${CHURN_SECS}s each"
info "Results → ${RESULTS_DIR}"
sep

for (( trial=1; trial<=MAX_TRIALS; trial++ )); do
  TRIAL_DIR="${RESULTS_DIR}/trial-$(printf '%02d' "${trial}")"
  mkdir -p "${TRIAL_DIR}"

  sep
  info "Trial ${trial}/${MAX_TRIALS}"

  # ---- 1. Ensure clean state -----------------------------------------------
  reset_kafka
  start_kafka

  # ---- 2. Run workload in background inside VM for CHURN_SECS seconds ------
  info "Starting workload for ${CHURN_SECS}s…"
  # Run workload via nohup so it survives the SSH session being torn
  vm_sudo -u kafka bash -c \
    "nohup bash /home/ubuntu/run-workload.sh >/tmp/workload.log 2>&1 &"
  sleep "${CHURN_SECS}"
  info "Churn complete — triggering hard crash (sysrq-b)…"

  # ---- 3. Hard-crash the kernel — SSH will be killed mid-flight ------------
  # We fire sysrq-b and immediately swallow the (expected) connection error.
  multipass exec "${VM_NAME}" -- \
    sudo sh -c 'echo b > /proc/sysrq-trigger' 2>/dev/null || true

  # Give the VM a moment to actually go down before we start polling
  sleep 5

  # ---- 4. Wait for VM to reboot --------------------------------------------
  wait_for_vm

  # ---- 5. Inspect LECs -------------------------------------------------------
  info "Inspecting leader-epoch-checkpoints…"
  INSPECT_OUT="${TRIAL_DIR}/lec-inspect.txt"

  # inspect-lec.sh exits 2 on PARSE_FAIL, 0 on clean — capture both
  set +e
  multipass exec "${VM_NAME}" -- \
    sudo bash /home/ubuntu/inspect-lec.sh \
    > "${INSPECT_OUT}" 2>&1
  INSPECT_RC=$?
  set -e

  # Also grab the systemd journal from this boot for context
  vm_sudo journalctl -u kafka -b --no-pager \
    > "${TRIAL_DIR}/kafka-journal.txt" 2>&1 || true

  if grep -q 'PARSE_FAIL\|EMPTY or missing' "${INSPECT_OUT}"; then
    PASS_COUNT=$(( PASS_COUNT + 1 ))
    ok "Trial ${trial}: PARSE_FAIL detected — torn LEC reproduced! 🎯"
    cp "${INSPECT_OUT}" "${RESULTS_DIR}/REPRODUCED-trial-$(printf '%02d' "${trial}").txt"
    FOUND_TRIAL=${trial}
    break
  else
    CLEAN_COUNT=$(( CLEAN_COUNT + 1 ))
    info "Trial ${trial}: clean (no corruption this time)"
  fi
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
sep
if [[ "${FOUND_TRIAL}" -gt 0 ]]; then
  ok "BUG REPRODUCED on trial ${FOUND_TRIAL}"
  ok "Inspect report: ${RESULTS_DIR}/REPRODUCED-trial-$(printf '%02d' "${FOUND_TRIAL}").txt"
  ok "Kafka journal:  ${RESULTS_DIR}/trial-$(printf '%02d' "${FOUND_TRIAL}")/kafka-journal.txt"
else
  info "No torn LEC detected across ${MAX_TRIALS} trials (clean_count=${CLEAN_COUNT})"
  info "This is expected on 3.8+ (bug fixed). On 3.7.0, try more trials or reduce CHURN_SECS."
  info "  CHURN_SECS=20 bash crash-loop.sh 40"
fi
info "All results: ${RESULTS_DIR}/"
sep

# Exit 0 if reproduced (caller can check for the results file), 1 if not
[[ "${FOUND_TRIAL}" -gt 0 ]]
