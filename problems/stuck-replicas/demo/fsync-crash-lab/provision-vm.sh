#!/usr/bin/env bash
# Provision an Ubuntu 22.04 VM for the fsync/LEC crash test (KAFKA-14242 / #16541).
#
# Requirements (host machine):
#   - multipass  (brew install --cask multipass  on macOS)
#   - This script must be run from the fsync-crash-lab/ directory, or set
#     LAB_DIR to the full path of that directory.
#
# Usage:
#   bash provision-ubuntu-vm.sh [kafka-version]
#
# Examples:
#   bash provision-ubuntu-vm.sh           # installs 3.7.0 (the #14242 window)
#   bash provision-ubuntu-vm.sh 3.8.1     # A/B control — should show no torn LECs
#
# After provisioning, the README explains how to run the trial:
#   1. Inside the VM (Terminal 1): sudo -u kafka bash /home/ubuntu/run-workload.sh
#   2. Inside the VM (Terminal 2, after ~30-60s): sudo sh -c 'echo b > /proc/sysrq-trigger'
#   3. After reboot:                              sudo bash /home/ubuntu/inspect-lec.sh

set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_VERSION="${1:-3.7.0}"
VM_NAME="kafka-fsync"
LAB_DIR="${LAB_DIR:-$(cd "$(dirname "$0")" && pwd)}"
CPUS=2
MEMORY=4G
DISK=20G

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { echo "▶ $*"; }
ok()    { echo "✓ $*"; }
die()   { echo "✗ $*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "'$1' not found. Install it and retry."
}

# ---------------------------------------------------------------------------
# Host preflight
#
# Packages needed on THIS machine (the host running multipass):
#   multipass   — VM manager; install instructions printed below if missing
#
# Packages installed INSIDE the VM by setup-guest.sh (not needed on host):
#   openjdk-17-jre-headless, curl, ca-certificates
#
# The Ubuntu 22.04 guest image itself ships with all other utilities used
# by the lab scripts (bash, awk, find, grep, wc, systemd, sysrq support).
# ---------------------------------------------------------------------------
if ! command -v multipass >/dev/null 2>&1; then
  cat >&2 <<'INSTALL'
✗ 'multipass' not found on this host.

Install it:
  macOS:   brew install --cask multipass
  Ubuntu:  sudo snap install multipass
  Windows: https://multipass.run/install

Then re-run this script.
INSTALL
  exit 1
fi
ok "multipass $(multipass version | awk 'NR==1{print $2}') found"

for f in setup-guest.sh run-workload.sh inspect-lec.sh; do
  [[ -f "${LAB_DIR}/${f}" ]] || die "Missing ${LAB_DIR}/${f}"
done

# ---------------------------------------------------------------------------
# Create VM if it does not already exist
# ---------------------------------------------------------------------------
if multipass info "${VM_NAME}" >/dev/null 2>&1; then
  info "VM '${VM_NAME}' already exists — skipping launch"
else
  info "Launching Ubuntu 22.04 VM '${VM_NAME}' (${CPUS} CPUs, ${MEMORY} RAM, ${DISK} disk)…"
  multipass launch 22.04 \
    --name "${VM_NAME}" \
    --cpus "${CPUS}" \
    --memory "${MEMORY}" \
    --disk "${DISK}"
  ok "VM launched"
fi

# ---------------------------------------------------------------------------
# Transfer lab scripts
# ---------------------------------------------------------------------------
info "Transferring lab scripts to VM…"
for f in setup-guest.sh run-workload.sh inspect-lec.sh; do
  multipass transfer "${LAB_DIR}/${f}" "${VM_NAME}:/home/ubuntu/${f}"
done
ok "Scripts transferred"

# ---------------------------------------------------------------------------
# Make scripts executable inside the VM
# ---------------------------------------------------------------------------
multipass exec "${VM_NAME}" -- bash -c "chmod +x /home/ubuntu/setup-guest.sh /home/ubuntu/run-workload.sh /home/ubuntu/inspect-lec.sh"

# ---------------------------------------------------------------------------
# Run guest setup (installs Java, Kafka under systemd, formats KRaft storage)
# ---------------------------------------------------------------------------
info "Running setup-guest.sh ${KAFKA_VERSION} inside VM — this takes ~2 min…"
multipass exec "${VM_NAME}" -- sudo bash /home/ubuntu/setup-guest.sh "${KAFKA_VERSION}"
ok "Kafka ${KAFKA_VERSION} installed and running inside VM"

# ---------------------------------------------------------------------------
# Quick smoke-check
# ---------------------------------------------------------------------------
info "Smoke-check: querying broker API versions…"
multipass exec "${VM_NAME}" -- \
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:9092 \
  | head -3
ok "Broker is responding"

# ---------------------------------------------------------------------------
# Print next steps
# ---------------------------------------------------------------------------
VM_IP="$(multipass info "${VM_NAME}" | awk '/IPv4/ {print $2}')"

cat <<EOF

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  VM ready: ${VM_NAME}  (${VM_IP})   Kafka ${KAFKA_VERSION}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SHELL INTO THE VM:
  multipass shell ${VM_NAME}

RUN A CRASH TRIAL (two terminals inside the VM):

  Terminal 1 — start workload (keep running):
    sudo bash /home/ubuntu/run-workload.sh

  Terminal 2 — after 30-60s of churn, hard-crash the guest WITHOUT sync:
    sudo sh -c 'echo b > /proc/sysrq-trigger'
    # or: sudo reboot -f
    # Do NOT use 'shutdown' or 'multipass stop' — those flush page cache.

  If the VM is wedged after sysrq, force-stop from this host:
    multipass stop ${VM_NAME} --force

AFTER REBOOT (inside the VM):
  sudo bash /home/ubuntu/inspect-lec.sh
  sudo systemctl start kafka || true
  sudo journalctl -u kafka -b --no-pager | head -200

REPEAT 10-20 trials. Torn LEC (PARSE_FAIL) = pass for #14242/#16541.

A/B CONTROL — install 3.8.1 and repeat to see if the bug is fixed:
  multipass exec ${VM_NAME} -- sudo bash /home/ubuntu/setup-guest.sh 3.8.1

CLEAN UP WHEN DONE:
  multipass delete ${VM_NAME} && multipass purge
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EOF
