#!/usr/bin/env bash
# Install Kafka (default 3.7.0 — #14242 window) on Ubuntu for OS-crash LEC tests.
set -euo pipefail

KAFKA_VERSION="${1:-3.7.0}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
INSTALL_ROOT="/opt/kafka"
DATA_DIR="/var/lib/kafka/data"
USER_NAME="kafka"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run as root: sudo bash $0 ${KAFKA_VERSION}" >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y openjdk-17-jre-headless curl ca-certificates

id -u "${USER_NAME}" >/dev/null 2>&1 || useradd --system --home "${DATA_DIR}" --shell /usr/sbin/nologin "${USER_NAME}"

TGZ="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${TGZ}"
TMP="/tmp/${TGZ}"

echo "Downloading ${URL}"
curl -fsSL -o "${TMP}" "${URL}"

rm -rf "${INSTALL_ROOT}"
mkdir -p /opt
tar -xzf "${TMP}" -C /opt
mv "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "${INSTALL_ROOT}"
chown -R "${USER_NAME}:${USER_NAME}" "${INSTALL_ROOT}"

mkdir -p "${DATA_DIR}"
chown -R "${USER_NAME}:${USER_NAME}" "${DATA_DIR}"

# Single combined broker+controller (KRaft) — enough to exercise LEC truncate paths.
CLUSTER_ID="$("${INSTALL_ROOT}/bin/kafka-storage.sh" random-uuid)"
cat > /etc/kafka-server.properties <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=${DATA_DIR}
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
default.replication.factor=1
min.insync.replicas=1
group.initial.rebalance.delay.ms=0
EOF
chown "${USER_NAME}:${USER_NAME}" /etc/kafka-server.properties

# Fresh metadata format for this version
rm -rf "${DATA_DIR:?}/"*
sudo -u "${USER_NAME}" "${INSTALL_ROOT}/bin/kafka-storage.sh" format \
  -t "${CLUSTER_ID}" -c /etc/kafka-server.properties

cat > /etc/systemd/system/kafka.service <<EOF
[Unit]
Description=Apache Kafka ${KAFKA_VERSION}
After=network.target

[Service]
Type=simple
User=${USER_NAME}
ExecStart=${INSTALL_ROOT}/bin/kafka-server-start.sh /etc/kafka-server.properties
Restart=on-failure
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable kafka
systemctl restart kafka

echo "Waiting for broker..."
for i in $(seq 1 60); do
  if "${INSTALL_ROOT}/bin/kafka-broker-api-versions.sh" \
      --bootstrap-server 127.0.0.1:9092 >/dev/null 2>&1; then
    echo "Kafka ${KAFKA_VERSION} is up. CLUSTER_ID=${CLUSTER_ID}"
    echo "Data dir: ${DATA_DIR}"
    exit 0
  fi
  sleep 2
done

echo "Broker did not become ready" >&2
journalctl -u kafka -n 80 --no-pager >&2 || true
exit 1
