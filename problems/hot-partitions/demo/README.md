# Hot partitions: recreate

Local Kafka (Docker Compose) plus broker CLI commands. No wrapper scripts.

## Prerequisites

- Docker with Compose (`docker compose` or `docker-compose`)
- About 1 GB free RAM

## 1. Start the broker

```bash
cd problems/hot-partitions/demo
docker compose up -d
```

Wait until healthy (`docker compose ps`), or until this succeeds:

```bash
docker exec kafka-hot-partitions \
  /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

Broker from the host: `localhost:9092`.

## 2. Create a 6-partition topic

```bash
docker exec -it kafka-hot-partitions \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders \
  --partitions 6 --replication-factor 1
```

## 3. Produce skewed traffic (hot partition)

About 90% of records share key `tenant-vip`; the rest use unique tenant keys. Pipe `key:value` lines into the console producer:

```bash
{
  for i in $(seq 1 9000); do printf 'tenant-vip:order-hot-%s\n' "$i"; done
  for i in $(seq 1 1000); do printf 'tenant-%s:order-other-%s\n' "$i" "$i"; done
} | docker exec -i kafka-hot-partitions \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property parse.key=true \
  --property key.separator=:
```

## 4. Inspect per-partition end offsets

```bash
docker exec kafka-hot-partitions \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic orders --time -1 | sort -t: -k2,2n
```

Expect one partition near offset `9000` and the others sharing about `1000`. Optionally describe leaders:

```bash
docker exec kafka-hot-partitions \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic orders
```

## 5. Produce balanced traffic (comparison)

Create a second topic and key by unique order id:

```bash
docker exec kafka-hot-partitions \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders-balanced \
  --partitions 6 --replication-factor 1

{
  for i in $(seq 1 10000); do printf 'order-%s:payload-%s\n' "$i" "$i"; done
} | docker exec -i kafka-hot-partitions \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders-balanced \
  --property parse.key=true \
  --property key.separator=:

docker exec kafka-hot-partitions \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic orders-balanced --time -1 | sort -t: -k2,2n
```

Offsets on `orders-balanced` should sit close together across all six partitions.

## 6. Reset (optional) and tear down

Delete and recreate `orders` for a clean re-run:

```bash
docker exec kafka-hot-partitions \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete --topic orders
# wait a couple of seconds, then re-run step 2
```

Stop the stack:

```bash
docker compose down -v
```

## How this maps to the playbook

1. Diagnose: step 4 is the per-partition size check.
2. Cause: step 3 uses a dominant key (`tenant-vip`).
3. Fix: step 5 uses a high-cardinality key (`order-id`), which is Fix A in the playbook.
