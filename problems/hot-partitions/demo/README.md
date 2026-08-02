# Hot partitions demo

Recreate a Kafka hot-partition scenario on your laptop with Docker Compose. No app code — produce skewed keys with the broker CLI, inspect per-partition offsets, then produce with balanced keys and compare.

## Prerequisites

- Docker
- Docker Compose (`docker compose` plugin **or** standalone `docker-compose`)
- ~1 GB free RAM for the broker container

## Quick start

```bash
cd problems/hot-partitions/demo
chmod +x scripts/*.sh

./scripts/up.sh                 # start broker + create topic `orders` (6 partitions)
./scripts/produce-hot.sh        # ~90% of records share key `tenant-vip`
./scripts/show-partition-sizes.sh
```

You should see **one partition** with an end offset near 9000 and the others sharing the remaining ~1000.

Then produce a balanced topic and compare:

```bash
./scripts/produce-balanced.sh   # writes to `orders-balanced` with unique order-id keys
```

Offsets on `orders-balanced` should sit close together across all six partitions.

Tear down when finished:

```bash
./scripts/down.sh
```

## What each script does

| Script | Purpose |
| --- | --- |
| `up.sh` | `docker compose up`, wait for health, create `orders` |
| `produce-hot.sh` | 10 000 records, 90% keyed `tenant-vip` → hot partition |
| `produce-balanced.sh` | 10 000 records keyed by unique `order-N` → even spread |
| `show-partition-sizes.sh` | Print end offsets + topic describe |
| `reset-topic.sh` | Delete/recreate `orders` for a clean re-run |
| `down.sh` | Stop the stack and remove volumes |

## Knobs

Override via env vars:

```bash
TOTAL=20000 HOT_PCT=95 HOT_KEY=tenant-vip ./scripts/produce-hot.sh
TOPIC=orders PARTITIONS=6 ./scripts/reset-topic.sh
```

## How this maps to the playbook

1. **Diagnose** — `show-partition-sizes.sh` is the per-partition lag/size check (step 1 in the playbook).
2. **Cause** — `produce-hot.sh` uses a low-cardinality / dominant key (`tenant-vip`).
3. **Fix** — `produce-balanced.sh` uses a high-cardinality key (`order-id`), which is Fix A in the playbook.

Salting / isolation are deliberate trade-offs for a single huge entity; this demo focuses on the most common fix: pick a better key.

## Broker endpoint

- From your host: `localhost:9092`
- From other Compose services: `broker:29092`
