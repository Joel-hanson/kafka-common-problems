# Stuck replicas: investigation lab

Three-broker KRaft cluster with durable volumes. Plain Compose + broker CLI — no wrapper scripts.

Take snapshots (healthy → broken → recovered), compare replica disks, and decide what broke before you reach for anyone else's runbook. What actually stuck in our run was a bad `leader-epoch-checkpoint` on one broker; mass kills alone healed on their own. Full write-up and a copy/paste command kit: [`../INVESTIGATION.md`](../INVESTIGATION.md#commands-we-used-constantly-copypaste).

## Prerequisites

- Docker with Compose (`docker-compose` or `docker compose`)
- About 2 GB free RAM
- `jq` optional

Commands below use `docker-compose`. Swap in `docker compose` if that's what you have.


## Layout

| Path | Role |
| --- | --- |
| `docker-compose.yml` | brokers `kafka-1` / `kafka-2` / `kafka-3` |
| `archives/` | your snapshots (gitignored); create per run |
| Log dirs inside containers | `/var/lib/kafka/data` |

Bootstrap:

- From the **host**: `localhost:29092` (or `39092` / `49092`)
- Inside **`docker exec`**: `kafka-1:19092` (inter-broker listener). Do not use `localhost:9092` inside containers — metadata advertises host ports that are unreachable from inside the network.

Internal topics use replication factor **3** and a small offsets partition count (**5**) so archives stay small.

## 1. Start the cluster

```bash
cd problems/stuck-replicas/demo
docker-compose up -d
```

Wait until this succeeds:

```bash
docker exec kafka-1 \
  /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka-1:19092
```

Confirm three brokers:

```bash
docker exec kafka-1 \
  /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092
```

## 2. Create workload topics (delete + compact)

Do **not** assume the failure is only on internal topics. Create both:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 \
  --create --topic events-delete \
  --partitions 3 --replication-factor 3

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 \
  --create --topic events-compact \
  --partitions 3 --replication-factor 3 \
  --config cleanup.policy=compact \
  --config segment.bytes=1048576 \
  --config min.cleanable.dirty.ratio=0.01
```

## 3. Generate activity

Produce to both topics and run a consumer group that commits offsets:

```bash
# Produce keyed data (compact topic needs keys)
{
  for i in $(seq 1 2000); do
    k=$((i % 50))
    printf 'key-%s:value-%s-%s\n' "$k" "$k" "$i"
  done
} | docker exec -i kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:19092 \
  --topic events-compact \
  --property parse.key=true \
  --property key.separator=:

{
  for i in $(seq 1 2000); do
    printf 'key-%s:msg-%s\n' "$i" "$i"
  done
} | docker exec -i kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:19092 \
  --topic events-delete \
  --property parse.key=true \
  --property key.separator=:
```

Consumer group (commits offsets → writes `__consumer_offsets`):

```bash
docker exec -d kafka-1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:19092 \
  --topic events-delete \
  --group lab-group-a \
  --from-beginning \
  --timeout-ms 15000
```

Wait ~15s, then inspect:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:19092 \
  --describe --group lab-group-a
```

## 4. Cluster inventory (before any archive)

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 --describe

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 --describe --under-replicated-partitions
```

List every topic, including internals. Note leader / replicas / ISR. Note `cleanup.policy` where set.

## 5. Snapshot protocol (do this at T0, T1, T2)

Pick a run id and label:

```bash
RUN=run-$(date +%Y%m%d-%H%M%S)
LABEL=T0-healthy   # later: T1-broken, T2-after-recovery
BASE="archives/${RUN}/${LABEL}"
mkdir -p "$BASE"/{cluster,dumps,logs,broker-1,broker-2,broker-3}
```

### 5a. Cluster text

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 --describe \
  > "$BASE/cluster/topics-describe.txt"

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 --describe --under-replicated-partitions \
  > "$BASE/cluster/under-replicated.txt"

docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:19092 --describe --group lab-group-a \
  > "$BASE/cluster/group-lab-group-a.txt" || true
```

### 5b. Consistent partition copies

Stop the cluster so files are quiescent, copy data dirs, then start again:

```bash
docker-compose stop

for b in 1 2 3; do
  docker cp "kafka-${b}:/var/lib/kafka/data/." "$BASE/broker-${b}/"
done

docker-compose start
```

Wait for brokers to accept connections again (repeat the api-versions check from step 1).

### 5c. Checksums + notes

```bash
( cd "$BASE" && find broker-1 broker-2 broker-3 -type f | sort | xargs shasum -a 256 ) > "$BASE/checksums.txt"

cat > "archives/${RUN}/NOTES.md" <<EOF
# ${RUN}

## Timeline
- T0: healthy baseline after produce + consume
- Perturb: (fill in what you did)
- T1: (fill in first broken signal)
- Recovery: (fill in what you tried)
- T2: (fill in outcome)

## Hypotheses
1.
2.

## Observations (no ticket language yet)
-
EOF
```

Repeat section 5 with `LABEL=T1-broken` and `LABEL=T2-after-recovery` at those points. Append to the same `NOTES.md`.

## 6. Diff a stuck partition across brokers

After T1, pick **one** stuck `topic-partition` from describe (example name `events-compact-0` — use whatever is actually stuck).

```bash
TP=events-compact-0   # change me
T1=archives/${RUN}/T1-broken

for b in 1 2 3; do
  echo "===== broker-${b} ====="
  ls -la "$T1/broker-${b}/${TP}" 2>/dev/null || echo "(missing)"
  echo "--- leader-epoch-checkpoint ---"
  cat "$T1/broker-${b}/${TP}/leader-epoch-checkpoint" 2>/dev/null || true
done
```

Dump segments from the archive (read-only), using any running broker image:

```bash
# Copy one archived partition into a temp path the container can read, or dump via docker run
docker run --rm -v "$(pwd)/${T1}/broker-1/${TP}:/data:ro" apache/kafka:3.9.1 \
  /opt/kafka/bin/kafka-dump-log.sh --files /data/*.log --print-data-log \
  > "$T1/dumps/broker-1-${TP}.txt" || true
```

Repeat for `broker-2` and `broker-3`. Compare offsets, epochs, and checksums — not vibes.

Also dump broker logs from the induce window:

```bash
docker logs kafka-1 --since 30m 2>&1 | grep -E "${TP}|Truncating|Non-monotonic|ReplicaFetcher|ProducerState" \
  > "$T1/logs/kafka-1.snippet.txt" || true
# same for kafka-2, kafka-3
```

## 7. Perturbations (one cause per run)

Start from a fresh T0 each time. Change **one** thing. Record it in `NOTES.md`.

### A. Simultaneous hard kill

```bash
docker kill kafka-1 kafka-2 kafka-3
docker-compose start
```

Watch under-replicated partitions for several minutes. Archive T1 if something sticks.

### B. Kill while producing

In one terminal, loop produces to `events-compact`. In another:

```bash
docker kill kafka-1 kafka-2 kafka-3
docker-compose start
```

### C. Empty one broker’s data (dangerous; lab only)

```bash
docker-compose stop
# Example: wipe only kafka-1 volume data — document exactly what you removed
docker run --rm -v demo_kafka-1-data:/data alpine rm -rf /data/*
docker-compose start
```

Volume names may differ (`docker volume ls | grep kafka`). Prefer wiping a **single partition directory** inside one broker after `docker-compose stop` if you want a narrower experiment:

```bash
docker-compose stop
docker run --rm -v demo_kafka-1-data:/data alpine \
  rm -rf /data/events-compact-0
docker-compose start
```

### D. Surgical file edit (lab only)

After `docker-compose stop`, alter **one** file on **one** broker for one partition (for example `leader-epoch-checkpoint`), leave the other brokers alone, start, archive T1, diff.

## 8. Questions to answer from archives

Write answers in `NOTES.md` before any recovery attempt:

1. Which topics/partitions are stuck? cleanup.policy?
2. Do end offsets match across replicas?
3. Do `leader-epoch-checkpoint` files match?
4. Are followers truncating to the leader repeatedly?
5. Which clients fail? Do they share an offsets partition?
6. Is this the same pattern on `events-delete` and `events-compact`?

Only after that: try a recovery of your own design, archive T2, and note what healed vs what made it worse.

In our run, with healthy ISR members still present:

- Restart the failed broker, or
- Stop it, delete only its partition dir, start it again

We did not need to wipe the leader or the other replicas. See [`../INVESTIGATION.md`](../INVESTIGATION.md).

## 9. Tear down

```bash
docker-compose down -v
# keep archives/ if you want history; delete when done
# rm -rf archives/run-*
```

## How this maps to the playbook

1. Inventory → steps 4 and 6  
2. Client impact → step 3 group describe + T1 group file  
3. Logs → step 6 snippets  
4. On-disk compare → step 5 archives + step 6 diffs  
5. Classify → `NOTES.md` questions in step 8  
6. Fix A/B in the playbook → restart failed broker / wipe that replica's dir  

