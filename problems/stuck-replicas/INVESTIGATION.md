# Investigation log: stuck replicas lab

Run `run-20260806-110124` · 2026-08-06 · three-broker Kafka 3.9.1 KRaft lab under `demo/`.

We wanted a local story we could trust: can you get a partition stuck under-replicated, see why from logs and disk, and fix it — without starting from someone else's runbook? Ticket notes stayed on the shelf until we had our own picture.

Raw scratch notes: [`demo/archives/run-20260806-110124/NOTES.md`](demo/archives/run-20260806-110124/NOTES.md).

---

## Ground rules

1. Don't assume it's only `__consumer_offsets` / `__transaction_state`.
2. Don't assume compact retention is required.
3. Archive cluster + disk so you can compare healthy vs broken vs recovered.
4. Change one thing per trial.

---

## Lab

| Piece | Choice |
| --- | --- |
| Brokers | `kafka-1` … `kafka-3` (broker + controller) |
| Image | `apache/kafka:3.9.1` |
| Data | named volumes → `/var/lib/kafka/data` |
| Offsets topic | RF=3, 5 partitions |
| Workload | `events-delete`, `events-compact` |
| Consumer | `lab-group-a` on `events-delete` |
| From host | `localhost:29092` |
| Inside containers | `kafka-1:19092` — `localhost:9092` lies to you (advertised host ports) |

This machine needed `docker-compose`, not `docker compose`.

### What we archived

Describe (+ under-replicated), group state, full data dirs after a clean stop, checksums, log greps, occasional `kafka-dump-log` on copies.

Useful folders under `archives/run-20260806-110124/`:

| Folder | What it is |
| --- | --- |
| `T0-healthy/` | Baseline |
| `T1-after-A-healed/` / `T1-after-B-healed/` | After kill trials (both healed) |
| `D-pre-edit/` / `T1-broken-D/` | Compact-topic LEC edit |
| `D2-pre-edit-delete/` / `T1-broken-D2-delete/` | Delete-topic LEC edit |
| `R-pre-recovery/` / `R1-after-restart-broker3/` | Restart recovery |
| `R2-pre-wipe/` / `R2-after-wipe-replica/` | Wipe-replica recovery |

---

## Baseline (T0)

Brought the cluster up, created both topics (3 partitions, RF=3), produced ~2000 keyed records each, ran `lab-group-a` until lag hit zero. Full ISR everywhere. Took a full T0 snapshot.

First footgun: tools inside the container must talk to `kafka-1:19092`. Bootstrap on `localhost:9092` gets metadata pointing at host ports the container can't reach.

---

## False starts

### A — kill everything

Idea: unclean shutdown of all three brokers leaves permanent stuck replicas.

We ran `docker kill` on all three, started them again, watched for three minutes.

For about 17 seconds some partitions missed broker 1 in the ISR. By +30s everything was full again and stayed that way. Offsets for `lab-group-a` were fine.

So: quiet mass kill → brief URP, not a stuck repro.

### B — kill under load

Same kill, but with producers hammering both topics and a consumer committing offsets.

Warmed ~15s, killed all three mid-flight, watched two minutes.

Under-replicated list was already empty on the first describe. Truncates showed up (normal catch-up). Zero Non-monotonic lines. Group had lag because the consumer died — not because the coordinator ate the offsets.

Kill-under-load didn't stick either. On this lab, "restart harder" isn't how you get the failure. We needed a controlled lie on disk.

---

## What actually stuck

### D — edit `leader-epoch-checkpoint` on `events-compact-0`

Idea: one replica's epoch map disagreeing with the others is enough.

Stopped the cluster. Leader of `events-compact-0` was broker 1. Rewrote only its checkpoint:

- Before: `0@0`, `5@480`, `9@1680`
- After: `0@0`, `3@50` (the lie)
- Brokers 2 and 3: untouched

Started up and watched.

For three minutes that partition sat at Leader=3, Isr `{3,2}` — broker 1 never came back. Partitions 1 and 2 on the same topic stayed healthy.

Kafka-1's logs told the story cleanly:

1. Loads log ending at 1680, becomes follower of 3
2. Truncates to offset 50 (epoch negotiation against the bad local file)
3. Non-monotonic high watermark 1680 → 50
4. `UnexpectedAppendOffsetException` — leader sends from 0, local next offset is 50
5. Fetcher marks the partition failed and goes quiet (not a hot loop)
6. Disk: broker 1's `.log` is empty; 2 and 3 still hold ~47KB

One caveat: when we stopped everything later to archive disks, broker 1 rejoined after the bounce. So this was a sustained failed-fetcher while running, not immortal corruption. The archive still caught the empty-log moment.

Mechanism in one line: bad epoch checkpoint → bad truncate → UnexpectedAppendOffset → failed fetcher → stuck URP.

And it was a normal user compact topic — not an internal one.

### D2 — same edit on `events-delete-0`

Same lie on the leader of a delete-policy topic (broker 3). Left it stuck for three minutes on purpose (no bounce).

Same shape: Isr missing the edited broker, same error chain, empty log on the bad replica, healthy peers.

Compact is not required. Whatever puts a bad epoch file on disk in production is a separate question; the thing that *keeps* you stuck here is replication/epoch handling.

---

## Recovery

Both recoveries assumed the same picture: ISR still has good copies; the odd broker out has an empty or junk local dir and a dead fetcher.

### R1 — restart the failed broker

`docker restart kafka-3` only.

Back in full ISR in about 15 seconds. Produce smoke passed.

Why: restart dropped the failed fetcher state, threw away the orphan snapshot@50, loaded an empty log at offset 0, and could append from the leader again.

### R2 — wipe only that replica's partition dir

Re-induced the stuck state, then:

1. Stop kafka-3 only
2. `rm -rf .../events-delete-0` on that broker
3. Start kafka-3

Also healed in ~15s. Dir came back, log matched peers (~60KB), produces worked.

Neither path needed deleting the leader or the healthy followers. The nuclear "wipe this partition on every broker, followers first" move is for when you don't have a good copy left — we didn't need it here.

| Move | When it fits |
| --- | --- |
| Restart failed broker | Fetcher failed; peers still good |
| Wipe that replica's dir | Local dir is garbage, or restart wasn't enough |
| Wipe everywhere | No healthy copy / sole ISR is corrupt — different incident |

---

## Scorecard

**Didn't hold (here):** mass kill alone, kill-under-load alone, "only internal topics", "only compact topics".

**Did hold:** one bad `leader-epoch-checkpoint` can park a single partition out of ISR; logs + log-byte diffs explain it; with healthy ISR members, restart or wipe-that-replica fixes it fast; partial salvage works if you protect a readable log (sole replica + repair LEC + expand RF), and empty peers can destroy that log if they become leader first.

**Still curious about:** hours-long wait without restart; natural path from unclean shutdown to a bad LEC (not hand-edited); client pain when the stuck shard is `__consumer_offsets`; Fix C empty-rebuild drill end-to-end; salvaging among multiple non-empty but CRC-corrupt segments (we salvaged a sole readable log + bad LEC, not competing half-corrupt logs).

---

## Trial D3 — partial salvage (ran in lab)

### Failed first attempt
Emptied partition dirs on brokers 1 and 2, left a full log on broker 3 with a bad LEC, started all three for quorum. An empty broker became leader and truncated the good log to 0 on every replica. Needed `D3-pre-partial-salvage/` to restore.

### What worked
1. Reassign `events-delete-0` to sole replica `[3]` while the log was healthy (65614 bytes, end offset 2593).
2. Induce bad LEC on broker 3 only; keep the `.log`.
3. Stop; restore good `leader-epoch-checkpoint` from the pre-induce archive; do not replace the log.
4. Start; confirm Leader=3, log still 65614, offsets 2593.
5. Reassign back to `[3,1,2]`; followers caught up to 65614; ISR full; produce smoke advanced partition 0 to 2607.

So Fix D is: protect the readable copy (sole replica / no empty leader), repair its metadata, rebuild others from it. Partial means you only get what that disk still held.

---

## Tips and tricks (what actually helped)

This is the debugging kit we ended up relying on. Most of it is useful beyond this one failure mode.

### Commands we used constantly (copy/paste)

Lab defaults from this demo. Inside containers use `kafka-1:19092`. On the host use `localhost:29092`. Swap topic/partition/broker ids for your case.

```bash
cd problems/stuck-replicas/demo
BS=kafka-1:19092          # in-container bootstrap
TP=events-delete-0        # stuck topic-partition dir name
TOPIC=events-delete
PART=0
BAD=3                     # broker id missing from ISR
```

**Cluster up / ready**

```bash
docker-compose up -d
docker exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BS"
docker-compose ps
```

**Inventory (start here on every incident)**

```bash
# Under-replicated only
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BS" --describe --under-replicated-partitions

# Full picture for one topic
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BS" --describe --topic "$TOPIC"

# All topics (including internals)
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BS" --describe

# End offsets
docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server "$BS" --topic "$TOPIC" --time -1

# Consumer group
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server "$BS" --describe --group lab-group-a
```

**Watch whether URP is stuck or just catching up**

```bash
for t in 30 60 120 180; do
  sleep 30
  echo "=== +${t}s ==="
  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BS" --describe --under-replicated-partitions
  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BS" --describe --topic "$TOPIC" | grep "Partition: $PART"
done
```

**Broker logs for one partition**

```bash
PAT='Truncating|Non-monotonic|UnexpectedAppend|marked as failed|ReplicaFetcher|ProducerState'
for b in 1 2 3; do
  echo "===== kafka-$b ====="
  docker logs "kafka-$b" --since 15m 2>&1 | grep -E "${TP}|$PAT" | tail -40
done

# Counts (flat over time => failed fetcher went idle, not a hot loop)
for b in 1 2 3; do
  echo -n "kafka-$b Non-monotonic="; docker logs "kafka-$b" --since 15m 2>&1 | grep -c Non-monotonic || true
  echo -n "kafka-$b Truncating ${TP}="; docker logs "kafka-$b" --since 15m 2>&1 | grep -c "Truncating partition ${TP}" || true
done
```

**Compare on-disk replicas (live)**

```bash
for b in 1 2 3; do
  echo "===== broker $b ====="
  docker exec "kafka-$b" sh -c "ls -la /var/lib/kafka/data/${TP}/ 2>/dev/null || echo MISSING"
  docker exec "kafka-$b" sh -c "wc -c /var/lib/kafka/data/${TP}/*.log 2>/dev/null || true"
  docker exec "kafka-$b" sh -c "echo LEC:; cat /var/lib/kafka/data/${TP}/leader-epoch-checkpoint 2>/dev/null || true"
done
```

**Archive snapshot (quiescent disks)**

```bash
RUN=run-$(date +%Y%m%d-%H%M%S)
LABEL=T0-healthy   # or T1-broken / R-pre-recovery / ...
BASE="archives/${RUN}/${LABEL}"
mkdir -p "$BASE"/{cluster,dumps,logs,broker-1,broker-2,broker-3}

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BS" --describe > "$BASE/cluster/topics-describe.txt"
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BS" --describe --under-replicated-partitions \
  > "$BASE/cluster/under-replicated.txt" || true

docker-compose stop
for b in 1 2 3; do
  docker cp "kafka-${b}:/var/lib/kafka/data/." "$BASE/broker-${b}/"
done
docker-compose start

# wait until ready again
until docker exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BS" >/dev/null 2>&1; do sleep 2; done

( cd "$BASE" && find broker-1 broker-2 broker-3 -type f | sort | xargs shasum -a 256 ) > "$BASE/checksums.txt"

for b in 1 2 3; do
  docker logs "kafka-$b" --since 30m 2>&1 | grep -E "${TP}|$PAT" \
    > "$BASE/logs/kafka-${b}.snippet.txt" || true
done
```

**Dump an archived segment (read-only)**

```bash
ARCH=archives/${RUN}/T1-broken-D/broker-1/${TP}
docker run --rm -v "$(pwd)/${ARCH}:/data:ro" apache/kafka:3.9.1 \
  /opt/kafka/bin/kafka-dump-log.sh --files /data/*.log --print-data-log | tail -50
```

**Fix A — restart failed broker**

```bash
docker restart "kafka-${BAD}"
# then re-run describe / URP / log-size loop above
```

**Fix B — wipe only that replica's partition dir**

```bash
docker stop "kafka-${BAD}"
docker run --rm --volumes-from "kafka-${BAD}" alpine \
  rm -rf "/var/lib/kafka/data/${TP}"
docker start "kafka-${BAD}"
# leave healthy ISR members running the whole time
```

**Fix D — protect readable copy, repair LEC, expand RF**

```bash
# 1) Shrink to sole replica on the broker that still has the good log (example: broker 3)
cat > /tmp/reassign-shrink.json <<EOF
{"version":1,"partitions":[{"topic":"${TOPIC}","partition":${PART},"replicas":[3],"log_dirs":["any"]}]}
EOF
docker cp /tmp/reassign-shrink.json kafka-1:/tmp/reassign-shrink.json
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server "$BS" --reassignment-json-file /tmp/reassign-shrink.json --execute
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server "$BS" --reassignment-json-file /tmp/reassign-shrink.json --verify

# 2) Stop; restore a known-good leader-epoch-checkpoint onto that broker (keep the .log)
docker-compose stop
# example: copy from your archive
# docker run --rm --volumes-from kafka-3 -v "$PWD/archives/.../broker-3:/good:ro" alpine \
#   cp /good/leader-epoch-checkpoint /var/lib/kafka/data/${TP}/leader-epoch-checkpoint

# 3) Start; confirm log size / offsets unchanged
docker-compose start

# 4) Expand RF again so peers rebuild from the salvaged leader
cat > /tmp/reassign-expand.json <<EOF
{"version":1,"partitions":[{"topic":"${TOPIC}","partition":${PART},"replicas":[3,1,2],"log_dirs":["any","any","any"]}]}
EOF
docker cp /tmp/reassign-expand.json kafka-1:/tmp/reassign-expand.json
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server "$BS" --reassignment-json-file /tmp/reassign-expand.json --execute
```

**Produce / consume smoke**

```bash
{
  for i in $(seq 1 50); do printf 'smoke-%s:payload-%s\n' "$i" "$i"; done
} | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server "$BS" --topic "$TOPIC" \
  --property parse.key=true --property key.separator=:

docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server "$BS" --topic "$TOPIC" --group lab-smoke \
  --from-beginning --timeout-ms 10000
```

**Tear down lab**

```bash
docker-compose down -v
# keep archives/run-* if you want the evidence
```

### Snapshot before you touch anything

1. **Take a healthy baseline (T0) while the cluster is fine.** Once you break something, you cannot reconstruct "what good looked like" from memory. We copied describe output, group state, and full data dirs before the first kill.

2. **Stop brokers (or at least the ones you copy) before `docker cp` / tar of log dirs.** Mid-write copies create fake corruption. Our protocol was: cluster text while up → `docker-compose stop` → `docker cp` each data dir → start → wait for API versions again.

3. **Use a fixed layout per run.** Something like:

   ```text
   archives/run-YYYYMMDD-HHMMSS/
     NOTES.md
     T0-healthy/{cluster,broker-1,broker-2,broker-3,dumps,logs,checksums.txt}
     T1-broken-.../
     R-pre-recovery/
   ```

   Same relative paths every time make `diff` and checksum compares trivial.

4. **Checksum the tree.** `find broker-1 broker-2 broker-3 -type f | sort | xargs shasum -a 256`. When you argue about whether two replicas match, hashes end the debate faster than eyeballing.

5. **Keep a NOTES.md with clock times.** Hypothesis, command, UTC timestamp, what describe said. Later you will not remember whether URP was at +17s or +3m.

6. **Archive the broken state before recovery.** We almost lost the empty-log evidence on broker 1 when a full bounce healed ISR. If you only snapshot after fix, the post-mortem is guesswork.

7. **Narrow copies when full data dirs are huge.** In production, archive only the stuck `topic-partition` dirs plus the parent checkpoint files (`leader-epoch-checkpoint` lives inside the partition dir; also grab `cleaner-offset-checkpoint` / `recovery-point-offset-checkpoint` from the log root if you suspect cleaner issues).

### Logging that tells a story

1. **Filter on the exact `topic-partition`.** Noise from other partitions drowns the signal. Grep `events-delete-0` (or whatever is stuck), not just `Truncating`.

2. **Useful patterns for this class of bug:**

   ```text
   Truncating partition <tp>
   Non-monotonic update of high watermark
   UnexpectedAppendOffsetException
   marked as failed
   ReplicaFetcher
   leader-epoch
   ProducerStateManager
   Loading producer state
   ```

3. **Time-box with `--since`.** `docker logs kafka-1 --since 10m` (or your platform equivalent) keeps the induce window small enough to read.

4. **Count, then sample.** We counted Non-monotonic / Truncating hits per broker, then pulled the first few lines. Flat counts over a 3-minute watch meant the fetcher had died once and gone idle, not a hot loop. That distinction changes the recovery story.

5. **Read the failed broker's log, not only the leader's.** The UnexpectedAppendOffset and "marked as failed" lines showed up on the replica that was out of ISR. Leader logs alone look almost healthy.

6. **Default logging was enough here.** We did not need TRACE. If you do crank verbosity, prefer `kafka.server.ReplicaFetcherThread` / log cleaner loggers for a short window, then turn them back down. Leaving TRACE on a busy cluster is its own incident.

7. **Save greps into the archive.** `logs/kafka-1.snippet.txt` next to the disk copy means you can reopen the case months later without the live cluster.

### Cluster inventory (before theories)

1. **List under-replicated partitions first.** Then full `--describe` for those topics. Write down Leader / Replicas / ISR for each stuck row.

2. **Check siblings on the same topic.** Partitions 1 and 2 full ISR while partition 0 stuck is a strong "local replica" signal, not "Kafka is on fire."

3. **Inventory every topic, including internals,** but do not stop there. We reproduced the same failure on a user delete topic. Starting from `__consumer_offsets` alone would have been a bias, not a method.

4. **Watch URP on a timer (+30s, +60s, +3m).** Transient catch-up clears. Sustained URP with a quiet fetcher is the stuck case. One describe right after restart will lie to you (Trial A looked scary at +17s and was fine by +30s).

5. **Map clients separately.** Which groups commit? Which fail? Lag after a killed consumer is not the same as coordinator inability to load offsets. We checked `kafka-consumer-groups --describe` before calling something a `__consumer_offsets` outage.

6. **End offsets help.** `kafka-get-offsets` (or describe) before/after a kill shows whether data moved. On disk, `wc -c` on the `.log` file across brokers is the blunt instrument that caught "broker 3 is empty, 1 and 2 still have 60KB."

### Reading replica disks

1. **`leader-epoch-checkpoint` format:** version line, entry count, then `epoch offset` pairs. Diff this file across all replicas for the stuck partition. The lie we introduced (`3 50` on one broker) was visible in a three-line cat.

2. **Log bytes and snapshots together.** Empty `.log` plus a snapshot named for offset 50 while peers have a full log and snapshot@2351 is the smoking gun picture.

3. **Prefer `kafka-dump-log` on archived copies,** not on a live mutative directory. Mount the archive read-only into a one-shot container if you need to decode segments.

4. **Who is out of ISR is who you operate on.** Describe said `Isr: 2,1` with replicas `3,1,2` → broker 3 is the patient. Restarting or wiping broker 2 (the leader) would have been the wrong surgery.

### Lab / Docker specifics that burned time

1. **Bootstrap inside containers:** use the inter-broker listener (`kafka-1:19092`). Host-advertised listeners (`localhost:29092`) are for clients on the host. Mixing them makes AdminClient spin on "Node may not be available" while the cluster is fine.

2. **Volume names depend on the Compose project directory.** From `demo/` we got `demo_kafka-1-data`, not `stuck-replicas_...`. Always `docker volume ls | grep kafka` before wipe experiments.

3. **`docker-compose stop` vs `docker kill`.** Stop is graceful-ish and good for consistent archives. Kill is the unclean-shutdown trial. Do not confuse them when writing NOTES.

4. **Keep `__consumer_offsets` partition count small in a lab** (we used 5) so archives and greps stay readable. Production defaults (50) are miserable to diff by hand.

### How to run the investigation without fooling yourself

1. **One change per trial.** Kill-all, then kill-under-load, then LEC edit. Mixing two causes in one run teaches nothing.

2. **Write the hypothesis before the command.** "I think mass kill sticks ISR" → run A → it healed → close that hypothesis. Negative results are data.

3. **Hold external runbooks until after classify.** We knew a "delete partition on every broker" story existed. Following it first would have skipped Fix A/B and destroyed the lesson about healthy ISR members.

4. **Recovery order we trust when peers are healthy:** confirm who is missing from ISR → compare disks → restart that broker → if still bad, wipe only that broker's partition dir → only then consider nuclear rebuild.

5. **Partial salvage when one readable log remains:** reassign to that broker alone (or otherwise keep empty peers from leading), repair its `leader-epoch-checkpoint`, then expand RF so others re-fetch. We lost data once by starting empty replicas alongside a good log. Archive before you try.

6. **After recovery, produce smoke and re-check URP.** ISR looking full is necessary; a quick produce/consume (or offset commit) confirms the partition is writable again.

### Translating to production

Same method, different shell:

- Describe / URP / consumer groups against a real bootstrap server.
- Logs from your collector (Loki, ELK, CloudWatch) with the same greps and a tight time window around the incident.
- Disk: `oc debug` / `kubectl debug` / SSH, then copy or inspect `/var/lib/kafka/data/.../<topic>-<p>/` (paths vary by image and JBOD mount).
- If an operator restarts pods while you delete dirs, pause or scale it down for the window (same idea as pausing Event Streams / Strimzi operators).
- Prefer Fix A/B when ISR still lists healthy brokers. The followers-then-leader wipe of every copy is for "no good replica left," not the default first step.

---

## If you write the blog

A shape that matches how the night actually went:

1. What operators see (URP that won't clear)
2. Lab + "archive before you touch disk," hold the ticket notes
3. A and B failing — honest negative results
4. One edited checkpoint and the error chain
5. Compact vs delete — same breakage
6. Recovery ladder: restart → wipe bad replica → nuclear only if nothing good remains
7. Takeaway: inventory every stuck partition, compare disks, don't delete healthy leaders first

Negative results aren't filler. They're why the induce step is believable.

See **Commands we used constantly** above for the full copy/paste set.

---

## Verdict

Killing all three brokers, even under load, only gave us a blip. Lying in one replica's `leader-epoch-checkpoint` stuck a partition on both compact and delete topics — truncate, Non-monotonic HWM, UnexpectedAppendOffset, failed fetcher. With good replicas still in the ISR, restarting the bad broker or deleting only its partition dir brought ISR back in seconds. First move: fix or wipe the bad copy. Not "delete the partition everywhere."
