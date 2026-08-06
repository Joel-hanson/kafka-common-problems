# Stuck replicas

A partition keeps a leader but one or more followers never rejoin the ISR. Replication for that partition stalls while the rest of the topic (and often the rest of the cluster) looks fine.

| Field | Value |
| --- | --- |
| Severity | High when it hits busy user partitions or `__consumer_offsets` / `__transaction_state` |
| Typical surface | Brokers (ISR, replica fetchers), consumers (join / commit), transactional producers |
| Related configs | Replication factor, `min.insync.replicas`, `unclean.leader.election.enable`, on-disk `leader-epoch-checkpoint` |

## Symptoms

- `kafka-topics --describe` shows Leader set, Replicas listing several brokers, and ISR smaller than that set for longer than a brief failover blip
- Sibling partitions on the same topic often stay fully in sync; the damage is usually one `topic-partition`
- Broker logs for that partition show truncate, a non-monotonic high watermark warning, then `UnexpectedAppendOffsetException` and the replica fetcher marking the partition failed
- Only some consumer groups or transactional workloads fail, while unrelated traffic keeps moving
- Incidents often line up with upgrades, forced kills, or messy multi-broker restarts, but a hard kill by itself does not always leave you stuck

## Why it happens

Followers do not only copy bytes. They reconcile with the leader using log data and leader-epoch metadata stored in each replica's `leader-epoch-checkpoint` file.

When that file on one broker disagrees with reality (wrong epoch-to-offset map after a bad restart, disk edit, or similar divergence), the ReplicaFetcher can truncate the local log to a bogus offset. The high watermark jumps backward. The next fetch from the leader then fails with `UnexpectedAppendOffsetException` because the leader still sends batches that start earlier than the follower's next offset. The fetcher marks the partition failed and stops making progress. ISR stays shrunk until that broker is restarted or its local partition directory is rebuilt from a healthy peer.

`cleanup.policy=compact` is not required for this failure. The same chain shows up on delete-policy topics. Internal compacted topics show up often in incident reports because they are always present and painful when broken, not because the mechanism is compact-only.

A mass broker kill can produce short-lived under-replication that clears as fetchers catch up. Sustained stuck replicas need lasting on-disk divergence (for example a bad epoch checkpoint), not only an unclean process exit.

## Diagnosis

Work these in order. Stop when the root cause is clear. Prefer read-only collection until you have classified the incident. Copy partition directories before you delete anything.

1. Confirm sustained under-replication, not a blip
   - Run `kafka-topics --describe` and `--under-replicated-partitions`.
   - Note leader, replicas, and ISR for each stuck partition.
   - Pattern: one partition stuck for minutes; siblings on the same topic often full ISR.
   - Cluster-wide URP that clears quickly after a rolling restart is usually catch-up, not this problem.

2. Map client impact
   - Which consumer groups, producers, or transactional apps fail? Which do not?
   - If only some groups fail, check whether they share a `__consumer_offsets` partition (group id hash). That is a clue about blast radius, not proof of root cause.
   - Healthy produce/consume on other partitions of the same topic supports a replica-local failure.

3. Read broker logs for one stuck `topic-partition`
   - Filter on the exact topic and partition id.
   - Look for truncate, non-monotonic high watermark, `UnexpectedAppendOffsetException`, then "marked as failed".
   - One failure then silence usually means a dead fetcher. A continuous truncate loop is a different story; treat it separately.

4. Compare on-disk replicas
   - On every broker that holds a replica, inspect that partition directory (after a consistent copy if you can afford a short stop).
   - Diff `leader-epoch-checkpoint`, `.log` sizes, and producer snapshots.
   - Bad picture: failed broker has a near-empty `.log` and an odd snapshot or checkpoint; peers still hold a full log.
   - Matching epoch files and matching log sizes point somewhere else (network, ACL, disk full, controller issues).

5. Classify before you change disks
   - Healthy copies still in ISR: recover the missing replica only (Fix A or B).
   - At least one replica still has a readable log, but metadata / other copies are junk: Fix D (partial salvage), then rebuild peers from that copy.
   - Every replica empty or unreadable, no backup: Fix C (rebuild empty) or restore from DR.
   - Cleaner / compaction-only symptoms (log growth, cleaner thread death) without this fetch error chain need a different playbook.

## Fixes

Match the fix to the classification. Prefer the smallest change that restores a full ISR without discarding healthy replicas. Fixes A, B, and D were exercised in the lab. Fix C is the empty-rebuild last resort when nothing readable remains.

Set these for the examples below (lab). In production, swap bootstrap, broker names, and the log dir path for your install.

```bash
BS=kafka-1:19092          # lab in-container bootstrap; host: localhost:29092
TOPIC=events-delete
PART=0
TP=${TOPIC}-${PART}       # directory name on disk
BAD=3                     # broker id missing from ISR
```

### Fix A: restart the failed broker

When logs show the fetcher marked failed and peers in the ISR still hold good data.

1. Confirm who is out of ISR and that peers still have data:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BS" --describe --topic "$TOPIC"

for b in 1 2 3; do
  echo "===== broker $b ====="
  docker exec "kafka-$b" sh -c "wc -c /var/lib/kafka/data/${TP}/*.log 2>/dev/null || echo MISSING"
done
```

2. Restart only the failed broker (leave healthy ISR members up):

```bash
docker restart "kafka-${BAD}"
```

In Kubernetes / OpenShift, restart that one pod (example): `kubectl delete pod <cluster>-kafka-${BAD}` (or your platform's equivalent). Do not roll the whole StatefulSet yet.

3. Wait until the broker is accepting requests, then watch ISR:

```bash
until docker exec "kafka-${BAD}" /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server "kafka-${BAD}:19092" >/dev/null 2>&1; do sleep 2; done

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BS" --describe --topic "$TOPIC" | grep "Partition: $PART"
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BS" --describe --under-replicated-partitions
```

4. Confirm the recovered broker's log caught up and produce a quick smoke message (see Verify).

Restart clears the failed fetcher state. An empty local log can re-fetch from offset 0 if the leader still has the data.

### Fix B: wipe only the bad replica's partition directory

When the local directory is clearly junk (empty log, nonsense checkpoint) or Fix A did not bring the replica back.

1. Record leader / ISR / log sizes (same commands as Fix A step 1). Archive if you can.

2. Stop only the failed broker. Leave brokers that still hold healthy ISR members running:

```bash
docker stop "kafka-${BAD}"
```

3. Delete that broker's directory for the stuck partition only:

```bash
# Lab (official image log dir)
docker run --rm --volumes-from "kafka-${BAD}" alpine \
  rm -rf "/var/lib/kafka/data/${TP}"

# Confirm it is gone
docker run --rm --volumes-from "kafka-${BAD}" alpine \
  ls "/var/lib/kafka/data/${TP}" 2>&1 || echo "dir removed"
```

Production paths often look like `/var/lib/kafka/data/kafka-log<broker-id>/${TP}` (Strimzi / Event Streams). Adjust the path; delete only that partition dir.

4. Start the broker again:

```bash
docker start "kafka-${BAD}"
```

5. Watch catch-up:

```bash
for t in 15 30 60; do
  sleep 15
  echo "=== +${t}s ==="
  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BS" --describe --topic "$TOPIC" | grep "Partition: $PART"
  for b in 1 2 3; do
    echo -n "b$b "; docker exec "kafka-$b" sh -c \
      "wc -c /var/lib/kafka/data/${TP}/*.log 2>/dev/null || echo missing"
  done
done
```

Do not delete the leader or other healthy followers as the first move when ISR already contains good copies.

### Fix C: rebuild empty when nothing readable remains

When every replica's log is empty or undumpable, and you have no backup or DR copy. This accepts data loss for that partition.

1. Plan an outage window. Identify blast radius (consumer groups hashing to a `__consumer_offsets` partition, transactional apps, etc.).

2. Pause anything that will recreate pods mid-delete (scale the cluster operator to 0, or equivalent).

3. Note current leader and followers from describe. Delete the partition directory on **followers first**, then the **leader**, one broker at a time:

```bash
# Pattern (lab). Repeat per broker in follower → leader order.
docker stop "kafka-${BAD}"
docker run --rm --volumes-from "kafka-${BAD}" alpine \
  rm -rf "/var/lib/kafka/data/${TP}"
# leave stopped until all listed replicas are wiped, or follow your platform runbook
```

4. Bring brokers back (or scale the operator up) so Kafka recreates empty dirs and forms a new ISR.

5. Reset offsets or accept rewind for anything that lived only on that partition:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server "$BS" --group <group> --topic "$TOPIC" \
  --reset-offsets --to-earliest --execute
```

Use Fix C only when Fix D has nothing to salvage.

### Fix D: partial salvage (lab-validated)

When other replicas are gone or empty, but one broker still has a non-empty `.log` you can keep. You recover whatever that disk still holds.

1. Archive every replica's partition dir before you change anything (you will need it if a bad step truncates the good log).

2. Identify the broker with the largest dumpable log. Call it `GOOD` (example: `3`).

3. Reassign the partition so that broker is the **only** replica. That blocks empty peers from becoming leader and wiping the good copy:

```bash
GOOD=3
cat > /tmp/reassign-shrink.json <<EOF
{"version":1,"partitions":[{"topic":"${TOPIC}","partition":${PART},"replicas":[${GOOD}],"log_dirs":["any"]}]}
EOF
docker cp /tmp/reassign-shrink.json kafka-1:/tmp/reassign-shrink.json
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server "$BS" --reassignment-json-file /tmp/reassign-shrink.json --execute
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server "$BS" --reassignment-json-file /tmp/reassign-shrink.json --verify

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BS" --describe --topic "$TOPIC" | grep "Partition: $PART"
# Expect: Replicas: ${GOOD}   Isr: ${GOOD}
```

4. If `leader-epoch-checkpoint` on that broker is wrong, repair metadata only (keep the `.log`):

```bash
docker-compose stop   # or stop just enough brokers for a consistent file edit in your environment

# Restore a known-good checkpoint from your archive (example)
docker run --rm --volumes-from "kafka-${GOOD}" \
  -v "$PWD/archives/<run>/D3-pre-partial-salvage/broker-${GOOD}:/good:ro" alpine \
  cp /good/leader-epoch-checkpoint /var/lib/kafka/data/${TP}/leader-epoch-checkpoint

docker-compose start
```

5. Confirm the salvaged log size / end offset still match the archive:

```bash
docker exec "kafka-${GOOD}" wc -c /var/lib/kafka/data/${TP}/*.log
docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server "$BS" --topic "$TOPIC" --time -1
```

6. Expand replication again so other brokers rebuild from the salvaged leader:

```bash
cat > /tmp/reassign-expand.json <<EOF
{"version":1,"partitions":[{"topic":"${TOPIC}","partition":${PART},"replicas":[3,1,2],"log_dirs":["any","any","any"]}]}
EOF
docker cp /tmp/reassign-expand.json kafka-1:/tmp/reassign-expand.json
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server "$BS" --reassignment-json-file /tmp/reassign-expand.json --execute

# Watch followers catch up to the same byte size as GOOD
for b in 1 2 3; do
  echo -n "b$b "; docker exec "kafka-$b" sh -c \
    "wc -c /var/lib/kafka/data/${TP}/*.log 2>/dev/null || echo missing"
done
```

7. Produce smoke and record the salvaged end offset for application owners.

Hazard: starting with empty replicas alongside a good log (without step 3) let an empty broker become leader and truncated the good log to zero on every replica in our lab. Never let empty competitors lead until the salvaged copy is sole replica (or otherwise protected).

### While you fix

- Keep under-replicated and ISR charts open.
- Avoid rolling the whole cluster unless you need it; prefer acting on the failed broker.
- If an orchestrator will recreate pods or rewrite volumes mid-delete, pause it for the window so it does not race your recovery.
- For Fix D, keep the salvaged leader up while followers rebuild; never let an empty replica become leader first.

## Verify

- Under-replicated list is empty for the stuck partition and stays empty across a later broker bounce
- The recovered broker's partition log size matches peers (for Fix D: matches the salvaged source you kept)
- Previously failing consumers can join and commit; producers (including transactional ones) succeed against that partition
- Logs for that `topic-partition` are quiet: no new UnexpectedAppendOffset or "marked as failed" spam
- After Fix D: record the salvaged end offset; tell owners if anything may still be missing versus pre-incident state

Watch through the next maintenance or traffic peak. A problem that only returns after the next unclean multi-broker restart still counts as unresolved process risk even if ISR looks healthy today.

## Recreate on your laptop

Three-broker KRaft Compose lab, disk archives, induce via a bad `leader-epoch-checkpoint`, and Fix A/B walkthrough: [`demo/README.md`](demo/README.md).

Summary: start three brokers, produce to delete and compact topics, rewrite one replica's epoch checkpoint, confirm sustained under-replication and the fetch error chain, then recover with a broker restart or by deleting only that replica's partition directory.

Session notes, negative trials, debugging tips, and copy/paste commands: [`INVESTIGATION.md`](INVESTIGATION.md) (jump to "Commands we used constantly").

## Related problems

- [Hot partitions](../hot-partitions/): also "one partition looks wrong," but from traffic skew rather than ISR / replication failure
