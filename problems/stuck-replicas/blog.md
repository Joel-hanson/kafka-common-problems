# Stuck replicas, clean vs unclean startup, and what changed from Kafka 3.6 to 3.7+

## Can we write about something we cannot reproduce?

Yes — if we are clear about **what kind of claim** we are making.

| What we can say | What we cannot say |
|---|---|
| We saw this in production (Event Streams upgrades, Kafka **3.6.1 → 3.7.0**, ZooKeeper mode) | “Press these three buttons and you will hit it every time” |
| Logs, ISR state, and the leader’s `leader-epoch-checkpoint` matched a known failure shape | We know the exact millisecond of SIGKILL that created the bad file |
| The fix that worked: add the missing epoch on the **leader** | Our lab mass-kill alone recreates the production race (it did not) |
| Kafka source shows how clean vs unclean startup works, and what KRaft added | KRaft makes every checkpoint bug impossible |

So this post is: **observation + repair we trust**, plus **code-backed explanation** of recovery. It is not a flaky repro recipe.

---

## The problem in plain language

Kafka keeps several copies of each partition. One broker is the **leader**. The others are **followers** that copy from it. The copies that are fully caught up are the **ISR**.

Each message batch is stamped with a **leader epoch** (which “generation” of leader wrote it). The leader also keeps a small file next to the log:

`leader-epoch-checkpoint` = map of `epoch → starting offset`

**What went wrong:** that map on the **leader** was missing an epoch that still appeared in the log. Followers fetched, got told to truncate back, fetched again, truncated again — same offset forever. They never rejoined the ISR.

**What that felt like**
- Cluster mostly up; other partitions fine
- Apps using the stuck partition (often `__consumer_offsets` / `__transaction_state`) failing
- Operator upgrade stuck because that partition never finished replicating

**Fix that worked:** add the missing epoch line to the **leader’s** checkpoint (earliest offset with that epoch is enough), restart that broker, let followers catch up.

Restarting followers or deleting a follower’s directory does **not** fix this, because they keep copying from the same leader.

---

## What’s on disk (one partition, one broker)

For a topic partition like `events-compact-0`, each replica keeps a directory under the broker log dir (lab path: `/var/lib/kafka/data`):

```text
/var/lib/kafka/data/
├── .kafka_cleanshutdown          ← log-dir marker (clean stop; may hold broker epoch on KRaft)
├── recovery-point-offset-checkpoint
├── meta.properties                 ← broker / cluster id (KRaft)
├── __cluster_metadata-0/           ← KRaft only: controller metadata log
└── events-compact-0/               ← one partition replica
    ├── 00000000000000000000.log    ← message batches (each stamped with a leader epoch)
    ├── 00000000000000000000.index
    ├── 00000000000000000000.timeindex
    ├── leader-epoch-checkpoint     ← epoch → starting offset map (this story)
    ├── partition.metadata          ← topic id, etc.
    └── ...producer snapshots...
```

**`leader-epoch-checkpoint` file format** (what `cat` shows):

```text
0              ← version
3              ← number of entries that follow
5 1000         ← epoch 5 starts at offset 1000
6 1500         ← epoch 6 starts at offset 1500
7 1800         ← epoch 7 starts at offset 1800
```

**Same map as a timeline** (what the log “means”):

```text
offset:   1000 -------- 1500 -------- 1800 -------- 2200 (LEO)
epoch:      5              6              7
          [batches…]    [batches…]    [batches…]
```

**Log batches** (what `kafka-dump-log` shows, abbreviated): each batch header carries `partitionLeaderEpoch`. That is independent of the checkpoint file — which is why “log has 7, checkpoint missing 7” is possible.

```text
baseOffset: 1000  lastOffset: 1499  partitionLeaderEpoch: 5
baseOffset: 1500  lastOffset: 1799  partitionLeaderEpoch: 6
baseOffset: 1800  lastOffset: 2199  partitionLeaderEpoch: 7   ← still in .log
```

---

## A tiny example

Healthy leader checkpoint:

```text
0
2
5 1000
6 1500
```

Log still has batches with epoch **7**, but the checkpoint has no line for 7:

```text
On disk (broken leader):
  events-compact-0/
    *.log                     ← still contains partitionLeaderEpoch: 7 from offset 1800
    leader-epoch-checkpoint   ← only 5 and 6  ← hole
```

```text
Follower:  "I last saw epoch 7"
Leader:    "I don't have 7 → fall back to epoch 6 → truncate to ~1500"
Follower:  truncates, fetches again, last batch again needs epoch 7
           → truncate to ~1500 again → loop
```

After the fix (add epoch 7):

```text
0
3
5 1000
6 1500
7 1800    ← earliest offset that still carries epoch 7
```

Next fetch can move forward. ISR fills in.

### The code path that turns a missing epoch into a loop

Nothing here is a “Kafka forgot to replicate” bug by itself. The replication protocol is doing what it was designed to do ([KIP-320](https://cwiki.apache.org/confluence/display/KAFKA/KIP-320:+Allow+fetchers+to+detect+and+handle+log+truncation)): if the follower’s last fetched epoch does not line up with the **leader’s epoch cache**, truncate and try again. The loop appears when the **leader’s on-disk checkpoint** (loaded into that cache) has a hole, so every retry truncates to the same place.

Snippets below are from the **3.7.0** tree (the version we were upgrading toward). The same idea exists on 3.6.x.

**1. Leader loads the checkpoint into memory**

[`LeaderEpochFileCache` constructor (3.7.0)](https://github.com/apache/kafka/blob/3.7.0/storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java) — whatever is in `leader-epoch-checkpoint` becomes the map used for every fetch:

```java
// LeaderEpochFileCache.java (3.7.0)
public LeaderEpochFileCache(TopicPartition topicPartition, LeaderEpochCheckpoint checkpoint) {
    this.checkpoint = checkpoint;
    this.topicPartition = topicPartition;
    // ...
    checkpoint.read().forEach(this::assign);  // disk file → in-memory epoch map
}
```

**Example — disk file → memory:**

```text
leader-epoch-checkpoint          in-memory epochs map
─────────────────────            ─────────────────────
0                                { 5 → 1000,
2                                  6 → 1500 }
5 1000                           // epoch 7 never loaded — it was not on disk
6 1500
```

If epoch `7` is missing from that file, it is missing from the cache.

**2. Looking up a missing epoch floors to an older one**

[`endOffsetFor`](https://github.com/apache/kafka/blob/3.7.0/storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java) — when the requested epoch is not the latest, Kafka uses the largest known epoch **≤** the request (`floorEntry`) and the start of the next known epoch:

```java
// LeaderEpochFileCache.endOffsetFor (3.7.0) — abbreviated
Map.Entry<Integer, EpochEntry> higherEntry = epochs.higherEntry(requestedEpoch);
Map.Entry<Integer, EpochEntry> floorEntry = epochs.floorEntry(requestedEpoch);
if (floorEntry == null) {
    // requested epoch older than anything we know
    epochAndOffset = /* requestedEpoch, higherEntry.startOffset */;
} else {
    // Hole in the middle (e.g. ask for 7, have 6 and 8):
    // return epoch 6 and the start offset of epoch 8 → truncate point
    epochAndOffset = new AbstractMap.SimpleImmutableEntry<>(
        floorEntry.getValue().epoch,
        higherEntry.getValue().startOffset);
}
```

**Example — follower asks for epoch 7, leader cache is `{5→1000, 6→1500}` (no 7, and 6 is latest):**

```text
request:  lastFetchedEpoch = 7, fetchOffset ≈ 2000
cache:    5→1000, 6→1500
floor(7) = 6, higher(7) = null  → UNDEFINED (or, with a later epoch 8→2500,
                                  floor=6 and endOffset = start of 8)

Our stuck shape (no later epoch in cache): leader cannot name “where 7 ended,”
so FETCH fails or returns a diverging older end tied to epoch 6 (~1500).
```

With checkpoint `{5→1000, 6→1500}` and a request for epoch `7` when `7` is absent and there is a later epoch in the cache, the leader answers with **epoch 6** and an older end offset — not “epoch 7 starts here.”

If the requested epoch is **newer than every entry** in the cache, `higherEntry` is null and the lookup returns undefined offsets instead:

```java
if (higherEntry == null) {
    epochAndOffset = new AbstractMap.SimpleImmutableEntry<>(
        UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET);
}
```

**3. Leader FETCH handling: undefined → error; older end → diverging epoch**

[`Partition.readRecords` (3.7.0)](https://github.com/apache/kafka/blob/3.7.0/core/src/main/scala/kafka/cluster/Partition.scala) uses that lookup on the follower’s `lastFetchedEpoch`:

```java
// Partition.scala (3.7.0) — readRecords, abbreviated
lastFetchedEpoch.ifPresent { fetchEpoch =>
  val epochEndOffset = lastOffsetForLeaderEpoch(currentLeaderEpoch, fetchEpoch, fetchOnlyFromLeader = false)

  if (epochEndOffset.endOffset == UNDEFINED_EPOCH_OFFSET ||
      epochEndOffset.leaderEpoch == UNDEFINED_EPOCH) {
    throw new OffsetOutOfRangeException(
      "Could not determine the end offset of the last fetched epoch " +
      s"$lastFetchedEpoch from the request")
  }

  // Follower thinks it is in epoch N, but leader's cache says the end is older → diverge
  if (epochEndOffset.leaderEpoch < fetchEpoch || epochEndOffset.endOffset < fetchOffset) {
    val divergingEpoch = new FetchResponseData.EpochEndOffset()
      .setEpoch(epochEndOffset.leaderEpoch)
      .setEndOffset(epochEndOffset.endOffset)
    return new LogReadInfo(FetchDataInfo.empty(fetchOffset), Optional.of(divergingEpoch), /* ... */)
  }
}
```

**Example — FETCH wire picture:**

```text
Follower → Leader  FETCH  events-compact-0
                      fetchOffset=2000
                      lastFetchedEpoch=7

Leader → Follower  (no records to append)
                      divergingEpoch: { epoch=6, endOffset=1500 }
                   // or OffsetOutOfRange if end is undefined
```

So a hole (or a too-short cache) on the leader becomes either a hard `OffsetOutOfRangeException` or a **divergingEpoch** in the fetch response.

**4. Follower truncates and fetches again**

[`AbstractFetcherThread`](https://github.com/apache/kafka/blob/3.7.0/core/src/main/scala/kafka/server/AbstractFetcherThread.scala) — on diverging epoch, skip appending data and truncate:

```java
// AbstractFetcherThread (3.7.0 idea; same in later trees)
if (leader.isTruncationOnFetchSupported && FetchResponse.isDivergingEpoch(partitionData)) {
  // truncate to divergingEpoch.endOffset; do not append this response's records
  divergingEndOffsets += topicPartition -> new EpochEndOffset()
    .setLeaderEpoch(partitionData.divergingEpoch.epoch)
    .setEndOffset(partitionData.divergingEpoch.endOffset)
} else {
  processPartitionData(...)  // normal append
}
// ...
if (divergingEndOffsets.nonEmpty)
  truncateOnFetchResponse(divergingEndOffsets)
```

**Example — follower disk before/after one loop iteration:**

```text
BEFORE truncate:  follower log LEO=2000 (still thinks it has epoch-7 data)
AFTER  truncate:  follower log LEO=1500
NEXT   fetch:     lastFetchedEpoch still resolves to the same mismatch
                  → truncate to 1500 again → stuck
```

Log line shape from the lab:

```text
Truncating partition events-compact-0 ... due to leader epoch and offset
EpochEndOffset(... leaderEpoch=6, endOffset=1500)
```

After truncate, the next fetch often ends in the same “last epoch” situation → same diverging answer → **loop** at a fixed offset.

**Why adding the missing epoch on the leader fixes it:** step 1 loads a complete map, step 2 can resolve epoch `7`, step 3 no longer returns a diverging older end, step 4 appends instead of truncating.

```text
After repair on leader:
  leader-epoch-checkpoint has 7 1800
  follower FETCH lastFetchedEpoch=7 → leader: “epoch 7 is fine, here are records”
  follower appends → ISR grows again
```

---

## The code path that writes (or fails to keep) `leader-epoch-checkpoint`

This is the **local log/replica path**. It is the same under **ZooKeeper mode and KRaft**. KRaft changes controller registration / ISR trust (next section); it does not change how this file is assigned, truncated, or loaded.

```text
Become leader / append batches
        ↓
maybeAssignEpochStartOffset(epoch, offset)
        ↓
LeaderEpochFileCache.assign → writeToFile(true)   // fsync
        ↓
leader-epoch-checkpoint on disk
```

Snippets from the **3.7.0** tree (the version we were upgrading toward).

### How the file is normally written

**1. New leader epoch is assigned at the current log end offset**

[`Partition.makeLeader` (3.7.0)](https://github.com/apache/kafka/blob/3.7.0/core/src/main/scala/kafka/cluster/Partition.scala):

```scala
// Partition.scala — become leader with a new epoch
val leaderEpochStartOffset = leaderLog.logEndOffset
leaderLog.maybeAssignEpochStartOffset(
  partitionState.leaderEpoch, leaderEpochStartOffset)
```

**Example — broker becomes leader for epoch 7 at LEO 1800:**

```text
Before makeLeader:
  LEO = 1800
  leader-epoch-checkpoint:
    0
    2
    5 1000
    6 1500

After maybeAssignEpochStartOffset(7, 1800):
  leader-epoch-checkpoint:
    0
    3
    5 1000
    6 1500
    7 1800          ← written with sync=true, often before any epoch-7 records exist
```

**2. Appends also update the cache from batch epochs**

[`UnifiedLog` append path (3.7.0)](https://github.com/apache/kafka/blob/3.7.0/core/src/main/scala/kafka/log/UnifiedLog.scala):

```scala
// update the epoch cache with the epoch stamped onto the message by the leader
validRecords.batches.forEach { batch =>
  if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
    maybeAssignEpochStartOffset(batch.partitionLeaderEpoch, batch.baseOffset)
  }
}
```

**Example — first produce under the new leader:**

```text
Append batch: baseOffset=1800, partitionLeaderEpoch=7
  → .log gains the batch
  → assign(7, 1800) is a no-op if makeLeader already wrote that line
  → later batches in epoch 7 do not add new checkpoint lines
     (same epoch, later offset — cache keeps the start offset only)
```

**3. `assign` persists with sync**

[`LeaderEpochFileCache.assign` (3.7.0)](https://github.com/apache/kafka/blob/3.7.0/storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java):

```java
public void assign(int epoch, long startOffset) {
    EpochEntry entry = new EpochEntry(epoch, startOffset);
    if (assign(entry)) {
        writeToFile(true);   // sync = true
    }
}
```

[`CheckpointFile.write`](https://github.com/apache/kafka/blob/3.7.0/server-common/src/main/java/org/apache/kafka/server/common/CheckpointFile.java): write `.tmp` → optional `fsync` → atomic rename.

**Example — write mechanics:**

```text
events-compact-0/
  leader-epoch-checkpoint.tmp   ← full new contents written here
       ↓ fsync (when sync=true)
  leader-epoch-checkpoint       ← atomic rename into place
```

So the happy path records the epoch around the time data is appended, and `assign` tries to make that durable.

### Code that can remove epochs or leave the file less durable

**A. Truncation updates the checkpoint without fsync (3.7.0)**

```java
// LeaderEpochFileCache (3.7.0)
public void truncateFromEnd(long endOffset) {
    // removes entries with startOffset >= endOffset
    writeToFile(false);  // intentionally NO fsync (perf)
}

public void truncateFromStart(long startOffset) {
    // drops/rewrites early entries
    writeToFile(false);  // NO fsync
}
```

**Example — `truncateFromEnd(1800)` after a dangling epoch-7 assign with no data:**

```text
Before:  5→1000, 6→1500, 7→1800
After:   5→1000, 6→1500          ← 7 removed (startOffset >= 1800)
Write:   writeToFile(false)      ← may hit disk later; crash timing matters
```

Comments say leftover **extra** stale epochs are OK because load will trim again. They do **not** claim load will recreate a **missing** middle epoch.

Called from e.g. [`UnifiedLog.truncateTo`](https://github.com/apache/kafka/blob/3.7.0/core/src/main/scala/kafka/log/UnifiedLog.scala) — including when truncate is a no-op on the log itself (broker assigned a new epoch at LEO, failed to append, then lost leadership → drop the dangling epoch entry). That explains an epoch line with **no** data; our production shape was the inverse: **data without the line**.

Related later hardening: [KAFKA-16541](https://github.com/apache/kafka/pull/15993) (checkpoint corruption / truncate durability).

**B. Conflicting assign can delete newer/conflicting entries**

```java
// LeaderEpochFileCache.maybeTruncateNonMonotonicEntries
removeFromEnd(entry ->
    entry.epoch >= newEntry.epoch || entry.startOffset >= newEntry.startOffset);
```

**Example — odd assign wipes the tail of the map:**

```text
Before:     5→1000, 6→1500, 7→1800
Assign:     6→1600  (conflict)
After:      5→1000, 6→1600     ← 7 gone from memory, then synced to disk
```

A bad/odd assign can wipe later epoch lines from the in-memory map, then `writeToFile(true)` persists that.

**C. Startup only trims the LEC to the log — it does not fill holes**

[`LogLoader.load` (3.7.0 — Scala on this release)](https://github.com/apache/kafka/blob/3.7.0/core/src/main/scala/kafka/log/LogLoader.scala):

```scala
leaderEpochCache.ifPresent(_.truncateFromEnd(nextOffset))
// "earliest leader epoch may not be flushed during a hard failure"
leaderEpochCache.ifPresent(_.truncateFromStart(logStartOffsetCheckpoint))

if (!hadCleanShutdown) {
  // recover unflushed segments only — may re-assign epochs seen there
  recoverSegment(segment)
}
```

**Example — clean start with a hole (our failure shape):**

```text
On disk:
  .kafka_cleanshutdown          present
  events-compact-0/*.log        has epoch 7 batches from offset 1800
  leader-epoch-checkpoint       5→1000, 6→1500   ← missing 7

LogLoader:
  hadCleanShutdown = true  → skip unflushed recover
  truncateFromEnd(LEO)     → only drops epochs with startOffset >= LEO
  truncateFromStart(...)   → only fixes the early edge
  result: hole for 7 remains → serve bad cache forever until repair
```

| Startup | What happens to LEC |
|---|---|
| **Clean** | Load checkpoint as-is; trim ends; **trust holes** |
| **Unclean** | Recover **unflushed** segments (can re-assign epochs seen there); still not a full rebuild of every epoch in the flushed log |

That is why a clean-looking restart can still serve a hole: nothing scans the whole log and rewrites a complete `epoch → startOffset` map. Manually adding the missing epoch on the leader supplies the map entry recovery never rebuilt.

### Most plausible story for our shape (code-backed, timing not proven)

```text
1. Log already has batches with epoch 7
2. leader-epoch-checkpoint on disk is missing 7
   (lost via truncate write without sync / incomplete write /
    never rebuilt after a messy roll — exact race not proven)
3. Clean marker present → LogLoader skips heavy recover
4. truncateFromEnd/Start only trim edges → hole stays
5. LeaderEpochFileCache loads the hole → KIP-320 truncate loop
```

Same local path whether the cluster metadata lived in ZooKeeper or in KRaft’s `__cluster_metadata`.

---

## Clean vs unclean startup (local broker)

On disk each log directory can have a **clean shutdown marker** (`.kafka_cleanshutdown` / `CleanShutdownFile`).

**Example — two boots of the same log dir:**

```text
Clean stop:
  /var/lib/kafka/data/.kafka_cleanshutdown   exists (KRaft JSON may include "brokerEpoch": 42)
  next start → hadCleanShutdown=true → fast path, trust LEC + flushed segments

Kill / crash:
  .kafka_cleanshutdown   missing (or unreadable)
  next start → hadCleanShutdown=false → recover from recovery-point through LEO
```

### Clean shutdown (broker finished stopping)

1. Marker is present → “last stop completed.”
2. Startup **skips** heavy recovery of the unflushed log tail.
3. Trust flushed segments and load checkpoints as-is.
4. Fast restart.

### Unclean shutdown (kill, crash, grace period expired, crash mid-load)

1. No good marker → “last stop did not finish.”
2. Startup **recovers** unflushed segments from the recovery point: check batches, truncate corruption, rebuild producer state, and update the leader-epoch cache while scanning **those** segments. Load also trims the cache to log start/end; it does **not** fill a missing middle epoch from the flushed log (see previous section).
3. Slow restart on large disks.

Marker handling (shared idea in both eras): read marker early, remember clean/unclean, **delete marker before load finishes** so a crash during load cannot look like a clean shutdown next time — see [KAFKA-10471](https://issues.apache.org/jira/browse/KAFKA-10471).

Code (3.7.0 / current tree; same idea as 3.6):

- [`LogLoader.scala` (3.7.0)](https://github.com/apache/kafka/blob/3.7.0/core/src/main/scala/kafka/log/LogLoader.scala) — if `!hadCleanShutdown`, recover unflushed segments; always `truncateFromEnd` / `truncateFromStart` on the epoch cache ([`LogLoader.java` on trunk](https://github.com/apache/kafka/blob/trunk/storage/src/main/java/org/apache/kafka/storage/internals/log/LogLoader.java))
- [`LeaderEpochFileCache.java`](https://github.com/apache/kafka/blob/3.7.0/storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java) — epoch map used in truncation / fetch / checkpoint writes

---

## ZooKeeper era (what we were on: 3.6.1 → 3.7.0 upgrade)

Our incidents were **ZooKeeper mode**. Metadata (ISR, leader) lived in ZK. Brokers got partition state via controller RPCs such as `LeaderAndIsr`.

**Example — where state lived (ZK era):**

```text
ZooKeeper                          Broker disk
─────────────────────────          ─────────────────────────
/brokers/ids/1                     events-compact-0/*.log
/brokers/topics/.../state          events-compact-0/leader-epoch-checkpoint
  (leader, ISR, …)                 .kafka_cleanshutdown
controller epoch                   (no PreviousBrokerEpoch on register)
```

Local LEC holes were a **disk** problem; ZK still thought ISR membership looked fine until followers fell out from failed fetch/truncate.

**What local recovery did**
- Same clean-marker + `LogLoader` idea: unclean → scan unflushed log; clean → skip that work.

**What the controller did *not* do (ZK)**
- There was no broker registration that said “my last session ended unclean, here is my previous broker epoch.”
- ZK did not run today’s KRaft “drop this broker from ISR/ELR because re-register looks unclean” path.

So after a messy rolling upgrade, durability mostly depended on **local** disk recovery and the old ISR/leader-election rules. A bad leader epoch map could leave followers looping with little cluster-level “this restart was unclean” handling.

Useful ZK-era pointers (3.6 line of the code):

- Broker still driven by ZK controller RPCs: see Kafka 3.6 [KRaft migration notes](https://kafka.apache.org/36/operations/kraft/) (ZK brokers receive `LeaderAndIsr` / `UpdateMetadata` while migrating)
- Truncation / fetch epoch logic already existed via [KIP-101](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation) / [KIP-320](https://cwiki.apache.org/confluence/display/KAFKA/KIP-320:+Allow+fetchers+to+detect+and+handle+log+truncation) — the loop is that protocol meeting a bad checkpoint, not “ZK forgot ISR”

Tag for our case: **seen during operator upgrades on ZK clusters moving 3.6.1 → 3.7.0**. We believe unclean shutdown during the roll contributed; we did not prove the exact kill timing in a lab.

---

## KRaft era (what got stronger from 3.7.0 onward)

Kafka **3.7.0** is a useful milestone for this story. Same release train we were upgrading into, and it shipped broker-side clean-shutdown detection for KRaft.

**What KRaft does *not* fix by itself:** a hole in the leader’s on-disk `leader-epoch-checkpoint` still feeds the KIP-320 truncate/fetch loop above. Our Compose lab is KRaft and can still stick a replica that way. What got stronger is the **cluster handshake after an unclean restart**: the controller can drop that broker from ISR/ELR and bump partition leader epochs so stale trust does not linger.

```text
Broker clean-shutdown file
        ↓ PreviousBrokerEpoch on register
Controller compares epochs
        ↓ mismatch = unclean
ReplicationControl rewrites partitions
        ↓ drop from ISR/ELR, bump leader epoch
Brokers apply new partition metadata
```

### The code path that makes unclean restarts safer

Snippets below mix **3.7.0** (broker clean-shutdown file) and **current trunk** (controller registration / partition updates). The registration + ISR/ELR cleanup ideas land via [KAFKA-15582](https://issues.apache.org/jira/browse/KAFKA-15582) / [KAFKA-15586](https://github.com/apache/kafka/pull/14706) / [KAFKA-18106](https://github.com/apache/kafka/pull/18045).

**1. Clean shutdown file carries broker epoch (3.7.0)**

[KAFKA-15582](https://issues.apache.org/jira/browse/KAFKA-15582): on clean shutdown, write the **broker epoch** into `.kafka_cleanshutdown`; on startup, read it and send `PreviousBrokerEpoch` on `BrokerRegistrationRequest`.

[`CleanShutdownFileHandler` (3.7.0)](https://github.com/apache/kafka/blob/3.7.0/storage/src/main/java/org/apache/kafka/storage/internals/checkpoint/CleanShutdownFileHandler.java) — note the path is under `storage/.../checkpoint/` (not the older `core/.../kafka/log/` location):

```java
// CleanShutdownFileHandler.java (3.7.0)
public static final String CLEAN_SHUTDOWN_FILE_NAME = ".kafka_cleanshutdown";

public void write(long brokerEpoch) throws Exception {
    write(brokerEpoch, CURRENT_VERSION);  // JSON includes brokerEpoch
}

public OptionalLong read() {
    // OptionalLong.of(brokerEpoch), or empty if missing / unreadable
}
```

**Example — clean vs kill on a KRaft broker:**

```text
Clean stop writes:
  /var/lib/kafka/data/.kafka_cleanshutdown
  { "version": 0, "brokerEpoch": 42 }

Next BrokerRegistrationRequest:
  previousBrokerEpoch = 42

Kill / crash:
  file missing → previousBrokerEpoch = -1 (or absent)
  → next boot cannot prove “last session finished cleanly”
```

`BrokerRegistrationRequest` (API key 62, versions 2+) adds the field — see [PR #14465](https://github.com/apache/kafka/pull/14465):

```json
{ "name": "PreviousBrokerEpoch", "type": "int64", "versions": "2+", "default": "-1",
  "about": "The epoch before a clean shutdown." }
```

ZK mode had no equivalent “here is my previous broker epoch” on re-register.

**2. Controller decides clean vs unclean**

[KAFKA-15586](https://github.com/apache/kafka/pull/14706): same `previousBrokerEpoch` as last session → **clean**; missing / different → **unclean**.

[`ClusterControlManager.registerBroker` (trunk)](https://github.com/apache/kafka/blob/trunk/metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java):

```java
// ClusterControlManager.registerBroker — abbreviated
long storedBrokerEpoch = existing != null ? existing.epoch() : -2;

if (!request.incarnationId().equals(prevIncarnationId)) {
    boolean isCleanShutdown = cleanShutdownDetectionEnabled
        ? storedBrokerEpoch == request.previousBrokerEpoch()
        : false;

    // unclean → generate partition cleanup records BEFORE new RegisterBrokerRecord
    brokerShutdownHandler.addRecordsForShutdown(
        request.brokerId(), isCleanShutdown, records);
}
```

**Example — controller arithmetic:**

```text
Last RegisterBrokerRecord for broker 3:  epoch = 42
New registration:                        previousBrokerEpoch = 42  → clean
New registration:                        previousBrokerEpoch = -1  → unclean
```

**3. Unclean → strip ISR/ELR and rewrite partitions**

[KAFKA-18106](https://github.com/apache/kafka/pull/18045): when registration looks unclean, emit the partition transitions (ISR/leader/epoch), not just a registration note.

[`ReplicationControlManager.handleBrokerShutdown` (trunk)](https://github.com/apache/kafka/blob/trunk/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java):

```java
// ReplicationControlManager.handleBrokerShutdown
void handleBrokerShutdown(int brokerId, boolean isCleanShutdown,
                          List<ApiMessageAndVersion> records) {
    if (featureControl.isElrFeatureEnabled() && !isCleanShutdown) {
        generateLeaderAndIsrUpdates("handleBrokerUncleanShutdown",
            NO_LEADER, NO_LEADER, brokerId, records,
            brokersToIsrs.partitionsWithBrokerInIsr(brokerId));
        generateLeaderAndIsrUpdates("handleBrokerUncleanShutdown",
            NO_LEADER, NO_LEADER, brokerId, records,
            brokersToElrs.partitionsWithBrokerInElr(brokerId));
    } else {
        // clean (or no ELR): treat more like a normal fence/shutdown
        generateLeaderAndIsrUpdates("handleBrokerShutdown",
            brokerId, NO_LEADER, NO_LEADER, records,
            brokersToIsrs.partitionsWithBrokerInIsr(brokerId));
    }
}
```

Inside `generateLeaderAndIsrUpdates`, an unclean broker is not an acceptable leader and is removed from ISR/ELR:

```java
// generateLeaderAndIsrUpdates — unclean broker excluded
IntPredicate isAcceptableLeader =
    r -> (r != brokerToRemove && r != brokerWithUncleanShutdown)
        && (r == brokerToAdd || clusterControl.isActive(r));

if (brokerWithUncleanShutdown != NO_LEADER) {
    builder.setUncleanShutdownReplicas(List.of(brokerWithUncleanShutdown));
}
builder.setTargetIsr(Replicas.toList(
    Replicas.copyWithout(partition.isr,
        new int[] { brokerToRemove, brokerWithUncleanShutdown })));
// builder.build() → PartitionChangeRecord (often bumps leader epoch)
```

**Example — metadata before/after unclean re-register of broker 3:**

```text
Before (in __cluster_metadata / describe):
  events-compact-0  Leader: 3  Replicas: 1,2,3  ISR: 1,2,3  LeaderEpoch: 7

Broker 3 re-registers unclean → PartitionChangeRecord(s):
  Leader: 1 (or 2)   ISR: 1,2   (3 removed)   LeaderEpoch: 8

Broker disks for events-compact-0:
  still have their own leader-epoch-checkpoint files
  (controller did not rewrite those local maps)
```

**Why this is different from the LEC loop path**

| Path | Where | What it fixes |
|---|---|---|
| Missing epoch → truncate loop | Leader’s local `LeaderEpochFileCache` + FETCH | Needs a **complete** checkpoint / repair |
| KRaft unclean register | Controller metadata (`__cluster_metadata`) | Stops trusting a **crashed broker** still in ISR/ELR |

### 4. Eligible Leader Replicas (later, Kafka 4)

[KIP-966](https://cwiki.apache.org/confluence/display/KAFKA/KIP-966+Eligible+Leader+Replicas) / [4.0 docs](https://kafka.apache.org/40/operations/eligible-leader-replicas/): safer leader choice after unclean events (“last replica standing” data-loss case). Preview in 4.0; check your metadata / feature version before assuming it is on.

### 5. Do not fake a clean marker (KRaft startup ordering)

[KAFKA-15375](https://issues.apache.org/jira/browse/KAFKA-15375): in KRaft, log load can run on a different path than classic startup; do not write a clean marker if recovery never finished.

### Simple picture

```text
ZK era (our 3.6.1→3.7.0 incidents)
  Unclean restart → local LogLoader works hard
  Controller/ZK: no “previous broker epoch” unclean handshake like KRaft

KRaft 3.7.0+
  Unclean restart → local LogLoader still works hard
                 → AND broker tells controller “last session unclean”
                 → controller can strip unsafe ISR/ELR trust and update partitions
```

That is why “same stuck ISR after upgrade” is much less discussed on Kafka 4: not because someone closed one JIRA for our exact loop, but because **startup + registration recovery got stricter** while ZK mode went away.
---

## Takeaways

1. **Problem:** leader checkpoint missing an epoch still in the log → truncate/fetch loop → stuck ISR.
2. **Evidence bar:** production diagnosis + successful repair; not a reliable lab repro of the race.
3. **LEC write path (ZK and KRaft):** `assign` syncs; truncate often writes without fsync; clean load trims ends but does not rebuild middle holes.
4. **Local recovery:** clean = skip unflushed scan and trust checkpoints; unclean = recover unflushed segments (may re-assign epochs seen there).
5. **Cluster recovery (KRaft, from 3.7.0):** broker epoch in clean-shutdown file + registration lets the controller treat unclean restarts as unsafe until caught up again (ISR/ELR strip + partition epoch bumps). That is not the same as repairing a bad `leader-epoch-checkpoint`.
6. **Ops:** still give brokers time to shut down cleanly when you can (`terminationGracePeriodSeconds`); recovery is a safety net, not a reason to SIGKILL early.

---

## Sources

- [KAFKA-10471](https://issues.apache.org/jira/browse/KAFKA-10471) — crash during log load stays unclean  
- [KAFKA-15582](https://issues.apache.org/jira/browse/KAFKA-15582) — clean shutdown + broker epoch (3.7.0)  
- [KAFKA-15586](https://github.com/apache/kafka/pull/14706) — controller unclean detection  
- [KAFKA-18106](https://github.com/apache/kafka/pull/18045) — partition updates on unclean register  
- [KAFKA-15375](https://issues.apache.org/jira/browse/KAFKA-15375) — no false clean marker  
- [KIP-966](https://cwiki.apache.org/confluence/display/KAFKA/KIP-966+Eligible+Leader+Replicas)  
- [KIP-320](https://cwiki.apache.org/confluence/display/KAFKA/KIP-320:+Allow+fetchers+to+detect+and+handle+log+truncation) — diverging epoch / last fetched epoch  
- [`LeaderEpochFileCache` (3.7.0)](https://github.com/apache/kafka/blob/3.7.0/storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java) — `assign` / truncate / `endOffsetFor`; checkpoint load  
- [`Partition.scala` (3.7.0)](https://github.com/apache/kafka/blob/3.7.0/core/src/main/scala/kafka/cluster/Partition.scala) — `makeLeader` assign; FETCH + `lastFetchedEpoch` → diverging epoch / OOR  
- [`UnifiedLog.scala` (3.7.0)](https://github.com/apache/kafka/blob/3.7.0/core/src/main/scala/kafka/log/UnifiedLog.scala) — append assigns epochs; `truncateTo`  
- [`CheckpointFile` (3.7.0)](https://github.com/apache/kafka/blob/3.7.0/server-common/src/main/java/org/apache/kafka/server/common/CheckpointFile.java) — tmp write + optional sync + rename  
- [`LogLoader.scala` (3.7.0)](https://github.com/apache/kafka/blob/3.7.0/core/src/main/scala/kafka/log/LogLoader.scala) — clean vs unclean load; trim LEC ends ([Java on trunk](https://github.com/apache/kafka/blob/trunk/storage/src/main/java/org/apache/kafka/storage/internals/log/LogLoader.java))  
- [`AbstractFetcherThread`](https://github.com/apache/kafka/blob/3.7.0/core/src/main/scala/kafka/server/AbstractFetcherThread.scala) — truncate on diverging fetch response  
- [KAFKA-16541](https://github.com/apache/kafka/pull/15993) — leader-epoch checkpoint truncate / fsync durability  
- [`CleanShutdownFileHandler` (3.7.0)](https://github.com/apache/kafka/blob/3.7.0/storage/src/main/java/org/apache/kafka/storage/internals/checkpoint/CleanShutdownFileHandler.java) — broker epoch in `.kafka_cleanshutdown`  
- [`ClusterControlManager` (trunk)](https://github.com/apache/kafka/blob/trunk/metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java) — `previousBrokerEpoch` clean vs unclean on register  
- [`ReplicationControlManager` (trunk)](https://github.com/apache/kafka/blob/trunk/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java) — unclean → ISR/ELR strip + `PartitionChangeRecord`  
- [Kafka 3.7.0 release notes](https://archive.apache.org/dist/kafka/3.7.0/RELEASE_NOTES.html) (includes KAFKA-15582)
