# run-20260806-110124

## Timeline
- T0: healthy baseline after produce + consume (events-delete, events-compact, lab-group-a)
- A: simultaneous kill → healed (~30s)
- B: kill under load → healed immediately
- D: LEC edit on events-compact-0 → stuck ≥3m (healed on full bounce)
- D2: LEC edit on events-delete-0 → stuck ≥3m
- R1: restart failed broker → healed
- R2: wipe only bad replica dir → healed

## Hypotheses (updated)
1. Epoch-checkpoint divergence can stick a single partition via failed ReplicaFetcher — CONFIRMED (D/D2)
2. Compact policy required — REJECTED (D2)
3. Hard kill alone sticks replicas — REJECTED (A/B)

See also: ../../INVESTIGATION.md (cleaned narrative for blog)

## Observations (no ticket language yet)
- T0: all partitions ISR full (1,2,3). lab-group-a committed offsets on events-delete with lag 0.
- Topics present: __consumer_offsets (compact, 5 parts), events-compact, events-delete. No under-replicated at T0.

## Perturb A — simultaneous docker kill (2026-08-06T05:33:25Z)

Commands: `docker kill kafka-1 kafka-2 kafka-3` then `docker-compose start`.

### Immediate signal (~17s after start)
Under-replicated (ISR missing broker 1 on several partitions):
- __consumer_offsets 1,2,3,4 — Isr: 3,2
- events-compact 1,2 — Isr: 3,2
- events-delete not listed in that first URP sample

### +30s through +180s
Under-replicated list empty. All partitions full ISR (1,2,3). Leadership consolidated on broker 3.

### Client check
lab-group-a still describable after A; post-consume describe saved under T1-after-A-healed.

### Conclusion for A alone
Hard kill of all three brokers produced **transient** under-replication only. Cluster self-healed within ~30s. **Did not** reproduce a stuck replica / permanent ISR shrink on this quiet workload.

### Next candidates
- B: kill while actively producing to compact + delete topics
- D: surgical leader-epoch-checkpoint edit on one broker
- C: wipe one partition dir on one broker

## Perturb B — kill under produce/consume load (2026-08-06T05:42:37Z)

Setup: background console producers to events-compact + events-delete, looping consumer on lab-group-a for ~15s warm-up, then `docker kill` all three brokers and `docker-compose start`.

Offsets just before kill (approx):
- events-compact: 1543 / 2309 / 2567
- events-delete: 2351 / 2318 / 2274

### Immediate signal (~17s after start)
Under-replicated list **empty**. Full ISR on all partitions. Leadership on broker 2.

### +30s / +60s / +120s
Still no under-replicated partitions.

### Conclusion for B
Kill-under-load alone also **did not** produce stuck replicas on this lab. Self-healed at least as cleanly as A (no visible lingering URP window in the first describe).

Extra log notes for B:
- Non-monotonic HWM count: 0 on all brokers in the window
- Truncating messages present (normal catch-up / epoch reconciliation), not a repeating stuck loop
- lab-group-a showed lag after B (consumer died mid-flight); offsets still present — not a coordinator failure


## Perturb D — surgical leader-epoch-checkpoint edit (2026-08-06T05:55–05:59Z)

Target: `events-compact-0` only (user compact topic). Leader before edit: broker 1.

### Edit (broker 1 only)
Before (broker 1):
```
0
3
0 0
5 480
9 1680
```
After (broker 1):
```
0
2
0 0
3 50
```
Followers 2 and 3 left unchanged (`0 0` / `5 480`).

### Result — STUCK
- Immediately and through +180s: `events-compact-0` Leader=3, Replicas=1,2,3, **Isr: 3,2** (broker 1 never rejoins)
- Sibling partitions 1 and 2 on same topic: full ISR
- Persists across archive stop/start cycle (still URP after restart)

### Causal chain (from kafka-1 logs)
1. Broker 1 loads log end 1680, then becomes follower of broker 3
2. ReplicaFetcher truncates to offset **50** due to leader-epoch negotiation (`EpochEndOffset ... endOffset=480` path; local corrupt LEC drove bad truncate)
3. `Non-monotonic update of high watermark from 1680 to 50`
4. `UnexpectedAppendOffsetException`: append starts at 0 but next offset is 50 — partition marked **failed**
5. On-disk after truncate: broker 1 log/index are **0 bytes**; brokers 2 and 3 still hold 47418-byte log + snapshot@1680

### Disk archive
`T1-broken-D/` — full broker data dirs, dumps, logs, checksums. Pre-edit copies in `D-pre-edit/`.

### Classification so far
- Mechanism: **epoch-checkpoint divergence → destructive truncate → fetcher failure → permanent URP**
- Not unique to `__consumer_offsets` / `__transaction_state` in this lab — reproduced on **user compact topic** `events-compact-0`
- A/B (hard kill alone) did not produce this; D (on-disk epoch lie) did

### Not done yet
- Client impact produce/consume on partition 0
- Compare same edit on `events-delete-0` (delete policy)
- Recovery experiments (do not apply ticket procedure yet)

### Correction after archive bounce
While running, URP held for ≥3 minutes. After `docker-compose stop` (full disk copy) + `start`, broker 1 **rejoined** ISR (`Isr: 3,2,1`). So this induce caused a **sustained-but-recoverable-on-full-bounce** stuck fetcher state, not an immortal corruption. The T1 archive still captured the broken on-disk moment (broker 1 empty log vs healthy 47KB on 2/3).

Open question: would it have healed eventually without bounce, or only via full restart + re-fetch? Mentions/trunc counts were flat during the 3-minute watch (not a hot loop), consistent with fetcher marked failed and idle.

## Perturb D2 — same LEC edit on events-delete-0 (delete policy)

Leader before edit: broker 3. Same corrupt LEC written on broker 3 only:
```
0
2
0 0
3 50
```
Followers 1/2 unchanged (`0 0` / `5 656`).

### Result
- Immediate + through +180s: `events-delete-0` under-replicated, broker 3 out of ISR (Leader moved to 2, Isr: 2,1)
- Sibling partitions on events-delete stayed full ISR
- Same class of failure as D on compact topic

### Conclusion vs compact hypothesis
**cleanup.policy=compact is not required** to induce this stuck-replica pattern via epoch-checkpoint divergence. Delete-policy `events-delete-0` failed the same way. Compact may still matter for *how* corruption arises in the wild, but the stuck-ISR mechanism here is replication/epoch, not compaction-specific.


## Recovery on live stuck events-delete-0

Pre-recovery: Leader=2, Isr: 2,1 (broker 3 out). Disk: brokers 1/2 healthy (~59KB log); broker 3 empty log + snapshot@50 + LEC `0 0` only.

### R1 — restart only failed broker (`docker restart kafka-3`)
**SUCCESS** within ~15s. ISR returned to 2,1,3 and stayed healthy through +90s. URP empty.

Why restart worked (kafka-3 logs):
- Reloaded empty log (logEndOffset=0), deleted the orphan `...00050.snapshot`
- Fetcher to leader 2 truncated to 0 (no-op), then could append from offset 0
- Contrast with stuck state: fetcher had been **marked failed** after UnexpectedAppendOffsetException (next offset was 50) and did not self-heal until process restart

### R2 — wipe bad replica dir
**Not needed** for this incident. Healthy leader/follower copies existed; restart alone was enough.

### Implications
- When ≥1 healthy replica remains in ISR, prefer **restart failed broker** (or delete only that replica's partition dir if restart fails) over wiping all replicas / leader.
- Nuclear "delete partition on all brokers, followers then leader" is for when **all** copies are bad or leader is the corrupt sole ISR — not the first move here.
- Produce smoke after R1: OK, no URP.


## Recovery R2 — wipe only bad replica partition dir

Re-induced stuck `events-delete-0` (LEC lie on broker 3). Pre-R2: Leader=2, Isr: 2,1; brokers 1/2 ~60KB log; broker 3 empty + snapshot@50.

### Procedure
1. `docker stop kafka-3` (failed broker only; leave healthy ISR members up)
2. `rm -rf /var/lib/kafka/data/events-delete-0` on kafka-3 only
3. `docker start kafka-3`

### Result
**SUCCESS** by +15s. ISR 2,1,3; URP empty through +90s. Broker 3 recreated partition and caught up (~60KB log). Produce smoke OK.

### R1 vs R2
- Both recover when healthy replicas remain in ISR
- R1 (restart only): clears failed fetcher; works if empty/corrupt local state can re-fetch cleanly
- R2 (wipe replica dir then start): forces clean re-replica; use when restart alone is insufficient or local dir is clearly garbage
- Neither requires deleting the leader or other healthy followers


## Trial D3 — partial salvage (lab-validated)

### Failed approach (do not repeat)
Emptied `events-delete-0` on brokers 1 and 2, kept full log on broker 3 with bad LEC, then started all three for KRaft quorum. An empty broker won leadership and truncated broker 3's good log to 0 bytes on all replicas. Pre-induce archive (`D3-pre-partial-salvage/`) was required to undo.

### Successful approach
1. Restored healthy logs from archive; reassigned partition 0 to **sole replica [3]** (RF temporarily 1).
2. Induced: bad LEC on broker 3 only; log kept (65614 bytes / end offset 2593).
3. Salvage: restored good `leader-epoch-checkpoint` on broker 3 from pre-induce copy; log untouched.
4. Verified Leader=3, log still 65614, offsets 2593.
5. Reassigned back to replicas [3,1,2]; followers caught up to 65614 within seconds; full ISR.
6. Produce smoke OK (partition 0 advanced to 2607).

### Conclusion
Partial salvage works when you keep a readable log as source of truth, repair its metadata, and only then rebuild other replicas from it. Never start empty replicas in a way that lets them become leader before the good copy is protected (reassign to sole good broker first, or keep the salvaged leader up while wiping followers).

