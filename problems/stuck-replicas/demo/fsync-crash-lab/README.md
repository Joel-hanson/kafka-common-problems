# Real #14242 / #16541 lab (OS crash, not LEC hand-edit)

This lab checks whether **Kafka 3.7.0** truncate without fsync ([PR #14242](https://github.com/apache/kafka/pull/14242) / [KAFKA-15046](https://issues.apache.org/jira/browse/KAFKA-15046)) can leave a **torn/corrupt** `leader-epoch-checkpoint` after an **OS-level** crash ([KAFKA-16541](https://issues.apache.org/jira/browse/KAFKA-16541), fixed in 3.8.0).

It is **not** the Compose “edit LEC” stuck-replica demo. No hand-editing checkpoints.

## VM vs Ubuntu cluster

| Setup | Use for this test? |
| --- | --- |
| **One Ubuntu VM** you can hard-reset | **Yes** — matches the JIRA (kernel dies before dirty pages flush) |
| Multi-node Ubuntu cluster | Only if you can **hard power-off one node** the same way; extra nodes do not help prove #14242 |
| Docker on a Mac that stays up | **No** — host kernel keeps running; `docker kill` is not this failure |

**Prefer a single VM.** A cluster is overkill for “did truncate’s unsynced write corrupt LEC on crash?”

## What counts as pass / fail

After hard reset + Kafka start:

| Result | Meaning |
| --- | --- |
| LEC unreadable, mid-line garbage, `count` ≠ lines, or broker fails to load checkpoint | **Pass** for #14242/#16541-class failure |
| LEC always clean/parseable | **Fail** this run (timing is rare — repeat many times) |
| Clean missing epoch + stuck ISR **without** corruption | Interesting, but **not** what #16541 documents; record separately |

Control: same scripts on **3.8.0+** should make torn LEC much harder.

## Guest setup (Ubuntu 22.04)

### Option A — Multipass (easy on macOS)

```bash
multipass launch 22.04 --name kafka-fsync --cpus 2 --memory 4G --disk 20G
multipass transfer setup-guest.sh kafka-fsync:/home/ubuntu/
multipass transfer run-workload.sh kafka-fsync:/home/ubuntu/
multipass transfer inspect-lec.sh kafka-fsync:/home/ubuntu/
multipass exec kafka-fsync -- bash /home/ubuntu/setup-guest.sh 3.7.0
```

### Option B — Cloud Ubuntu VM / bare metal

Copy the three scripts onto the box and run `setup-guest.sh 3.7.0`.

Kafka runs **natively** under systemd (not Docker), log dir `/var/lib/kafka/data`, so a guest hard reset hits the real page cache for that volume.

## Run one trial

On the guest (after setup):

```bash
# Terminal 1 — keep truncate / leader-churn traffic going
sudo -u kafka bash /home/ubuntu/run-workload.sh

# Terminal 2 — when churn has been running ~30–60s, hard reset WITHOUT sync:
sudo sh -c 'echo b > /proc/sysrq-trigger'
# or: sudo reboot -f
```

From the **host** (if Multipass guest is wedged after sysrq):

```bash
# last resort hard stop of the VM process (tool-dependent)
multipass stop kafka-fsync --force   # if available in your multipass version
# or destroy/recreate after noting you need a second disk/snapshot for data
```

Prefer **sysrq-b / `reboot -f` inside the guest** so the guest kernel does not sync. Avoid `shutdown` / `multipass stop` (graceful).

## After boot

```bash
sudo bash /home/ubuntu/inspect-lec.sh
sudo systemctl start kafka || true
sudo journalctl -u kafka -b --no-pager | head -200
```

Look for checkpoint parse errors and dump every `leader-epoch-checkpoint`.

Repeat 10–20+ trials; one clean boot proves nothing.

## A/B control

```bash
# wipe data dir, reinstall bits for 3.8.1 (or 3.9.x), repeat workload + sysrq-b
sudo bash /home/ubuntu/setup-guest.sh 3.8.1
```

If 3.7.0 shows torn LECs and 3.8+ does not under the same stress, that supports the fsync story.

## Relation to the stuck-replica blog

- **This lab:** real durability failure of LEC truncate write on OS crash (3.7.0 window).
- **Compose LEC edit:** proves the **truncate/fetch loop symptom**, not that #14242 created your production hole.

If production archives only show a **parseable missing epoch**, this lab may pass (corruption) while still not matching that incident shape — say so in notes.
