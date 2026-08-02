# Hot partitions

Most of the produce and consume traffic lands on one (or a few) partitions while the rest stay quiet. Throughput, lag, and broker load follow that skew.

| Field | Value |
| --- | --- |
| Severity | High when it hits production SLAs |
| Typical surface | Producers (key / partitioner), brokers (disk / CPU / network), consumers (lag on one partition) |
| Related configs | Message key, custom partitioner, topic partition count, consumer concurrency |

## Symptoms

- Consumer lag climbs on one partition while sibling partitions stay near zero
- One broker shows higher disk growth, network, or CPU than peers that own quieter partitions
- Produce latency or timeouts cluster around a specific partition
- Scaling consumers does not help: extra instances sit idle because there is nothing left to assign
- Downstream processing falls behind even though "average" topic lag looks acceptable

## Why it happens

Kafka assigns each record to a partition. With a key, the default hash partitioner maps `hash(key) % numPartitions`. With no key, records round-robin (or sticky-batch) across partitions.

A hot partition appears when that mapping concentrates traffic:

- Low-cardinality keys: country, status, tenant with one huge tenant, `true`/`false`, etc.
- One dominant key: a single user, device, or account produces far more than the rest
- Null or missing keys with an unexpected partitioner or producer setting that collapses load
- Key design that includes a coarse bucket (hour-of-day, region, or product line) that is uneven in the real world
- Too few partitions for the key space you actually have, so hash collisions stack on the same partition

Consumers read one partition at a time per assigned member. If partition 3 has 80% of the bytes, the member that owns partition 3 becomes the bottleneck no matter how many other consumers you add.

## Diagnosis

Work these in order. Stop when the root cause is clear.

1. Confirm skew in size or lag, not just "the topic is slow"
   - Compare end offset, log size, and lag per partition for the topic.
   - Hot partition pattern: one partition's lag or size is an order of magnitude above the median.
   - Flat lag across all partitions points somewhere else (consumer bug, broker-wide issue, under-partitioning without skew).

2. Map the hot partition to a broker
   - Note which broker is leader (and which holds replicas) for the hot partition.
   - Check that broker's disk fill rate, network, and request queue against the others.
   - Sustained pressure on one broker that owns the hot partition supports the diagnosis.

3. Inspect how records are keyed
   - Sample recent keys for the hot partition (console consumer with partition filter, or your observability tooling).
   - Ask: is there one key, a small set of keys, or many keys that still hash together?
   - Check producer code / Connect SMT config for `key` field selection. Wrong field is a common root cause.

4. Check partition count vs. parallelism needs
   - Count of partitions caps consumer parallelism for that topic.
   - Even with good keys, a 3-partition topic cannot use 12 consumers effectively.
   - Under-partitioning plus mild key skew is enough to create a visible hot spot.

5. Rule out consumer-side illusion
   - A slow processor on one assigned partition looks like "hot partition lag."
   - Compare produce rate into that partition with consume rate.
   - If produce rates are even and one consumer is slow, fix the consumer, not the key.

## Fixes

Match the fix to the cause you found. Prefer the smallest change that rebalances load.

### Fix A: change the partition key (most common)

When keys are null, coarse, or dominated by one value:

1. Choose a key with high cardinality and even real-world distribution (order id, event id, device id, not status or country alone).
2. Keep ordering requirements in mind: same key means same partition, so records stay ordered per key. If you need order per customer, customer id is fine unless one customer is enormous.
3. For a known hot entity (one huge tenant), see Fix C. Do not hash everything on tenant id alone.
4. Roll out producer / Connect changes, then verify new traffic spreads. Old data on the hot partition will drain on its own as it is consumed or expires.

### Fix B: increase partitions (only after keys are sane)

When keys look fine but partition count is too low for throughput:

1. Increase the topic's partition count to match needed consumer parallelism and broker spread.
2. Remember: existing keys do not reshuffle. Old records stay on their partitions; only new records use the wider hash space.
3. Plan for consumer group rebalance and any downstream systems that assume a fixed partition count.
4. Do not use partition expansion as the first fix for a single dominant key. That key still lands on one partition.

### Fix C: isolate or salt hot keys

When a few keys are inherently hot (VIP tenant, broadcast entity, single busy device):

1. Isolate: route that key to a dedicated topic (or dedicated partitions via a custom partitioner) so the main topic stays balanced.
2. Salt: append a rotating suffix to the key (`tenantId#0` … `tenantId#N`) so one logical entity fans out across N partitions.
3. If you salt, consumers must merge those partitions (or you accept out-of-order processing for that entity). Document the trade-off.
4. Prefer isolation when ordering for that entity must stay strict. Prefer salting when throughput matters more than per-entity order.

### Fix D: custom partitioner (special cases)

When business rules need explicit placement:

1. Implement a partitioner that spreads known heavy keys while keeping normal keys on the default hash path.
2. Deploy it on all producers of that topic. Mixed partitioners create chaos.
3. Treat this as an exception. Most teams fix the key instead.

### Fix E: relieve the broker while you fix the root cause

When the hot partition is already hurting a broker:

1. Reassign replicas so the hot partition's leader / followers are not stacked on an already loaded broker (temporary relief only).
2. Confirm disk and retention on that partition will not fill the volume before the key fix lands.
3. Do not stop at reassignment. Without a key or topology fix, the heat follows the partition.

## Verify

- Per-partition lag and size move toward the topic median over a traffic cycle
- Broker disk / network charts no longer show one outlier tied to that partition
- Adding consumers actually reduces lag (assignments are useful again)
- For salted or isolated keys: confirm the hot entity's throughput and ordering meet the agreed trade-off

Watch for a day of normal peak traffic before calling it done. Skew that only appears at peak is still a hot partition problem.

## Recreate on your laptop

Step-by-step CLI walkthrough (Docker Compose + broker tools, no wrapper scripts): [`demo/README.md`](demo/README.md).

Summary: start the broker, create a 6-partition topic, produce about 90% with key `tenant-vip`, compare end offsets, then produce a second topic keyed by unique order ids and confirm even spread.

## Related problems

*(Add links as new playbooks land, e.g. consumer lag with even partitions, under-partitioned topics, producer batching skew.)*
