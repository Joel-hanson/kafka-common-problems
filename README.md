# Kafka Common Problems

Practical playbooks for Kafka issues that show up in production. Each problem covers symptoms, diagnosis, and fixes, plus optional recreate steps you can run locally. Prefer documented CLI commands over wrapper scripts. Keep a `demo/` folder only for Compose or similar fixtures.

## Problems

| Problem | Summary | Recreate |
| --- | --- | --- |
| [Hot partitions](problems/hot-partitions/) | One (or a few) partitions carry most of the traffic, creating lag, broker pressure, and uneven consumer load | [steps](problems/hot-partitions/demo/) |
| [Stuck replicas](problems/stuck-replicas/) | A partition stays under-replicated: followers never rejoin ISR after epoch / log divergence | [steps](problems/stuck-replicas/demo/) |

## How to use this repo

1. Start from the symptoms table in a problem folder.
2. Work the diagnosis checklist in order. Stop when you have a clear root cause.
3. Apply the matching fix. Prefer the smallest change that rebalances load.
4. Optionally walk the recreate steps to see the failure mode on a laptop.

## Contributing a new problem

Copy [`templates/PROBLEM.md`](templates/PROBLEM.md) into `problems/<slug>/README.md` and fill it in. Keep the tone operational: what to look at, what to change, and what to verify afterward. If you add a local recreate path, write the steps as numbered commands. Avoid shell wrappers unless something cannot be expressed inline.

## Related

Blog write-ups live on [joel-hanson.github.io](https://joel-hanson.github.io). The first post in this series covers [hot partitions](https://joel-hanson.github.io/posts/31-when-one-kafka-partition-takes-all-the-heat/).
