# Kafka Common Problems

Practical playbooks for Kafka issues that show up in production. Each problem is a short set of **symptoms → diagnosis steps → fixes** you can walk through with a customer or your own team. No demo clusters, no throwaway scripts — just the checks and changes that usually resolve the issue.

## Problems

| Problem | Summary |
| --- | --- |
| [Hot partitions](problems/hot-partitions/) | One (or a few) partitions carry most of the traffic, creating lag, broker pressure, and uneven consumer load |

## How to use this repo

1. Start from the symptoms table in a problem folder.
2. Work the diagnosis checklist in order — stop when you have a clear root cause.
3. Apply the matching fix. Prefer the smallest change that rebalances load.

## Contributing a new problem

Copy [`templates/PROBLEM.md`](templates/PROBLEM.md) into `problems/<slug>/README.md` and fill it in. Keep the tone operational: what to look at, what to change, and what to verify afterward.

## Related

Blog write-ups live on [joel-hanson.github.io](https://joel-hanson.github.io). The first post in this series covers [hot partitions](https://joel-hanson.github.io/posts/31-when-one-kafka-partition-takes-all-the-heat/).
