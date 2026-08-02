# Problem title

One-sentence description of what goes wrong.

| Field | Value |
| --- | --- |
| Severity | Low / Medium / High |
| Typical surface | Producers / Brokers / Consumers / Connect / Streams |
| Related configs | `key` examples |

## Symptoms

- What people notice first
- Metrics or console signals that line up with this problem

## Why it happens

Short explanation of the mechanism. Keep it concrete.

## Diagnosis

Work these in order. Stop when the root cause is clear.

1. Check … (what to look at and what "bad" looks like)
2. Check …
3. Check …

## Fixes

Match the fix to the cause you found.

### Fix A: when …

Steps the customer can take.

### Fix B: when …

Steps the customer can take.

## Verify

- How to confirm the imbalance / failure is gone
- What to watch for the next few hours or days

## Recreate

Numbered steps a reader can run locally (or against a lab cluster) to see the problem. Prefer plain CLI / Compose commands over wrapper scripts. Keep a `demo/` folder only when you need a compose file or similar fixtures. Document the steps in `demo/README.md`, or inline here if short.

1. …
2. …
3. Expected signal: …

## Related problems

- Link other playbooks in this repo when useful
