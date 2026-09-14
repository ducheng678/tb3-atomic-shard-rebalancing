# Verification strategy

## Verification target

The candidate deliverable is the complete Python source tree at
`/app/sharddb/engine`. The verifier evaluates behavior through the published
host ABI and public contracts. It does not compare source code, field names,
message names, state-machine phases, or execution order with the private
reference solution.

The final verifier contains 30 business scenarios plus one artifact-admission
check. Nineteen scenarios retain the baseline transaction, migration,
compatibility, restart, replay, and snapshot behavior. Eleven additional
scenarios cover group exchange, renamed and multi-key topologies, co-location,
unresolved commits, live reads, persistence cuts, owner-vector CAS recovery,
offline retired sources, target restart with replay, and rebalancing after a
legacy migration.

## Execution boundary

Student code runs in isolated worker processes with private roots and durable
stores. The trusted controller owns the event schedule, network delivery,
crash/restart actions, metadata service, client history, and report generation.
Only the submitted engine source is transferred to the separate verifier. The
student process is stopped before scoring, and verifier results are not exposed
to the student environment.

The agent and verifier images share the public runtime and host SDK bytes. Test
drivers, scenario definitions, trusted history checker, fixtures, and reference
solution exist only on the verifier side.

## What is checked

The verifier derives every assertion from the published specifications:

- response schemas and request identity;
- durable COMMIT/ABORT immutability;
- exactly-once effects for committed transaction identifiers;
- strict serializability of successful writes and snapshots;
- correct authority after ownership changes;
- recoverable and idempotent placement-plan completion;
- rejection of stale owners and stale installation images;
- continued service after finite faults and retired-source shutdown;
- compatibility with documented version-1 durable checkpoints.

For successful histories, an independent bounded search enumerates transaction
sets and legal orders subject to real-time precedence. It accepts any order that
explains every successful snapshot and the final values with each committed
increment applied once. This separates correctness from the reference
implementation's protocol.

Ownership and plan-completion facts are checked at the event where they occur,
not against a later final placement. Public decisions retain logical
coordinator identity even when two logical shards share one physical worker.

## Progress and error classification

The public logical limits are:

| Operation class | Limit |
| --- | ---: |
| Independent healthy C request | 1,000 rounds |
| Healthy rebalancing plan | 2,000 rounds |
| Stable recovery and service phase | 10,000 rounds |

These limits are availability bounds, not performance rankings. Host, fixture,
controller, collection, and verifier failures are reported as `RUN_ERROR` and
do not receive a normal model-failure reward. A candidate receives reward 1 only
when the artifact check and all 30 business scenarios pass; a public-contract
violation receives reward 0.

## Positive and negative validation

Before student evaluation, the private reference passed all 30 business
scenarios and the artifact check with reward 1. A second legal author profile
also passed while using a different placement publisher, delaying successful
read responses until release acknowledgement, and running maintenance at a
different interval. This demonstrates that the tests do not require one exact
message schedule.

The unchanged starter received reward 0. Two targeted incorrect controls were
also rejected:

- preserving values from an obsolete physical owner across a cycle; and
- reinstalling an old handoff image after newer work.

Both controls fail through histories that no legal serial execution can explain,
not by source-pattern matching or artificial time exhaustion. The recorded
results are in
[`evidence/checks/author-validation`](../evidence/checks/author-validation/).

## Uniform final grading

The three independent Codex submissions were all evaluated by the same final
verifier. Its recorded image ID is
`sha256:cb3576d16948f114abe0ddf40bd549bb77f88d19bfbe340b12b910614dbd957b`,
and the trusted group-scenario source fingerprint is
`fcd4228d8b9295be0e6743cb2cd84b59d2384d913fd833782b387a9bb3016b3a`.
The aggregate result is
[`series.json`](../evidence/final-grades/series.json).

The verifier is deterministic over the recorded schedules, but this finite
suite is not a proof over every possible implementation or interleaving. The
adversarial reward-0 run shows that one black-box shortcut attempt failed; it
does not prove that no conceivable bypass exists.
