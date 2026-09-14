# Task design and iteration

## Outcome

The final artifact is one original Terminal-Bench 3 task,
[`atomic-shard-rebalancing`](../tasks/atomic-shard-rebalancing/). It asks the
solver to repair a small asynchronous transactional key-value engine so that a
set of logical shards can move atomically between reusable physical workers
without losing transaction, snapshot, recovery, or exactly-once obligations.

The task was not selected by generating a large batch of candidates and keeping
one that happened to fail. It was developed from a fixed baseline through three
evidence-driven hypotheses. Each completed student result changed the next
design decision, including two full successes that caused earlier directions to
be weakened or abandoned.

## Methodological sources

The literature informed the task-generation method, not the expected solution
or a claim that any paper guarantees model failure.

| Source | Idea used here | Boundary in this project |
| --- | --- | --- |
| [HardGen](https://arxiv.org/abs/2601.01498) | Use errors exposed by real execution to guide the next construction. | An intermediate coding error is not a model failure if the agent fixes it before submission. |
| [EXIF](https://arxiv.org/abs/2506.04287) | Explore in an executable environment and update the next direction from feedback. | The student model was held fixed; only the task changed. |
| [LOGIGEN](https://arxiv.org/abs/2603.00540) | Construct difficult states at constraint boundaries and judge them programmatically. | The verifier accepts any legal implementation and serial order rather than a private reference state. |
| [SCA](https://arxiv.org/abs/2506.01716) | Develop the task, verifier, reference solution, and incorrect controls together. | A passing reference is necessary but not sufficient; every hidden assertion must follow from a public rule. |
| [TRACE](https://arxiv.org/abs/2510.00415) | Ground analysis in real executions and replayable traces. | A reference trace demonstrates one feasible path, not the only permitted protocol. |
| [TASTE](https://arxiv.org/abs/2605.28556) | Recheck interaction and state coverage after constructing a task. | Difficulty may not come from omitted requirements or deliberate ambiguity. |

This led to a teacher-guided loop:

> inspect a complete student result → state a concrete failure hypothesis →
> build one fair and solvable candidate → validate it → run a fresh student →
> retain, narrow, or abandon the hypothesis from the final result

## Fixed baseline

All candidates began from the frozen `shard-txn-recovery r1` task, not from a
previous student's patch. Its first complete Codex/Sol run passed all 19
business scenarios. That success established two important facts: the supplied
engine was tractable for the target agent, and ordinary one-shard migration was
not sufficiently difficult. The baseline and iteration outcomes are summarized
in [`iteration-summary.json`](../evidence/design/iteration-summary.json).

## Candidate-selection correction

The iterations were replacements, not a cumulative feature ladder. H2 did not
retain H1's split requirement, and H3 did not combine the split and guard tasks.
Each candidate independently extended the same frozen r1 baseline so that a
success on one direction could not be hidden by stacking unrelated features on
top of it.

The complete H2 success also exposed a weakness in the author's selection
strategy: adding a field or enlarging a participant set was still naturally
absorbed by the student's existing durable-lock and recovery framework. From
that point, candidate selection required a stronger justification:

1. identify a simplifying assumption used by a successful implementation;
2. introduce a realistic mechanism interaction for which that assumption is no
   longer sufficient;
3. predict a concrete public error that could survive a full coding-and-testing
   attempt; and
4. retain normal budgets, complete public rules, and an independently passing
   reference solution.

This is constrained task-level red-team search, not a convergence
process and not a promise that difficulty rises monotonically. Larger changes
were allowed when they formed one coherent engineering problem; unrelated
feature accumulation, hidden requirements, verifier mistakes, and reduced
budgets were not accepted as difficulty.

## Iteration 1 — one-to-many shard split

The first hypothesis, H1, was that an agent able to implement one-to-one
migration might confuse an old logical participant with two physical successor
shards. The candidate added an atomic A-to-L/R split while unresolved
transactions and reads remained live. It required the implementation to carry
old obligations to both successors without duplicating effects or reviving an
obsolete owner.

The complete feedback did not support the predicted semantic failure: a fresh
independent run passed all 30 business scenarios. The agent had constructed a
different but valid proxy design that preserved old logical identity across the
split. H1 was therefore weakened rather than extended with more small
conditions.

The decision and result are summarized in
[`iteration-summary.json`](../evidence/design/iteration-summary.json).

## Iteration 2 — guarded transactions

H2 moved to a different protocol decision. It added atomic conditional
transactions in which read-only guard keys could live on shards absent from the
write set. The predicted mistakes were checking guards outside the transaction
lock, omitting guard-only participants from migration, or reevaluating a guard
after a durable decision.

The complete Sol/xhigh run passed all 28 business scenarios. The final program
correctly formed the participant union, protected guard keys, migrated the
obligations, and preserved terminal retry results. That result weakened H2 and
showed that adding another field to the existing protocol was not a productive
direction.

See [`iteration-summary.json`](../evidence/design/iteration-summary.json).

## Iteration 3 — atomic multi-shard rebalancing

H3 made a larger, evidence-grounded change. Instead of another local feature,
the task introduced a placement plan that can:

- exchange A and B in one atomic owner-vector change;
- co-locate and later separate logical shards on a physical worker;
- return a shard to a worker that still has historical disk state;
- execute up to three placement generations;
- overlap with unresolved transactions, completed snapshots whose internal
  cleanup is still pending, worker restarts, and delayed old messages.

This composition breaks assumptions that were valid in the earlier tasks. A
worker may simultaneously export one logical shard and import another; target
readiness must cover the whole vector before publication; co-location cannot
collapse logical coordinator identities; and an old physical copy must not
become current authority merely because the worker is selected again.

The public host deliberately provides only request routing, isolated durable
stores, message transport and an atomic owner-vector compare-and-swap. It does not
transfer business state, choose a cut, freeze participants, establish target
readiness, recover transactions, or decide when a plan is safe to complete.
Those remain the student's work.

The resulting candidate was feasible for the private reference and detected
two purpose-built incorrect implementations. Three fresh Codex/Sol submissions
then independently failed the same final verifier. The failures narrowed H3:
the observed weakness was not generic atomic placement corruption, but the
lifecycle of a completed snapshot's read responsibility across ownership
changes and later healthy writes.

The baseline, all three candidate decisions, and their completed outcomes are
summarized in
[`iteration-summary.json`](../evidence/design/iteration-summary.json). The
reported trial scores and causal analyses are recorded separately in
[`trial-results.md`](trial-results.md) and
[`failure-analysis.md`](failure-analysis.md).

## Why this is a fair red-team task

The design process sought a genuine engineering decision boundary, not
convergence toward one reference implementation. Difficulty increased only
after a complete success falsified an earlier hypothesis. The final task makes
the state-space interaction substantially wider, but keeps the workload small,
the interfaces explicit, the schedules deterministic, and the expert scope
bounded.

Each standard student received the same public instruction, starter engine,
specifications, resources, time budgets, and model settings in a fresh session.
No prior submission, hidden trace, teacher diagnosis, or reference code entered
another student's input. The three submitted source trees and their manifests
are retained under [`evidence/trials`](../evidence/trials/).
