# Failure analysis

## Common validity test

A reward-0 result was counted only when the student completed normally and the
final verifier identified a public-contract violation. Infrastructure failures,
timeouts, missing responses caused by the harness, and intermediate mistakes
that the agent repaired were not counted.

The three counted Codex submissions all mishandled the boundary between a
completed snapshot and a later ownership transition. Internally retained
`ReadGrant` state survived longer than the public read operation. The programs
then treated that cleanup responsibility as a live business conflict and made an
irreversible ABORT decision for a later healthy transaction. Waiting for cleanup
would have been legal; returning a durable terminal ABORT was not.

## Codex run 1 — post-exchange false abort

Trial `atomic-shard-rebalancing__TkEximK` passed 28/30 business scenarios. In
both the ordinary and renamed multi-key cycle cases, the public sequence was:

1. a write completed;
2. a snapshot returned `OK`;
3. the A/B exchange returned `COMPLETE`; and
4. only then a new healthy transaction was invoked.

The new owner still held a grant belonging to the completed snapshot. Its
conflict path persisted ABORT and returned `ABORTED` for the new transaction.
Public requirement R7 requires a healthy nonoverlapping transaction to commit;
R2 and R3 make the persisted terminal decision immutable.

The accepted read had therefore ended at the public API while its residual grant
still changed a later terminal outcome. The final verifier report is in
[`codex-standard-01`](../evidence/final-grades/codex-standard-01/).

## Codex run 2 — false abort after a completed plan

Trial `atomic-shard-rebalancing__3UbXvR8` passed 29/30 business scenarios. The
original read returned the contract-permitted `UNKNOWN`; the placement plan then
returned `COMPLETE`; a new healthy cross-shard transaction was invoked afterward.
The current A owner retained the old read grant, classified it as a current
write conflict, persisted ABORT, and returned `ABORTED`.

Because the read and plan had already reached public terminal responses, there
was no overlapping client operation that could justify the abort. The final
verifier therefore classifies this as a genuine R7/R2/R3 violation. The final
report is under
[`codex-standard-02`](../evidence/final-grades/codex-standard-02/).

## Codex run 3 — stale responsibility after owner reuse

Trial `atomic-shard-rebalancing__dL6Z5XX` passed 28/30 business scenarios. A
snapshot returned `OK`, and three placement plans subsequently completed,
eventually returning A to a former physical owner. No client operation or
injected fault overlapped the later `returned-new` transaction.

The current A store contained no unresolved write intent, but it still contained
the completed snapshot's read grant. The participant persisted ABORT, the
coordinator adopted it, and the client received `ABORTED`. This is a terminal
business error rather than a progress timeout. The final report is under
[`codex-standard-03`](../evidence/final-grades/codex-standard-03/).

## Cross-run pattern

The three implementations differed in size and protocol details, but the final
failures share a deeper modeling error: physical state movement was handled more
completely than logical operation lifetime. A successful read response ended
the client operation, while internal cleanup remained movable state. After a
placement change, the implementations could not distinguish “responsibility to
finish cleanup” from “a currently active read that may force a new transaction
to abort.”

This is the useful model-failure finding. It is narrower than saying the agents
could not implement rebalancing at all: across the three submissions, 28 or 29
of the 30 scenarios passed, including crashes, replay, compatibility, co-location,
and several multi-generation cases.

## DeepSeek adversarial run

The adversarial submission received reward 0 with 28/30 business scenarios
passing. Its two independent public-contract violations were:

- retrying an already completed migration by proposing an unregistered owner
  epoch transition; and
- completing a placement plan without ever replying `OK` or `UNKNOWN` to an
  already accepted read request.

The attempt did not obtain or modify the private verifier. It tried to shortcut
the protocol through public host operations, but the unchanged host ABI and
behavioral checks rejected the result. See the final report in
[`deepseek-cheat-01`](../evidence/final-grades/deepseek-cheat-01/).
