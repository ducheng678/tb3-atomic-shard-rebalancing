# Atomic Shard Rebalancing — Terminal-Bench 3 task submission

This repository packages one original Terminal-Bench 3 task, its author-side validation evidence, the design and iteration history that led to it, three completed Codex standard trials, and one completed DeepSeek adversarial trial.

The task asks an agent to repair a small asynchronous transaction KV engine so that logical shards can be rebalanced atomically across reusable workers while transactions, snapshots, crashes, delayed messages, and historical disk state remain live. The complete task package is in [`tasks/atomic-shard-rebalancing`](tasks/atomic-shard-rebalancing/).

## Evidence status

| Item | Recorded result | Interpretation |
| --- | --- | --- |
| Static checks | 22/22 scripts exited successfully | Twenty checks exercised applicable configuration; two had no applicable Compose or GPU configuration. |
| Docker build | PASS | Recorded author build passed. |
| Oracle | reward 1; 30/30 business scenarios plus artifact check passed | Demonstrates a feasible reference solution for the tested schedules. |
| Nop/starter | reward 0 | Confirms the starter does not already solve the task. |
| GPT implementation rubric | 34 PASS, 0 FAIL, 1 NOT_APPLICABLE | The current complete GPT-5.6 Sol/max review passes every applicable criterion; only `do_not_modify_enforced` does not apply. |
| Codex standard trial 1 | final verifier reward 0; 28/30 business scenarios passed | Genuine student failure: a healthy new transaction was durably aborted after a completed snapshot and shard exchange. |
| Codex standard trial 2 | final verifier reward 0; 29/30 business scenarios passed | Genuine student failure: a residual read grant caused a healthy new transaction to be durably aborted after rebalancing completed. |
| Codex standard trial 3 | final verifier reward 0; 28/30 business scenarios passed | Genuine student failure: a completed snapshot's residual grant survived three ownership changes and caused a false terminal abort. |
| DeepSeek adversarial trial | reward 0; 28/30 business scenarios passed | Valid `/cheat`-style adversarial zero using DeepSeek V4.1 Flash through TokenHarbor, under the evaluator-provided substitution allowance. |

All three Codex solution runs were independent. Their exact, hash-verified submissions were evaluated with the same final verifier: run 1 passed 28/30 business scenarios, run 2 passed 29/30, and run 3 passed 28/30. Each failure is a public-contract violation rather than an agent, API, container, collection, timeout, or verifier-framework failure.

This repository still does **not** claim complete assignment acceptance. The
unrun model groups remain disclosed below.

## Repository guide

- [`docs/task-design-and-iteration.md`](docs/task-design-and-iteration.md) explains the research influences, fixed baseline, hypothesis changes, candidate selection, and feedback-driven iterations.
- [`docs/verification-strategy.md`](docs/verification-strategy.md) explains the independent verifier, semantic oracle, scenario coverage, controls, and isolation boundary.
- [`docs/check-results.md`](docs/check-results.md) records static, rubric, build, oracle, nop, and evaluation outcomes.
- [`docs/trial-results.md`](docs/trial-results.md) records commands, configurations, scores, and validity decisions for the four reported trials.
- [`docs/failure-analysis.md`](docs/failure-analysis.md) gives short causal analyses for the three genuine Codex failures and the DeepSeek adversarial result.
- [`docs/commands-and-configurations.md`](docs/commands-and-configurations.md) provides reproducible command templates and recorded configurations.
- [`docs/requirements-matrix.md`](docs/requirements-matrix.md) maps the hiring assignment to the evidence present here and the remaining gaps.
- [`SUBMISSION-CHECKLIST.md`](SUBMISSION-CHECKLIST.md) lists the actions that still require the contributor or evaluator.
- [`evidence`](evidence/) contains the selected machine-readable records, run configurations, final verifier reports, submitted engine trees, and iteration provenance used by the reports.

## Development note

LLMs and coding agents were used extensively to develop and evaluate this task.
The repository preserves the design path, verification strategy, configurations,
results, and failure analyses needed to review that work.

No API key, OAuth token, provider credential, or credential file is included in this repository.
