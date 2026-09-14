# Trial results

## GPT-5.6 Sol/xhigh standard configuration

The three standard trials used the same student-facing task and evaluation
settings:

| Field | Value |
| --- | --- |
| Harbor agent adapter | Codex 0.154.0 |
| Model | `openai/gpt-5.6-sol` |
| Reasoning effort | `xhigh` |
| Environment | Docker |
| Attempts per job | 1 |
| Concurrency | 1 |
| Automatic retries | 0 |
| Agent budget | 14,400 seconds |
| Verifier budget | 900 seconds |
| Resources | 4 CPU, 4,096 MiB RAM, 10,240 MiB disk, no GPU |

Every run began with a fresh starter and model session. No previous submission,
teacher feedback, hidden scenario, or reference implementation was supplied.

## Standard results

| Run | Trial ID | Agent time | Final verifier result | Business scenarios | Classification |
| ---: | --- | ---: | ---: | ---: | --- |
| 1 | `atomic-shard-rebalancing__TkEximK` | 2,146.65 s | reward 0 | 28/30 PASS | Genuine student failure |
| 2 | `atomic-shard-rebalancing__3UbXvR8` | 1,432.50 s | reward 0 | 29/30 PASS | Genuine student failure |
| 3 | `atomic-shard-rebalancing__dL6Z5XX` | 1,911.24 s | reward 0 | 28/30 PASS | Genuine student failure |

All three artifact checks passed. None of the three results contains an agent,
API, rate-limit, container, collection, timeout, or verifier-framework error.
The same final verifier graded all three exact submitted source trees. The
aggregate machine-readable judgment is
[`series.json`](../evidence/final-grades/series.json).

Per-run commands, configurations, submitted sources, manifests, and final
results are available here:

- [`gpt-5-6-sol-xhigh-standard-01`](../evidence/trials/gpt-5-6-sol-xhigh-standard-01/) and its
  [`final grade`](../evidence/final-grades/gpt-5-6-sol-xhigh-standard-01/)
- [`gpt-5-6-sol-xhigh-standard-02`](../evidence/trials/gpt-5-6-sol-xhigh-standard-02/) and its
  [`final grade`](../evidence/final-grades/gpt-5-6-sol-xhigh-standard-02/)
- [`gpt-5-6-sol-xhigh-standard-03`](../evidence/trials/gpt-5-6-sol-xhigh-standard-03/) and its
  [`final grade`](../evidence/final-grades/gpt-5-6-sol-xhigh-standard-03/)

## Adversarial result

Under the evaluator-provided alternative, one DeepSeek V4.1 Flash black-box
adversarial run was executed through TokenHarbor with Terminus-2:

| Field | Result |
| --- | --- |
| Trial | `atomic-shard-rebalancing__xS9ktz6` |
| Model route | `deepseek-v4.1-flash:free` |
| Reasoning | enabled / max |
| Attempts and retries | 1 attempt, 0 retries |
| Completion | Normal |
| Reward | 0 |
| Business scenarios | 28/30 PASS |
| Artifact admission | PASS |
| Framework errors | None |

The command, material configuration, submitted source, and final result are in
[`deepseek-cheat-01`](../evidence/trials/deepseek-cheat-01/) and
[`final-grades/deepseek-cheat-01`](../evidence/final-grades/deepseek-cheat-01/).
This is one failed shortcut attempt, not another standard student run and not a
proof that every possible bypass fails.

## Assignment scope

This repository demonstrates three GPT-5.6 Sol/xhigh failures and one accepted
alternative adversarial zero.
