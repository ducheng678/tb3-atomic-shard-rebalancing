# Assignment requirements matrix

The hiring assignment asks for one original Terminal-Bench 3 task, current CI-style checks, standard and adversarial model evaluations, a GitHub repository, and documentation of commands, configurations, results, and failure analysis. The matrix below states exactly what this package demonstrates.

| Requirement | Evidence in this repository | Status |
| --- | --- | --- |
| Original TB3 task | `tasks/atomic-shard-rebalancing/` | Present |
| Static checks | `evidence/checks/static/` | All 22 scripts exited successfully; two had no applicable Compose or GPU configuration; exact-final-revision full rerun still recommended |
| Implementation rubric | `evidence/checks/rubric/current-main-model-review.json` and `status.json` | Current complete GPT-5.6 Sol/max review: 34 pass, 0 fail, 1 not applicable; all applicable criteria pass |
| Docker build | `evidence/checks/author-validation/build-result.json` | Recorded PASS |
| Oracle validation | `evidence/checks/author-validation/oracle-result.json` | Recorded reward 1; 30/30 business cases and artifact check passed |
| Nop validation | `evidence/checks/author-validation/nop-result.json` | Recorded reward 0 / CANDIDATE_FAIL, as expected |
| GPT-5.6 Sol/xhigh standard trials | `evidence/trials/gpt-5-6-sol-xhigh-standard-01/`, `-02/`, `-03/`; final grades in `evidence/final-grades/` | Three independent completed solution runs. Their exact submissions were evaluated by the same final verifier, and all three genuinely failed. |
| DeepSeek V4.1 Flash adversarial trial | `evidence/trials/deepseek-cheat-01/` | Evaluator-approved alternative adversarial run completed with reward 0 |
| Check documentation | `docs/check-results.md` | Present |
| Trial documentation | `docs/trial-results.md` | Present |
| Failure analysis | `docs/failure-analysis.md` | Present |
| Commands and configurations | `docs/commands-and-configurations.md` and per-trial evidence | Present |
| Design and iteration discussion | `docs/task-design-and-iteration.md` and `evidence/design/` | Present |
| Required task README sections | `tasks/atomic-shard-rebalancing/README.md` | Present and accepted by the current complete implementation-rubric review |
| GitHub repository | `https://github.com/ducheng678/klavis-tb3-atomic-shard-rebalancing` | Public repository created |

The most important distinction is between a zero score and a valid assignment failure. Each counted result is supported by a public-contract violation under the final verifier; infrastructure failures are never converted into model failures.
