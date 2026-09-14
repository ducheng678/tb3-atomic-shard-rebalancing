# Assignment requirements matrix

The hiring assignment asks for one original Terminal-Bench 3 task, current CI-style checks, standard and adversarial model evaluations, a GitHub repository, and documentation of commands, configurations, results, and failure analysis. The matrix below states exactly what this package demonstrates.

| Requirement | Evidence in this repository | Status |
| --- | --- | --- |
| Original TB3 task | `tasks/atomic-shard-rebalancing/` | Present |
| Static checks | `evidence/checks/static/` | Recorded PASS: 20 pass, 2 not applicable; exact-final-revision full rerun still recommended |
| Implementation rubric | `evidence/checks/rubric/status.json` | Recorded GPT full review: 33 pass, 1 fail, 1 N/A; the current criterion passes a scoped author check, but no complete current model rerun is claimed |
| Docker build | `evidence/checks/author-validation/build-result.json` | Recorded PASS |
| Oracle validation | `evidence/checks/author-validation/oracle-result.json` | Recorded reward 1; 30/30 business cases and artifact check passed |
| Nop validation | `evidence/checks/author-validation/nop-result.json` | Recorded reward 0 / CANDIDATE_FAIL, as expected |
| Codex standard trials | `evidence/trials/codex-standard-01/`, `-02/`, `-03/`; final grades in `evidence/final-grades/` | Three independent completed solution runs. Their exact submissions were evaluated by the same final verifier, and all three genuinely failed. |
| Claude/Opus standard trials | None | Not run under the contributor's model restriction; evaluator waiver or runs would be needed for literal compliance |
| Codex adversarial trial | DeepSeek substitution record at `evidence/trials/deepseek-cheat-01/` | The original Codex `/cheat` request was platform-blocked; later evaluator guidance allowed GLM 5.3 or DeepSeek V4.1 Flash as a replacement. DeepSeek completed with reward 0 |
| Claude/Opus adversarial trial | None | Not run; evaluator clarification or waiver required |
| Check documentation | `docs/check-results.md` | Present |
| Trial documentation | `docs/trial-results.md` | Present |
| Failure analysis | `docs/failure-analysis.md` | Present |
| Commands and configurations | `docs/commands-and-configurations.md` and per-trial evidence | Present |
| Design and iteration discussion | `docs/task-design-and-iteration.md` and `evidence/design/` | Present |
| Human-authored task README sections | `tasks/atomic-shard-rebalancing/README.md` | Pending contributor rewrite |
| GitHub repository | Local repository staging directory | Remote creation pending repository name and visibility |

The most important distinction is between a zero score and a valid assignment failure. Each counted result is supported by a public-contract violation under the final verifier; infrastructure failures are never converted into model failures.
