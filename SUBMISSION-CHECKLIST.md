# Submission checklist

This checklist separates recorded evidence from work that still must be completed before claiming full compliance with the hiring assignment.

## Already packaged

- [x] One complete original task at `tasks/atomic-shard-rebalancing/`.
- [x] Public instruction, normative contracts, starter engine, reproducible examples, private reference solution, separate verifier, and pinned Docker environments.
- [x] Task-design and iteration narrative from the original r1 baseline through H1, H2, and H3.
- [x] Static-check evidence: all 22 scripts exited successfully; two had no applicable Compose or GPU configuration.
- [x] Recorded Docker build PASS.
- [x] Recorded oracle reward 1 with all 30 business scenarios passing.
- [x] Recorded nop/starter reward 0.
- [x] Current complete GPT-5.6 Sol/max implementation-rubric review: 34 PASS, 0 FAIL, 1 NOT_APPLICABLE.
- [x] Three completed GPT-5.6 Sol/xhigh standard runs with their final submitted source trees.
- [x] One valid DeepSeek V4.1 Flash adversarial run with reward 0.
- [x] Concise failure analysis for all three GPT-5.6 Sol/xhigh submissions under the same final verifier.
- [x] Three independent GPT-5.6 Sol/xhigh submissions with genuine reward-0 results.
- [x] Final student/verifier isolation and stop-before-verifier behavior documented.
- [x] Selected commands, configs, raw score results, verifier summaries, and CTRF reports.
- [x] Tracked-file secret scan passed before remote creation or first push.

## Contributor actions required

- [ ] Review all documents for accuracy and confirm the first-person design account matches what the contributor is prepared to discuss.
- [ ] Choose a repository license, or deliberately leave the repository unlicensed.
- [x] Publish the public GitHub repository as `ducheng678/klavis-tb3-atomic-shard-rebalancing`.

## Evidence gaps before claiming full assignment compliance

- [ ] Run the exact packaged bytes through the current complete TB3 static, implementation-rubric, Docker build, oracle, and nop pipeline. The present records were collected as scoped checks rather than one final all-in-one CI run.

## Do not overclaim

- Describe the current rubric result as 34 PASS, 0 FAIL, 1 NOT_APPLICABLE rather than 35 PASS; `do_not_modify_enforced` does not apply.
- Do not claim that one reward-0 adversarial run proves the verifier is impossible to bypass.
- Do not claim official TB3 acceptance; this is an independent hiring-evaluation package and no upstream PR is required.
