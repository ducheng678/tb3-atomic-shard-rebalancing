# Submission checklist

This checklist separates recorded evidence from work that still must be completed before claiming full compliance with the hiring assignment.

## Already packaged

- [x] One complete original task at `tasks/atomic-shard-rebalancing/`.
- [x] Public instruction, normative contracts, starter engine, reproducible examples, private reference solution, separate verifier, and pinned Docker environments.
- [x] Task-design and iteration narrative from the original r1 baseline through H1, H2, and H3.
- [x] Static-check evidence: 20 PASS and 2 NOT_APPLICABLE.
- [x] Recorded Docker build PASS.
- [x] Recorded oracle reward 1 with all 30 business scenarios passing.
- [x] Recorded nop/starter reward 0.
- [x] Recorded GPT implementation-rubric output and the current criterion status.
- [x] Three completed Codex standard runs and their final submitted source trees.
- [x] One valid DeepSeek V4.1 Flash adversarial run with reward 0.
- [x] Concise failure analysis for all three Codex submissions under the same final verifier.
- [x] Three independent Codex submissions with genuine reward-0 results.
- [x] Final student/verifier isolation and stop-before-verifier behavior documented.
- [x] Selected commands, configs, raw score results, verifier summaries, and CTRF reports.
- [x] Tracked-file secret scan passed before remote creation or first push.

## Contributor actions required

- [ ] Rewrite the four required sections in `tasks/atomic-shard-rebalancing/README.md` completely in the contributor's own words: Difficulty explanation, Solution explanation, Verification explanation, and Relevant experience. Do not ask an LLM to produce the final wording. The personal-experience section must be factually supplied by the contributor.
- [ ] Review all documents for accuracy and confirm the first-person design account matches what the contributor is prepared to discuss.
- [ ] Choose a repository license, or deliberately leave the repository unlicensed.
- [ ] Choose the GitHub repository name and whether it is public or private.

## Evidence gaps before claiming full assignment compliance

- [ ] Run the exact packaged bytes through the current complete TB3 static, implementation-rubric, Docker build, oracle, and nop pipeline. The present records were collected as scoped checks rather than one final all-in-one CI run.
- [ ] Resolve the assignment's Claude/Opus standard-trial requirement or obtain a written evaluator waiver. No Claude or Anthropic run is included.
- [ ] Resolve the remaining adversarial-model requirement with the evaluator. DeepSeek substituted for the blocked Codex `/cheat` run under the later allowance, but no Claude adversarial run is included.

## Do not overclaim

- Do not describe the current rubric evidence as a complete current 35/35 model pass.
- Do not claim that one reward-0 adversarial run proves the verifier is impossible to bypass.
- Do not claim official TB3 acceptance; this is an independent hiring-evaluation package and no upstream PR is required.
