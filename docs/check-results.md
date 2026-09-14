# Check results

## Summary

| Check | Result | Evidence |
| --- | --- | --- |
| Pinned TB3 static checks | 22/22 scripts exited successfully; 2 had no applicable configuration | [`static/summary.json`](../evidence/checks/static/summary.json), [`static/acceptance.json`](../evidence/checks/static/acceptance.json) |
| Agent/verifier Docker build | PASS | [`build-result.json`](../evidence/checks/author-validation/build-result.json) |
| Oracle validation | reward 1; artifact PASS; 30/30 business scenarios PASS | [`oracle-result.json`](../evidence/checks/author-validation/oracle-result.json) |
| Nop/starter validation | reward 0 / `CANDIDATE_FAIL` | [`nop-result.json`](../evidence/checks/author-validation/nop-result.json) |
| Implementation rubric | Current complete GPT-5.6 Sol/max review: 34 PASS, 0 FAIL, 1 NOT_APPLICABLE | [`current-main-model-review.json`](../evidence/checks/rubric/current-main-model-review.json), [`status.json`](../evidence/checks/rubric/status.json) |
| Final Codex series | Three independent genuine reward-0 submissions under one final verifier | [`series.json`](../evidence/final-grades/series.json) |
| DeepSeek adversarial run | Valid reward 0 | [`result.json`](../evidence/final-grades/deepseek-cheat-01/result.json) |

## Static checks

The 22 pinned shell checks came from Terminal-Bench commit
`83c7a6172d629c6575b785ab12c8db787bb2e323`, and every script exited successfully.
Twenty exercised applicable configuration. The Compose host-bind and GPU-type
checks had no corresponding configuration because this task has neither an
environment Compose file nor a GPU type declaration. There were no static-check
errors. The complete command ledger is
[`static/commands.json`](../evidence/checks/static/commands.json).

## Build, oracle, and nop

The recorded verifier image built successfully. The private reference then
passed the artifact check and all 30 business scenarios, producing reward 1.
The unchanged starter produced reward 0, demonstrating that the task is not
already solved. The author report also records a passing legal alternative
profile and two rejected semantic controls:
[`author-validation/report.md`](../evidence/checks/author-validation/report.md).

## Implementation rubric

The current complete 35-criterion review used the model running this Codex
conversation: `openai/gpt-5.6-sol`, reasoning effort `max`, Codex
`0.154.0-alpha.6.2`. It applied the unmodified rubric directly to the current
task directory and produced 34 PASS, zero FAIL, and one NOT_APPLICABLE. The
non-applicable criterion is `do_not_modify_enforced`: the task does not tell the
student to preserve a concrete existing artifact unchanged. Therefore every
applicable criterion passes.

The historical GPT-5.6 Sol/xhigh counts and former `task_readme` failure remain
preserved as a separate block in `status.json`; they are not presented as the
current result.

## Result integrity

All three counted standard results have:

- a normally completed Codex/Sol execution;
- reward 0 from the same final verifier;
- a passing artifact-admission check;
- no Harbor exception, collection error, or verifier framework error; and
- a concrete public-contract violation in the submitted program.

The repository contains no API key, OAuth token, provider secret, or credential
file. A final secret scan should still be run immediately before the first push.

## Remaining assignment-level actions

The existing check records were collected in scoped stages rather than as one
final all-in-one CI invocation over the packaged repository. A literal claim of
complete assignment compliance would also require the model configurations that
the contributor chose not to run, or an evaluator waiver. These items are kept
in [`SUBMISSION-CHECKLIST.md`](../SUBMISSION-CHECKLIST.md) rather than being
presented as completed results.
