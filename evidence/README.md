# Evidence index

This directory contains only evidence needed to support the reported assignment
results:

- `design/iteration-summary.json` records the fixed baseline and the H1/H2/H3
  decisions described in the design narrative.
- `checks/` contains static, build, oracle, nop, and rubric results.
- `trials/` contains the material run configuration, submitted source tree, and
  source manifest for each reported execution.
- `final-grades/` contains the final verifier result, summary, and CTRF report
  for all three Codex submissions and the DeepSeek adversarial submission.

The three Codex source trees are distinct independent submissions. Their
individual manifests bind the exact files evaluated by the same final verifier.
Infrastructure failures are not included as reward-zero task results.
