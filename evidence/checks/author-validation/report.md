# Author validation summary

Candidate: `atomic-shard-rebalancing`, independently derived from the fixed
`shard-txn-recovery r1` baseline.

- The reference solution received reward 1: artifact admission and all 30
  business scenarios passed. Nineteen scenarios retain baseline behavior and
  eleven exercise atomic rebalancing.
- A legal alternative policy also passed 30/30. It changes placement-publisher
  selection, waits for read-release acknowledgements before successful replies,
  and uses a 120-round maintenance interval instead of 30. It demonstrates
  protocol freedom while sharing the same core transaction primitives.
- The highest measured use of the public 1,000/2,000/10,000-round progress
  bounds was 58/205/332 rounds for the reference and 58/190/301 for the legal
  alternative. The public limits and 900-second verifier watchdog were retained.
- The unchanged starter received reward 0 with `CANDIDATE_FAIL` and no framework
  error.
- The `keep-old-values` and `reinstall-old-image` controls were rejected by the
  independent serial-history oracle in the scenarios where their intended bugs
  became observable. Their failures were semantic inconsistencies, not progress
  timeouts.
- Nineteen interface, metadata-ordering, serial-history, and scale-bound checks
  passed, and the public exchange reproduction exited successfully.

These finite checks establish feasibility, measured budget headroom, and
discrimination of selected semantic errors. They do not prove correctness over
all schedules, guarantee that a student will fail, or replace the recorded model
rubric and student trials.
