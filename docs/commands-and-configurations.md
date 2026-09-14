# Commands and configurations

## Task resources

The task configuration in
[`task.toml`](../tasks/atomic-shard-rebalancing/task.toml) fixes the following
limits:

```toml
[verifier]
timeout_sec = 900

[agent]
timeout_sec = 14400

[environment]
build_timeout_sec = 600
cpus = 4
memory_mb = 4096
storage_mb = 10240
gpus = 0
```

## GPT-5.6 Sol/xhigh standard runs

The human-readable direct CLI form of the standard configuration is:

```bash
harbor run -p tasks/atomic-shard-rebalancing \
  --agent codex \
  --model openai/gpt-5.6-sol \
  --env docker \
  --yes \
  --ae CODEX_FORCE_AUTH_JSON=1 \
  --ak reasoning_effort=xhigh
```

The actual jobs were launched from frozen one-attempt JSON configurations:

```bash
harbor run --config <frozen-config.json> --yes
```

Each config set `n_attempts=1`, `n_concurrent_trials=1`, and
`retry.max_retries=0`. The material, credential-free configurations are
retained for each run:

- [`run 1 config`](../evidence/trials/gpt-5-6-sol-xhigh-standard-01/run-config.json)
- [`run 2 config`](../evidence/trials/gpt-5-6-sol-xhigh-standard-02/run-config.json)
- [`run 3 config`](../evidence/trials/gpt-5-6-sol-xhigh-standard-03/run-config.json)

Credentials are not part of the command templates or repository. Authentication
was supplied to the host process.

## DeepSeek adversarial run

The alternative adversarial job also used one attempt, concurrency one, and no
automatic retry. Its model route was `deepseek-v4.1-flash:free` through an
OpenAI-compatible TokenHarbor endpoint, with reasoning enabled. The agent ran
through Terminus-2 and the same frozen task and resource budgets.

The material non-secret configuration is in
[`run-config.json`](../evidence/trials/deepseek-cheat-01/run-config.json).

The provider key was passed only in the host environment and is not present in
the saved config, process argument list, student environment, or repository.

## Static checks

The static suite ran the 22 pinned Terminal-Bench shell checks against the task
inside a Docker container. The credential-free command arrays, exit codes,
durations, and captured-output filenames are recorded in
[`commands.json`](../evidence/checks/static/commands.json); machine-local source
roots are represented by descriptive placeholders. Each check followed this
form:

```bash
docker exec -w /app/task-under-review <container> \
  env LC_ALL=C.UTF-8 LANG=C.UTF-8 \
  bash /opt/terminal-bench-pinned/scripts/checks/<check>.sh \
  /app/task-under-review/atomic-shard-rebalancing
```

## Build and verifier entry point

The task images are defined by:

```bash
docker build -f tasks/atomic-shard-rebalancing/environment/Dockerfile \
  tasks/atomic-shard-rebalancing/environment

docker build -f tasks/atomic-shard-rebalancing/tests/Dockerfile \
  tasks/atomic-shard-rebalancing/tests
```

The verifier container executes
[`tests/test.sh`](../tasks/atomic-shard-rebalancing/tests/test.sh), which invokes
the trusted Python verifier and emits `reward.txt`, `summary.json`, and
`ctrf.json` on the verifier side. The oracle stages the private reference engine;
the nop check stages the unchanged starter. Their recorded outputs are linked
from [`check-results.md`](check-results.md).

## Reproduction guidance

Do not reuse one student's working directory or model session for another run.
For each standard trial, start from the task starter, retain the full time and
resource budgets, disable automatic retry, save the final engine source and its
manifest, and apply the same final verifier. A provider or container failure is
an invalid attempt, not a reward-0 student result.
