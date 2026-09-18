# TLC change-workflow operator boundary

Status: source-only implementation. This runbook does not enable the path,
install a plugin, configure a runner, grant authority, or admit production use.

## Bounded invocation

`hive factory tlc-change-workflow` is the only command surface added for the
`tlc-change-continuation/v1` intake-to-report call:

```text
hive factory tlc-change-workflow \
  --human <operator> \
  --store <eventgraph-dsn> \
  --invocation <exact-invocation.json> \
  --identity <configured-exact-identity.json> \
  --plugin-root <absolute-installed-plugin-root> \
  --runner <absolute-runner-executable> \
  --timeout 15m
```

The identity file contains exactly `plugin_name`, `plugin_version`,
`skill_name`, `contract_version`, `schema_sha256`, `skill_sha256`, and
`contract_core_sha256`, plus `runner_sha256` and `runner_argv_sha256`. Hive
independently reads `.codex-plugin/plugin.json`
and `adapter-binding.json`, hashes the installed schema, skill, and shipped
conformance core plus the configured runner bytes and effective argv, and
refuses a mismatch before passing source bytes to the runner. The runner receives exact invocation JSON on stdin
and must emit only exact report JSON on stdout. No shell or legacy `tlc-v1`
fallback is used.

## Durable failure behavior

Before launch, Hive records matching EventGraph and Work source/invocation
twins and wins one durable compare-and-swap claim. After a successful call it
records the exact report twin before completing the claim. Restart behavior is:

- exact completed report: validate and reuse without dispatch;
- claimed call with an exact persisted report: complete and reuse;
- claimed call without an exact report: record `consumed_unknown` and stop;
- `consumed_unknown`: stop permanently; a fresh authenticated source/selection
  is required where policy permits one.

Dirty interrupted worktrees are captured as non-authoritative quarantined
partial evidence. `runner.RecoverContinuationWorktree` persists that evidence
before creating a separate worktree at the admitted base commit. It never
cleans, resets, deletes, or resumes the interrupted directory.

## Continuation effects

Continuation worktree creation must use
`runner.RecoverContinuationWorktree`. Continuation issue-scan draft-PR creation
must use `Runtime.CreateIssueScanDraftPRFromApprovedContinuation`. Both place
the TLC RepoX/frontier check and the four-state external observation immediately
before the mutation callback:

- `exact`: reuse; do not mutate;
- authenticated `absent`: the same idempotent operation may run once only when
  TLC continuation, exact external authority, and budget are all present;
- `conflict` or `unknown`: stop for Human disposition.

The older issue-scan method remains only for legacy records and is not a valid
entrypoint for `tlc-change-continuation/v1` work.

## Authority boundary

The command, report, durable claim, EventGraph/Work record, test, and this
runbook grant no worktree, branch, push, PR, comment, ready, merge, release,
installation, adoption, runtime, or deployment authority. The source-only path
remains inactive until separately reviewed and explicitly enabled.
