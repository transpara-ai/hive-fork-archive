# TLC continuation reconciliation

**Version:** v0.1.0  
**Date:** 2026-09-18  
**Issue:** `transpara-ai/hive#326`  
**Historical snapshot:** `transpara-ai/hive#327` at
`5ea94c9637d0dd3cb3c4be46cc4218b463c574ab`

## Decision

Preserve the historical `tlc-change-continuation/v1` implementation as source
evidence, but do not restore its embedded Factory v1 runtime on current Hive.
The snapshot was built on `pkg/hive/factoryv1`,
`FactoryV1EventGraphStore`, and `FactoryV1WorkStore`. Current Hive deliberately
removed that runtime in commit
`ff20fdb46a3798ba59f8c51bdc9edc8db56f6767`.

Current TLC also assigns persistence, retries, crash recovery, worktree
recovery, provider execution, and effect guards to the caller or Hive. It does
not publish the historical `tlc-change-continuation/v1` machine contract.
Restoring the snapshot wholesale would therefore recreate a retired runtime
and bind Hive to a contract that canonical TLC 0.1.2 does not expose.

## Capability disposition

| Historical capability | Current disposition | Current evidence |
|---|---|---|
| Exact provider-attempt replay | Retained in the Civilization provider boundary. | `pkg/hive/civilization/codex_provider_test.go::TestCodexCLIReplaysExactAttemptFromDurableReceipt` |
| Durable run restart and idempotency | Retained in the EventGraph-backed Civilization runtime. | `pkg/hive/civilization_eventgraph_test.go::TestCivilizationEventGraphRestartAndIdempotency` |
| Recovered worktree validation | Retained through artifact verification and reimplementation when recorded output is unavailable. | `pkg/hive/civilization/runner_provider_test.go::TestRunnerArtifactRecoveryAndTampering`; `pkg/hive/civilization/engine_test.go::TestEngineReimplementsWhenRecoveredWorktreeLostRecordedDiff` |
| Exact base and independently verified publication | Retained in repository effects. | `pkg/hive/civilization/repository_effects_test.go::TestGitHubEffectsPrepareUsesExactBaseAndPublishDefaultsOff`; `TestGitHubEffectsPublishesOnlyIndependentlyVerifiedExactDiff` |
| Separate authority for repository effects | Retained in repository effects and issue-scan draft-PR creation. | `pkg/hive/civilization/repository_effects_test.go::TestGitHubEffectsRequiresSeparateAuthorityForEnabledEffects`; `pkg/hive/issue_intake_test.go::TestCreateIssueScanDraftPRFromApprovedRequestRequiresApprovedDecision` |
| Observe before repeating a possibly consumed external write | Retained for draft-PR creation through durable reservation and receipt checks. | `pkg/hive/issue_intake_test.go::TestCreateIssueScanDraftPRFromApprovedRequestRefusesAfterReservationWithoutReceipt`; `TestCreateIssueScanDraftPRFromApprovedRequestRefusesAfterReceipt` |
| Exact-head review evidence | Retained in issue-scan review and ready-state processing. | `pkg/hive/issue_intake_test.go::TestRecordIssueScanAdversarialReviewReceiptRejectsHeadMismatch`; `TestProgressIssueScanLifecycleReadyPRFinalizerRejectsMovedHead` |
| Expiring Human authority | Retained in the current authority-decision boundary. | `pkg/hive/authority_decision_expiry_test.go::TestApprovedIssueScanDraftPRAuthorityRequestForRunEnforcesDecisionExpiry` |
| Historical plugin-byte identity contract | Retired. Canonical TLC is an externally installed workflow skill and exposes no provider-execution protocol to Hive. | TLC 0.1.2 execution and transition boundary |
| Historical Factory v1 EventGraph/Work twins | Retired with the embedded Factory v1 runtime. | Hive commit `ff20fdb46a3798ba59f8c51bdc9edc8db56f6767` |
| Generic `consumed_unknown` continuation state | Modified. Current external effects use operation-specific reservations, receipts, exact-head observations, and indeterminate outcomes. | Draft-PR and ready-PR tests named above; `cmd/hive/factory_issue_scan_ready_pr_github_test.go::TestIssueScanReadyPRGitHubClientPostDispatchFailureIsIndeterminate` |

## Regression rule

Future changes to the current evidence paths must preserve the named behavioral
tests or replace them with equal or stronger tests in the same change. A file,
branch, or package name does not establish compatibility. Compatibility is
established by the behavior and exact-subject checks exercised above.

The historical snapshot remains reviewable at Hive PR #327. It must not be
used as a runtime dependency, copied into current Hive, or deleted as the only
source record until the cross-repository retirement record has merged.

## Verification

The current-main reconciliation requires:

1. all named focused tests;
2. `make verify` in the current Hive checkout; and
3. confirmation that the change adds no runtime command, route, provider
   invocation, external effect, or authority grant.

## Changelog

- v0.1.0: Records the current-base disposition of the unpublished continuation
  implementation and binds retained guarantees to current behavioral tests.
