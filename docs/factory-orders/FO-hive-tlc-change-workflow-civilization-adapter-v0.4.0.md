---
doc_id: FO-HIVE-TLC-CHANGE-WORKFLOW-CIVILIZATION-ADAPTER
title: Civilization adapter for the portable TLC change workflow
doc_type: factory-order
status: draft
version: 0.4.0
created: 2026-08-30
owner: Michael Saucier
steward: Codex/OpenAI
model_authors:
  - Codex/OpenAI
  - Claude Fable 5/Anthropic
track: H
source_channel: authenticated_human_request
source_content_sha256: 37fb6f2694df168f89f2cf6644d169b7e65a54cecd5602ccd7927c9ce2010a8c
prior_source_content_sha256: 09edf1a80cbbbc5c82f3ad786f4c07d2261ca32f22671fec9464084527ccd7e5
fable_selected_for_current_source: false
tlc_contract_design: TLC-MODULAR-BOOTSTRAP-WORKFLOW-DESIGN@0.8.0
tlc_contract_design_blob: 16ba2640df5f39c2e75e388c4c8e331515957adf
tlc_contract_design_sha256: fee9388a6768b75f41101a18f8ebf7d2916e628f8f3bb25b730e6dbc37c4aa63
supersedes_draft_path: docs/factory-orders/FO-hive-tlc-change-workflow-civilization-adapter-v0.3.0.md
supersedes_draft_blob: 924797d10f96e404aa01d98c1c35e085b381daa7
supersedes_draft_sha256: 85f1c1d025ac9573fcfb04c965294079bcb0920e7e106b038b47f06ed8e977ab
design_base_commit: 138472d26708cc6277625210ad46ae9b12aee10c
canonical: false
operative: false
---

# Factory Order: Civilization adapter for the portable TLC change workflow

## Source and authorization

The authenticated Human requested two designs and a pause before any
implementation:

> update the TLC design with the crisp boundary explicitly defined. Pause when
> you have a completed design for my review. Then implement it.
>
> create a design document for revising the Civilization to implement the
> remaining footprint in the appropriate repo. Pause for my review to determine
> the next step, which may or may not be implementation.

The subsequent authenticated correction request is:

> Implement the fixes you proposed, start by estimating the time to complete,
> then once done compare the true time to the estimated time

The current authenticated request is:

> OK implement your guideline for inclusion of Fable Max infrequently, and only
> if selected by the Human. What do you recommend we do for the
> Design/Refine/Formalize sequence in either Human-driven workflows like
> semantic document ingest or for the machine-driven Issue scrape and refine?

The authenticated correction request and referenced model-authored correction
set have SHA-256
`09edf1a80cbbbc5c82f3ad786f4c07d2261ca32f22671fec9464084527ccd7e5`.
The current request and exact-source record have SHA-256
`37fb6f2694df168f89f2cf6644d169b7e65a54cecd5602ccd7927c9ce2010a8c`.
The later authenticated requests `implement this` and `implement 0.8.0`
authorize the local implementation buildout of the repaired TLC 0.8 contract
and this adapter. They do not select Fable for the current authoring pass and
do not authorize commit, push, pull request, release, installation, deployment,
Issue mutation, or another protected external effect.

## Intent

Revise Hive's existing Civilization Factory v1 seams so Civilization can call
the installed TLC `tlc-change-workflow`, persist and transport the exact records
it requires, enforce RepoX execution placement, and recover interrupted work
only from TLC's prerequisite-closed admitted evidence and continuation frontier.

The architecture boundary is fixed:

> TLC decides where work may safely continue. Hive performs the continuation.

Hive owns source-principal authentication, source capture, provider binding,
credentials, author and reviewer dispatch, attestation capture, candidate
persistence and separately authorized commit, task state, time, filesystem
placement, Git and GitHub effects, interruption detection, relaunch,
reconciliation, budgets, external-authority read-back, and operator projection.
Hive does not resolve Human policy, classify, select a TLC track or artifact
contract, validate candidate sufficiency, select reviewer routes or credit,
compute a continuation frontier, lower a floor, or manufacture authority.

Track H applies because this changes autonomous runtime orchestration, durable
recovery, repository mutation, GitHub effects, and optional Human-selected
collaboration routing.

## Existing implementation baseline

The design is a delta to `transpara-ai/hive` default-branch commit
`138472d26708cc6277625210ad46ae9b12aee10c`:

- Factory v1 already normalizes `issue_scan`, `human_idea`, and
  `completed_factory_order` channels.
- EventGraph accepted-order events are the durable queue and Work is the
  execution projection.
- The scheduler records `running` before an attempt and reconciles a running
  attempt without a terminal transition before retry.
- repository roots, issue-scan PR creation, reservation records, and runner
  worktrees are already Hive-owned effects.
- the current fixed `tlc-v1` twelve-stage allowlist duplicates lifecycle policy
  and must not substitute for the installed portable TLC workflow.
- a terminal EventGraph stage event can currently be committed before the Work
  artifact fails, and current replay repairs accepted-order linkage but not that
  stage-artifact split.

## Requirements

### R1 — Consume one exact TLC boundary contract

Hive consumes `tlc-change-continuation/v1` from the installed
`transpara-governance-gates` plugin and invokes
`transpara-governance-gates:tlc-change-workflow`. Hive validates every request
and response against the exact installed schema identity before persisting or
acting on it.

Missing, mismatched, unreadable, or invalid TLC skill/schema bytes block the
order. Hive does not fall back to its legacy fixed `tlc-v1` stage list for new
orders.

### R2 — Exact Human and Issue source capture

Hive appends predecessor-linked records only for externally observed Human
ideas, Human replies, and exact Issue snapshots containing the selected Issue
and comment bytes. Model outputs, artifact candidates, reviews, and preserved
partial evidence remain auxiliary records outside that chain. Optional Human
`collaboration_selection` records are also auxiliary and each names an
already-existing exact chain head. Hive retains exact
Human bytes, stable principal identity, capture identity, and transport
authentication evidence as separate fields. Issue and comment authors use
provider-authenticated actor references; `human_idea` uses the authenticated
operator identity. Hive maps no principal to a TLC policy and hard-codes no
default Human or collaborator.

Issue snapshots bind provider repository numeric identity and owner/name, Issue
number and node identity, included item authors and body digests, snapshot time,
and record digest. Edits and replies append snapshots; they never mutate prior
records.

### R3 — Fable Maximum is dispatched only from an exact Human selection

Hive transports authenticated Human semantic records to the installed workflow
without summarizing them. With no `collaboration_selection`, Hive dispatches no
Fable provider. It submits an exact author result when one exists or consumes
TLC's role-only author requirement; no ambient author is inferred. No selection
is not an ingestion failure.

Hive may capture a selection only from an explicit authenticated Human action.
It binds the exact source-chain head, one phase (`design`, `refine`, or
`formalize`), bounded objective, profile, and one permitted invocation. An Issue
scanner, bot, label, repository setting, model, retry rule, or Hive default may
not select or renew Fable. A new head, phase, or completed invocation strands
the selection.

When TLC validates a selection and emits the requirement, Hive resolves the
provider binding and holds credentials outside TLC records. It persists a
`not_started` intent, atomically persists the sole `dispatch_claimed` launch
claim, then dispatches and captures effective argv, runner identity, prompt/source transport and
in-session digests, exit state, and lineage for TLC validation. At this base, a
valid Michael Saucier selection resolves to Claude Fable 5 at `max` effort.
Recovery of a claim without an exact completed result records
`consumed_unknown` and never retries it. Invalid or failed selections receive
no fallback. Hive never treats selection or Fable output as review, Human
approval, or authority.

### R4 — Design, Refine, Formalize, and response capture

Hive carries TLC's collaboration phase in its projection but does not choose a
transition. `design` explores meaning, alternatives, constraints, and decision
points. `refine` resolves repository facts, scope, non-goals, tests, specialists,
contradictions, and authority boundaries. `formalize` renders the bounded result
into TLC's selected artifact contract and may introduce no new semantic
decision. If it would, Hive surfaces TLC's return to Refine.

The same sequence applies to semantic Human input and machine-captured Issues.
The transport changes who supplies the next exact source record, not the phase
semantics. Phase values create no stage, evidence, review, approval, or
authority.

Hive persists TLC questions and contradictions as a
`refinement_requested` record bound to the source-chain head and exposes it in
the operator projection. Posting the same questions to an Issue is an optional
GitHub effect requiring separate exact authority and an idempotency record.

A Human answer reaches TLC only after Hive captures it as an authenticated
Human record or new exact Issue snapshot. Sufficiency does not auto-submit an
FO. TLC selects an artifact contract; Hive dispatches the configured author and
persists candidate bytes; TLC validates the candidate; Hive commits exact bytes
only under separate authority; and Human acceptance must name the committed
blob, not a pre-commit candidate digest.

### R5 — Repository identity read-back

Before any repository mutation, Hive resolves source affinity to a local
checkout and reads back:

- provider numeric repository identity and normalized owner/name;
- Git common directory identity;
- origin identity;
- default/base branch and exact base commit; and
- current checkout head and cleanliness.

Hive supplies those observations to TLC. No worktree, branch, push, or PR may
proceed without an affirmative affinity verdict on the exact observations.

### R6 — RepoX owns RepoX worktrees, branches, and PRs

For an Issue in RepoX, Hive creates work only from RepoX's Git common directory.
The filesystem path may live under Hive's workspace, but its Git object store,
branch, push remote, PR repository, PR head repository, and PR base repository
must all be RepoX. Fork PRs are not admitted by this contract.

If work requires RepoY, Hive blocks that slice until an exact RepoY source and
authority record exists. It never redirects a RepoX Issue into a RepoY PR.

### R7 — Durable attempts and interruption detection

Before a runner or external effect, Hive writes a durable attempt or effect
intent bound to chain head, target repository, exact subject, provider binding,
budget, attempt ordinal, operation ID, and idempotency key. Process exit,
daemon restart, canceled context, or a running attempt without a terminal
record marks the attempt `interrupted`, never passed or failed by inference.

Hive may use its existing running-transition replay for single-daemon restart.
If a lease or heartbeat is added for live orphan detection, that mechanism is
Hive state and cannot become TLC evidence by itself.

### R8 — Evidence persistence and continuation-frontier consumption

Hive persists C0–C7 evidence candidates, subject scopes, prerequisite
references, and matching payload digests through its existing EventGraph and
Work adapters. EventGraph supplies causal ordering; Work supplies task/artifact
projection. Hive never admits, ranks, or calls one candidate highest.

On initial launch or recovery, Hive supplies all durable candidates plus fresh
repository, authority, and external-state observations to TLC. It consumes the
returned prerequisite-closed admitted set and deterministic
`continuation_frontier` per repository slice. Reordering submitted candidates
cannot change the frontier. Incomparable frontier points remain separate; Hive
continues only the next action named for the applicable point.

### R9 — Preserve partial bytes without promoting them

Before abandoning or replacing an interrupted worktree, Hive captures a
digested patch, untracked-file manifest, base/head identity, tool outcome, and
author lineage as non-authoritative `partial_evidence`. It then creates a new
clean recovery worktree at the exact commit named by the applicable frontier
point. The interrupted
worktree is quarantined until retention policy permits cleanup; it is not reset
or deleted before evidence capture.

Partial evidence may inform a new attempt but never satisfies a stage, review,
authority, branch, or PR predicate.

### R10 — Observe before every external-effect retry

Every Issue comment, branch push, PR create/update, status publication, and
review invocation uses a deterministic operation ID and idempotency key. After
interruption Hive observes the external system before retry and records exactly
one state:

- `exact` — the intended effect exists at the exact subject, so reuse it;
- `absent` — the effect is proven absent, so retry the same operation if still
  authorized;
- `conflict` — incompatible state exists, so block for Human disposition; or
- `unknown` — observation cannot prove exact or absent, so block.

The design does not claim distributed exactly-once semantics.

### R11 — Repair EventGraph and Work split records

Evidence and terminal records carry the same normalized payload digest in
EventGraph and Work. Replay must repair a missing Work projection from an exact
valid EventGraph payload before submitting that candidate to TLC. Until both
reads match, the candidate is absent. A conflicting twin is quarantined and
opens Human intervention. A Work-only record cannot invent EventGraph causal
truth.

### R12 — TLC route replaces fixed stage policy for new orders

The current `tlc-v1` fixed twelve-stage list remains readable historical
evidence for existing orders. New contract-version orders persist the TLC
workflow's collaboration phase, selected information state, track, applicable
artifact ladder, evidence subjects, and next safe action instead of advancing
an unconditional stage list. Hive never maps the fixed stage list onto Design,
Refine, Formalize, or C0–C7 evidence categories.

Hive validates record shape and exact identity but contains no classifier,
track selection, artifact-sufficiency logic, review-family policy, or authority
inference.

### R13 — Authority remains external and exact

An Issue, source chain, collaboration selection, Fable output, sufficiency
result, TLC report, evidence candidate, recovery record, passing test, or
existing capability cannot grant an effect.
Hive reads authenticated bounded external authority and submits it to TLC. Hive
performs an action only when TLC reports literal applicability to the exact
action and subject and Hive's own operating policy also permits dispatch.

The default terminal boundary is a draft PR or Human-required report. Readying,
merge, release, deployment, settings, production, Issue mutation, and every
other protected effect require their own authority.

### R14 — Operator projection without Hive UI

Hive exposes source-chain head, collaboration phase, optional Human selection,
remaining invocation count, requirement and attestation state, refinement need,
artifact contract and validation,
prerequisite-closed admitted evidence, frontier points, rejected candidates,
blocked slices, interruption state, preserved partials, review requirement and
credit, external observation state, authority applicability, and next safe
action as structured events/API projection. Site may render those TLC report
fields verbatim under `/ops/*` through a separate RepoX-owned change; Hive adds
no browser UI or fallback governance computation.

### R15 — Review dispatch is external to TLC

Hive dispatches reviewer providers only from TLC review-requirement records,
captures exact prompt/result/argv attestations, and returns them to TLC. Hive
may reject malformed transport data but never computes reviewer independence,
chooses a TLC review route, or marks a gate satisfied.

### R16 — Artifact roles remain distinct

Hive persists TLC artifact contracts, dispatches configured authors, records
candidate lineage and digests, and returns candidates for TLC validation. A
separately authorized commit is followed by exact blob read-back. Human
acceptance is captured only when its authenticated record names that committed
blob and action.

### R17 — Authentication and authority remain external facts

Hive authenticates principals and external observations but does not map Human
policy. It supplies authentication and authority evidence to TLC, retains
credentials outside the contract, and performs an effect only after both TLC
applicability and Hive operating policy permit it. A `human_idea`, Issue, label,
collaboration selection, requested action, TLC requirement, report, or candidate
is never write authority.

## Acceptance criteria

| ID | Criterion | Verification |
| --- | --- | --- |
| AC1 | With no authenticated Human selection, Human-idea and Issue paths dispatch no Fable provider and continue only through an exact author requirement/result with lineage. | No-selection adapter tests and forbidden-dispatch assertions. |
| AC2 | One valid Michael Saucier selection reaches Fable 5 at `max` only after the durable `dispatch_claimed` launch transition. Recovery without an exact result becomes `consumed_unknown`; stale, wrong-phase, exhausted, lower-effort, and failed selections do not fall back. | Selection-scope, dispatch-claim, crash, and exact-attestation matrix with fake runner. |
| AC3 | Machine scrape, bot, label, repository setting, prior head, prior phase, and retry policy cannot select or renew Fable. | Machine-selection and noninheritance negative tests. |
| AC4 | Design and Refine can iterate; Formalize begins only on TLC-reported sufficiency and returns to Refine on missing facts or semantic invention. Phase state supplies no evidence or authority. | Human-idea and Issue phase-transition tests. |
| AC5 | Two or more Human refinements append one valid immutable chain and require exact Human submission of the final candidate. | Intake/API integration test. |
| AC6 | An Issue in RepoX can produce only a RepoX-owned Git worktree, branch, push, and PR; RepoY and fork observations block before mutation. | Repository-affinity integration matrix. |
| AC7 | New orders use installed TLC route output and never the fixed `tlc-v1` allowlist as fallback. | Missing-plugin and legacy-compatibility tests. |
| AC8 | Killing the runner or daemon at each applicable C0–C7 boundary resubmits all candidates and consumes the TLC frontier per slice without ranking or later-partial credit. | Kill/restart, candidate-reordering, and incomparable-slice matrix. |
| AC9 | Interrupted dirty bytes are preserved as non-authoritative evidence and the recovery attempt starts in a clean worktree at the admitted commit. | Worktree recovery integration test. |
| AC10 | After a crash Hive never retries an external effect blindly: `exact` reuses, authenticated `absent` may repeat the same idempotent operation, and `conflict` or `unknown` blocks. | Observe-before-retry matrix with all four states. |
| AC11 | EventGraph-only evidence repairs Work before TLC submission; Work-only and conflicting twins remain inadmissible and require bounded repair or Human action. | Dual-store split and conflict tests. |
| AC12 | No Hive package contains TLC phase-transition, classification, track, sufficiency, evidence-admission, frontier, reviewer-route/independence, Human-policy, or authority-applicability logic. | Forbidden-copy static audit. |
| AC13 | An Issue alone never authorizes a worktree, push, PR, comment, ready transition, merge, or closure. | Authority negative tests. |
| AC14 | Hive projection reports unknown and blocked states honestly and adds no browser UI. | Projection/API tests and repository diff audit. |
| AC15 | Legacy Factory v1 orders and events remain readable and immutable. | Replay regression tests. |
| AC16 | Mermaid blocks render and the exact design subject passes repository documentation checks. | Mermaid CLI and link checks. |
| AC17 | The authorized local implementation passes focused tests and `make verify`; production admission remains separately gated. | Hive verification. |
| AC18 | Hive dispatches author and reviewer providers from TLC requirements, captures exact attestations, and cannot self-credit a review. | Dispatch/attestation and no-credit tests. |
| AC19 | Candidate validation and Human acceptance do not transfer across a changed committed blob. | Candidate/commit/acceptance binding matrix. |
| AC20 | Unauthenticated selection principals stop that collaboration pass without default identity; `human_idea`, Issue, label, request, selection, or report never becomes write authority. | Principal, selection, and authority negative matrix. |

## Required domain expertise

A data-recovery specialist must review the interruption model, prerequisite
closure, EventGraph/Work split repair, dirty-worktree preservation, and
observe-before-retry semantics. An authentication/identity specialist must
review principal authentication, attestation capture, and external-authority
binding before implementation approval.

## Expected implementation footprint

The authorized local implementation lives
in Hive's existing Factory v1 intake, scheduler, runner, EventGraph adapter,
Work adapter, issue-scan PR, operator-projection, and tests. It consumes the
installed TLC schema/skill rather than importing the TLC repository.

EventGraph and Work repository changes are not assumed: Hive should first use
their existing event and artifact interfaces. A necessary substrate change
becomes a separately sourced RepoX slice and PR under the TLC repository-affinity
contract. Site rendering is likewise a separate Site-repository slice.

## Stop conditions

Stop if implementation requires Hive to copy TLC policy, resolve Human policy,
choose a collaboration phase, select Fable without an exact authenticated Human
record, carry a selection across a chain head, phase, or completed invocation,
infer Fable or review credit, select reviewer independence, rank evidence,
compute a frontier, accept path strings as repository identity, retry an unknown
effect, promote partial bytes, destroy an interrupted worktree before evidence
capture, silently repair conflicting dual-store truth, or treat an Issue, Human
request, selection, TLC requirement, artifact contract, candidate, or report as
authority.

Stop before any formal independent review if no reviewer is materially independent from every
OpenAI and Anthropic design author/collaborator lineage.

## Non-authorizations

This Factory Order does not itself create authority. The later authenticated
Human request authorizes local implementation and proportionate local review,
but not commit, push, PR, Issue comment, formal provider review invocation,
status, merge, release, installation, deployment, provider setting, production
action, value allocation, or another protected effect.
