---
doc_id: HIVE-DF-OPERATOR-UI-CONTRACT
title: Hive Operator UI Contract
doc_type: runtime-contract
status: draft
version: 0.2.1
created: 2026-06-03
updated: 2026-06-17
owner: Michael Saucier
steward: assistant
project: dark-factory
canonical: false
runtime_repo: transpara-ai/hive
ui_repo: transpara-ai/site
canonical_route: /ops/hive
---

# Hive Operator UI Contract

## Revision History

| Version | Date | Description |
|---------|------|-------------|
| 0.1.0 | 2026-06-03 | Initial repo-ready runtime contract for the Dark Factory Human Operator UI. |
| 0.1.1 | 2026-06-04 | Added standard Transpara frontmatter, semver, and revision history. |
| 0.1.2 | 2026-06-08 | Committed to version control (rescued from the uncommitted working tree before age-out); owner corrected to Michael Saucier per the Dark Factory doc convention. Still a pre-acceptance draft — none of these endpoints exist yet (tracked by hive#127). |
| 0.1.3 | 2026-06-09 | Added phase-1 read-only model-selection data on the operator projection for hive#128; catalog loading is startup-static and edit/hot-reload surfaces remain future work. |
| 0.2.0 | 2026-06-09 | Added phase-2 Hive-owned model catalog hot reload metadata and validated per-run model overrides for hive#128. |
| 0.2.1 | 2026-06-17 | Added read-only runtime evidence projection fields that separate queued launch intent from observed Hive runtime events and deployment proof. |

## Boundary

Hive is not the browser UI owner. Hive owns runtime orchestration, agent loops, diagnostics, authority checks, event emission, and projections. Site owns the human operator UI.

Hive must expose structured data that Site can render:

- Intakes.
- Factory runs.
- Run projections.
- Events.
- Approval requests.
- Artifacts.
- Agent states.
- Resource usage.
- Guardian alerts.

## Required runtime endpoints

```text
POST /api/hive/intakes/{id}/derive
POST /api/hive/runs
GET  /api/hive/runs/{id}
GET  /api/hive/runs/{id}/events
GET  /api/hive/runs/{id}/approvals
POST /api/hive/approvals/{id}/resolve
```

## Launch request

```json
{
  "operator_id": "user_123",
  "intake_id": "intake_123",
  "title": "Build onboarding flow",
  "brief": {},
  "sources": [],
  "authority": { "initial_level": "required" },
  "budget": { "max_iterations": 20, "max_cost_usd": 50 },
  "model_overrides": [
    {
      "role": "guardian",
      "model": "api-sonnet",
      "auth_mode": "api-key",
      "max_cost_per_call_usd": 3.5
    }
  ],
  "target_repos": ["transpara-ai/site"]
}
```

## Launch response

```json
{
  "run_id": "run_123",
  "status": "queued",
  "first_event_id": "evt_123"
}
```

## Projection response

```json
{
  "run_id": "run_123",
  "title": "Build onboarding flow",
  "status": "active",
  "active_phase": "design",
  "guardian_state": "clear",
  "agents": [],
  "pipeline": [],
  "approvals": [],
  "events": [],
  "artifacts": [],
  "resources": {},
  "runtime_evidence": {
    "source": "eventgraph",
    "status": "running|completed|not_observed",
    "last_run": {
      "started_event_id": "evt_started",
      "conversation_id": "conv_hive_123",
      "started_at": "2026-06-17T12:00:00Z",
      "seed_idea": "Build onboarding flow",
      "repo_path": "/Transpara/transpara-ai/repos/hive",
      "completed_event_id": "evt_completed",
      "completed_at": "2026-06-17T12:10:00Z",
      "agent_count": 4,
      "duration_ms": 600000,
      "total_cost": 0
    },
    "agent_events": {
      "scope": "events_since_latest_hive.run.started",
      "spawned": 1,
      "stopped": 0,
      "observed_active": 1,
      "active_agents": [
        {
          "name": "builder",
          "role": "implementer",
          "model": "claude-opus-4-6",
          "actor_id": "actor_builder",
          "spawned_event_id": "evt_spawned",
          "spawned_at": "2026-06-17T12:01:00Z"
        }
      ]
    },
    "last_queued_run_request": {
      "event_id": "evt_requested",
      "conversation_id": "conv_hive_run_123",
      "run_id": "run_123",
      "title": "Build onboarding flow",
      "operator_id": "user_123",
      "status": "queued",
      "target_repos": ["transpara-ai/site"],
      "authority_initial_level": "Required",
      "authority_scope": "site:onboarding",
      "budget_max_iterations": 20,
      "budget_max_cost_usd": 50,
      "source_event_id": "evt_source",
      "brief_event_id": "evt_brief",
      "evidence_kind": "queued_request_not_runtime_start",
      "created_at": "2026-06-17T11:58:00Z"
    },
    "artifacts": [
      {
        "event_id": "evt_artifact",
        "run_id": "run_123",
        "artifact_id": "artifact_factory_brief",
        "label": "factory_brief",
        "title": "Factory brief",
        "media_type": "text/markdown",
        "uri": "eventgraph://artifact/artifact_factory_brief",
        "summary": "brief ready for operator inspection",
        "producer_actor_id": "actor_builder",
        "causes": [
          {
            "event_id": "evt_spawned",
            "event_type": "hive.agent.spawned",
            "scope": "run"
          }
        ],
        "cause_status": "caused",
        "created_at": "2026-06-17T12:02:00Z"
      }
    ],
    "run_events": [
      {
        "event_id": "evt_artifact",
        "event_type": "factory.artifact.created",
        "conversation_id": "conv_hive_run_123",
        "created_at": "2026-06-17T12:02:00Z",
        "causes": ["evt_spawned"],
        "inspector_kind": "curated_eventgraph_event",
        "content": {
          "run_id": "run_123",
          "artifact_id": "artifact_factory_brief",
          "label": "factory_brief",
          "title": "Factory brief",
          "media_type": "text/markdown",
          "uri": "eventgraph://artifact/artifact_factory_brief",
          "summary": "brief ready for operator inspection",
          "producer_actor_id": "actor_builder"
        }
      }
    ],
    "causal_graph": {
      "scope": "latest_run_conversation",
      "conversation_id": "conv_hive_run_123",
      "limit": 50,
      "truncated": false,
      "nodes": [
        {
          "event_id": "evt_spawned",
          "event_type": "hive.agent.spawned",
          "label": "Agent spawned: implementer/builder",
          "scope": "run",
          "created_at": "2026-06-17T12:01:00Z"
        },
        {
          "event_id": "evt_artifact",
          "event_type": "factory.artifact.created",
          "label": "Artifact: Factory brief",
          "artifact_id": "artifact_factory_brief",
          "scope": "run",
          "created_at": "2026-06-17T12:02:00Z"
        }
      ],
      "edges": [
        {
          "from_event_id": "evt_spawned",
          "to_event_id": "evt_artifact",
          "scope": "run"
        }
      ]
    },
    "limitations": [
      "factory.run.requested is queued launch intent, not runtime-start proof",
      "hive.run.started and hive.run.completed prove Hive runtime event emission, not production deployment",
      "runtime start, agent, and completion events are correlated by EventGraph conversation ID",
      "runtime event order follows EventGraph store order, not wall-clock timestamp order",
      "artifact and causal graph projections are bounded by the operator projection limit"
    ]
  },
  "model_selection": {
    "source": "hive",
    "catalog_source": "embedded-defaults",
    "tier_defaults": {
      "judgment": "gpt-6-astra",
      "execution": "claude-fable-5-1",
      "volume": "gpt-5.6-sol"
    },
    "loaded_at": "2026-06-09T09:00:00Z",
    "reload_mode": "startup-static|hot-reload",
    "hot_reload": false,
    "last_reload_at": "2026-06-09T09:10:00Z",
    "models": [],
    "assignments": [],
    "errors": []
  }
}
```

## Runtime evidence projection

Hive exposes `runtime_evidence` as a read-only part of `/api/hive/operator-projection`.
Site may render this data as operational evidence, but Hive remains the source of truth and EventGraph remains the audit record.

The projection includes:

- `status`: `not_observed` when no `hive.run.started` event exists in the bounded projection window, `running` after the latest `hive.run.started`, and `completed` after a later `hive.run.completed`.
- `last_run`: the latest observed Hive runtime start and completion event IDs, EventGraph conversation ID, timestamps, seed idea, repo path, agent count, duration, and cost fields when present.
- `agent_events`: spawn/stop counts and active-agent observations scoped to events since the latest `hive.run.started`.
- `last_queued_run_request`: the latest `factory.run.requested` event with run ID, operator, target repos, authority envelope, budget, and source/brief event references.
- `artifacts`: newest-first `factory.artifact.created` events from the latest run conversation, including artifact metadata and EventGraph causes.
- `run_events`: bounded event-inspector payloads for the latest run conversation, including event ID, type, timestamp, causes, and allowlisted curated JSON event content. Non-allowlisted event types remain inspectable by ID/type/causes but report `content_error` instead of raw content.
- `causal_graph`: run-local graph nodes and edges derived from EventGraph event IDs and causes. External or out-of-window causes are represented explicitly rather than invented.
- `limitations`: machine-readable boundary text Site can surface or log when presenting runtime evidence.

Important boundaries:

- `factory.run.requested` records accepted queued launch intent only. It is not proof that Hive started a runtime, spawned agents, touched a target repo, or completed work.
- `hive.run.started`, `hive.agent.spawned`, `hive.agent.stopped`, and `hive.run.completed` are observed Hive runtime events. They prove event emission into Hive's EventGraph store, not production deployment, human authorization, source-repo adoption, or protected side effects.
- Runtime start, agent, and completion events are correlated by EventGraph conversation ID. A completion event from another conversation is not attached to the latest run projection.
- Completion proof fields use explicit observed values. A completed run with zero cost projects `total_cost: 0`; absence of completion keeps completion fields absent.
- Queued budget fields also use explicit observed values. A queued request with zero budget projects `budget_max_iterations: 0` or `budget_max_cost_usd: 0`; absence of a queued request keeps queued budget fields absent.
- Runtime anchoring reads the latest `hive.run.started` independently from agent-event volume, then reads bounded events from that run conversation in EventGraph store order. Agent counts are bounded event tallies, not invariants; `spawned` and `stopped` may differ when the bounded conversation window excludes matching events.
- Artifact causes are the EventGraph causes on the artifact event. Hive does not duplicate cause truth inside artifact content; Site must render the projected causes and `cause_status`. `caused` means at least one cause is in the run window, `caused_external_only` means causes exist only outside the bounded run window, and `missing_causes` means the artifact event has no EventGraph causes.
- The event inspector exposes curated, allowlisted EventGraph event content for operator inspection. It is not a raw event dump, execution log, deploy log, human approval record, or source-repo adoption record. Curated content is capped at 8 KiB per event.
- The causal graph is scoped to the latest run conversation. Causes outside the bounded run window are labeled `external_or_out_of_window` and must not be presented as missing runtime evidence.
- The projection limit bounds the latest-run conversation event read. External cause nodes can be added beyond that count so the graph can label out-of-window provenance without claiming those causes are run-local events.
- This projection defines the read-side contract. Runtime artifact production still requires Hive to append `factory.artifact.created` with real EventGraph causes that point at the events that actually produced the artifact.
- Runtime evidence has no approval powers and cannot resolve authority. Site must continue using Hive's approval resolution endpoint for authority decisions.
- If the projection window excludes older events, `status` and counts are bounded by the returned EventGraph reads, not by unstated runtime history.

## Model selection projection

Hive exposes model-selection data as a read-only part of `/api/hive/operator-projection`.
Site may render this data, but Hive remains the source of truth.

The projection includes:

- Resolved `tier_defaults`, mapping each routing tier to the model the active catalog selects.
- Model catalog entries with provider, auth mode, tier, capabilities, context window, output-token limit, and pricing metadata.
- Starter civic-role assignments after Hive applies existing `modelconfig.Resolver` policy, defaults, and `CanOperate` constraints.
- Catalog load metadata: `catalog_source`, `loaded_at`, `reload_mode`, `hot_reload`, and `last_reload_at` when a reload has occurred.

Important boundaries:

- Subscription (`claude-cli`) remains the default catalog path.
- API-key (`anthropic`) models may appear in the catalog, but role assignment to them requires explicit catalog/policy configuration.
- `CanOperate` roles must continue resolving only to Operate-capable providers such as `claude-cli` or `codex-cli`.
- `startup-static` means Hive loaded the catalog once at process start.
- `hot-reload` means Hive owns a runtime reload loop for the configured catalog path. A failed reload is projected in `model_selection.errors`; Hive keeps the last valid resolver active.
- Runtime reload affects future resolver reads and future dynamic spawns. It does not silently rebind provider instances already running inside an agent loop.
- The projection is not an edit/write API.

## Launch-record model override metadata

`POST /api/hive/runs` may include `model_overrides`, a list of explicit role model/profile override metadata for the queued run request.
Hive validates each override before appending any launch events by resolving it through `modelconfig.ResolutionInput.TaskOverride`.
The launch API records durable request evidence only; it does not start agents, rebind running providers, or mutate global role policy.

Rules:

- `role` is required and must name a starter civic role.
- At least one of `model`, `profile`, `provider`, `preferred_tier`, `required_capabilities`, or `max_cost_per_call_usd` must be set.
- Provider/model/auth selections must resolve to a coherent catalog tuple; a provider override cannot relabel a subscription model as a metered provider.
- A resolved `api-key` model requires an explicit request `auth_mode: api-key`; otherwise Hive rejects it before any launch events are written.
- Duplicate role overrides are rejected.
- Unknown, malformed, over-budget, or `CanOperate`-incompatible overrides are rejected before `source.ingested`, `brief.derived`, or `factory.run.requested` events are written.
- Accepted overrides are recorded on `factory.run.requested` with requested fields plus resolved model, provider, and auth mode for downstream launch execution.
- Overrides are scoped to that run request metadata. They do not mutate global role defaults.

## Intake/factory event types

```text
source.ingested
source.parsed
brief.derived
brief.edited
brief.approved
factory.run.requested
factory.run.started
factory.phase.started
factory.phase.completed
factory.artifact.created
factory.run.completed
factory.run.halted
factory.run.abandoned
```

## Authority resolution

Site must never directly mutate authority state. It sends a resolution request to Hive. Hive records the authority event and exposes the updated projection.

```json
{
  "request_id": "auth_123",
  "approved": true,
  "resolver_id": "user_123",
  "reason": "Scope is bounded and tests are required."
}
```

## Invariants

- Every run has a source cause.
- Every artifact has causes.
- Every approval resolution has a human-visible reason.
- Every rejected approval has a reason and optional requested revision.
- Every self-modification path uses Required authority.
- Guardian/HALT state is projected immediately.
- Budget state is visible before and during execution.


## Daemon runtime observations (2026-09-08)

For a local daemon and ops API, set `HIVE_RUNTIME_SNAPSHOT_FILE` on the daemon
and `HIVE_OPS_RUNTIME_SNAPSHOT_FILE` on the ops API to the same file in a shared
directory. Only the daemon may write that directory; mount it read-only in the
ops API. The daemon atomically refreshes the typed `hive-runtime-observation/v1`
report every three seconds. No credentials or task contents are included.

Mission Control joins the report by immutable actor ID and matching role. It
shows live loop state (`working`, `idle`, `waiting for budget`, `stopped`) and
per-role running counts. Idle and budget-waiting loops remain running. Historical
actors absent from this daemon are explicitly outside its current runtime.
A missing, malformed, duplicated, future-dated or more-than-15-second-old report
cannot grant liveness. Observations remain projected-only; they do not replace
EventGraph lifecycle history or grant assignment, approval, or execution authority.
One-shot runs do not publish into the daemon's observation file.

The optional `hive_runtime` service observation extends Mission Control v2 when
configured. Deploy Site support before enabling the new observation on Hive;
older Site versions reject unknown service IDs. Unconfigured deployments retain
the existing unavailable process-liveness display.

The shared model catalog lives in EventGraph's Go `modelconfig` package. Model
choices and verification dates flow through Hive to Site; listing a model does
not establish authentication or account entitlement. Existing explicit role pins
remain stable. Current Codex models accept maximum reasoning, while older model
pins retain their supported effort validation.

### Prepared result review (2026-09-08)

`GET /api/civilization/v1/work` includes `prepared_result_id` (the immutable preparation event), `prepared_result_digest`, and optional `result_review` / `revision_of`. Existing saved prepared results acquire these identity fields during replay; no data rewrite is needed.

`POST /api/civilization/v1/work/{workID}/result-review` takes `result_id`, `workspace_digest`, `decision` (`approve`, `reject`, or `request_changes`), `feedback`, and `reviewed_by`. Site asserts the reviewer from its existing authenticated viewer, as with human ownership. Feedback is required for rejection or changes. The action accepts only the exact prepared artifact and records one signed `civilization.result.reviewed` event. Conflicting decisions or stale identities return 409; exact retries are idempotent. Approval and rejection are recorded result dispositions, not repository publication, merge, deployment, or elevated authorization. Both retain the original artifact and provider evidence.

Requested changes records `changes_requested` and a deterministic linked revision. The new work retains the original request, feedback, model selection and a typed reference to the earlier immutable artifact; it uses its own normal isolated worktree. The implementation receives that prior artifact as reference data and must verify the complete revised result. A new TLC brief requires confirmation before implementation. Creation interrupted after recording the decision is retried by the reconciler or by the same request. Original work and evidence remain unchanged. Current publication and merge configuration remains enforced.

Design (TLC Designed): keep each delivered result immutable and create a linked revision, instead of changing accepted evidence in place. Bind each decision to the preparation event and digest, with a shared store idempotency key serializing competing callers. Deploy Hive before Site so actions only appear when the backend supplies a result identity. Tests cover replay, conflicting callers, stale results, interrupted revision creation, confirmation, original-artifact preservation, and no execution/publication effects from approval or rejection.
