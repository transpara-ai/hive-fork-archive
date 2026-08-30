---
review_record_version: 1
gate: IADA
result: BLOCKED
reviewed_at: 2026-08-30T18:54:20Z
reviewer: Codex/OpenAI
reviewer_family: OpenAI
independent: false
author_lineages:
  - OpenAI
  - Anthropic
packet_id: HIVE-TLC-CHANGE-WORKFLOW-CIVILIZATION-ADAPTER
packet_version: 0.3.0
packet_path: docs/designs/tlc-change-workflow-civilization-adapter-v0.3.0.md
packet_blob: b6a5f08adb0be58fd5c5efc35ac7ae971c7126ae
packet_sha256: 65dc1695faf51bfe1ea73af20ed39cbe0c23349280f2e9c2c4e51692a398605d
factory_order_blob: 924797d10f96e404aa01d98c1c35e085b381daa7
implementation_request_blob: 67d51c454ab7655f083626fb8becac40be218a4a
base_head: 138472d26708cc6277625210ad46ae9b12aee10c
blocker_count: 3
ready_for_cfada: false
---

# IADA — Civilization TLC workflow adapter design v0.3.0

## Subject and verdict

This is OpenAI author-family self-review of exact design blob
`b6a5f08adb0be58fd5c5efc35ac7ae971c7126ae`, Factory Order blob
`924797d10f96e404aa01d98c1c35e085b381daa7`, and implementation-request blob
`67d51c454ab7655f083626fb8becac40be218a4a`.

`IADA_RESULT: BLOCKED`. RepoX affinity, executor ownership, dual-store repair,
partial-work preservation, exact authority, and Human-only optional Fable
selection remain the correct architecture. Three blockers and two majors make
the exact adapter design premature to implement.

## Findings

| ID | Severity | Evidence and consequence | Smallest corrective disposition |
| --- | --- | --- | --- |
| IADA-HIVE-WF-001 | blocker | The adapter consumes the exact TLC v0.7 contract, which the TLC IADA found unimplementable because derived outputs self-strand, the default author is undefined, and phase determinism lacks total predicates. Hive cannot compensate without copying TLC policy. | Repair and re-review the TLC contract first; update this design to the new exact contract identity only after that subject is fixed. |
| IADA-HIVE-WF-002 | blocker | The prerequisites say Hive implementation cannot begin until the successor TLC plugin is published and installed, while publication and installation are separately withheld effects after TLC implementation. This makes implementation sequencing circular. | Permit adapter implementation and isolated contract tests against an exact reviewed development schema; keep runtime admission disabled until a separately published and authenticated installed package exists. |
| IADA-HIVE-WF-003 | blocker | An Issue reply may “explicitly select” Fable, but no structured API/control or comment grammar distinguishes an authenticated selection from ordinary prose. Implementing semantic inference would let the machine select. | Define a dedicated authenticated operator action/API that creates the auxiliary selection. Issue text and comments alone never create one. |
| IADA-HIVE-WF-004 | major | A selected provider invocation may crash after dispatch but before attestation. The general retry protocol cannot prove absence for an opaque model call, while the selection permits only one invocation. | Persist intent before dispatch; after an unknown/interrupted call, mark the selection consumed-and-blocked. Reuse only an exactly observable durable result; require a fresh Human selection for another call. |
| IADA-HIVE-WF-005 | major | AC10 promises zero duplicate external effects, while the design correctly disclaims distributed exactly-once semantics. An eventually consistent provider may report apparent absence after an effect. | Limit the guarantee to providers whose authenticated observation proves exact or authoritative absence; otherwise block on `unknown` and state that duplicates are prevented by no-retry, not globally proven. |

## Validation and boundary

- Exact Factory Order, design, implementation-request, and referenced TLC
  design identities were recomputed.
- All five adapter Mermaid blocks rendered with Mermaid CLI 11.12.0.
- Hive lifecycle invariants and build passed.
- Full `make verify` is independently blocked in unchanged code by
  `TestIssueScanDraftPRGitBaseCommitSHAFetchesRemoteBase`, which cannot resolve
  `origin/main`; no implementation byte was changed.
- The active multi-author reviewer-route intersection is empty, and required
  data-recovery plus authentication/identity specialist evidence is absent.

This IADA is self-review only. It authorizes no design repair, reviewer-family
exception, implementation, commit, push, PR, release, installation, runtime,
or external effect.
