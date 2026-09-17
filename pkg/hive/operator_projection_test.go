package hive

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/transpara-ai/eventgraph/go/pkg/event"
	"github.com/transpara-ai/eventgraph/go/pkg/modelconfig"
	"github.com/transpara-ai/eventgraph/go/pkg/store"
	"github.com/transpara-ai/eventgraph/go/pkg/types"
	"github.com/transpara-ai/work"
)

func TestBuildOperatorProjectionPendingAndDecisions(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)

	pendingRequestID := appendEvent(EventTypeAuthorityRequestRecorded, AuthorityRequestRecordedContent{
		RequestID:         newTestEventID(t),
		RequestingActor:   actorID,
		ActionName:        "agent.retire",
		Target:            "actor_target",
		Environment:       "production",
		RequestedOutcome:  "retire agent",
		Justification:     "operator requested retirement",
		RiskSummary:       "protected lifecycle action",
		ProposedOperation: "retire",
	}).Content().(AuthorityRequestRecordedContent).RequestID

	decidedRequestID := newTestEventID(t)
	appendEvent(EventTypeAuthorityRequestRecorded, AuthorityRequestRecordedContent{
		RequestID:        decidedRequestID,
		RequestingActor:  actorID,
		RequestingRole:   "operator",
		ActionName:       "agent.revoke",
		Target:           "actor_revoked",
		Environment:      "production",
		RiskClass:        "critical",
		RequestedOutcome: "revoke agent",
		Justification:    "compromised key",
		RiskSummary:      "key compromise",
		Scope:            []string{"agent.revoke"},
	})
	appendEvent(EventTypeAuthorityDecisionRecorded, AuthorityDecisionRecordedContent{
		DecisionID:     "decision-1",
		RequestID:      decidedRequestID,
		ApproverActor:  actorID,
		DeciderRole:    "operator",
		Outcome:        "approved",
		ApprovedTarget: "actor_revoked",
		ApprovedAction: "agent.revoke",
		Scope:          []string{"agent.revoke"},
		Rationale:      "valid revocation evidence",
	})

	projection := BuildOperatorProjection(s, 50)

	if len(projection.PendingApprovals) != 1 {
		t.Fatalf("pending approvals = %d, want 1: %+v", len(projection.PendingApprovals), projection.PendingApprovals)
	}
	if projection.PendingApprovals[0].RequestID != pendingRequestID.Value() {
		t.Fatalf("pending request ID = %q, want %q", projection.PendingApprovals[0].RequestID, pendingRequestID.Value())
	}
	if len(projection.AuthorityDecisions) != 1 {
		t.Fatalf("authority decisions = %d, want 1", len(projection.AuthorityDecisions))
	}
	decision := projection.AuthorityDecisions[0]
	if decision.RequestID != decidedRequestID.Value() || decision.Outcome != "approved" {
		t.Fatalf("decision projection = %+v", decision)
	}
	if decision.RequestedAction != "agent.revoke" || decision.RequestedTarget != "actor_revoked" {
		t.Fatalf("decision did not join request details: %+v", decision)
	}
	if decision.DeciderRole != "operator" {
		t.Fatalf("decision decider role = %q, want operator", decision.DeciderRole)
	}
	if len(decision.Scope) != 1 || decision.Scope[0] != "agent.revoke" {
		t.Fatalf("decision scope = %#v, want [agent.revoke]", decision.Scope)
	}
}

func TestBuildCivilizationAssemblyProjectionDerivesOperatorRuntimeState(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	requestID := newTestEventID(t)
	appendEvent(EventTypeAuthorityRequestRecorded, AuthorityRequestRecordedContent{
		RequestID:        requestID,
		RequestingActor:  actorID,
		RequestingRole:   "operator",
		ActionName:       "pull_request.create",
		Target:           "pr://transpara-ai/site/42",
		Environment:      "review",
		RiskClass:        "medium",
		RequestedOutcome: "draft PR",
		Justification:    "bounded implementation ready",
		RiskSummary:      "protected GitHub write",
		Scope:            []string{"repo:transpara-ai/site", "head:abc123"},
	})
	appendEvent(EventTypeAgentIdentityRegistered, AgentIdentityRegisteredContent{
		ActorID:          actorID,
		DisplayName:      "Reviewer",
		Role:             "reviewer",
		PublicKey:        types.MustPublicKey(make([]byte, 32)),
		KeyProvenance:    "generated",
		Environment:      "review",
		IdentityMode:     "persistent",
		LifecycleStatus:  "active",
		AuthorityScope:   "hive:review",
		RegistrationPath: "generated",
	})
	appendEvent(EventTypeRunStarted, RunStartedContent{
		Idea:     "runtime visualization",
		RepoPath: "/Transpara/transpara-ai/repos/site",
	})
	appendEvent(EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "builder",
		Role:    "implementer",
		Model:   "claude",
		ActorID: actorID.Value(),
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if projection.ProjectionSubject != civilizationAssemblyProjectionSubject {
		t.Fatalf("subject = %q, want %q", projection.ProjectionSubject, civilizationAssemblyProjectionSubject)
	}
	if projection.DerivationStatus != civilizationAssemblyStatusComplete {
		t.Fatalf("derivation status = %q, want complete; failures=%+v", projection.DerivationStatus, projection.FailureReasons)
	}
	if projection.AuthorityState.Status != civilizationAssemblyFieldAvailable || len(projection.AuthorityState.AuthorityRequests) != 1 {
		t.Fatalf("authority state = %+v, want one available request", projection.AuthorityState)
	}
	if got := projection.AuthorityState.AuthorityRequests[0].TargetType; got != "pr" {
		t.Fatalf("target type = %q, want pr", got)
	}
	if projection.ExternalCommitteeState.Status != civilizationAssemblyFieldUnavailable || len(projection.ExternalCommitteeState.DecisionRefs) != 0 {
		t.Fatalf("external committee state = %+v, want unavailable with no Hive decision refs", projection.ExternalCommitteeState)
	}
	if len(projection.ActorRoster) == 0 {
		t.Fatalf("actor roster empty: %+v", projection)
	}
	if !civilizationProjectionHasRoleBinding(projection, actorID.Value(), "reviewer") {
		t.Fatalf("missing reviewer role binding: %+v", projection.RoleBindings)
	}
	if !civilizationProjectionHasRoleBinding(projection, actorID.Value(), "implementer") {
		t.Fatalf("missing runtime implementer role binding: %+v", projection.RoleBindings)
	}
	if projection.WorkEvidenceSummary.Status != civilizationAssemblyFieldAvailable {
		t.Fatalf("work evidence = %+v, want available runtime evidence", projection.WorkEvidenceSummary)
	}
	if !civilizationProjectionHasLifecycleStatus(projection, actorID.Value(), "active") {
		t.Fatalf("missing normalized active lifecycle status: %+v", projection.AgentLifecycleSummary)
	}
	if got := civilizationProjectionLifecycleCount(projection, actorID.Value()); got != 1 {
		t.Fatalf("lifecycle rows for %s = %d, want 1: %+v", actorID.Value(), got, projection.AgentLifecycleSummary)
	}
	if projection.FactoryOrderSummary == nil || projection.RoleBindings == nil || projection.AgentLifecycleSummary == nil || projection.ResidualRiskSummary == nil {
		t.Fatalf("top-level slices must serialize as arrays, got factory=%#v bindings=%#v lifecycle=%#v risks=%#v", projection.FactoryOrderSummary, projection.RoleBindings, projection.AgentLifecycleSummary, projection.ResidualRiskSummary)
	}
	if len(projection.OpenGateSummary) != 1 || projection.OpenGateSummary[0].ID != requestID.Value() {
		t.Fatalf("open gates = %+v, want pending request %s", projection.OpenGateSummary, requestID.Value())
	}
	if projection.SourceEventGraphHeadOrStateVersion == "" || projection.SourceEventGraphHeadOrStateVersion == "eventgraph head unavailable" {
		t.Fatalf("source head unavailable: %q", projection.SourceEventGraphHeadOrStateVersion)
	}
	if !containsString(projection.BoundaryFlags, "read_only_site_consumer") || !containsString(projection.ValidationRefs, civilizationAssemblyReadOnlyRoutePath) {
		t.Fatalf("missing read-only boundary evidence: flags=%+v validation=%+v", projection.BoundaryFlags, projection.ValidationRefs)
	}
}

func TestBuildCivilizationAssemblyProjectionDerivesCompletedRuntimeRoster(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)
	appendEvent(EventTypeRunStarted, RunStartedContent{
		Idea:     "runtime visualization",
		RepoPath: "/Transpara/transpara-ai/repos/hive",
	})
	appendEvent(EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "builder",
		Role:    "implementer",
		Model:   "claude-opus-4-6",
		ActorID: "actor_runtime_builder",
	})
	appendEvent(EventTypeRunCompleted, RunCompletedContent{
		AgentCount: 1,
		DurationMs: 1234,
		TotalCost:  0.25,
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if len(projection.ActorRoster) != 1 {
		t.Fatalf("actor roster = %+v, want one runtime-observed actor", projection.ActorRoster)
	}
	actor := projection.ActorRoster[0]
	if actor.ActorID != "actor_runtime_builder" || actor.IdentityMode != "runtime_observed" || actor.Status != "observed_completed_run" {
		t.Fatalf("actor roster entry = %+v, want observed completed runtime actor", actor)
	}
	if !civilizationProjectionHasRoleBinding(projection, "actor_runtime_builder", "implementer") {
		t.Fatalf("missing completed-run implementer role binding: %+v", projection.RoleBindings)
	}
	if !civilizationProjectionHasLifecycleStatus(projection, "actor_runtime_builder", "observed_completed_run") {
		t.Fatalf("missing completed-run lifecycle status: %+v", projection.AgentLifecycleSummary)
	}
	if civilizationProjectionHasUnavailableField(projection, "actor_roster") {
		t.Fatalf("actor_roster marked unavailable despite completed runtime evidence: %+v", projection.WithheldOrUnavailableFields)
	}
}

func TestBuildCivilizationAssemblyProjectionRuntimeStatusOverridesLifecycleLiveness(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	appendEvent(EventTypeAgentIdentityRegistered, AgentIdentityRegisteredContent{
		ActorID:          actorID,
		DisplayName:      "Reviewer",
		Role:             "reviewer",
		PublicKey:        types.MustPublicKey(make([]byte, 32)),
		KeyProvenance:    "generated",
		Environment:      "review",
		IdentityMode:     "persistent",
		LifecycleStatus:  "active",
		AuthorityScope:   "hive:review",
		RegistrationPath: "generated",
	})
	appendEvent(EventTypeRunStarted, RunStartedContent{
		Idea:     "runtime visualization",
		RepoPath: "/Transpara/transpara-ai/repos/hive",
	})
	appendEvent(EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "reviewer",
		Role:    "reviewer",
		Model:   "gpt-5",
		ActorID: actorID.Value(),
	})
	appendEvent(EventTypeRunCompleted, RunCompletedContent{
		AgentCount: 1,
		DurationMs: 1234,
		TotalCost:  0.25,
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if !civilizationProjectionHasActorModeStatus(projection, actorID.Value(), "persistent", "observed_completed_run") {
		t.Fatalf("runtime status did not override lifecycle liveness: %+v", projection.ActorRoster)
	}
	if !civilizationProjectionHasLifecycleStatus(projection, actorID.Value(), "observed_completed_run") {
		t.Fatalf("runtime lifecycle status did not override stale active lifecycle: %+v", projection.AgentLifecycleSummary)
	}
}

func TestBuildCivilizationAssemblyProjectionRunCompletionOverridesMidRunLifecycleLiveness(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	requestID := newTestEventID(t)
	decisionID := newTestEventID(t)
	appendEvent(EventTypeRunStarted, RunStartedContent{
		Idea:     "runtime visualization",
		RepoPath: "/Transpara/transpara-ai/repos/hive",
	})
	appendEvent(EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "reviewer",
		Role:    "reviewer",
		Model:   "gpt-5",
		ActorID: actorID.Value(),
	})
	time.Sleep(time.Millisecond)
	appendEvent(EventTypeAgentLifecycleTransitioned, AgentLifecycleTransitionedContent{
		ActorID:            actorID,
		PreviousState:      "candidate",
		RequestedState:     "active",
		ResultingState:     "active",
		RequestingActor:    actorID,
		AuthorityRequestID: requestID,
		DecisionEventID:    decisionID,
		Reason:             "activated during run",
	})
	time.Sleep(time.Millisecond)
	appendEvent(EventTypeRunCompleted, RunCompletedContent{
		AgentCount: 1,
		DurationMs: 1234,
		TotalCost:  0.25,
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if !civilizationProjectionHasActorModeStatus(projection, actorID.Value(), "projected", "observed_completed_run") {
		t.Fatalf("run completion did not override mid-run lifecycle liveness: %+v", projection.ActorRoster)
	}
	if !civilizationProjectionHasLifecycleStatus(projection, actorID.Value(), "observed_completed_run") {
		t.Fatalf("run completion did not update lifecycle status: %+v", projection.AgentLifecycleSummary)
	}
}

func TestBuildCivilizationAssemblyProjectionLaterLifecycleStatusOverridesRuntimeLiveness(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	requestID := newTestEventID(t)
	decisionID := newTestEventID(t)
	appendEvent(EventTypeAgentIdentityRegistered, AgentIdentityRegisteredContent{
		ActorID:          actorID,
		DisplayName:      "Reviewer",
		Role:             "reviewer",
		PublicKey:        types.MustPublicKey(make([]byte, 32)),
		KeyProvenance:    "generated",
		Environment:      "review",
		IdentityMode:     "persistent",
		LifecycleStatus:  "active",
		AuthorityScope:   "hive:review",
		RegistrationPath: "generated",
	})
	appendEvent(EventTypeRunStarted, RunStartedContent{
		Idea:     "runtime visualization",
		RepoPath: "/Transpara/transpara-ai/repos/hive",
	})
	appendEvent(EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "reviewer",
		Role:    "reviewer",
		Model:   "gpt-5",
		ActorID: actorID.Value(),
	})
	time.Sleep(time.Millisecond)
	appendEvent(EventTypeAgentLifecycleTransitioned, AgentLifecycleTransitionedContent{
		ActorID:            actorID,
		PreviousState:      "active",
		RequestedState:     "retired",
		ResultingState:     "retired",
		RequestingActor:    actorID,
		AuthorityRequestID: requestID,
		DecisionEventID:    decisionID,
		Reason:             "completed mandate",
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if !civilizationProjectionHasActorModeStatus(projection, actorID.Value(), "persistent", "retired") {
		t.Fatalf("later lifecycle status did not override older runtime liveness: %+v", projection.ActorRoster)
	}
	if !civilizationProjectionHasLifecycleStatus(projection, actorID.Value(), "retired") {
		t.Fatalf("later lifecycle row did not remain terminal: %+v", projection.AgentLifecycleSummary)
	}
}

func TestBuildCivilizationAssemblyProjectionClassifiesStoppedRunningAgentsAsObserved(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)
	appendEvent(EventTypeRunStarted, RunStartedContent{
		Idea:     "runtime visualization",
		RepoPath: "/Transpara/transpara-ai/repos/hive",
	})
	appendEvent(EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "builder",
		Role:    "implementer",
		Model:   "claude-opus-4-6",
		ActorID: "actor_active_builder",
	})
	appendEvent(EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "reviewer",
		Role:    "reviewer",
		Model:   "gpt-5",
		ActorID: "actor_stopped_reviewer",
	})
	appendEvent(EventTypeAgentStopped, AgentStoppedContent{
		Name:       "reviewer",
		Role:       "reviewer",
		StopReason: "handoff",
		Iterations: 1,
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if !civilizationProjectionHasActorStatus(projection, "actor_active_builder", "active") {
		t.Fatalf("missing active runtime actor: %+v", projection.ActorRoster)
	}
	if !civilizationProjectionHasActorModeStatus(projection, "actor_active_builder", "runtime", "active") {
		t.Fatalf("active runtime actor did not keep runtime identity mode: %+v", projection.ActorRoster)
	}
	if !civilizationProjectionHasActorStatus(projection, "actor_stopped_reviewer", "observed") {
		t.Fatalf("missing observed stopped runtime actor: %+v", projection.ActorRoster)
	}
	if !civilizationProjectionHasActorModeStatus(projection, "actor_stopped_reviewer", "runtime_observed", "observed") {
		t.Fatalf("stopped runtime actor did not use observed identity mode: %+v", projection.ActorRoster)
	}
	if !civilizationProjectionHasLifecycleStatus(projection, "actor_active_builder", "active") {
		t.Fatalf("missing active lifecycle row: %+v", projection.AgentLifecycleSummary)
	}
	if !civilizationProjectionHasLifecycleStatus(projection, "actor_stopped_reviewer", "observed") {
		t.Fatalf("missing observed stopped lifecycle row: %+v", projection.AgentLifecycleSummary)
	}
	if !civilizationProjectionHasRoleBinding(projection, "actor_active_builder", "implementer") || !civilizationProjectionHasRoleBinding(projection, "actor_stopped_reviewer", "reviewer") {
		t.Fatalf("missing mixed runtime role bindings: %+v", projection.RoleBindings)
	}
}

func TestBuildCivilizationAssemblyProjectionEmptyStoreKeepsUnavailableFieldsAndArrays(t *testing.T) {
	s, _, _ := newOperatorProjectionStore(t)

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if projection.DerivationStatus != civilizationAssemblyStatusComplete {
		t.Fatalf("derivation status = %q, want complete", projection.DerivationStatus)
	}
	if projection.ActorRoster == nil || projection.RoleBindings == nil || projection.AgentLifecycleSummary == nil || projection.FactoryOrderSummary == nil || projection.ResidualRiskSummary == nil {
		t.Fatalf("top-level slices must be arrays, got actor=%#v bindings=%#v lifecycle=%#v factory=%#v risks=%#v", projection.ActorRoster, projection.RoleBindings, projection.AgentLifecycleSummary, projection.FactoryOrderSummary, projection.ResidualRiskSummary)
	}
	if projection.WorkEvidenceSummary.Status != civilizationAssemblyFieldUnavailable || len(projection.WorkEvidenceSummary.SourceRefs) != 0 {
		t.Fatalf("work evidence = %+v, want unavailable with no refs", projection.WorkEvidenceSummary)
	}
	if !civilizationProjectionHasUnavailableField(projection, "actor_roster") {
		t.Fatalf("missing actor_roster unavailable field: %+v", projection.WithheldOrUnavailableFields)
	}
	if !civilizationProjectionHasUnavailableField(projection, "factory_order_summary") {
		t.Fatalf("missing factory_order_summary unavailable field: %+v", projection.WithheldOrUnavailableFields)
	}
}

func TestBuildCivilizationAssemblyProjectionProjectsParkedIssueScanCanary(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	parked := appendEvent(EventTypeIssueScanRunParked, IssueScanRunParkedContent{
		RunID:             "level1_canary_transpara-ai_docs_226",
		Repository:        "transpara-ai/docs",
		IssueNumber:       226,
		LifecycleVersion:  IssueScanParkLifecycleLevel1Canary,
		EvidenceClass:     IssueScanParkEvidenceClassLevel1Canary,
		AuthorityBoundary: IssueScanParkAuthorityBoundaryLevel1Canary,
		BlockerType:       IssueScanParkBlockerHumanScope,
		Detail:            "transpara-ai/docs#226 is labeled cc:needs-human-scope",
		RequiredAction:    "human must clarify scope and remove the human-scope blocker before Hive may continue",
		SourceRefs:        []string{"https://github.com/transpara-ai/docs/issues/226", "canary://level1-dark-factory/issue-discovery"},
		ParkedBy:          actorID,
		TargetIssueState:  "open",
		TargetIssueLabels: []string{"cc:intake", "cc:needs-human-scope", "cc:protected-action"},
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if projection.IssueIntakeProjection.Status != civilizationAssemblyFieldAvailable {
		t.Fatalf("issue intake status = %q, want available", projection.IssueIntakeProjection.Status)
	}
	if len(projection.IssueIntakeProjection.Issues) != 1 {
		t.Fatalf("issue intake issues = %+v, want one", projection.IssueIntakeProjection.Issues)
	}
	issue := projection.IssueIntakeProjection.Issues[0]
	if issue.Repo != "transpara-ai/docs" || issue.Number != 226 {
		t.Fatalf("issue intake row identity = %+v", issue)
	}
	if issue.RiskClass != "protected-action" || issue.Readiness != "parked: protected-action authority required" {
		t.Fatalf("issue intake row risk/readiness = %+v, want protected-action singular fields", issue)
	}
	if issue.UpdatedAt == "" {
		t.Fatalf("issue intake row missing updated_at: %+v", issue)
	}
	if _, err := time.Parse(time.RFC3339Nano, issue.UpdatedAt); err != nil {
		t.Fatalf("issue updated_at = %q, want RFC3339 parked-evidence timestamp: %v", issue.UpdatedAt, err)
	}
	if !containsString(projection.IssueIntakeProjection.ScannerBoundaries, "updated_at is the EventGraph parked-evidence timestamp, not the GitHub issue last-modified time") {
		t.Fatalf("scanner boundaries = %+v, want updated_at evidence-time boundary", projection.IssueIntakeProjection.ScannerBoundaries)
	}
	if !containsString(projection.IssueIntakeProjection.ScannerBoundaries, "touched_substrate is derived only from the repository slug; no code or substrate analysis is performed") {
		t.Fatalf("scanner boundaries = %+v, want repo-derived substrate boundary", projection.IssueIntakeProjection.ScannerBoundaries)
	}
	if !containsString(projection.IssueIntakeProjection.ScannerBoundaries, "issue intake rows are point-in-time parked snapshots and may be superseded by later FactoryOrder lifecycle evidence") {
		t.Fatalf("scanner boundaries = %+v, want parked snapshot supersession boundary", projection.IssueIntakeProjection.ScannerBoundaries)
	}
	if !containsString(issue.RiskClasses, "human-scope") || !containsString(issue.RiskClasses, "protected-action") {
		t.Fatalf("issue risk classes = %+v, want human-scope and protected-action", issue.RiskClasses)
	}
	if !containsString(issue.ReadinessStates, "parked: human scope required") || !containsString(issue.ReadinessStates, "parked: protected-action authority required") {
		t.Fatalf("issue readiness states = %+v, want human-scope and protected-action readiness", issue.ReadinessStates)
	}
	if len(projection.IssueScanProjection.Runs) != 1 || projection.IssueScanProjection.Runs[0].State != "human_action" {
		t.Fatalf("issue-scan runs = %+v, want human_action run", projection.IssueScanProjection.Runs)
	}
	if projection.IssueScanProjection.Runs[0].LifecycleVersion != IssueScanParkLifecycleLevel1Canary {
		t.Fatalf("issue-scan lifecycle version = %q, want canary event lifecycle", projection.IssueScanProjection.Runs[0].LifecycleVersion)
	}
	if issue.AuthorityBoundary != IssueScanParkAuthorityBoundaryLevel1Canary {
		t.Fatalf("issue authority boundary = %q, want canary event boundary", issue.AuthorityBoundary)
	}
	if len(projection.IssueScanProjection.Blockers) != 2 {
		t.Fatalf("issue-scan blockers = %+v, want human-scope and protected-action blockers", projection.IssueScanProjection.Blockers)
	}
	blockers := map[string]CivilizationIssueScanBlockerProjected{}
	for _, blocker := range projection.IssueScanProjection.Blockers {
		blockers[blocker.BlockerType] = blocker
	}
	for _, blockerType := range []string{IssueScanParkBlockerHumanScope, IssueScanParkBlockerProtectedAction} {
		blocker, ok := blockers[blockerType]
		if !ok || !containsString(blocker.EvidenceRefs, parked.ID().Value()) {
			t.Fatalf("blocker %s = %+v, want evidence ref %s", blockerType, blocker, parked.ID().Value())
		}
	}
	if !containsString(projection.SourceEventIDsOrQueryWindow, parked.ID().Value()) {
		t.Fatalf("source refs = %+v, want parked event %s", projection.SourceEventIDsOrQueryWindow, parked.ID().Value())
	}
}

func TestBuildCivilizationAssemblyProjectionDoesNotRelabelLifecycleParkingAsCanary(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	appendEvent(EventTypeIssueScanRunParked, IssueScanRunParkedContent{
		RunID:             "run_issue_scan_docs_226",
		FactoryOrderID:    "fo_issue_scan_docs_226",
		Repository:        "transpara-ai/docs",
		IssueNumber:       226,
		StageID:           "research_issue_and_repo_context",
		BlockerType:       IssueScanParkBlockerStaleTarget,
		Detail:            "transpara-ai/docs#226 is closed",
		RequiredAction:    "confirm the issue is still in scope or queue a fresh run against a live target",
		SourceRefs:        []string{"https://github.com/transpara-ai/docs/issues/226"},
		ParkedBy:          actorID,
		TargetIssueState:  "closed",
		TargetIssueLabels: []string{IssueScanPRReadyLabel},
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if len(projection.IssueScanProjection.Runs) != 1 {
		t.Fatalf("issue-scan runs = %+v, want one lifecycle parked run", projection.IssueScanProjection.Runs)
	}
	run := projection.IssueScanProjection.Runs[0]
	if run.LifecycleVersion == IssueScanParkLifecycleLevel1Canary || run.LifecycleVersion != issueScanLifecycleVersion {
		t.Fatalf("issue-scan lifecycle version = %q, want lifecycle version without canary relabel", run.LifecycleVersion)
	}
	if run.State != "human_action" {
		t.Fatalf("issue-scan run state = %q, want human_action for stale target required action", run.State)
	}
	if len(projection.IssueIntakeProjection.Issues) != 1 {
		t.Fatalf("issue intake issues = %+v, want one", projection.IssueIntakeProjection.Issues)
	}
	issue := projection.IssueIntakeProjection.Issues[0]
	if issue.AuthorityBoundary != IssueScanParkAuthorityBoundaryLifecycle || strings.Contains(issue.AuthorityBoundary, "canary") {
		t.Fatalf("issue authority boundary = %q, want lifecycle boundary without canary relabel", issue.AuthorityBoundary)
	}
}

func TestBuildCivilizationAssemblyProjectionTreatsNotPRReadyAsHumanAction(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	appendEvent(EventTypeIssueScanRunParked, IssueScanRunParkedContent{
		RunID:             "level1_canary_transpara-ai_docs_226",
		Repository:        "transpara-ai/docs",
		IssueNumber:       226,
		LifecycleVersion:  IssueScanParkLifecycleLevel1Canary,
		EvidenceClass:     IssueScanParkEvidenceClassLevel1Canary,
		AuthorityBoundary: IssueScanParkAuthorityBoundaryLevel1Canary,
		BlockerType:       IssueScanParkBlockerNotPRReady,
		Detail:            "transpara-ai/docs#226 lacks cc:pr-ready",
		RequiredAction:    "human must mark the issue PR-ready before Hive may continue",
		SourceRefs:        []string{"https://github.com/transpara-ai/docs/issues/226", "canary://level1-dark-factory/issue-discovery"},
		ParkedBy:          actorID,
		TargetIssueState:  "open",
		TargetIssueLabels: []string{"cc:intake"},
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if len(projection.IssueScanProjection.Runs) != 1 || projection.IssueScanProjection.Runs[0].State != "human_action" {
		t.Fatalf("issue-scan runs = %+v, want not-pr-ready human_action run", projection.IssueScanProjection.Runs)
	}
	if len(projection.IssueIntakeProjection.Issues) != 1 {
		t.Fatalf("issue intake issues = %+v, want one", projection.IssueIntakeProjection.Issues)
	}
	issue := projection.IssueIntakeProjection.Issues[0]
	if issue.RiskClass != "not-pr-ready" || issue.Readiness != "parked: not PR-ready" {
		t.Fatalf("issue projection = %+v, want not-pr-ready risk/readiness", issue)
	}
}

func TestBuildCivilizationAssemblyProjectionTreatsUnknownBlockerAsHumanAction(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	appendEvent(EventTypeIssueScanRunParked, IssueScanRunParkedContent{
		RunID:             "level1_canary_transpara-ai_docs_226",
		Repository:        "transpara-ai/docs",
		IssueNumber:       226,
		LifecycleVersion:  IssueScanParkLifecycleLevel1Canary,
		EvidenceClass:     IssueScanParkEvidenceClassLevel1Canary,
		AuthorityBoundary: IssueScanParkAuthorityBoundaryLevel1Canary,
		BlockerType:       "future_blocker",
		Detail:            "future blocker type projected from event",
		RequiredAction:    "human must inspect the unknown blocker before Hive may continue",
		SourceRefs:        []string{"https://github.com/transpara-ai/docs/issues/226", "canary://level1-dark-factory/issue-discovery"},
		ParkedBy:          actorID,
		TargetIssueState:  "open",
		TargetIssueLabels: []string{"cc:intake"},
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if len(projection.IssueScanProjection.Runs) != 1 || projection.IssueScanProjection.Runs[0].State != "human_action" {
		t.Fatalf("issue-scan runs = %+v, want unknown blocker human_action run", projection.IssueScanProjection.Runs)
	}
	if len(projection.IssueIntakeProjection.Issues) != 1 {
		t.Fatalf("issue intake issues = %+v, want one", projection.IssueIntakeProjection.Issues)
	}
	issue := projection.IssueIntakeProjection.Issues[0]
	if issue.RiskClass != "unknown" || issue.Readiness != "parked: unknown blocker" {
		t.Fatalf("issue projection = %+v, want unknown risk/readiness", issue)
	}
	if containsString(issue.RiskClasses, "not-pr-ready") {
		t.Fatalf("unknown blocker risk classes = %+v, must not masquerade as not-pr-ready", issue.RiskClasses)
	}
}

func TestBuildCivilizationAssemblyProjectionDefaultsMissingParkedIssueStateToUnknown(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	appendEvent(EventTypeIssueScanRunParked, IssueScanRunParkedContent{
		RunID:             "level1_canary_transpara-ai_docs_226",
		Repository:        "transpara-ai/docs",
		IssueNumber:       226,
		LifecycleVersion:  IssueScanParkLifecycleLevel1Canary,
		EvidenceClass:     IssueScanParkEvidenceClassLevel1Canary,
		AuthorityBoundary: IssueScanParkAuthorityBoundaryLevel1Canary,
		BlockerType:       IssueScanParkBlockerNotPRReady,
		Detail:            "transpara-ai/docs#226 lacks cc:pr-ready",
		RequiredAction:    "human must mark the issue PR-ready before Hive may continue",
		SourceRefs:        []string{"https://github.com/transpara-ai/docs/issues/226", "canary://level1-dark-factory/issue-discovery"},
		ParkedBy:          actorID,
		TargetIssueLabels: []string{"cc:intake"},
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if len(projection.IssueIntakeProjection.Issues) != 1 {
		t.Fatalf("issue intake issues = %+v, want one", projection.IssueIntakeProjection.Issues)
	}
	issue := projection.IssueIntakeProjection.Issues[0]
	if issue.State != "unknown" {
		t.Fatalf("issue state = %q, want unknown when parked event has no target issue state", issue.State)
	}
}

func TestBuildCivilizationAssemblyProjectionSignalsIssueScanTruncation(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	for i := 1; i <= 3; i++ {
		appendEvent(EventTypeIssueScanRunParked, IssueScanRunParkedContent{
			RunID:             fmt.Sprintf("level1_canary_transpara-ai_docs_%03d", i),
			Repository:        "transpara-ai/docs",
			IssueNumber:       200 + i,
			LifecycleVersion:  IssueScanParkLifecycleLevel1Canary,
			EvidenceClass:     IssueScanParkEvidenceClassLevel1Canary,
			AuthorityBoundary: IssueScanParkAuthorityBoundaryLevel1Canary,
			BlockerType:       IssueScanParkBlockerNotPRReady,
			Detail:            "issue lacks cc:pr-ready",
			RequiredAction:    "human must mark the issue PR-ready before Hive may continue",
			SourceRefs:        []string{fmt.Sprintf("https://github.com/transpara-ai/docs/issues/%d", 200+i)},
			ParkedBy:          actorID,
			TargetIssueState:  "open",
			TargetIssueLabels: []string{"cc:intake"},
		})
	}

	projection := BuildCivilizationAssemblyProjection(s, 2)

	if len(projection.IssueIntakeProjection.Issues) != 2 {
		t.Fatalf("issue intake issues = %d, want page-limited 2", len(projection.IssueIntakeProjection.Issues))
	}
	if !strings.Contains(projection.IssueIntakeProjection.Summary, "truncated at 2") {
		t.Fatalf("issue intake summary = %q, want truncation note", projection.IssueIntakeProjection.Summary)
	}
	if !civilizationProjectionHasResidualRisk(projection, "issue_scan_parked_source_limit_01") {
		t.Fatalf("residual risks = %+v, want issue-scan truncation residual", projection.ResidualRiskSummary)
	}
}

func TestBuildCivilizationAssemblyProjectionProjectsWorkFactoryOrders(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	taskEvent := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:                  "Implement issue scan result",
		Description:            "Seeded from queued Hive run.",
		CreatedBy:              actorID,
		FactoryOrderID:         "fo_run_issue_scan_001",
		RequirementIDs:         []string{"req_run_issue_scan_001"},
		AcceptanceCriterionIDs: []string{"ac_run_issue_scan_001"},
		RiskClass:              "high",
		ExpectedOutputs:        []string{"ready-for-Human result PR"},
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if len(projection.FactoryOrderSummary) != 1 {
		t.Fatalf("factory orders = %+v, want one", projection.FactoryOrderSummary)
	}
	order := projection.FactoryOrderSummary[0]
	if order.ID != "fo_run_issue_scan_001" || order.Status != "work_task_seeded" {
		t.Fatalf("factory order identity/status = %+v", order)
	}
	if order.RiskClass != "high" || order.ReleasePolicy != "human_required_before_merge" {
		t.Fatalf("factory order policy = %+v", order)
	}
	if len(order.RequirementRefs) != 1 || order.RequirementRefs[0] != "req_run_issue_scan_001" {
		t.Fatalf("requirement refs = %+v", order.RequirementRefs)
	}
	if len(order.AcceptanceCriterionRefs) != 1 || order.AcceptanceCriterionRefs[0] != "ac_run_issue_scan_001" {
		t.Fatalf("acceptance refs = %+v", order.AcceptanceCriterionRefs)
	}
	if len(order.TaskRefs) != 1 || order.TaskRefs[0] != taskEvent.ID().Value() {
		t.Fatalf("task refs = %+v, want %s", order.TaskRefs, taskEvent.ID().Value())
	}
	if projection.WorkEvidenceSummary.Status != civilizationAssemblyFieldAvailable {
		t.Fatalf("work evidence = %+v, want available", projection.WorkEvidenceSummary)
	}
	if !strings.Contains(projection.WorkEvidenceSummary.Summary, "no runtime run is observed") {
		t.Fatalf("work evidence summary = %q, want no-runtime boundary", projection.WorkEvidenceSummary.Summary)
	}
	if !containsString(projection.WorkEvidenceSummary.TaskRefs, taskEvent.ID().Value()) {
		t.Fatalf("work evidence task refs = %+v, want %s", projection.WorkEvidenceSummary.TaskRefs, taskEvent.ID().Value())
	}
	if !containsString(projection.SourceEventIDsOrQueryWindow, taskEvent.ID().Value()) {
		t.Fatalf("source refs = %+v, want task event %s", projection.SourceEventIDsOrQueryWindow, taskEvent.ID().Value())
	}
	if civilizationProjectionHasUnavailableField(projection, "factory_order_summary") {
		t.Fatalf("factory_order_summary marked unavailable despite Work seed task: %+v", projection.WithheldOrUnavailableFields)
	}
}

func TestBuildCivilizationAssemblyProjectionProjectsIssueScanStageTaskDrafts(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	sourceEventID := newTestEventID(t)
	briefEventID := newTestEventID(t)
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 323,
		Title:  "Project issue-scan stage task drafts",
		URL:    "https://github.com/transpara-ai/hive/issues/323",
		Body:   "Stage task drafts should appear in the Civilization Work evidence projection.",
		Labels: []string{"civilization"},
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	appendEvent(EventTypeFactoryRunRequested, FactoryRunRequestedContent{
		RunID:      "run_issue_scan_stage_tasks_001",
		IntakeID:   "intake_issue_scan_stage_tasks_001",
		OperatorID: "operator_michael",
		Title:      "Resolve transpara-ai/hive#323",
		Status:     "queued",
		Authority: RunLaunchAuthority{
			InitialLevel: event.AuthorityLevelRequired,
			Scope:        "transpara-ai issue scan to ready-for-Human PR; no merge or deploy",
			PolicyRef:    IssueScanDefaultPolicyRef,
			Rationale:    "Civilization selected a Transpara-AI GitHub issue for governed factory execution.",
		},
		Budget: RunLaunchBudget{
			MaxIterations: 12,
			MaxCostUSD:    25,
		},
		TargetRepos:   []string{"transpara-ai/hive"},
		SourceEventID: sourceEventID,
		BriefEventID:  briefEventID,
		Brief:         brief,
	})
	orderID := "fo_run_issue_scan_stage_tasks_001"
	parentTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:                  "Resolve transpara-ai/hive#323",
		Description:            "FactoryOrder seed task.",
		CreatedBy:              actorID,
		FactoryOrderID:         orderID,
		RequirementIDs:         []string{"req_run_issue_scan_stage_tasks_001"},
		AcceptanceCriterionIDs: []string{"ac_run_issue_scan_stage_tasks_001"},
		Cell:                   "implementation",
		RiskClass:              "high",
		ExpectedOutputs:        []string{"ready-for-Human result PR"},
	})
	researchTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:                  "Issue-scan stage: Research issue and repo context",
		Description:            "Stage ID: research_issue_and_repo_context",
		CreatedBy:              actorID,
		CanonicalTaskID:        issueScanLifecycleStageTaskCanonicalID(orderID, "research_issue_and_repo_context"),
		FactoryOrderID:         orderID,
		RequirementIDs:         []string{"req_run_issue_scan_stage_tasks_001"},
		AcceptanceCriterionIDs: []string{"ac_run_issue_scan_stage_tasks_001"},
		Cell:                   "planning",
		RiskClass:              "high",
		ExpectedOutputs:        []string{"stage declaration artifact remains pending runtime evidence", "repo_context_packet"},
	})
	debateTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:                  "Issue-scan stage: Debate with correct civic roles",
		Description:            "Stage ID: debate_with_correct_civic_roles",
		CreatedBy:              actorID,
		CanonicalTaskID:        issueScanLifecycleStageTaskCanonicalID(orderID, "debate_with_correct_civic_roles"),
		FactoryOrderID:         orderID,
		RequirementIDs:         []string{"req_run_issue_scan_stage_tasks_001"},
		AcceptanceCriterionIDs: []string{"ac_run_issue_scan_stage_tasks_001"},
		Cell:                   "planning",
		RiskClass:              "high",
		ExpectedOutputs:        []string{"stage declaration artifact remains pending runtime evidence", "decision_record"},
	})
	dependencyEvent := appendEvent(work.EventTypeTaskDependencyAdded, work.TaskDependencyContent{
		TaskID:      debateTask.ID(),
		DependsOnID: researchTask.ID(),
		AddedBy:     actorID,
	})
	secondOrderID := "fo_run_issue_scan_stage_tasks_002"
	secondParentTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:                  "Resolve transpara-ai/hive#324",
		Description:            "Second FactoryOrder seed task.",
		CreatedBy:              actorID,
		FactoryOrderID:         secondOrderID,
		RequirementIDs:         []string{"req_run_issue_scan_stage_tasks_002"},
		AcceptanceCriterionIDs: []string{"ac_run_issue_scan_stage_tasks_002"},
		Cell:                   "implementation",
		RiskClass:              "high",
		ExpectedOutputs:        []string{"ready-for-Human result PR"},
	})
	secondResearchTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:                  "Issue-scan stage: Research second issue",
		Description:            "Stage ID: research_issue_and_repo_context",
		CreatedBy:              actorID,
		CanonicalTaskID:        issueScanLifecycleStageTaskCanonicalID(secondOrderID, "research_issue_and_repo_context"),
		FactoryOrderID:         secondOrderID,
		RequirementIDs:         []string{"req_run_issue_scan_stage_tasks_002"},
		AcceptanceCriterionIDs: []string{"ac_run_issue_scan_stage_tasks_002"},
		Cell:                   "planning",
		RiskClass:              "high",
		ExpectedOutputs:        []string{"stage declaration artifact remains pending runtime evidence", "repo_context_packet"},
	})
	unrelatedStageTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:           "Unrelated issue-scan stage: Implement on branch",
		Description:     "Stage ID: implement_on_branch",
		CreatedBy:       actorID,
		CanonicalTaskID: issueScanLifecycleStageTaskCanonicalID("fo_run_issue_scan_other_001", "implement_on_branch"),
		FactoryOrderID:  "fo_run_issue_scan_other_001",
		ExpectedOutputs: []string{"implementation_patch"},
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if len(projection.WorkEvidenceSummary.Tasks) != 6 {
		t.Fatalf("task evidence = %+v, want two parents plus four stage task drafts", projection.WorkEvidenceSummary.Tasks)
	}
	parentEvidence := civilizationProjectionTaskEvidenceByID(projection.WorkEvidenceSummary.Tasks, parentTask.ID().Value())
	if parentEvidence == nil || parentEvidence.LifecycleStageID != "" || parentEvidence.FactoryOrderID != orderID {
		t.Fatalf("parent task evidence = %+v", parentEvidence)
	}
	if parentEvidence.Status != "work_task_seeded" || parentEvidence.Ready || parentEvidence.Blocked {
		t.Fatalf("parent task status/readiness = %+v", parentEvidence)
	}
	researchEvidence := civilizationProjectionTaskEvidenceByID(projection.WorkEvidenceSummary.Tasks, researchTask.ID().Value())
	if researchEvidence == nil || researchEvidence.LifecycleStageID != "research_issue_and_repo_context" {
		t.Fatalf("research task evidence = %+v", researchEvidence)
	}
	if researchEvidence.Status != "work_task_seeded" || researchEvidence.Ready || researchEvidence.Blocked {
		t.Fatalf("research task status/readiness = %+v", researchEvidence)
	}
	if researchEvidence.CanonicalTaskID != issueScanLifecycleStageTaskCanonicalID(orderID, "research_issue_and_repo_context") || researchEvidence.Cell != "planning" {
		t.Fatalf("research task metadata = %+v", researchEvidence)
	}
	if !containsString(researchEvidence.ExpectedOutputs, "repo_context_packet") || len(researchEvidence.DependsOnRefs) != 0 {
		t.Fatalf("research task outputs/deps = %+v", researchEvidence)
	}
	debateEvidence := civilizationProjectionTaskEvidenceByID(projection.WorkEvidenceSummary.Tasks, debateTask.ID().Value())
	if debateEvidence == nil || debateEvidence.LifecycleStageID != "debate_with_correct_civic_roles" {
		t.Fatalf("debate task evidence = %+v", debateEvidence)
	}
	if debateEvidence.Status != "work_task_blocked" || debateEvidence.Ready || !debateEvidence.Blocked {
		t.Fatalf("debate task status/readiness = %+v", debateEvidence)
	}
	if !containsString(debateEvidence.DependsOnRefs, researchTask.ID().Value()) {
		t.Fatalf("debate task deps = %+v, want research task %s", debateEvidence.DependsOnRefs, researchTask.ID().Value())
	}
	if !containsString(debateEvidence.SourceRefs, dependencyEvent.ID().Value()) {
		t.Fatalf("debate task source refs = %+v, want dependency event %s", debateEvidence.SourceRefs, dependencyEvent.ID().Value())
	}
	queued := projection.QueuedRunRequest
	if queued == nil {
		t.Fatalf("queued run request = nil")
	}
	researchStage := queuedRunLifecycleStageByID(queued.DevelopmentLifecycle, "research_issue_and_repo_context")
	if researchStage == nil || researchStage.EvidenceStatus != civilizationAssemblyQueuedStageTaskDraftStatus {
		t.Fatalf("research stage = %+v, want task draft seeded evidence", researchStage)
	}
	debateStage := queuedRunLifecycleStageByID(queued.DevelopmentLifecycle, "debate_with_correct_civic_roles")
	if debateStage == nil || debateStage.EvidenceStatus != civilizationAssemblyQueuedStageTaskDraftStatus {
		t.Fatalf("debate stage = %+v, want task draft seeded evidence", debateStage)
	}
	implementStage := queuedRunLifecycleStageByID(queued.DevelopmentLifecycle, "implement_on_branch")
	if implementStage == nil || implementStage.EvidenceStatus != "expected_not_observed" {
		t.Fatalf("implement stage = %+v, want no task draft evidence", implementStage)
	}
	strategistStep := queuedRunAgentPlanStepByRole(queued.AgentExecutionPlan, "research_issue_and_repo_context", "strategist")
	if strategistStep == nil || strategistStep.EvidenceStatus != civilizationAssemblyQueuedPlanStepTaskDraftStatus {
		t.Fatalf("strategist step = %+v, want task draft seeded evidence", strategistStep)
	}
	if !containsString(projection.SourceEventIDsOrQueryWindow, dependencyEvent.ID().Value()) {
		t.Fatalf("source refs = %+v, want dependency event %s", projection.SourceEventIDsOrQueryWindow, dependencyEvent.ID().Value())
	}
	secondParentEvidence := civilizationProjectionTaskEvidenceByID(projection.WorkEvidenceSummary.Tasks, secondParentTask.ID().Value())
	if secondParentEvidence == nil || secondParentEvidence.LifecycleStageID != "" || secondParentEvidence.FactoryOrderID != secondOrderID {
		t.Fatalf("second parent task evidence = %+v", secondParentEvidence)
	}
	secondResearchEvidence := civilizationProjectionTaskEvidenceByID(projection.WorkEvidenceSummary.Tasks, secondResearchTask.ID().Value())
	if secondResearchEvidence == nil || secondResearchEvidence.LifecycleStageID != "research_issue_and_repo_context" {
		t.Fatalf("second research task evidence = %+v", secondResearchEvidence)
	}
	unrelatedStageEvidence := civilizationProjectionTaskEvidenceByID(projection.WorkEvidenceSummary.Tasks, unrelatedStageTask.ID().Value())
	if unrelatedStageEvidence == nil || unrelatedStageEvidence.LifecycleStageID != "implement_on_branch" || unrelatedStageEvidence.FactoryOrderID != "fo_run_issue_scan_other_001" {
		t.Fatalf("unrelated stage task evidence = %+v", unrelatedStageEvidence)
	}
}

func TestBuildCivilizationAssemblyProjectionDerivesStageTaskIDWithoutQueuedRun(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	orderID := "fo_run_issue_scan_stage_tasks_no_queue"
	stageID := "research_issue_and_repo_context"
	stageTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:           "Issue-scan stage: Research issue and repo context",
		Description:     "Stage task draft remains identifiable without queued runtime evidence.",
		CreatedBy:       actorID,
		CanonicalTaskID: issueScanLifecycleStageTaskCanonicalID(orderID, stageID),
		FactoryOrderID:  orderID,
		Cell:            "planning",
		RiskClass:       "high",
	})
	unknownStageTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:           "Non-issue-scan task with task-shaped canonical ID",
		Description:     "Unknown stage IDs should not be projected as issue-scan lifecycle stages.",
		CreatedBy:       actorID,
		CanonicalTaskID: "tsk_" + factoryOrderIDSuffix(orderID) + "_not_a_real_issue_scan_stage",
		FactoryOrderID:  orderID,
		Cell:            "planning",
		RiskClass:       "high",
	})
	mismatchedOrderTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:           "Task with another order's stage canonical ID",
		Description:     "Canonical task IDs must match the task FactoryOrderID suffix.",
		CreatedBy:       actorID,
		CanonicalTaskID: issueScanLifecycleStageTaskCanonicalID("fo_other_issue_scan_order", stageID),
		FactoryOrderID:  orderID,
		Cell:            "planning",
		RiskClass:       "high",
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	stageEvidence := civilizationProjectionTaskEvidenceByID(projection.WorkEvidenceSummary.Tasks, stageTask.ID().Value())
	if stageEvidence == nil || stageEvidence.LifecycleStageID != stageID {
		t.Fatalf("stage task evidence = %+v, want lifecycle stage %q", stageEvidence, stageID)
	}
	unknownStageEvidence := civilizationProjectionTaskEvidenceByID(projection.WorkEvidenceSummary.Tasks, unknownStageTask.ID().Value())
	if unknownStageEvidence == nil || unknownStageEvidence.LifecycleStageID != "" {
		t.Fatalf("unknown stage task evidence = %+v, want no lifecycle stage", unknownStageEvidence)
	}
	mismatchedOrderEvidence := civilizationProjectionTaskEvidenceByID(projection.WorkEvidenceSummary.Tasks, mismatchedOrderTask.ID().Value())
	if mismatchedOrderEvidence == nil || mismatchedOrderEvidence.LifecycleStageID != "" {
		t.Fatalf("mismatched order task evidence = %+v, want no lifecycle stage", mismatchedOrderEvidence)
	}
}

func TestBuildCivilizationAssemblyProjectionRejectsMismatchedRoleContracts(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	orderID := "fo_run_issue_scan_role_contracts_001"
	stageID := "research_issue_and_repo_context"
	task := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:           "Issue-scan stage: Research issue and repo context",
		Description:     "Stage task draft with a wrong-stage role contract.",
		CreatedBy:       actorID,
		CanonicalTaskID: issueScanLifecycleStageTaskCanonicalID(orderID, stageID),
		FactoryOrderID:  orderID,
		Cell:            "planning",
		RiskClass:       "high",
	})
	appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    task.ID(),
		Label:     IssueScanStageRoleContractArtifactLabel,
		MediaType: issueScanExecutionPlanArtifactMediaType,
		Body:      issueScanRoleContractBodyForProjectionTest(t, orderID, "run_adversarial_review", []string{"reviewer"}),
		CreatedBy: actorID,
	})
	wrongOrderTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:           "Issue-scan stage: Research second issue",
		Description:     "Stage task draft with a wrong-order role contract.",
		CreatedBy:       actorID,
		CanonicalTaskID: issueScanLifecycleStageTaskCanonicalID(orderID, stageID),
		FactoryOrderID:  orderID,
		Cell:            "planning",
		RiskClass:       "high",
	})
	appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    wrongOrderTask.ID(),
		Label:     IssueScanStageRoleContractArtifactLabel,
		MediaType: issueScanExecutionPlanArtifactMediaType,
		Body:      issueScanRoleContractBodyForProjectionTest(t, "fo_run_issue_scan_role_contracts_other", stageID, []string{"strategist"}),
		CreatedBy: actorID,
	})
	divergentTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:           "Issue-scan stage: Debate with correct civic roles",
		Description:     "Stage task draft with duplicate divergent role contracts.",
		CreatedBy:       actorID,
		CanonicalTaskID: issueScanLifecycleStageTaskCanonicalID(orderID, "debate_with_correct_civic_roles"),
		FactoryOrderID:  orderID,
		Cell:            "planning",
		RiskClass:       "high",
	})
	appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    divergentTask.ID(),
		Label:     IssueScanStageRoleContractArtifactLabel,
		MediaType: issueScanExecutionPlanArtifactMediaType,
		Body:      issueScanRoleContractBodyForProjectionTest(t, orderID, "debate_with_correct_civic_roles", []string{"strategist", "guardian"}),
		CreatedBy: actorID,
	})
	appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    divergentTask.ID(),
		Label:     IssueScanStageRoleContractArtifactLabel,
		MediaType: issueScanExecutionPlanArtifactMediaType,
		Body:      issueScanRoleContractBodyForProjectionTest(t, "fo_run_issue_scan_role_contracts_other", "run_adversarial_review", []string{"reviewer"}),
		CreatedBy: actorID,
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	for _, taskID := range []types.EventID{task.ID(), wrongOrderTask.ID()} {
		taskEvidence := civilizationProjectionTaskEvidenceByID(projection.WorkEvidenceSummary.Tasks, taskID.Value())
		if taskEvidence == nil {
			t.Fatalf("projection missing task evidence for %s", taskID)
		}
		if len(taskEvidence.RequiredRoles) != 0 || len(taskEvidence.RoleContractRefs) != 0 || len(taskEvidence.AgentExecutionPlan) != 0 {
			t.Fatalf("mismatched role contract projected for task %s: %+v", taskID, taskEvidence)
		}
	}
	divergentEvidence := civilizationProjectionTaskEvidenceByID(projection.WorkEvidenceSummary.Tasks, divergentTask.ID().Value())
	if divergentEvidence == nil {
		t.Fatalf("projection missing divergent task evidence for %s", divergentTask.ID())
	}
	if len(divergentEvidence.RequiredRoles) != 0 || len(divergentEvidence.RoleContractRefs) != 0 || len(divergentEvidence.AgentExecutionPlan) != 0 {
		t.Fatalf("divergent task role evidence = %+v, want role evidence withheld", divergentEvidence)
	}
	if !civilizationProjectionFailureContains(projection, "divergent role contract") {
		t.Fatalf("failure reasons = %+v, want divergent role contract projection error", projection.FailureReasons)
	}
}

func TestBuildCivilizationAssemblyProjectionRejectsMismatchedOutputContracts(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	orderID := "fo_run_issue_scan_output_contracts_001"
	stageID := "research_issue_and_repo_context"
	task := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:           "Issue-scan stage: Research issue and repo context",
		Description:     "Stage task draft with a wrong-stage output contract.",
		CreatedBy:       actorID,
		CanonicalTaskID: issueScanLifecycleStageTaskCanonicalID(orderID, stageID),
		FactoryOrderID:  orderID,
		Cell:            "planning",
		RiskClass:       "high",
	})
	appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    task.ID(),
		Label:     IssueScanStageOutputContractArtifactLabel,
		MediaType: issueScanExecutionPlanArtifactMediaType,
		Body:      issueScanOutputContractBodyForProjectionTest(t, orderID, "run_adversarial_review", []string{"reviewer"}, []string{"exact_head_review_artifact"}),
		CreatedBy: actorID,
	})
	wrongOrderTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:           "Issue-scan stage: Research second issue",
		Description:     "Stage task draft with a wrong-order output contract.",
		CreatedBy:       actorID,
		CanonicalTaskID: issueScanLifecycleStageTaskCanonicalID(orderID, stageID),
		FactoryOrderID:  orderID,
		Cell:            "planning",
		RiskClass:       "high",
	})
	appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    wrongOrderTask.ID(),
		Label:     IssueScanStageOutputContractArtifactLabel,
		MediaType: issueScanExecutionPlanArtifactMediaType,
		Body:      issueScanOutputContractBodyForProjectionTest(t, "fo_run_issue_scan_output_contracts_other", stageID, []string{"strategist"}, []string{"repo_context_packet"}),
		CreatedBy: actorID,
	})
	divergentTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:           "Issue-scan stage: Debate with correct civic roles",
		Description:     "Stage task draft with duplicate divergent output contracts.",
		CreatedBy:       actorID,
		CanonicalTaskID: issueScanLifecycleStageTaskCanonicalID(orderID, "debate_with_correct_civic_roles"),
		FactoryOrderID:  orderID,
		Cell:            "planning",
		RiskClass:       "high",
	})
	appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    divergentTask.ID(),
		Label:     IssueScanStageOutputContractArtifactLabel,
		MediaType: issueScanExecutionPlanArtifactMediaType,
		Body:      issueScanOutputContractBodyForProjectionTest(t, orderID, "debate_with_correct_civic_roles", []string{"strategist", "guardian"}, []string{"decision_record"}),
		CreatedBy: actorID,
	})
	appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    divergentTask.ID(),
		Label:     IssueScanStageOutputContractArtifactLabel,
		MediaType: issueScanExecutionPlanArtifactMediaType,
		Body:      issueScanOutputContractBodyForProjectionTest(t, "fo_run_issue_scan_output_contracts_other", "run_adversarial_review", []string{"reviewer"}, []string{"exact_head_review_artifact"}),
		CreatedBy: actorID,
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	for _, taskID := range []types.EventID{task.ID(), wrongOrderTask.ID(), divergentTask.ID()} {
		taskEvidence := civilizationProjectionTaskEvidenceByID(projection.WorkEvidenceSummary.Tasks, taskID.Value())
		if taskEvidence == nil {
			t.Fatalf("projection missing task evidence for %s", taskID)
		}
		if len(taskEvidence.RequiredEvidence) != 0 || len(taskEvidence.OutputContractRefs) != 0 || len(taskEvidence.RoleOutputContracts) != 0 {
			t.Fatalf("mismatched output contract projected for task %s: %+v", taskID, taskEvidence)
		}
	}
	if !civilizationProjectionFailureContains(projection, "divergent output contract") {
		t.Fatalf("failure reasons = %+v, want divergent output contract projection error", projection.FailureReasons)
	}
}

func TestBuildCivilizationAssemblyProjectionProjectsWorkFactoryOrderLifecycleEvidence(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	taskEvent := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:                  "Research and implement issue scan",
		Description:            "FactoryOrder lifecycle should be visible to Site.",
		CreatedBy:              actorID,
		FactoryOrderID:         "fo_run_issue_scan_ready_001",
		RequirementIDs:         []string{"req_issue_scan_ready_001"},
		AcceptanceCriterionIDs: []string{"ac_issue_scan_ready_001"},
		RiskClass:              "high",
		ExpectedOutputs:        []string{"ready-for-Human result PR"},
	})
	transitionEvent := appendEvent(work.EventTypeTaskLifecycleTransitioned, work.TaskLifecycleTransitionContent{
		TaskID:       taskEvent.ID(),
		FromState:    work.StatusCreated,
		ToState:      work.StatusReady,
		Reason:       "FactoryOrder has enough evidence to schedule",
		EvidenceRefs: []string{"readiness_evidence_001"},
		ChangedBy:    actorID,
	})
	verificationEvent := appendEvent(work.EventTypeTaskVerificationAttached, work.TaskVerificationAttachedContent{
		TaskID:        taskEvent.ID(),
		TestRunIDs:    []string{"test_run_issue_scan_001"},
		GateResultIDs: []string{"gate_result_issue_scan_001"},
		Summary:       "readiness verification attached",
		AttachedBy:    actorID,
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if len(projection.FactoryOrderSummary) != 1 {
		t.Fatalf("factory orders = %+v, want one", projection.FactoryOrderSummary)
	}
	order := projection.FactoryOrderSummary[0]
	if order.Status != "work_task_ready" {
		t.Fatalf("factory order status = %q, want work_task_ready", order.Status)
	}
	if !containsString(projection.WorkEvidenceSummary.TestRunRefs, "test_run_issue_scan_001") {
		t.Fatalf("test run refs = %+v, want test_run_issue_scan_001", projection.WorkEvidenceSummary.TestRunRefs)
	}
	if !containsString(projection.WorkEvidenceSummary.GateResultRefs, "gate_result_issue_scan_001") {
		t.Fatalf("gate result refs = %+v, want gate_result_issue_scan_001", projection.WorkEvidenceSummary.GateResultRefs)
	}
	if !containsString(projection.SourceEventIDsOrQueryWindow, taskEvent.ID().Value()) {
		t.Fatalf("source refs = %+v, want task event %s", projection.SourceEventIDsOrQueryWindow, taskEvent.ID().Value())
	}
	if !containsString(projection.SourceEventIDsOrQueryWindow, transitionEvent.ID().Value()) {
		t.Fatalf("source refs = %+v, want transition event %s", projection.SourceEventIDsOrQueryWindow, transitionEvent.ID().Value())
	}
	if !containsString(projection.SourceEventIDsOrQueryWindow, verificationEvent.ID().Value()) {
		t.Fatalf("source refs = %+v, want verification event %s", projection.SourceEventIDsOrQueryWindow, verificationEvent.ID().Value())
	}
}

func TestBuildCivilizationAssemblyProjectionProjectsWorkFactoryOrderArtifacts(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	taskEvent := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:                  "Implement issue scan result",
		Description:            "FactoryOrder artifact should be visible to Site.",
		CreatedBy:              actorID,
		FactoryOrderID:         "fo_run_issue_scan_artifact_001",
		RequirementIDs:         []string{"req_issue_scan_artifact_001"},
		AcceptanceCriterionIDs: []string{"ac_issue_scan_artifact_001"},
		RiskClass:              "high",
		ExpectedOutputs:        []string{"ready-for-Human result PR"},
	})
	artifactEvent := appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    taskEvent.ID(),
		Label:     "Implementation artifact",
		MediaType: "text/markdown",
		Body:      "commit: abc123",
		CreatedBy: actorID,
	})
	unrelatedTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:     "Unrelated Work task",
		CreatedBy: actorID,
		RiskClass: "low",
	})
	unrelatedArtifact := appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    unrelatedTask.ID(),
		Label:     "Unrelated artifact",
		MediaType: "text/markdown",
		Body:      "not part of the FactoryOrder",
		CreatedBy: actorID,
	})
	appendEvent(EventTypeRunStarted, RunStartedContent{
		Idea:     "render runtime artifact refs",
		RepoPath: "/tmp/hive-runtime",
	})
	runtimeArtifact := appendEvent(EventTypeFactoryArtifactCreated, FactoryArtifactCreatedContent{
		RunID:      "run_runtime_artifact_001",
		ArtifactID: "runtime_artifact_001",
		Label:      "Runtime artifact",
		MediaType:  "text/markdown",
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if !containsString(projection.WorkEvidenceSummary.ArtifactRefs, artifactEvent.ID().Value()) {
		t.Fatalf("artifact refs = %+v, want FactoryOrder artifact %s", projection.WorkEvidenceSummary.ArtifactRefs, artifactEvent.ID().Value())
	}
	if containsString(projection.WorkEvidenceSummary.ArtifactRefs, unrelatedArtifact.ID().Value()) {
		t.Fatalf("artifact refs = %+v, want no unrelated artifact %s", projection.WorkEvidenceSummary.ArtifactRefs, unrelatedArtifact.ID().Value())
	}
	artifactEvidence := civilizationProjectionArtifactEvidenceByID(projection.WorkEvidenceSummary.Artifacts, artifactEvent.ID().Value())
	if artifactEvidence == nil {
		t.Fatalf("artifact evidence = %+v, want FactoryOrder artifact %s", projection.WorkEvidenceSummary.Artifacts, artifactEvent.ID().Value())
	}
	if artifactEvidence.TaskRef != taskEvent.ID().Value() {
		t.Fatalf("artifact task ref = %q, want %s", artifactEvidence.TaskRef, taskEvent.ID())
	}
	if artifactEvidence.Label != "Implementation artifact" || artifactEvidence.MediaType != "text/markdown" {
		t.Fatalf("artifact metadata = %+v, want implementation markdown artifact", artifactEvidence)
	}
	if !containsString(artifactEvidence.SourceRefs, artifactEvent.ID().Value()) {
		t.Fatalf("artifact source refs = %+v, want %s", artifactEvidence.SourceRefs, artifactEvent.ID())
	}
	if civilizationProjectionArtifactEvidenceByID(projection.WorkEvidenceSummary.Artifacts, unrelatedArtifact.ID().Value()) != nil {
		t.Fatalf("artifact evidence = %+v, want no unrelated artifact %s", projection.WorkEvidenceSummary.Artifacts, unrelatedArtifact.ID())
	}
	if !containsString(projection.WorkEvidenceSummary.ArtifactRefs, "runtime_artifact_001") {
		t.Fatalf("artifact refs = %+v, want runtime artifact logical ref", projection.WorkEvidenceSummary.ArtifactRefs)
	}
	if civilizationProjectionArtifactEvidenceByID(projection.WorkEvidenceSummary.Artifacts, runtimeArtifact.ID().Value()) != nil {
		t.Fatalf("artifact evidence = %+v, want no structured runtime artifact %s", projection.WorkEvidenceSummary.Artifacts, runtimeArtifact.ID())
	}
	if civilizationProjectionArtifactEvidenceByID(projection.WorkEvidenceSummary.Artifacts, "runtime_artifact_001") != nil {
		t.Fatalf("artifact evidence = %+v, want no structured runtime artifact logical ref", projection.WorkEvidenceSummary.Artifacts)
	}
	if !containsString(projection.SourceEventIDsOrQueryWindow, artifactEvent.ID().Value()) {
		t.Fatalf("source refs = %+v, want artifact event %s", projection.SourceEventIDsOrQueryWindow, artifactEvent.ID().Value())
	}
	if !containsString(projection.SourceEventIDsOrQueryWindow, runtimeArtifact.ID().Value()) {
		t.Fatalf("source refs = %+v, want runtime artifact event %s", projection.SourceEventIDsOrQueryWindow, runtimeArtifact.ID().Value())
	}
	if containsString(projection.SourceEventIDsOrQueryWindow, unrelatedArtifact.ID().Value()) {
		t.Fatalf("source refs = %+v, want no unrelated artifact %s", projection.SourceEventIDsOrQueryWindow, unrelatedArtifact.ID().Value())
	}
}

func TestCompactCivilizationAssemblyArtifactEvidenceMergesAndSorts(t *testing.T) {
	artifacts := compactCivilizationAssemblyArtifactEvidence([]CivilizationAssemblyArtifactEvidence{
		{
			ID:         "artifact_b",
			TaskRef:    "task_b",
			Label:      "Beta artifact",
			MediaType:  "application/json",
			SourceRefs: []string{"source_b"},
		},
		{
			ID:         "artifact_a",
			Label:      "Alpha artifact",
			SourceRefs: []string{"source_a2"},
		},
		{
			ID:         "artifact_a",
			TaskRef:    "task_a",
			MediaType:  "text/markdown",
			SourceRefs: []string{"source_a1"},
		},
	})

	if len(artifacts) != 2 {
		t.Fatalf("artifact count = %d, want 2: %+v", len(artifacts), artifacts)
	}
	if artifacts[0].ID != "artifact_a" || artifacts[1].ID != "artifact_b" {
		t.Fatalf("artifact order = %+v, want artifact_a then artifact_b", artifacts)
	}
	alpha := artifacts[0]
	if alpha.TaskRef != "task_a" || alpha.Label != "Alpha artifact" || alpha.MediaType != "text/markdown" {
		t.Fatalf("merged artifact_a = %+v, want backfilled task, label, media type", alpha)
	}
	if len(alpha.SourceRefs) != 2 || alpha.SourceRefs[0] != "source_a1" || alpha.SourceRefs[1] != "source_a2" {
		t.Fatalf("artifact_a source refs = %+v, want sorted merged refs", alpha.SourceRefs)
	}
}

func TestBuildCivilizationAssemblyProjectionProjectsQueuedIssueScanLifecycle(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)
	sourceEventID := newTestEventID(t)
	briefEventID := newTestEventID(t)
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
		Body:   "The Civilization should scan Transpara-AI repos, then surface a ready-for-Human PR.",
		Labels: []string{"civilization"},
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	requestEvent := appendEvent(EventTypeFactoryRunRequested, FactoryRunRequestedContent{
		RunID:      "run_issue_scan_001",
		IntakeID:   "intake_issue_scan_001",
		OperatorID: "operator_michael",
		Title:      "Resolve transpara-ai/hive#321",
		Status:     "queued",
		Authority: RunLaunchAuthority{
			InitialLevel: event.AuthorityLevelRequired,
			Scope:        "transpara-ai issue scan to ready-for-Human PR; no merge or deploy",
			PolicyRef:    IssueScanDefaultPolicyRef,
			Rationale:    "Civilization selected a Transpara-AI GitHub issue for governed factory execution.",
		},
		Budget: RunLaunchBudget{
			MaxIterations: 12,
			MaxCostUSD:    25,
		},
		TargetRepos:   []string{"transpara-ai/hive"},
		SourceEventID: sourceEventID,
		BriefEventID:  briefEventID,
		Brief:         brief,
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	queued := projection.QueuedRunRequest
	if queued == nil {
		t.Fatalf("queued run request was not projected: %+v", projection)
	}
	if queued.RunID != "run_issue_scan_001" || queued.EvidenceKind != "queued_request_not_runtime_start" {
		t.Fatalf("queued run request = %+v", queued)
	}
	if queued.BriefKind != issueScanBriefKind || queued.LifecycleVersion != issueScanLifecycleVersion {
		t.Fatalf("queued brief metadata = %+v", queued)
	}
	if queued.SelectionPolicy == nil || queued.SelectionPolicy.PolicyID != "deterministic_actionability_rank_v0.2" || queued.SelectionPolicy.SelectedRank != 1 || queued.SelectionPolicy.CandidateCount != 1 {
		t.Fatalf("queued selection policy = %+v", queued.SelectionPolicy)
	}
	if !containsModelProjectionString(queued.SelectionPolicy.RankingInputs, "actionability_keyword_score") || !strings.Contains(queued.SelectionPolicy.Rationale, "actionability signals") {
		t.Fatalf("queued selection policy details = %+v", queued.SelectionPolicy)
	}
	if queued.RoleSeparationPolicy == nil || queued.RoleSeparationPolicy.PolicyID != "civilization_issue_scan_role_separation_v0.1" {
		t.Fatalf("queued role separation policy = %+v", queued.RoleSeparationPolicy)
	}
	implementerPolicy := queuedRunRolePolicyByActor(queued.RoleSeparationPolicy.ActorPolicies, "implementer")
	if implementerPolicy == nil || !implementerPolicy.CanOperateBranch || implementerPolicy.CanMutateGitHub || implementerPolicy.CanOpenPullRequest || implementerPolicy.CanApproveOrMerge {
		t.Fatalf("implementer role separation policy = %+v", implementerPolicy)
	}
	reviewerPolicy := queuedRunRolePolicyByActor(queued.RoleSeparationPolicy.ActorPolicies, "reviewer")
	if reviewerPolicy == nil || reviewerPolicy.CanOperateBranch || reviewerPolicy.CanApproveOrMerge || !containsModelProjectionString(reviewerPolicy.MapsToStageRoles, "reviewer") {
		t.Fatalf("reviewer role separation policy = %+v", reviewerPolicy)
	}
	humanPolicy := queuedRunRolePolicyByActor(queued.RoleSeparationPolicy.ActorPolicies, "human_authority")
	if humanPolicy == nil || !humanPolicy.CanApproveOrMerge || !humanPolicy.CanMutateGitHub || !containsModelProjectionString(humanPolicy.ForbiddenWithoutHuman, "hive_self_authorized_merge") {
		t.Fatalf("human authority role separation policy = %+v", humanPolicy)
	}
	if !containsModelProjectionString(queued.RoleSeparationPolicy.GlobalNonClaims, "github_issue_is_not_authority") || !containsModelProjectionString(queued.RoleSeparationPolicy.GlobalNonClaims, "no_autonomy_increase") {
		t.Fatalf("role separation non-claims = %+v", queued.RoleSeparationPolicy.GlobalNonClaims)
	}
	if queued.AutonomyGuardPolicy == nil || queued.AutonomyGuardPolicy.PolicyID != "civilization_issue_scan_autonomy_guard_v0.1" {
		t.Fatalf("queued autonomy guard policy = %+v", queued.AutonomyGuardPolicy)
	}
	if queued.AutonomyGuardPolicy.RecommendationPosture != "recommendation_only_no_authority" || queued.AutonomyGuardPolicy.AutonomyCeiling != "level_0_no_autonomy_increase" {
		t.Fatalf("queued autonomy guard posture = %+v", queued.AutonomyGuardPolicy)
	}
	if !containsModelProjectionString(queued.AutonomyGuardPolicy.EvidenceInputs, "selected_issue_cc_labels") || !containsModelProjectionString(queued.AutonomyGuardPolicy.EvidenceInputs, "role_separation_policy") {
		t.Fatalf("queued autonomy guard evidence inputs = %+v", queued.AutonomyGuardPolicy.EvidenceInputs)
	}
	if !containsModelProjectionString(queued.AutonomyGuardPolicy.FailClosedOutputs, "human_scope_required") || !containsModelProjectionString(queued.AutonomyGuardPolicy.FailClosedOutputs, "autonomy_increase_blocked") {
		t.Fatalf("queued autonomy guard fail-closed outputs = %+v", queued.AutonomyGuardPolicy.FailClosedOutputs)
	}
	if !containsModelProjectionString(queued.AutonomyGuardPolicy.HumanScopeTriggers, "protected_action") || !containsModelProjectionString(queued.AutonomyGuardPolicy.HumanScopeTriggers, "autonomy_increase") {
		t.Fatalf("queued autonomy guard human-scope triggers = %+v", queued.AutonomyGuardPolicy.HumanScopeTriggers)
	}
	if !containsModelProjectionString(queued.AutonomyGuardPolicy.ForbiddenAuthorityClaims, "recommendation_authorizes_autonomy_increase") || !containsModelProjectionString(queued.AutonomyGuardPolicy.ForbiddenAuthorityClaims, "recommendation_marks_test_001_green") {
		t.Fatalf("queued autonomy guard forbidden authority claims = %+v", queued.AutonomyGuardPolicy.ForbiddenAuthorityClaims)
	}
	if queued.HumanRequiredClassificationPolicy == nil || queued.HumanRequiredClassificationPolicy.PolicyID != "civilization_issue_scan_human_required_classification_v0.1" {
		t.Fatalf("queued human-required classification policy = %+v", queued.HumanRequiredClassificationPolicy)
	}
	if queued.HumanRequiredClassificationPolicy.ClassificationPosture != "human_required_for_protected_or_authority_sensitive_work" {
		t.Fatalf("queued human-required classification posture = %+v", queued.HumanRequiredClassificationPolicy)
	}
	if !containsModelProjectionString(queued.HumanRequiredClassificationPolicy.HumanRequiredTriggers, "protected_action") || !containsModelProjectionString(queued.HumanRequiredClassificationPolicy.HumanRequiredTriggers, "authority_decision_required") {
		t.Fatalf("queued human-required triggers = %+v", queued.HumanRequiredClassificationPolicy.HumanRequiredTriggers)
	}
	if !containsModelProjectionString(queued.HumanRequiredClassificationPolicy.EscalationOutputs, "issue_parking_required") || !containsModelProjectionString(queued.HumanRequiredClassificationPolicy.EscalationOutputs, "no_token_burn") {
		t.Fatalf("queued human-required escalation outputs = %+v", queued.HumanRequiredClassificationPolicy.EscalationOutputs)
	}
	if !containsModelProjectionString(queued.HumanRequiredClassificationPolicy.ForbiddenWithoutHuman, "write_eventgraph_truth") || !containsModelProjectionString(queued.HumanRequiredClassificationPolicy.ForbiddenWithoutHuman, "create_or_merge_pr") {
		t.Fatalf("queued human-required forbidden actions = %+v", queued.HumanRequiredClassificationPolicy.ForbiddenWithoutHuman)
	}
	if !containsModelProjectionString(queued.HumanRequiredClassificationPolicy.NonAuthorityClaims, "classification_is_not_authority_decision") || !containsModelProjectionString(queued.HumanRequiredClassificationPolicy.NonAuthorityClaims, "no_protected_action_execution") {
		t.Fatalf("queued human-required non-authority claims = %+v", queued.HumanRequiredClassificationPolicy.NonAuthorityClaims)
	}
	if queued.AuthorityRecommendationPolicy == nil || queued.AuthorityRecommendationPolicy.PolicyID != "civilization_issue_scan_authority_recommendation_v0.1" {
		t.Fatalf("queued authority recommendation policy = %+v", queued.AuthorityRecommendationPolicy)
	}
	if queued.AuthorityRecommendationPolicy.RecommendationPosture != "recommendation_only_not_authority_decision" || queued.AuthorityRecommendationPolicy.DecisionBoundary != "external_human_scoped_authority_decision_required" {
		t.Fatalf("queued authority recommendation posture = %+v", queued.AuthorityRecommendationPolicy)
	}
	if !containsModelProjectionString(queued.AuthorityRecommendationPolicy.EvidenceInputs, "selected_issue_scope_evidence") || !containsModelProjectionString(queued.AuthorityRecommendationPolicy.EvidenceInputs, "human_required_classification_policy") {
		t.Fatalf("queued authority recommendation evidence inputs = %+v", queued.AuthorityRecommendationPolicy.EvidenceInputs)
	}
	if !containsModelProjectionString(queued.AuthorityRecommendationPolicy.RecommendationOutputs, "no_authority_decision") || !containsModelProjectionString(queued.AuthorityRecommendationPolicy.RecommendationOutputs, "no_write_no_action") {
		t.Fatalf("queued authority recommendation outputs = %+v", queued.AuthorityRecommendationPolicy.RecommendationOutputs)
	}
	if !containsModelProjectionString(queued.AuthorityRecommendationPolicy.RequiredHumanAuthority, "external_authority_decision_ref") || !containsModelProjectionString(queued.AuthorityRecommendationPolicy.RequiredHumanAuthority, "human_approval_ref") {
		t.Fatalf("queued authority recommendation human refs = %+v", queued.AuthorityRecommendationPolicy.RequiredHumanAuthority)
	}
	if !containsModelProjectionString(queued.AuthorityRecommendationPolicy.ForbiddenAuthorityClaims, "recommendation_is_authority_decision") || !containsModelProjectionString(queued.AuthorityRecommendationPolicy.ForbiddenAuthorityClaims, "pr_ready_label_authorizes_protected_action") {
		t.Fatalf("queued authority recommendation forbidden claims = %+v", queued.AuthorityRecommendationPolicy.ForbiddenAuthorityClaims)
	}
	if queued.ValueAllocationBoundaryPolicy == nil || queued.ValueAllocationBoundaryPolicy.PolicyID != "civilization_issue_scan_value_allocation_boundary_v0.1" {
		t.Fatalf("queued value-allocation boundary policy = %+v", queued.ValueAllocationBoundaryPolicy)
	}
	if queued.ValueAllocationBoundaryPolicy.BoundaryPosture != "deny_by_default_human_required" || queued.ValueAllocationBoundaryPolicy.DisplayMode != "pull_display_only_not_routing_or_action" {
		t.Fatalf("queued value-allocation boundary posture = %+v", queued.ValueAllocationBoundaryPolicy)
	}
	if !containsModelProjectionString(queued.ValueAllocationBoundaryPolicy.ClassificationOutputs, "value_allocation_candidate") || !containsModelProjectionString(queued.ValueAllocationBoundaryPolicy.ClassificationOutputs, "no_allocation_authority") {
		t.Fatalf("queued value-allocation classification outputs = %+v", queued.ValueAllocationBoundaryPolicy.ClassificationOutputs)
	}
	if !containsModelProjectionString(queued.ValueAllocationBoundaryPolicy.RequiredHumanAuthority, "external_committee_value_allocation_decision") || !containsModelProjectionString(queued.ValueAllocationBoundaryPolicy.RequiredHumanAuthority, "human_approval_ref") {
		t.Fatalf("queued value-allocation required human authority = %+v", queued.ValueAllocationBoundaryPolicy.RequiredHumanAuthority)
	}
	if !containsModelProjectionString(queued.ValueAllocationBoundaryPolicy.ForbiddenSystemActions, "allocate_value") || !containsModelProjectionString(queued.ValueAllocationBoundaryPolicy.ForbiddenSystemActions, "route_value_allocation_candidate") || !containsModelProjectionString(queued.ValueAllocationBoundaryPolicy.ForbiddenSystemActions, "notify_as_action") {
		t.Fatalf("queued value-allocation forbidden system actions = %+v", queued.ValueAllocationBoundaryPolicy.ForbiddenSystemActions)
	}
	if !containsModelProjectionString(queued.ValueAllocationBoundaryPolicy.ForbiddenAuthorityClaims, "display_authorizes_allocation") || !containsModelProjectionString(queued.ValueAllocationBoundaryPolicy.ForbiddenAuthorityClaims, "human_required_label_is_approval") {
		t.Fatalf("queued value-allocation forbidden authority claims = %+v", queued.ValueAllocationBoundaryPolicy.ForbiddenAuthorityClaims)
	}
	if !containsModelProjectionString(queued.ValueAllocationBoundaryPolicy.NonAuthorityClaims, "display_is_not_authority_decision") || !containsModelProjectionString(queued.ValueAllocationBoundaryPolicy.NonAuthorityClaims, "no_value_allocation_execution") {
		t.Fatalf("queued value-allocation non-authority claims = %+v", queued.ValueAllocationBoundaryPolicy.NonAuthorityClaims)
	}
	if queued.LifecycleEvidenceKind != "expected_lifecycle_not_runtime_progress" {
		t.Fatalf("queued lifecycle evidence kind = %q", queued.LifecycleEvidenceKind)
	}
	if len(queued.DevelopmentLifecycle) != 7 {
		t.Fatalf("queued lifecycle = %+v, want 7 stages", queued.DevelopmentLifecycle)
	}
	readyStage := queuedRunLifecycleStageByID(queued.DevelopmentLifecycle, "surface_ready_for_Human_result_PR")
	if readyStage == nil || readyStage.AuthorityBoundary != "human_approval_required_no_merge" {
		t.Fatalf("ready stage = %+v", readyStage)
	}
	if len(queued.AgentExecutionPlan) != 18 {
		t.Fatalf("queued agent plan = %+v, want 18 steps", queued.AgentExecutionPlan)
	}
	implementStep := queuedRunAgentPlanStepByRole(queued.AgentExecutionPlan, "implement_on_branch", "implementer")
	if implementStep == nil || !implementStep.CanOperate || !containsModelProjectionString(implementStep.RequiredOutputs, "validation_output") {
		t.Fatalf("implementer step = %+v", implementStep)
	}
	reviewStep := queuedRunAgentPlanStepByRole(queued.AgentExecutionPlan, "run_adversarial_review", "reviewer")
	if reviewStep == nil || reviewStep.CanOperate || !containsModelProjectionString(reviewStep.RequiredOutputs, "exact_head_review_artifact") {
		t.Fatalf("reviewer step = %+v", reviewStep)
	}
	if !containsString(projection.SourceEventIDsOrQueryWindow, requestEvent.ID().Value()) {
		t.Fatalf("source refs = %+v, want queued request event %s", projection.SourceEventIDsOrQueryWindow, requestEvent.ID().Value())
	}
	if !containsString(projection.WorkEvidenceSummary.SourceRefs, sourceEventID.Value()) || !containsString(projection.WorkEvidenceSummary.SourceRefs, briefEventID.Value()) {
		t.Fatalf("work evidence source refs = %+v, want source %s and brief %s", projection.WorkEvidenceSummary.SourceRefs, sourceEventID.Value(), briefEventID.Value())
	}
	if !civilizationProjectionHasResidualRisk(projection, "runtime_limitation_06") {
		t.Fatalf("residual risks = %+v, want queued lifecycle limitation", projection.ResidualRiskSummary)
	}
}

func TestBuildCivilizationAssemblyProjectionMarksQueuedLifecycleStageDeclarations(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	sourceEventID := newTestEventID(t)
	briefEventID := newTestEventID(t)
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 322,
		Title:  "Project declared stage evidence",
		URL:    "https://github.com/transpara-ai/hive/issues/322",
		Body:   "The Civilization should show declared lifecycle stage evidence without claiming runtime completion.",
		Labels: []string{"civilization"},
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	request := FactoryRunRequestedContent{
		RunID:      "run_issue_scan_stage_declared_001",
		IntakeID:   "intake_issue_scan_stage_declared_001",
		OperatorID: "operator_michael",
		Title:      "Resolve transpara-ai/hive#322",
		Status:     "queued",
		Authority: RunLaunchAuthority{
			InitialLevel: event.AuthorityLevelRequired,
			Scope:        "transpara-ai issue scan to ready-for-Human PR; no merge or deploy",
			PolicyRef:    IssueScanDefaultPolicyRef,
			Rationale:    "Civilization selected a Transpara-AI GitHub issue for governed factory execution.",
		},
		Budget: RunLaunchBudget{
			MaxIterations: 12,
			MaxCostUSD:    25,
		},
		TargetRepos:   []string{"transpara-ai/hive"},
		SourceEventID: sourceEventID,
		BriefEventID:  briefEventID,
		Brief:         brief,
	}
	appendEvent(EventTypeFactoryRunRequested, request)
	taskEvent := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:          "Implement declared lifecycle stage projection",
		Description:    "FactoryOrder lifecycle stage artifact should enrich the queued run view.",
		CreatedBy:      actorID,
		FactoryOrderID: "fo_run_issue_scan_stage_declared_001",
		RiskClass:      "high",
		ExpectedOutputs: []string{
			"queued lifecycle stage declaration status",
		},
	})
	appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:           "Issue-scan stage: Research issue and repo context",
		Description:     "Matching task draft should take precedence over declared stage artifact status.",
		CreatedBy:       actorID,
		CanonicalTaskID: issueScanLifecycleStageTaskCanonicalID("fo_run_issue_scan_stage_declared_001", "research_issue_and_repo_context"),
		FactoryOrderID:  "fo_run_issue_scan_stage_declared_001",
		RiskClass:       "high",
	})
	stageArtifacts, err := issueScanLifecycleStageArtifacts(request)
	if err != nil {
		t.Fatalf("issueScanLifecycleStageArtifacts: %v", err)
	}
	stageArtifactByLabel := func(artifacts []issueScanDispatchArtifact, label string) issueScanDispatchArtifact {
		t.Helper()
		for _, artifact := range artifacts {
			if artifact.Label == label {
				return artifact
			}
		}
		t.Fatalf("missing stage artifact %q in %+v", label, artifacts)
		return issueScanDispatchArtifact{}
	}
	researchArtifact := stageArtifactByLabel(stageArtifacts, IssueScanLifecycleStageArtifactLabel("research_issue_and_repo_context"))
	debateArtifact := stageArtifactByLabel(stageArtifacts, IssueScanLifecycleStageArtifactLabel("debate_with_correct_civic_roles"))
	researchArtifactEvent := appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    taskEvent.ID(),
		Label:     researchArtifact.Label,
		MediaType: researchArtifact.MediaType,
		Body:      researchArtifact.Body,
		CreatedBy: actorID,
	})
	appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    taskEvent.ID(),
		Label:     debateArtifact.Label,
		MediaType: debateArtifact.MediaType,
		Body:      debateArtifact.Body,
		CreatedBy: actorID,
	})
	otherRunRequest := request
	otherRunRequest.RunID = "run_issue_scan_other_001"
	otherStageArtifacts, err := issueScanLifecycleStageArtifacts(otherRunRequest)
	if err != nil {
		t.Fatalf("issueScanLifecycleStageArtifacts other run: %v", err)
	}
	otherImplementArtifact := stageArtifactByLabel(otherStageArtifacts, IssueScanLifecycleStageArtifactLabel("implement_on_branch"))
	appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    taskEvent.ID(),
		Label:     otherImplementArtifact.Label,
		MediaType: otherImplementArtifact.MediaType,
		Body:      otherImplementArtifact.Body,
		CreatedBy: actorID,
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	queued := projection.QueuedRunRequest
	if queued == nil {
		t.Fatalf("queued run request was not projected: %+v", projection)
	}
	researchStage := queuedRunLifecycleStageByID(queued.DevelopmentLifecycle, "research_issue_and_repo_context")
	if researchStage == nil || researchStage.EvidenceStatus != civilizationAssemblyQueuedStageTaskDraftStatus {
		t.Fatalf("research stage = %+v, want task draft seeded evidence", researchStage)
	}
	debateStage := queuedRunLifecycleStageByID(queued.DevelopmentLifecycle, "debate_with_correct_civic_roles")
	if debateStage == nil || debateStage.EvidenceStatus != civilizationAssemblyQueuedStageDeclaredStatus {
		t.Fatalf("debate stage = %+v, want declared pending runtime evidence", debateStage)
	}
	implementStage := queuedRunLifecycleStageByID(queued.DevelopmentLifecycle, "implement_on_branch")
	if implementStage == nil || implementStage.EvidenceStatus != "expected_not_observed" {
		t.Fatalf("implement stage = %+v, want unrelated run artifact ignored", implementStage)
	}
	strategistStep := queuedRunAgentPlanStepByRole(queued.AgentExecutionPlan, "research_issue_and_repo_context", "strategist")
	if strategistStep == nil || strategistStep.EvidenceStatus != civilizationAssemblyQueuedPlanStepTaskDraftStatus {
		t.Fatalf("strategist step = %+v, want task draft seeded evidence", strategistStep)
	}
	debatePlannerStep := queuedRunAgentPlanStepByRole(queued.AgentExecutionPlan, "debate_with_correct_civic_roles", "planner")
	if debatePlannerStep == nil || debatePlannerStep.EvidenceStatus != civilizationAssemblyQueuedPlanStepDeclaredStatus {
		t.Fatalf("debate planner step = %+v, want declared pending runtime evidence", debatePlannerStep)
	}
	implementStep := queuedRunAgentPlanStepByRole(queued.AgentExecutionPlan, "implement_on_branch", "implementer")
	if implementStep == nil || implementStep.EvidenceStatus != "expected_not_observed" {
		t.Fatalf("implementer step = %+v, want unrelated run artifact ignored", implementStep)
	}
	if civilizationProjectionArtifactEvidenceByID(projection.WorkEvidenceSummary.Artifacts, researchArtifactEvent.ID().Value()) == nil {
		t.Fatalf("artifact evidence = %+v, want research stage artifact %s", projection.WorkEvidenceSummary.Artifacts, researchArtifactEvent.ID().Value())
	}
	operatorProjection := BuildOperatorProjection(s, 50)
	operatorResearchStage := queuedRunLifecycleStageByID(operatorProjection.RuntimeEvidence.LastQueuedRunRequest.DevelopmentLifecycle, "research_issue_and_repo_context")
	if operatorResearchStage == nil || operatorResearchStage.EvidenceStatus != "expected_not_observed" {
		t.Fatalf("operator projection research stage = %+v, want raw queued evidence unchanged", operatorResearchStage)
	}
}

func TestCivilizationAssemblyQueuedRunRequestWithStageEvidencePreservesRuntimeStatuses(t *testing.T) {
	queued := &OperatorQueuedRunRequestEvidence{
		RunID: "run_status_preservation",
		AutonomyGuardPolicy: &OperatorQueuedRunAutonomyGuardPolicy{
			PolicyID:                 "civilization_issue_scan_autonomy_guard_v0.1",
			EvidenceInputs:           []string{"selected_issue_cc_labels"},
			FailClosedOutputs:        []string{"human_scope_required"},
			HumanScopeTriggers:       []string{"autonomy_increase"},
			ForbiddenAuthorityClaims: []string{"recommendation_authorizes_autonomy_increase"},
		},
		HumanRequiredClassificationPolicy: &OperatorQueuedRunHumanRequiredClassificationPolicy{
			PolicyID:              "civilization_issue_scan_human_required_classification_v0.1",
			HumanRequiredTriggers: []string{"protected_action"},
			EscalationOutputs:     []string{"issue_parking_required"},
			ForbiddenWithoutHuman: []string{"write_eventgraph_truth"},
			NonAuthorityClaims:    []string{"classification_is_not_authority_decision"},
		},
		AuthorityRecommendationPolicy: &OperatorQueuedRunAuthorityRecommendationPolicy{
			PolicyID:                 "civilization_issue_scan_authority_recommendation_v0.1",
			EvidenceInputs:           []string{"selected_issue_scope_evidence"},
			RecommendationOutputs:    []string{"no_authority_decision"},
			RequiredHumanAuthority:   []string{"external_authority_decision_ref"},
			ForbiddenAuthorityClaims: []string{"recommendation_is_authority_decision"},
		},
		ValueAllocationBoundaryPolicy: &OperatorQueuedRunValueAllocationBoundaryPolicy{
			PolicyID:                 "civilization_issue_scan_value_allocation_boundary_v0.1",
			BoundaryPosture:          "deny_by_default_human_required",
			DisplayMode:              "pull_display_only_not_routing_or_action",
			ClassificationOutputs:    []string{"value_allocation_candidate"},
			RequiredHumanAuthority:   []string{"external_committee_value_allocation_decision"},
			ForbiddenSystemActions:   []string{"allocate_value"},
			ForbiddenAuthorityClaims: []string{"display_authorizes_allocation"},
			NonAuthorityClaims:       []string{"display_is_not_authority_decision"},
		},
		DevelopmentLifecycle: []OperatorQueuedRunLifecycleStage{
			{ID: "runtime_completed_stage", EvidenceStatus: "runtime_completed"},
			{ID: "declared_stage", EvidenceStatus: "expected_not_observed"},
		},
		AgentExecutionPlan: []OperatorQueuedRunAgentPlanStep{
			{StageID: "runtime_completed_stage", Role: "guardian", EvidenceStatus: "runtime_completed"},
			{StageID: "declared_stage", Role: "implementer", EvidenceStatus: "expected_not_observed"},
		},
	}

	out := civilizationAssemblyQueuedRunRequestWithStageEvidence(queued, []civilizationAssemblyStageArtifactRef{
		{RunID: "run_status_preservation", StageID: "declared_stage", EventRef: "artifact_declared"},
	}, []CivilizationAssemblyTaskEvidence{
		{ID: "task_runtime_stage", FactoryOrderID: "fo_run_status_preservation", LifecycleStageID: "runtime_completed_stage"},
	})
	if out == nil {
		t.Fatal("queued run with stage evidence returned nil")
	}
	if out.AutonomyGuardPolicy == nil || !containsModelProjectionString(out.AutonomyGuardPolicy.FailClosedOutputs, "human_scope_required") {
		t.Fatalf("autonomy guard policy = %+v", out.AutonomyGuardPolicy)
	}
	out.AutonomyGuardPolicy.FailClosedOutputs[0] = "mutated"
	if queued.AutonomyGuardPolicy.FailClosedOutputs[0] != "human_scope_required" {
		t.Fatalf("source autonomy guard policy mutated: %+v", queued.AutonomyGuardPolicy)
	}
	if out.HumanRequiredClassificationPolicy == nil || !containsModelProjectionString(out.HumanRequiredClassificationPolicy.EscalationOutputs, "issue_parking_required") {
		t.Fatalf("human-required classification policy = %+v", out.HumanRequiredClassificationPolicy)
	}
	out.HumanRequiredClassificationPolicy.EscalationOutputs[0] = "mutated"
	if queued.HumanRequiredClassificationPolicy.EscalationOutputs[0] != "issue_parking_required" {
		t.Fatalf("source human-required classification policy mutated: %+v", queued.HumanRequiredClassificationPolicy)
	}
	if out.AuthorityRecommendationPolicy == nil || !containsModelProjectionString(out.AuthorityRecommendationPolicy.RecommendationOutputs, "no_authority_decision") {
		t.Fatalf("authority recommendation policy = %+v", out.AuthorityRecommendationPolicy)
	}
	out.AuthorityRecommendationPolicy.RecommendationOutputs[0] = "mutated"
	if queued.AuthorityRecommendationPolicy.RecommendationOutputs[0] != "no_authority_decision" {
		t.Fatalf("source authority recommendation policy mutated: %+v", queued.AuthorityRecommendationPolicy)
	}
	if out.ValueAllocationBoundaryPolicy == nil || !containsModelProjectionString(out.ValueAllocationBoundaryPolicy.ForbiddenSystemActions, "allocate_value") {
		t.Fatalf("value-allocation boundary policy = %+v", out.ValueAllocationBoundaryPolicy)
	}
	out.ValueAllocationBoundaryPolicy.ForbiddenSystemActions[0] = "mutated"
	if queued.ValueAllocationBoundaryPolicy.ForbiddenSystemActions[0] != "allocate_value" {
		t.Fatalf("source value-allocation boundary policy mutated: %+v", queued.ValueAllocationBoundaryPolicy)
	}
	out.ValueAllocationBoundaryPolicy.ClassificationOutputs[0] = "mutated"
	if queued.ValueAllocationBoundaryPolicy.ClassificationOutputs[0] != "value_allocation_candidate" {
		t.Fatalf("source value-allocation classification outputs mutated: %+v", queued.ValueAllocationBoundaryPolicy)
	}
	out.ValueAllocationBoundaryPolicy.RequiredHumanAuthority[0] = "mutated"
	if queued.ValueAllocationBoundaryPolicy.RequiredHumanAuthority[0] != "external_committee_value_allocation_decision" {
		t.Fatalf("source value-allocation required human authority mutated: %+v", queued.ValueAllocationBoundaryPolicy)
	}
	out.ValueAllocationBoundaryPolicy.ForbiddenAuthorityClaims[0] = "mutated"
	if queued.ValueAllocationBoundaryPolicy.ForbiddenAuthorityClaims[0] != "display_authorizes_allocation" {
		t.Fatalf("source value-allocation forbidden authority claims mutated: %+v", queued.ValueAllocationBoundaryPolicy)
	}
	out.ValueAllocationBoundaryPolicy.NonAuthorityClaims[0] = "mutated"
	if queued.ValueAllocationBoundaryPolicy.NonAuthorityClaims[0] != "display_is_not_authority_decision" {
		t.Fatalf("source value-allocation non-authority claims mutated: %+v", queued.ValueAllocationBoundaryPolicy)
	}
	runtimeStage := queuedRunLifecycleStageByID(out.DevelopmentLifecycle, "runtime_completed_stage")
	if runtimeStage == nil || runtimeStage.EvidenceStatus != "runtime_completed" {
		t.Fatalf("runtime stage = %+v, want runtime_completed preserved", runtimeStage)
	}
	declaredStage := queuedRunLifecycleStageByID(out.DevelopmentLifecycle, "declared_stage")
	if declaredStage == nil || declaredStage.EvidenceStatus != civilizationAssemblyQueuedStageDeclaredStatus {
		t.Fatalf("declared stage = %+v, want declared pending runtime evidence", declaredStage)
	}
	runtimeStep := queuedRunAgentPlanStepByRole(out.AgentExecutionPlan, "runtime_completed_stage", "guardian")
	if runtimeStep == nil || runtimeStep.EvidenceStatus != "runtime_completed" {
		t.Fatalf("runtime step = %+v, want runtime_completed preserved", runtimeStep)
	}
	declaredStep := queuedRunAgentPlanStepByRole(out.AgentExecutionPlan, "declared_stage", "implementer")
	if declaredStep == nil || declaredStep.EvidenceStatus != civilizationAssemblyQueuedPlanStepDeclaredStatus {
		t.Fatalf("declared step = %+v, want declared pending runtime evidence", declaredStep)
	}
	if queued.DevelopmentLifecycle[1].EvidenceStatus != "expected_not_observed" || queued.AgentExecutionPlan[1].EvidenceStatus != "expected_not_observed" {
		t.Fatalf("source queued evidence mutated: %+v %+v", queued.DevelopmentLifecycle, queued.AgentExecutionPlan)
	}
}

func TestCivilizationAssemblyIssueScanStageArtifactRefRejectsMalformedBodies(t *testing.T) {
	label := IssueScanLifecycleStageArtifactLabel("research_issue_and_repo_context")
	tests := []struct {
		name  string
		label string
		body  string
		want  string
	}{
		{
			name:  "empty body",
			label: label,
			body:  "",
			want:  "empty artifact body",
		},
		{
			name:  "invalid json",
			label: label,
			body:  `{`,
			want:  "decode artifact body",
		},
		{
			name:  "kind mismatch",
			label: label,
			body:  `{"kind":"other","run_id":"run_issue","stage":{"id":"research_issue_and_repo_context"}}`,
			want:  "does not match",
		},
		{
			name:  "missing run",
			label: label,
			body:  `{"kind":"issue_scan_lifecycle_stage","stage":{"id":"research_issue_and_repo_context"}}`,
			want:  "missing run_id",
		},
		{
			name:  "missing stage",
			label: label,
			body:  `{"kind":"issue_scan_lifecycle_stage","run_id":"run_issue","stage":{}}`,
			want:  "missing stage.id",
		},
		{
			name:  "stage mismatch",
			label: IssueScanLifecycleStageArtifactLabel("implement_on_branch"),
			body:  `{"kind":"issue_scan_lifecycle_stage","run_id":"run_issue","stage":{"id":"research_issue_and_repo_context"}}`,
			want:  "does not match body stage",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok, err := civilizationAssemblyIssueScanStageArtifactRef("artifact_event", tt.label, tt.body)
			if ok {
				t.Fatalf("artifact ref ok = true, want false")
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("artifact ref error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestCompactCivilizationAssemblyStageArtifactRefsDedupesAndSorts(t *testing.T) {
	refs := compactCivilizationAssemblyStageArtifactRefs([]civilizationAssemblyStageArtifactRef{
		{RunID: "run_b", StageID: "stage_b", EventRef: "event_002"},
		{RunID: "run_a", StageID: "stage_b", EventRef: "event_003"},
		{RunID: "run_a", StageID: "stage_a", EventRef: "event_001"},
		{RunID: "run_a", StageID: "stage_a", EventRef: "event_001"},
		{RunID: "", StageID: "stage_a", EventRef: "event_ignored"},
	})
	if len(refs) != 3 {
		t.Fatalf("stage refs = %+v, want 3 compact refs", refs)
	}
	want := []civilizationAssemblyStageArtifactRef{
		{RunID: "run_a", StageID: "stage_a", EventRef: "event_001"},
		{RunID: "run_a", StageID: "stage_b", EventRef: "event_003"},
		{RunID: "run_b", StageID: "stage_b", EventRef: "event_002"},
	}
	for i := range want {
		if refs[i] != want[i] {
			t.Fatalf("stage ref[%d] = %+v, want %+v; all refs=%+v", i, refs[i], want[i], refs)
		}
	}
}

func TestBuildCivilizationAssemblyProjectionDistinguishesFactoryOrderQueryFailure(t *testing.T) {
	s, _, _ := newOperatorProjectionStore(t)
	projection := BuildCivilizationAssemblyProjection(factoryOrderReadFailureStore{Store: s}, 50)

	if projection.DerivationStatus != civilizationAssemblyStatusPartial {
		t.Fatalf("derivation status = %q, want partial; failures=%+v", projection.DerivationStatus, projection.FailureReasons)
	}
	reason := civilizationProjectionUnavailableReason(projection, "factory_order_summary")
	if !strings.Contains(reason, "query failed") {
		t.Fatalf("factory_order_summary unavailable reason = %q, want query failure", reason)
	}
	if len(projection.FailureReasons) == 0 || !strings.Contains(projection.FailureReasons[0], "work.task.created") {
		t.Fatalf("failure reasons = %+v, want work.task.created query failure", projection.FailureReasons)
	}
}

func TestBuildCivilizationAssemblyProjectionMarksFactoryOrderSummaryLimit(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:          "First FactoryOrder",
		CreatedBy:      actorID,
		FactoryOrderID: "fo_limit_001",
		RiskClass:      "medium",
	})
	appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:          "Second FactoryOrder",
		CreatedBy:      actorID,
		FactoryOrderID: "fo_limit_002",
		RiskClass:      "medium",
	})

	projection := BuildCivilizationAssemblyProjection(s, 1)

	if !civilizationProjectionHasResidualRisk(projection, "factory_order_summary_limit_01") {
		t.Fatalf("residual risks = %+v, want factory_order_summary_limit_01", projection.ResidualRiskSummary)
	}
}

func TestBuildCivilizationAssemblyProjectionMarksFactoryOrderArtifactSourceLimit(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	taskEvent := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:          "Bounded artifact provenance",
		CreatedBy:      actorID,
		FactoryOrderID: "fo_artifact_limit_001",
		RiskClass:      "medium",
	})
	appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    taskEvent.ID(),
		Label:     "First artifact",
		MediaType: "text/markdown",
		Body:      "commit: abc123",
		CreatedBy: actorID,
	})
	appendEvent(work.EventTypeTaskArtifact, work.TaskArtifactContent{
		TaskID:    taskEvent.ID(),
		Label:     "Second artifact",
		MediaType: "text/markdown",
		Body:      "review: passed",
		CreatedBy: actorID,
	})

	projection := BuildCivilizationAssemblyProjection(s, 1)

	if !civilizationProjectionHasResidualRisk(projection, "factory_order_artifact_source_limit_01") {
		t.Fatalf("residual risks = %+v, want factory_order_artifact_source_limit_01", projection.ResidualRiskSummary)
	}
}

func TestBuildCivilizationAssemblyProjectionMarksFactoryOrderVerificationSourceLimit(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	taskEvent := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:          "Bounded verification provenance",
		CreatedBy:      actorID,
		FactoryOrderID: "fo_verification_limit_001",
		RiskClass:      "medium",
	})
	appendEvent(work.EventTypeTaskVerificationAttached, work.TaskVerificationAttachedContent{
		TaskID:        taskEvent.ID(),
		TestRunIDs:    []string{"test_run_limit_001"},
		GateResultIDs: []string{"gate_result_limit_001"},
		AttachedBy:    actorID,
	})
	appendEvent(work.EventTypeTaskVerificationAttached, work.TaskVerificationAttachedContent{
		TaskID:        taskEvent.ID(),
		TestRunIDs:    []string{"test_run_limit_002"},
		GateResultIDs: []string{"gate_result_limit_002"},
		AttachedBy:    actorID,
	})

	projection := BuildCivilizationAssemblyProjection(s, 1)

	if !civilizationProjectionHasResidualRisk(projection, "factory_order_verification_source_limit_01") {
		t.Fatalf("residual risks = %+v, want factory_order_verification_source_limit_01", projection.ResidualRiskSummary)
	}
}

func TestBuildCivilizationAssemblyProjectionMarksFactoryOrderLifecycleSourceLimit(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	taskEvent := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:          "Bounded lifecycle provenance",
		CreatedBy:      actorID,
		FactoryOrderID: "fo_lifecycle_limit_001",
		RiskClass:      "medium",
	})
	appendEvent(work.EventTypeTaskLifecycleTransitioned, work.TaskLifecycleTransitionContent{
		TaskID:    taskEvent.ID(),
		FromState: work.StatusCreated,
		ToState:   work.StatusReady,
		Reason:    "first transition",
		ChangedBy: actorID,
	})
	appendEvent(work.EventTypeTaskLifecycleTransitioned, work.TaskLifecycleTransitionContent{
		TaskID:    taskEvent.ID(),
		FromState: work.StatusReady,
		ToState:   work.StatusRunning,
		Reason:    "second transition",
		ChangedBy: actorID,
	})

	projection := BuildCivilizationAssemblyProjection(s, 1)

	if !civilizationProjectionHasResidualRisk(projection, "factory_order_lifecycle_source_limit_01") {
		t.Fatalf("residual risks = %+v, want factory_order_lifecycle_source_limit_01", projection.ResidualRiskSummary)
	}
}

func TestBuildCivilizationAssemblyProjectionMergesFactoryOrderTaskRefs(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	firstTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:                  "FactoryOrder shell",
		CreatedBy:              actorID,
		FactoryOrderID:         "fo_merge_001",
		RequirementIDs:         []string{"req_merge_001"},
		AcceptanceCriterionIDs: []string{"ac_merge_001"},
	})
	secondTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:                  "FactoryOrder implementation",
		CreatedBy:              actorID,
		FactoryOrderID:         "fo_merge_001",
		RequirementIDs:         []string{"req_merge_002"},
		AcceptanceCriterionIDs: []string{"ac_merge_002"},
		RiskClass:              "medium",
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if len(projection.FactoryOrderSummary) != 1 {
		t.Fatalf("factory orders = %+v, want one merged order", projection.FactoryOrderSummary)
	}
	order := projection.FactoryOrderSummary[0]
	if order.RiskClass != "medium" {
		t.Fatalf("risk class = %q, want medium after unknown upgrade", order.RiskClass)
	}
	for _, want := range []string{firstTask.ID().Value(), secondTask.ID().Value()} {
		if !containsString(order.TaskRefs, want) {
			t.Fatalf("task refs = %+v, missing %s", order.TaskRefs, want)
		}
	}
	for _, want := range []string{"req_merge_001", "req_merge_002"} {
		if !containsString(order.RequirementRefs, want) {
			t.Fatalf("requirement refs = %+v, missing %s", order.RequirementRefs, want)
		}
	}
	for _, want := range []string{"ac_merge_001", "ac_merge_002"} {
		if !containsString(order.AcceptanceCriterionRefs, want) {
			t.Fatalf("acceptance refs = %+v, missing %s", order.AcceptanceCriterionRefs, want)
		}
	}
}

func TestCivilizationAssemblyFactoryOrderStatusRankCoversWorkStatuses(t *testing.T) {
	tests := map[work.TaskStatus]int{
		work.StatusCreated:             10,
		work.StatusReady:               40,
		work.StatusRunning:             80,
		work.StatusBlocked:             100,
		work.StatusFailed:              100,
		work.StatusRepairRequired:      100,
		work.StatusRepairRunning:       90,
		work.StatusRepaired:            70,
		work.StatusVerificationRunning: 80,
		work.StatusVerified:            60,
		work.StatusCertified:           60,
		work.StatusRejected:            100,
		work.StatusSuperseded:          10,
		work.StatusPolicyBlocked:       100,
	}
	for status, wantRank := range tests {
		statusString := "work_task_" + string(status)
		if status == work.StatusCreated {
			statusString = "work_task_seeded"
		}
		if got := civilizationAssemblyFactoryOrderStatusRank(statusString); got != wantRank {
			t.Fatalf("rank(%s) = %d, want %d", statusString, got, wantRank)
		}
	}
}

func TestBuildCivilizationAssemblyProjectionRollsUpFactoryOrderStatusAndRisk(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	certifiedTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:          "Certified FactoryOrder task",
		CreatedBy:      actorID,
		FactoryOrderID: "fo_rollup_001",
		RiskClass:      "low",
	})
	failedTask := appendEvent(work.EventTypeTaskCreated, work.TaskCreatedContent{
		Title:          "Failed FactoryOrder task",
		CreatedBy:      actorID,
		FactoryOrderID: "fo_rollup_001",
		RiskClass:      "high",
	})
	appendEvent(work.EventTypeTaskLifecycleTransitioned, work.TaskLifecycleTransitionContent{
		TaskID:    certifiedTask.ID(),
		FromState: work.StatusCreated,
		ToState:   work.StatusCertified,
		Reason:    "certified for rollup test",
		ChangedBy: actorID,
	})
	appendEvent(work.EventTypeTaskLifecycleTransitioned, work.TaskLifecycleTransitionContent{
		TaskID:    failedTask.ID(),
		FromState: work.StatusCreated,
		ToState:   work.StatusFailed,
		Reason:    "failure should dominate rollup",
		ChangedBy: actorID,
	})

	projection := BuildCivilizationAssemblyProjection(s, 50)

	if len(projection.FactoryOrderSummary) != 1 {
		t.Fatalf("factory orders = %+v, want one merged order", projection.FactoryOrderSummary)
	}
	order := projection.FactoryOrderSummary[0]
	if order.Status != "work_task_failed" {
		t.Fatalf("rollup status = %q, want work_task_failed", order.Status)
	}
	if order.RiskClass != "high" {
		t.Fatalf("rollup risk class = %q, want high", order.RiskClass)
	}
}

func TestBuildCivilizationAssemblyProjectionToleratesOpaqueSharedStoreHead(t *testing.T) {
	s, actorID, _ := newOperatorProjectionStore(t)
	opaqueHead := appendOpaqueSharedStoreEvent(t, s, actorID)

	event.SetFallbackUnmarshaler(event.RawFallback)
	t.Cleanup(func() { event.SetFallbackUnmarshaler(nil) })

	serializedStore := serializationRoundTripStore{Store: s}
	projection := BuildCivilizationAssemblyProjection(serializedStore, 50)

	if projection.DerivationStatus != civilizationAssemblyStatusComplete {
		t.Fatalf("derivation status = %q, want complete; failures=%+v", projection.DerivationStatus, projection.FailureReasons)
	}
	if len(projection.FailureReasons) != 0 {
		t.Fatalf("failure reasons = %+v, want none", projection.FailureReasons)
	}
	if projection.SourceEventGraphHeadOrStateVersion != opaqueHead.ID().Value() {
		t.Fatalf("source head = %q, want opaque head %q", projection.SourceEventGraphHeadOrStateVersion, opaqueHead.ID().Value())
	}
}

func TestCivilizationAssemblyTargetType(t *testing.T) {
	tests := map[string]string{
		"pr://transpara-ai/site/42": "pr",
		"actor_123":                 "actor",
		"repo:transpara-ai/site":    "repo",
		"plain-target":              "target",
		"":                          "",
	}
	for target, want := range tests {
		if got := civilizationAssemblyTargetType(target); got != want {
			t.Fatalf("target type for %q = %q, want %q", target, got, want)
		}
	}
}

func civilizationProjectionHasRoleBinding(projection CivilizationAssemblyProjection, actorID, role string) bool {
	for _, binding := range projection.RoleBindings {
		if binding.ActorID == actorID && binding.Role == role {
			return true
		}
	}
	return false
}

func civilizationProjectionHasActorStatus(projection CivilizationAssemblyProjection, actorID, status string) bool {
	for _, actor := range projection.ActorRoster {
		if actor.ActorID == actorID && actor.Status == status {
			return true
		}
	}
	return false
}

func civilizationProjectionHasActorModeStatus(projection CivilizationAssemblyProjection, actorID, identityMode, status string) bool {
	for _, actor := range projection.ActorRoster {
		if actor.ActorID == actorID && actor.IdentityMode == identityMode && actor.Status == status {
			return true
		}
	}
	return false
}

func civilizationProjectionHasLifecycleStatus(projection CivilizationAssemblyProjection, actorID, status string) bool {
	for _, lifecycle := range projection.AgentLifecycleSummary {
		if lifecycle.ActorID == actorID && lifecycle.Status == status {
			return true
		}
	}
	return false
}

func civilizationProjectionLifecycleCount(projection CivilizationAssemblyProjection, actorID string) int {
	count := 0
	for _, lifecycle := range projection.AgentLifecycleSummary {
		if lifecycle.ActorID == actorID {
			count++
		}
	}
	return count
}

func civilizationProjectionHasUnavailableField(projection CivilizationAssemblyProjection, field string) bool {
	for _, item := range projection.WithheldOrUnavailableFields {
		if item.Field == field {
			return true
		}
	}
	return false
}

func civilizationProjectionUnavailableReason(projection CivilizationAssemblyProjection, field string) string {
	for _, item := range projection.WithheldOrUnavailableFields {
		if item.Field == field {
			return item.Reason
		}
	}
	return ""
}

func civilizationProjectionHasResidualRisk(projection CivilizationAssemblyProjection, id string) bool {
	for _, item := range projection.ResidualRiskSummary {
		if item.ID == id {
			return true
		}
	}
	return false
}

func TestBuildOperatorProjectionPendingScansBeyondDecisionDisplayLimit(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)

	decidedRequestID := newTestEventID(t)
	appendEvent(EventTypeAuthorityRequestRecorded, AuthorityRequestRecordedContent{
		RequestID:        decidedRequestID,
		RequestingActor:  actorID,
		ActionName:       "agent.retire",
		Target:           "actor_decided",
		Environment:      "production",
		RequestedOutcome: "retire agent",
		Justification:    "completed mandate",
		RiskSummary:      "protected lifecycle action",
	})
	appendEvent(EventTypeAuthorityDecisionRecorded, AuthorityDecisionRecordedContent{
		DecisionID:     "decision-older",
		RequestID:      decidedRequestID,
		ApproverActor:  actorID,
		Outcome:        "approved",
		ApprovedTarget: "actor_decided",
		ApprovedAction: "agent.retire",
		Rationale:      "valid evidence",
	})

	pendingRequestID := newTestEventID(t)
	appendEvent(EventTypeAuthorityRequestRecorded, AuthorityRequestRecordedContent{
		RequestID:        pendingRequestID,
		RequestingActor:  actorID,
		ActionName:       "agent.revoke",
		Target:           "actor_pending",
		Environment:      "production",
		RequestedOutcome: "revoke agent",
		Justification:    "needs operator decision",
		RiskSummary:      "protected lifecycle action",
	})

	for i := 0; i < 3; i++ {
		appendEvent(EventTypeAuthorityDecisionRecorded, AuthorityDecisionRecordedContent{
			DecisionID:     "decision-unrelated",
			RequestID:      newTestEventID(t),
			ApproverActor:  actorID,
			Outcome:        "approved",
			ApprovedTarget: "unrelated",
			ApprovedAction: "agent.spawn.persistent",
			Rationale:      "unrelated history",
		})
	}

	projection := BuildOperatorProjection(s, 2)

	if len(projection.PendingApprovals) != 1 {
		t.Fatalf("pending approvals = %d, want 1: %+v", len(projection.PendingApprovals), projection.PendingApprovals)
	}
	if projection.PendingApprovals[0].RequestID != pendingRequestID.Value() {
		t.Fatalf("pending request ID = %q, want %q", projection.PendingApprovals[0].RequestID, pendingRequestID.Value())
	}
}

func TestBuildOperatorProjectionLifecycleAndKeyAudit(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	publicKey := types.MustPublicKey(make([]byte, 32))
	newPublicKeyBytes := make([]byte, 32)
	newPublicKeyBytes[0] = 1
	newPublicKey := types.MustPublicKey(newPublicKeyBytes)
	requestID := newTestEventID(t)
	decisionID := newTestEventID(t)

	appendEvent(EventTypeAgentIdentityRegistered, AgentIdentityRegisteredContent{
		ActorID:          actorID,
		DisplayName:      "builder",
		Role:             "builder",
		PublicKey:        publicKey,
		KeyProvenance:    "generated",
		Environment:      "production",
		IdentityMode:     "persistent",
		LifecycleStatus:  "active",
		AuthorityScope:   "hive:read",
		RegistrationPath: "generated",
	})
	appendEvent(EventTypeAgentAuthorityScopeAssigned, AgentAuthorityScopeAssignedContent{
		ActorID:         actorID,
		AuthorityScope:  "hive:operate",
		AssignedBy:      actorID,
		DecisionEventID: decisionID,
	})
	appendEvent(EventTypeAgentLifecycleTransitioned, AgentLifecycleTransitionedContent{
		ActorID:            actorID,
		PreviousState:      "active",
		RequestedState:     "retired",
		ResultingState:     "retired",
		RequestingActor:    actorID,
		AuthorityRequestID: requestID,
		DecisionEventID:    decisionID,
		Reason:             "completed mandate",
	})
	appendEvent(EventTypeAgentKeyRegistered, AgentKeyRegisteredContent{
		ActorID:       actorID,
		PublicKey:     publicKey,
		KeyProvenance: "generated",
		Environment:   "production",
		IdentityMode:  "persistent",
	})
	appendEvent(EventTypeAgentKeyRotated, AgentKeyRotatedContent{
		ActorID:            actorID,
		OldPublicKey:       publicKey,
		NewPublicKey:       newPublicKey,
		Reason:             "scheduled rotation",
		RequestingActor:    actorID,
		AuthorityRequestID: requestID,
		DecisionEventID:    decisionID,
		EffectiveAt:        types.Now(),
	})
	appendEvent(EventTypeAgentAuditLinked, AgentAuditLinkedContent{
		SubjectActorID: actorID,
		RecordKind:     "key-rotation",
		KeyEvidence:    []types.EventID{requestID},
		Rationale:      "rotation linked to authority decision",
	})

	projection := BuildOperatorProjection(s, 50)

	if len(projection.Lifecycle) != 1 {
		t.Fatalf("lifecycle records = %d, want 1", len(projection.Lifecycle))
	}
	lifecycle := projection.Lifecycle[0]
	if lifecycle.ActorID != actorID.Value() || lifecycle.LifecycleStatus != "retired" {
		t.Fatalf("lifecycle projection = %+v", lifecycle)
	}
	if lifecycle.AuthorityScope != "hive:operate" {
		t.Fatalf("authority scope = %q, want hive:operate", lifecycle.AuthorityScope)
	}

	if len(projection.KeyAuditTraces) != 3 {
		t.Fatalf("key audit traces = %d, want 3: %+v", len(projection.KeyAuditTraces), projection.KeyAuditTraces)
	}
	if projection.KeyAuditTraces[0].EventType != EventTypeAgentAuditLinked.Value() {
		t.Fatalf("newest key audit trace = %q, want %q", projection.KeyAuditTraces[0].EventType, EventTypeAgentAuditLinked.Value())
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceDistinguishesQueuedIntent(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)
	sourceEventID := newTestEventID(t)
	briefEventID := newTestEventID(t)
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
		Body:   "The Civilization should scan Transpara-AI repos, then surface a ready-for-Human PR.",
		Labels: []string{"civilization"},
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}

	appendEvent(EventTypeFactoryRunRequested, FactoryRunRequestedContent{
		RunID:      "run_queued",
		IntakeID:   "intake_001",
		OperatorID: "operator_001",
		Title:      "Test 001 evidence run",
		Status:     "queued",
		Authority: RunLaunchAuthority{
			InitialLevel: event.AuthorityLevelRequired,
			Scope:        "test-001",
			PolicyRef:    "DF-GATE-K",
			Rationale:    "operator request requires explicit authority",
		},
		Budget: RunLaunchBudget{
			MaxIterations: 3,
			MaxCostUSD:    12.5,
		},
		TargetRepos:   []string{"transpara-ai/hive", "transpara-ai/eventgraph"},
		SourceEventID: sourceEventID,
		BriefEventID:  briefEventID,
		Brief:         brief,
	})

	projection := BuildOperatorProjection(s, 50)
	runtimeEvidence := projection.RuntimeEvidence

	if runtimeEvidence.Status != "not_observed" {
		t.Fatalf("runtime status = %q, want not_observed", runtimeEvidence.Status)
	}
	if runtimeEvidence.LastRun != nil {
		t.Fatalf("last run = %+v, want nil for queued request only", runtimeEvidence.LastRun)
	}
	if runtimeEvidence.LastQueuedRunRequest == nil {
		t.Fatal("last queued run request is nil")
	}
	queued := runtimeEvidence.LastQueuedRunRequest
	if queued.RunID != "run_queued" || queued.EvidenceKind != "queued_request_not_runtime_start" {
		t.Fatalf("queued evidence = %+v", queued)
	}
	if queued.ConversationID == "" {
		t.Fatalf("queued conversation ID is empty: %+v", queued)
	}
	if queued.AuthorityInitialLevel != string(event.AuthorityLevelRequired) || queued.AuthorityScope != "test-001" {
		t.Fatalf("queued authority evidence = %+v", queued)
	}
	if queued.BudgetMaxIterations == nil || *queued.BudgetMaxIterations != 3 || queued.BudgetMaxCostUSD == nil || *queued.BudgetMaxCostUSD != 12.5 {
		t.Fatalf("queued budget evidence = %+v", queued)
	}
	if len(queued.TargetRepos) != 2 || queued.TargetRepos[0] != "transpara-ai/hive" {
		t.Fatalf("queued target repos = %+v", queued.TargetRepos)
	}
	if queued.BriefKind != issueScanBriefKind || queued.LifecycleVersion != issueScanLifecycleVersion {
		t.Fatalf("queued brief metadata = %+v", queued)
	}
	if queued.RoleSeparationPolicy == nil || queuedRunRolePolicyByActor(queued.RoleSeparationPolicy.ActorPolicies, "scanner") == nil {
		t.Fatalf("queued role separation policy missing scanner boundary: %+v", queued.RoleSeparationPolicy)
	}
	if queued.AutonomyGuardPolicy == nil || !containsModelProjectionString(queued.AutonomyGuardPolicy.FailClosedOutputs, "protected_action_blocked") {
		t.Fatalf("queued autonomy guard policy missing fail-closed boundary: %+v", queued.AutonomyGuardPolicy)
	}
	if queued.HumanRequiredClassificationPolicy == nil || !containsModelProjectionString(queued.HumanRequiredClassificationPolicy.EscalationOutputs, "issue_parking_required") {
		t.Fatalf("queued human-required classification policy missing escalation boundary: %+v", queued.HumanRequiredClassificationPolicy)
	}
	if queued.AuthorityRecommendationPolicy == nil || !containsModelProjectionString(queued.AuthorityRecommendationPolicy.RecommendationOutputs, "no_authority_decision") {
		t.Fatalf("queued authority recommendation policy missing no-authority boundary: %+v", queued.AuthorityRecommendationPolicy)
	}
	if queued.ValueAllocationBoundaryPolicy == nil || !containsModelProjectionString(queued.ValueAllocationBoundaryPolicy.ForbiddenSystemActions, "allocate_value") || !containsModelProjectionString(queued.ValueAllocationBoundaryPolicy.NonAuthorityClaims, "no_value_allocation_execution") {
		t.Fatalf("queued value-allocation boundary policy missing no-allocation boundary: %+v", queued.ValueAllocationBoundaryPolicy)
	}
	if queued.LifecycleEvidenceKind != "expected_lifecycle_not_runtime_progress" {
		t.Fatalf("lifecycle evidence kind = %q", queued.LifecycleEvidenceKind)
	}
	if len(queued.DevelopmentLifecycle) != 7 {
		t.Fatalf("development lifecycle = %+v, want 7 stages", queued.DevelopmentLifecycle)
	}
	if queued.DevelopmentLifecycle[0].ID != "research_issue_and_repo_context" || queued.DevelopmentLifecycle[0].EvidenceStatus != "expected_not_observed" {
		t.Fatalf("first lifecycle stage = %+v", queued.DevelopmentLifecycle[0])
	}
	reviewStage := queuedRunLifecycleStageByID(queued.DevelopmentLifecycle, "run_adversarial_review")
	if reviewStage == nil {
		t.Fatalf("queued lifecycle missing review stage: %+v", queued.DevelopmentLifecycle)
	}
	if !containsModelProjectionString(reviewStage.RequiredRoles, "reviewer") || !containsModelProjectionString(reviewStage.RequiredRoles, "guardian") {
		t.Fatalf("review stage roles = %+v", reviewStage.RequiredRoles)
	}
	if !containsModelProjectionString(reviewStage.RequiredEvidence, "exact_head_review_artifact") || !containsModelProjectionString(reviewStage.RequiredEvidence, "finding_disposition") {
		t.Fatalf("review stage evidence = %+v", reviewStage.RequiredEvidence)
	}
	readyStage := queuedRunLifecycleStageByID(queued.DevelopmentLifecycle, "surface_ready_for_Human_result_PR")
	if readyStage == nil || readyStage.AuthorityBoundary != "human_approval_required_no_merge" {
		t.Fatalf("ready lifecycle stage = %+v", readyStage)
	}
	if len(queued.AgentExecutionPlan) != 18 {
		t.Fatalf("agent execution plan = %+v, want 18 expected steps", queued.AgentExecutionPlan)
	}
	implementStep := queuedRunAgentPlanStepByRole(queued.AgentExecutionPlan, "implement_on_branch", "implementer")
	if implementStep == nil || !implementStep.CanOperate || implementStep.EvidenceStatus != "expected_not_observed" {
		t.Fatalf("implementer plan step = %+v", implementStep)
	}
	if !containsModelProjectionString(implementStep.RequiredOutputs, "validation_output") {
		t.Fatalf("implementer required outputs = %+v", implementStep.RequiredOutputs)
	}
	reviewStep := queuedRunAgentPlanStepByRole(queued.AgentExecutionPlan, "run_adversarial_review", "reviewer")
	if reviewStep == nil || reviewStep.CanOperate || !containsModelProjectionString(reviewStep.RequiredOutputs, "exact_head_review_artifact") {
		t.Fatalf("reviewer plan step = %+v", reviewStep)
	}
	readyGuardianStep := queuedRunAgentPlanStepByRole(queued.AgentExecutionPlan, "surface_ready_for_Human_result_PR", "guardian")
	if readyGuardianStep == nil || readyGuardianStep.AuthorityBoundary != "human_approval_required_no_merge" || !containsModelProjectionString(readyGuardianStep.RequiredOutputs, "human_approval_boundary_check") {
		t.Fatalf("ready guardian plan step = %+v", readyGuardianStep)
	}
	if runtimeEvidence.AgentEvents.Scope != "none" || runtimeEvidence.AgentEvents.ObservedActive != 0 {
		t.Fatalf("agent evidence = %+v, want no runtime agents", runtimeEvidence.AgentEvents)
	}
	if !containsModelProjectionString(runtimeEvidence.Limitations, "factory.run.requested is queued launch intent, not runtime-start proof") {
		t.Fatalf("limitations = %+v, want queued-intent boundary", runtimeEvidence.Limitations)
	}
	if !containsModelProjectionString(runtimeEvidence.Limitations, "queued run development_lifecycle stages are expected evidence, not completed stage proof") {
		t.Fatalf("limitations = %+v, want lifecycle-not-complete boundary", runtimeEvidence.Limitations)
	}
}

func TestQueuedRunLifecycleFromBriefLeavesGenericBriefEmpty(t *testing.T) {
	kind, version, lifecycle, agentPlan, err := queuedRunLifecycleFromBrief([]byte(`{"goal":"generic launch brief"}`))
	if err != nil {
		t.Fatalf("queuedRunLifecycleFromBrief: %v", err)
	}
	if kind != "" || version != "" || len(lifecycle) != 0 || len(agentPlan) != 0 {
		t.Fatalf("metadata = kind %q version %q lifecycle %+v plan %+v, want empty lifecycle", kind, version, lifecycle, agentPlan)
	}
}

func TestQueuedRunLifecycleFromBriefRejectsIssueScanBriefWithEmptyLifecycle(t *testing.T) {
	raw := []byte(fmt.Sprintf(`{"kind":"%s","lifecycle_version":"%s","development_lifecycle":[]}`, issueScanBriefKind, issueScanLifecycleVersion))
	_, _, lifecycle, agentPlan, err := queuedRunLifecycleFromBrief(raw)
	if err == nil || !strings.Contains(err.Error(), "missing development lifecycle") {
		t.Fatalf("queuedRunLifecycleFromBrief error = %v, lifecycle = %+v plan = %+v; want missing lifecycle error", err, lifecycle, agentPlan)
	}
}

func TestQueuedRunLifecycleFromBriefAllowsLegacyKindOnlyIssueScanBrief(t *testing.T) {
	raw := []byte(`{"kind":"transpara_ai_github_issue_scan","required_agent_flow":["research_issue_and_repo_context"]}`)
	kind, version, lifecycle, agentPlan, err := queuedRunLifecycleFromBrief(raw)
	if err != nil {
		t.Fatalf("queuedRunLifecycleFromBrief: %v", err)
	}
	if kind != "" || version != "" || len(lifecycle) != 0 || len(agentPlan) != 0 {
		t.Fatalf("metadata = kind %q version %q lifecycle %+v plan %+v, want empty lifecycle for legacy kind-only issue scan", kind, version, lifecycle, agentPlan)
	}
}

func TestQueuedRunLifecycleFromBriefSupportsLegacyV03LifecycleWithAgentPlan(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	raw, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	doc["lifecycle_version"] = issueScanLifecycleVersionV03
	legacy, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal legacy v0.3 brief: %v", err)
	}

	kind, version, lifecycle, agentPlan, err := queuedRunLifecycleFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunLifecycleFromBrief: %v", err)
	}
	if kind != issueScanBriefKind || version != issueScanLifecycleVersionV03 || len(lifecycle) != 7 || len(agentPlan) != 18 {
		t.Fatalf("metadata = kind %q version %q lifecycle %d plan %d, want v0.3 with lifecycle and plan", kind, version, len(lifecycle), len(agentPlan))
	}
}

func TestQueuedRunRoleSeparationPolicyFromBriefSupportsLegacyV04WithoutPolicy(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	doc["lifecycle_version"] = issueScanLifecycleVersionV04
	delete(doc, "role_separation_policy")
	legacy, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal legacy v0.4 brief: %v", err)
	}

	kind, version, lifecycle, agentPlan, err := queuedRunLifecycleFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunLifecycleFromBrief: %v", err)
	}
	if kind != issueScanBriefKind || version != issueScanLifecycleVersionV04 || len(lifecycle) != 7 || len(agentPlan) != 18 {
		t.Fatalf("metadata = kind %q version %q lifecycle %d plan %d, want v0.4 with lifecycle and plan", kind, version, len(lifecycle), len(agentPlan))
	}
	policy, err := queuedRunRoleSeparationPolicyFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunRoleSeparationPolicyFromBrief: %v", err)
	}
	if policy != nil {
		t.Fatalf("legacy v0.4 role separation policy = %+v, want nil", policy)
	}
}

func TestQueuedRunRoleSeparationPolicyFromBriefRejectsCurrentBriefWithoutPolicy(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	delete(doc, "role_separation_policy")
	mutated, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal missing policy brief: %v", err)
	}

	policy, err := queuedRunRoleSeparationPolicyFromBrief(mutated)
	if err == nil || !strings.Contains(err.Error(), "role_separation_policy is required") {
		t.Fatalf("queuedRunRoleSeparationPolicyFromBrief error = %v policy = %+v; want required policy error", err, policy)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceRecordsRoleSeparationPolicyError(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	policy, ok := doc["role_separation_policy"].(map[string]any)
	if !ok {
		t.Fatalf("role_separation_policy = %#v, want object", doc["role_separation_policy"])
	}
	actorPolicies, ok := policy["actor_policies"].([]any)
	if !ok || len(actorPolicies) == 0 {
		t.Fatalf("actor_policies = %#v, want actor policy array", policy["actor_policies"])
	}
	firstActor, ok := actorPolicies[0].(map[string]any)
	if !ok {
		t.Fatalf("first actor policy = %#v, want object", actorPolicies[0])
	}
	firstActor["can_approve_or_merge"] = true
	mutated, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal mutated role policy brief: %v", err)
	}

	appendEvent(EventTypeFactoryRunRequested, FactoryRunRequestedContent{
		RunID:        "run_bad_role_policy",
		IntakeID:     "intake_bad_role_policy",
		OperatorID:   "operator_001",
		Title:        "Bad role policy still projects queued request",
		Status:       "queued",
		TargetRepos:  []string{"transpara-ai/hive"},
		BriefEventID: newTestEventID(t),
		Brief:        mutated,
	})

	projection := BuildOperatorProjection(s, 50)
	queued := projection.RuntimeEvidence.LastQueuedRunRequest
	if queued == nil {
		t.Fatal("last queued run request is nil")
	}
	if queued.RoleSeparationPolicy != nil {
		t.Fatalf("queued role separation policy = %+v, want nil after validation error", queued.RoleSeparationPolicy)
	}
	if len(projection.Errors) == 0 || !strings.Contains(projection.Errors[0], "role separation policy projection") || !strings.Contains(projection.Errors[0], "can_approve_or_merge") {
		t.Fatalf("projection errors = %+v, want role separation policy validation error", projection.Errors)
	}
}

func TestQueuedRunAutonomyGuardPolicyFromBriefSupportsLegacyV05WithoutPolicy(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	doc["lifecycle_version"] = issueScanLifecycleVersionV05
	delete(doc, "autonomy_guard_policy")
	legacy, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal legacy v0.5 brief: %v", err)
	}

	kind, version, lifecycle, agentPlan, err := queuedRunLifecycleFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunLifecycleFromBrief: %v", err)
	}
	if kind != issueScanBriefKind || version != issueScanLifecycleVersionV05 || len(lifecycle) != 7 || len(agentPlan) != 18 {
		t.Fatalf("metadata = kind %q version %q lifecycle %d plan %d, want v0.5 with lifecycle and plan", kind, version, len(lifecycle), len(agentPlan))
	}
	rolePolicy, err := queuedRunRoleSeparationPolicyFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunRoleSeparationPolicyFromBrief: %v", err)
	}
	if rolePolicy == nil || queuedRunRolePolicyByActor(rolePolicy.ActorPolicies, "scanner") == nil {
		t.Fatalf("legacy v0.5 role separation policy = %+v, want parsed policy", rolePolicy)
	}
	autonomyPolicy, err := queuedRunAutonomyGuardPolicyFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunAutonomyGuardPolicyFromBrief: %v", err)
	}
	if autonomyPolicy != nil {
		t.Fatalf("legacy v0.5 autonomy guard policy = %+v, want nil", autonomyPolicy)
	}
}

func TestQueuedRunAutonomyGuardPolicyFromBriefRejectsCurrentBriefWithoutPolicy(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	delete(doc, "autonomy_guard_policy")
	mutated, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal missing autonomy policy brief: %v", err)
	}

	policy, err := queuedRunAutonomyGuardPolicyFromBrief(mutated)
	if err == nil || !strings.Contains(err.Error(), "autonomy_guard_policy is required") {
		t.Fatalf("queuedRunAutonomyGuardPolicyFromBrief error = %v policy = %+v; want required policy error", err, policy)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceRecordsAutonomyGuardPolicyError(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	policy, ok := doc["autonomy_guard_policy"].(map[string]any)
	if !ok {
		t.Fatalf("autonomy_guard_policy = %#v, want object", doc["autonomy_guard_policy"])
	}
	policy["autonomy_ceiling"] = "level_1_autonomy_increase"
	mutated, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal mutated autonomy policy brief: %v", err)
	}

	appendEvent(EventTypeFactoryRunRequested, FactoryRunRequestedContent{
		RunID:        "run_bad_autonomy_guard",
		IntakeID:     "intake_bad_autonomy_guard",
		OperatorID:   "operator_001",
		Title:        "Bad autonomy guard still projects queued request",
		Status:       "queued",
		TargetRepos:  []string{"transpara-ai/hive"},
		BriefEventID: newTestEventID(t),
		Brief:        mutated,
	})

	projection := BuildOperatorProjection(s, 50)
	queued := projection.RuntimeEvidence.LastQueuedRunRequest
	if queued == nil {
		t.Fatal("last queued run request is nil")
	}
	if queued.AutonomyGuardPolicy != nil {
		t.Fatalf("queued autonomy guard policy = %+v, want nil after validation error", queued.AutonomyGuardPolicy)
	}
	found := false
	for _, projectionErr := range projection.Errors {
		if strings.Contains(projectionErr, "autonomy guard policy projection") && strings.Contains(projectionErr, "autonomy_ceiling") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("projection errors = %+v, want autonomy guard policy validation error", projection.Errors)
	}
}

func TestQueuedRunHumanRequiredClassificationPolicyFromBriefSupportsLegacyV06WithoutPolicy(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	doc["lifecycle_version"] = issueScanLifecycleVersionV06
	delete(doc, "human_required_classification_policy")
	legacy, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal legacy v0.6 brief: %v", err)
	}

	kind, version, lifecycle, agentPlan, err := queuedRunLifecycleFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunLifecycleFromBrief: %v", err)
	}
	if kind != issueScanBriefKind || version != issueScanLifecycleVersionV06 || len(lifecycle) != 7 || len(agentPlan) != 18 {
		t.Fatalf("metadata = kind %q version %q lifecycle %d plan %d, want v0.6 with lifecycle and plan", kind, version, len(lifecycle), len(agentPlan))
	}
	rolePolicy, err := queuedRunRoleSeparationPolicyFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunRoleSeparationPolicyFromBrief: %v", err)
	}
	if rolePolicy == nil || queuedRunRolePolicyByActor(rolePolicy.ActorPolicies, "scanner") == nil {
		t.Fatalf("legacy v0.6 role separation policy = %+v, want parsed policy", rolePolicy)
	}
	autonomyPolicy, err := queuedRunAutonomyGuardPolicyFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunAutonomyGuardPolicyFromBrief: %v", err)
	}
	if autonomyPolicy == nil || !containsModelProjectionString(autonomyPolicy.FailClosedOutputs, "protected_action_blocked") {
		t.Fatalf("legacy v0.6 autonomy guard policy = %+v, want parsed policy", autonomyPolicy)
	}
	classificationPolicy, err := queuedRunHumanRequiredClassificationPolicyFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunHumanRequiredClassificationPolicyFromBrief: %v", err)
	}
	if classificationPolicy != nil {
		t.Fatalf("legacy v0.6 human-required classification policy = %+v, want nil", classificationPolicy)
	}
}

func TestQueuedRunHumanRequiredClassificationPolicyFromBriefRejectsCurrentBriefWithoutPolicy(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	delete(doc, "human_required_classification_policy")
	mutated, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal missing human-required classification policy brief: %v", err)
	}

	policy, err := queuedRunHumanRequiredClassificationPolicyFromBrief(mutated)
	if err == nil || !strings.Contains(err.Error(), "human_required_classification_policy is required") {
		t.Fatalf("queuedRunHumanRequiredClassificationPolicyFromBrief error = %v policy = %+v; want required policy error", err, policy)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceRecordsHumanRequiredClassificationPolicyError(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	policy, ok := doc["human_required_classification_policy"].(map[string]any)
	if !ok {
		t.Fatalf("human_required_classification_policy = %#v, want object", doc["human_required_classification_policy"])
	}
	policy["classification_posture"] = "classification_authorizes_action"
	mutated, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal mutated human-required classification policy brief: %v", err)
	}

	appendEvent(EventTypeFactoryRunRequested, FactoryRunRequestedContent{
		RunID:        "run_bad_human_required_classification",
		IntakeID:     "intake_bad_human_required_classification",
		OperatorID:   "operator_001",
		Title:        "Bad human-required classification policy still projects queued request",
		Status:       "queued",
		TargetRepos:  []string{"transpara-ai/hive"},
		BriefEventID: newTestEventID(t),
		Brief:        mutated,
	})

	projection := BuildOperatorProjection(s, 50)
	queued := projection.RuntimeEvidence.LastQueuedRunRequest
	if queued == nil {
		t.Fatal("last queued run request is nil")
	}
	if queued.HumanRequiredClassificationPolicy != nil {
		t.Fatalf("queued human-required classification policy = %+v, want nil after validation error", queued.HumanRequiredClassificationPolicy)
	}
	found := false
	for _, projectionErr := range projection.Errors {
		if strings.Contains(projectionErr, "human-required classification policy projection") && strings.Contains(projectionErr, "classification_posture") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("projection errors = %+v, want human-required classification policy validation error", projection.Errors)
	}
}

func TestQueuedRunAuthorityRecommendationPolicyFromBriefSupportsLegacyV07WithoutPolicy(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	doc["lifecycle_version"] = issueScanLifecycleVersionV07
	delete(doc, "authority_recommendation_policy")
	legacy, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal legacy v0.7 brief: %v", err)
	}

	kind, version, lifecycle, agentPlan, err := queuedRunLifecycleFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunLifecycleFromBrief: %v", err)
	}
	if kind != issueScanBriefKind || version != issueScanLifecycleVersionV07 || len(lifecycle) != 7 || len(agentPlan) != 18 {
		t.Fatalf("metadata = kind %q version %q lifecycle %d plan %d, want v0.7 with lifecycle and plan", kind, version, len(lifecycle), len(agentPlan))
	}
	classificationPolicy, err := queuedRunHumanRequiredClassificationPolicyFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunHumanRequiredClassificationPolicyFromBrief: %v", err)
	}
	if classificationPolicy == nil || !containsModelProjectionString(classificationPolicy.EscalationOutputs, "issue_parking_required") {
		t.Fatalf("legacy v0.7 human-required classification policy = %+v, want parsed policy", classificationPolicy)
	}
	authorityPolicy, err := queuedRunAuthorityRecommendationPolicyFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunAuthorityRecommendationPolicyFromBrief: %v", err)
	}
	if authorityPolicy != nil {
		t.Fatalf("legacy v0.7 authority recommendation policy = %+v, want nil", authorityPolicy)
	}
}

func TestQueuedRunAuthorityRecommendationPolicyFromBriefRejectsCurrentBriefWithoutPolicy(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	delete(doc, "authority_recommendation_policy")
	mutated, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal missing authority recommendation policy brief: %v", err)
	}

	policy, err := queuedRunAuthorityRecommendationPolicyFromBrief(mutated)
	if err == nil || !strings.Contains(err.Error(), "authority_recommendation_policy is required") {
		t.Fatalf("queuedRunAuthorityRecommendationPolicyFromBrief error = %v policy = %+v; want required policy error", err, policy)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceRecordsAuthorityRecommendationPolicyError(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	policy, ok := doc["authority_recommendation_policy"].(map[string]any)
	if !ok {
		t.Fatalf("authority_recommendation_policy = %#v, want object", doc["authority_recommendation_policy"])
	}
	policy["recommendation_posture"] = "recommendation_is_authority_decision"
	mutated, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal mutated authority recommendation policy brief: %v", err)
	}

	appendEvent(EventTypeFactoryRunRequested, FactoryRunRequestedContent{
		RunID:        "run_bad_authority_recommendation",
		IntakeID:     "intake_bad_authority_recommendation",
		OperatorID:   "operator_001",
		Title:        "Bad authority recommendation policy still projects queued request",
		Status:       "queued",
		TargetRepos:  []string{"transpara-ai/hive"},
		BriefEventID: newTestEventID(t),
		Brief:        mutated,
	})

	projection := BuildOperatorProjection(s, 50)
	queued := projection.RuntimeEvidence.LastQueuedRunRequest
	if queued == nil {
		t.Fatal("last queued run request is nil")
	}
	if queued.AuthorityRecommendationPolicy != nil {
		t.Fatalf("queued authority recommendation policy = %+v, want nil after validation error", queued.AuthorityRecommendationPolicy)
	}
	found := false
	for _, projectionErr := range projection.Errors {
		if strings.Contains(projectionErr, "authority recommendation policy projection") && strings.Contains(projectionErr, "recommendation_posture") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("projection errors = %+v, want authority recommendation policy validation error", projection.Errors)
	}
}

func TestQueuedRunValueAllocationBoundaryPolicyFromBriefSupportsLegacyV08WithoutPolicy(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	doc["lifecycle_version"] = issueScanLifecycleVersionV08
	delete(doc, "value_allocation_boundary_policy")
	legacy, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal legacy v0.8 brief: %v", err)
	}

	kind, version, lifecycle, agentPlan, err := queuedRunLifecycleFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunLifecycleFromBrief: %v", err)
	}
	if kind != issueScanBriefKind || version != issueScanLifecycleVersionV08 || len(lifecycle) != 7 || len(agentPlan) != 18 {
		t.Fatalf("metadata = kind %q version %q lifecycle %d plan %d, want v0.8 with lifecycle and plan", kind, version, len(lifecycle), len(agentPlan))
	}
	rolePolicy, err := queuedRunRoleSeparationPolicyFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunRoleSeparationPolicyFromBrief: %v", err)
	}
	if rolePolicy == nil || queuedRunRolePolicyByActor(rolePolicy.ActorPolicies, "scanner") == nil {
		t.Fatalf("legacy v0.8 role separation policy = %+v, want parsed policy", rolePolicy)
	}
	autonomyPolicy, err := queuedRunAutonomyGuardPolicyFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunAutonomyGuardPolicyFromBrief: %v", err)
	}
	if autonomyPolicy == nil || !containsModelProjectionString(autonomyPolicy.FailClosedOutputs, "human_scope_required") {
		t.Fatalf("legacy v0.8 autonomy guard policy = %+v, want parsed policy", autonomyPolicy)
	}
	classificationPolicy, err := queuedRunHumanRequiredClassificationPolicyFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunHumanRequiredClassificationPolicyFromBrief: %v", err)
	}
	if classificationPolicy == nil || !containsModelProjectionString(classificationPolicy.EscalationOutputs, "issue_parking_required") {
		t.Fatalf("legacy v0.8 human-required classification policy = %+v, want parsed policy", classificationPolicy)
	}
	authorityPolicy, err := queuedRunAuthorityRecommendationPolicyFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunAuthorityRecommendationPolicyFromBrief: %v", err)
	}
	if authorityPolicy == nil || !containsModelProjectionString(authorityPolicy.RecommendationOutputs, "no_authority_decision") {
		t.Fatalf("legacy v0.8 authority recommendation policy = %+v, want parsed policy", authorityPolicy)
	}
	valuePolicy, err := queuedRunValueAllocationBoundaryPolicyFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunValueAllocationBoundaryPolicyFromBrief: %v", err)
	}
	if valuePolicy != nil {
		t.Fatalf("legacy v0.8 value-allocation boundary policy = %+v, want nil", valuePolicy)
	}
}

func TestQueuedRunValueAllocationBoundaryPolicyFromBriefRejectsCurrentBriefWithoutPolicy(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	delete(doc, "value_allocation_boundary_policy")
	mutated, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal missing value-allocation boundary policy brief: %v", err)
	}

	policy, err := queuedRunValueAllocationBoundaryPolicyFromBrief(mutated)
	if err == nil || !strings.Contains(err.Error(), "value_allocation_boundary_policy is required") {
		t.Fatalf("queuedRunValueAllocationBoundaryPolicyFromBrief error = %v policy = %+v; want required policy error", err, policy)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceRecordsValueAllocationBoundaryPolicyError(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	policy, ok := doc["value_allocation_boundary_policy"].(map[string]any)
	if !ok {
		t.Fatalf("value_allocation_boundary_policy = %#v, want object", doc["value_allocation_boundary_policy"])
	}
	policy["boundary_posture"] = "display_authorizes_allocation"
	mutated, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal mutated value-allocation boundary policy brief: %v", err)
	}

	appendEvent(EventTypeFactoryRunRequested, FactoryRunRequestedContent{
		RunID:        "run_bad_value_allocation_boundary",
		IntakeID:     "intake_bad_value_allocation_boundary",
		OperatorID:   "operator_001",
		Title:        "Bad value-allocation boundary policy still projects queued request",
		Status:       "queued",
		TargetRepos:  []string{"transpara-ai/hive"},
		BriefEventID: newTestEventID(t),
		Brief:        mutated,
	})

	projection := BuildOperatorProjection(s, 50)
	queued := projection.RuntimeEvidence.LastQueuedRunRequest
	if queued == nil {
		t.Fatal("last queued run request is nil")
	}
	if queued.ValueAllocationBoundaryPolicy != nil {
		t.Fatalf("queued value-allocation boundary policy = %+v, want nil after validation error", queued.ValueAllocationBoundaryPolicy)
	}
	found := false
	for _, projectionErr := range projection.Errors {
		if strings.Contains(projectionErr, "value-allocation boundary policy projection") && strings.Contains(projectionErr, "boundary_posture") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("projection errors = %+v, want value-allocation boundary policy validation error", projection.Errors)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceIgnoresLegacyBriefWithoutRoleSeparationPolicy(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)
	appendEvent(EventTypeFactoryRunRequested, FactoryRunRequestedContent{
		RunID:        "run_kind_only_legacy",
		IntakeID:     "intake_kind_only_legacy",
		OperatorID:   "operator_001",
		Title:        "Legacy kind-only issue scan brief",
		Status:       "queued",
		TargetRepos:  []string{"transpara-ai/hive"},
		BriefEventID: newTestEventID(t),
		Brief:        []byte(`{"kind":"transpara_ai_github_issue_scan","required_agent_flow":["research_issue_and_repo_context"]}`),
	})

	projection := BuildOperatorProjection(s, 50)
	queued := projection.RuntimeEvidence.LastQueuedRunRequest
	if queued == nil {
		t.Fatal("last queued run request is nil")
	}
	if queued.RoleSeparationPolicy != nil {
		t.Fatalf("queued role separation policy = %+v, want nil for legacy kind-only brief", queued.RoleSeparationPolicy)
	}
	if queued.AutonomyGuardPolicy != nil {
		t.Fatalf("queued autonomy guard policy = %+v, want nil for legacy kind-only brief", queued.AutonomyGuardPolicy)
	}
	if queued.HumanRequiredClassificationPolicy != nil {
		t.Fatalf("queued human-required classification policy = %+v, want nil for legacy kind-only brief", queued.HumanRequiredClassificationPolicy)
	}
	if queued.AuthorityRecommendationPolicy != nil {
		t.Fatalf("queued authority recommendation policy = %+v, want nil for legacy kind-only brief", queued.AuthorityRecommendationPolicy)
	}
	if queued.ValueAllocationBoundaryPolicy != nil {
		t.Fatalf("queued value-allocation boundary policy = %+v, want nil for legacy kind-only brief", queued.ValueAllocationBoundaryPolicy)
	}
	if len(projection.Errors) != 0 {
		t.Fatalf("projection errors = %+v, want none for legacy kind-only brief", projection.Errors)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceIgnoresForeignBriefRoleSeparationPolicy(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)
	appendEvent(EventTypeFactoryRunRequested, FactoryRunRequestedContent{
		RunID:        "run_foreign_brief",
		IntakeID:     "intake_foreign_brief",
		OperatorID:   "operator_001",
		Title:        "Foreign queued brief",
		Status:       "queued",
		TargetRepos:  []string{"transpara-ai/hive"},
		BriefEventID: newTestEventID(t),
		Brief:        []byte(`{"kind":"foreign_run_request","goal":"generic queued launch"}`),
	})

	projection := BuildOperatorProjection(s, 50)
	queued := projection.RuntimeEvidence.LastQueuedRunRequest
	if queued == nil {
		t.Fatal("last queued run request is nil")
	}
	if queued.RoleSeparationPolicy != nil {
		t.Fatalf("queued role separation policy = %+v, want nil for foreign brief", queued.RoleSeparationPolicy)
	}
	if queued.AutonomyGuardPolicy != nil {
		t.Fatalf("queued autonomy guard policy = %+v, want nil for foreign brief", queued.AutonomyGuardPolicy)
	}
	if queued.HumanRequiredClassificationPolicy != nil {
		t.Fatalf("queued human-required classification policy = %+v, want nil for foreign brief", queued.HumanRequiredClassificationPolicy)
	}
	if queued.AuthorityRecommendationPolicy != nil {
		t.Fatalf("queued authority recommendation policy = %+v, want nil for foreign brief", queued.AuthorityRecommendationPolicy)
	}
	if queued.ValueAllocationBoundaryPolicy != nil {
		t.Fatalf("queued value-allocation boundary policy = %+v, want nil for foreign brief", queued.ValueAllocationBoundaryPolicy)
	}
	if len(projection.Errors) != 0 {
		t.Fatalf("projection errors = %+v, want none for foreign brief", projection.Errors)
	}
}

func TestQueuedRunLifecycleFromBriefSupportsLegacyV02LifecycleWithoutAgentPlan(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	doc["lifecycle_version"] = issueScanLifecycleVersionV02
	delete(doc, "agent_execution_plan")
	stages, ok := doc["development_lifecycle"].([]any)
	if !ok || len(stages) == 0 {
		t.Fatalf("development_lifecycle = %#v, want stages", doc["development_lifecycle"])
	}
	foundReadyStage := false
	for _, rawStage := range stages {
		stage, ok := rawStage.(map[string]any)
		if !ok || stage["id"] != "surface_ready_for_Human_result_PR" {
			continue
		}
		foundReadyStage = true
		evidence, ok := stage["required_evidence"].([]any)
		if !ok {
			t.Fatalf("ready stage evidence = %#v, want array", stage["required_evidence"])
		}
		for i, item := range evidence {
			if item == "ready_pr_url" {
				evidence[i] = "draft_pr_url"
			}
		}
	}
	if !foundReadyStage {
		t.Fatalf("development_lifecycle missing ready stage: %#v", stages)
	}
	legacy, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal legacy brief: %v", err)
	}

	kind, version, lifecycle, agentPlan, err := queuedRunLifecycleFromBrief(legacy)
	if err != nil {
		t.Fatalf("queuedRunLifecycleFromBrief: %v", err)
	}
	if kind != issueScanBriefKind || version != issueScanLifecycleVersionV02 {
		t.Fatalf("metadata = kind %q version %q, want legacy issue-scan v0.2", kind, version)
	}
	if len(lifecycle) != 7 || len(agentPlan) != 0 {
		t.Fatalf("legacy lifecycle = %+v plan = %+v, want 7 stages and no agent plan", lifecycle, agentPlan)
	}
	readyStage := queuedRunLifecycleStageByID(lifecycle, "surface_ready_for_Human_result_PR")
	if readyStage == nil || !containsModelProjectionString(readyStage.RequiredEvidence, "draft_pr_url") || containsModelProjectionString(readyStage.RequiredEvidence, "ready_pr_url") {
		t.Fatalf("legacy ready stage = %+v, want draft_pr_url evidence", readyStage)
	}
}

func TestQueuedRunLifecycleFromBriefRejectsAgentPlanStepCountMismatch(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	plan, ok := doc["agent_execution_plan"].([]any)
	if !ok || len(plan) == 0 {
		t.Fatalf("agent_execution_plan = %#v, want plan steps", doc["agent_execution_plan"])
	}
	doc["agent_execution_plan"] = plan[:len(plan)-1]
	mutated, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal mutated brief: %v", err)
	}

	_, _, lifecycle, agentPlan, err := queuedRunLifecycleFromBrief(mutated)
	if err == nil || !strings.Contains(err.Error(), "agent execution plan step count") {
		t.Fatalf("queuedRunLifecycleFromBrief error = %v, lifecycle = %+v plan = %+v; want plan count mismatch", err, lifecycle, agentPlan)
	}
}

func TestQueuedRunLifecycleFromBriefRejectsLegacyV02WithAgentPlan(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	doc["lifecycle_version"] = issueScanLifecycleVersionV02
	stages, ok := doc["development_lifecycle"].([]any)
	if !ok || len(stages) == 0 {
		t.Fatalf("development_lifecycle = %#v, want stages", doc["development_lifecycle"])
	}
	for _, rawStage := range stages {
		stage, ok := rawStage.(map[string]any)
		if !ok || stage["id"] != "surface_ready_for_Human_result_PR" {
			continue
		}
		evidence, ok := stage["required_evidence"].([]any)
		if !ok {
			t.Fatalf("ready stage evidence = %#v, want array", stage["required_evidence"])
		}
		for i, item := range evidence {
			if item == "ready_pr_url" {
				evidence[i] = "draft_pr_url"
			}
		}
	}
	legacyWithPlan, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal legacy brief: %v", err)
	}

	_, _, lifecycle, agentPlan, err := queuedRunLifecycleFromBrief(legacyWithPlan)
	if err == nil || !strings.Contains(err.Error(), "agent execution plan is not supported") {
		t.Fatalf("queuedRunLifecycleFromBrief error = %v, lifecycle = %+v plan = %+v; want v0.2 agent plan rejection", err, lifecycle, agentPlan)
	}
}

func TestQueuedRunLifecycleFromBriefRejectsMutatedLifecycle(t *testing.T) {
	issue := GitHubIssueCandidate{
		Repo:   "transpara-ai/hive",
		Number: 321,
		Title:  "Teach the Civilization to scan issues",
		URL:    "https://github.com/transpara-ai/hive/issues/321",
	}
	brief, err := issueScanBriefJSON([]GitHubIssueCandidate{issue}, issue)
	if err != nil {
		t.Fatalf("issueScanBriefJSON: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(brief, &doc); err != nil {
		t.Fatalf("unmarshal brief: %v", err)
	}
	stages, ok := doc["development_lifecycle"].([]any)
	if !ok || len(stages) == 0 {
		t.Fatalf("development_lifecycle = %#v, want stages", doc["development_lifecycle"])
	}
	first, ok := stages[0].(map[string]any)
	if !ok {
		t.Fatalf("first stage = %#v, want object", stages[0])
	}
	first["required_roles"] = []any{"unknown_role"}
	mutated, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal mutated brief: %v", err)
	}

	_, _, lifecycle, agentPlan, err := queuedRunLifecycleFromBrief(mutated)
	if err == nil || !strings.Contains(err.Error(), "roles") {
		t.Fatalf("queuedRunLifecycleFromBrief error = %v, lifecycle = %+v plan = %+v; want role validation failure", err, lifecycle, agentPlan)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceRecordsBriefDecodeError(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)
	appendEvent(EventTypeFactoryRunRequested, FactoryRunRequestedContent{
		RunID:      "run_bad_brief",
		IntakeID:   "intake_bad_brief",
		OperatorID: "operator_001",
		Title:      "Bad brief still projects queued request",
		Status:     "queued",
		Authority: RunLaunchAuthority{
			InitialLevel: event.AuthorityLevelRequired,
			Scope:        "operator-launch",
		},
		Budget: RunLaunchBudget{
			MaxIterations: 1,
			MaxCostUSD:    0,
		},
		TargetRepos: []string{"transpara-ai/hive"},
		Brief:       []byte(`{"development_lifecycle":{}}`),
	})

	projection := BuildOperatorProjection(s, 50)
	queued := projection.RuntimeEvidence.LastQueuedRunRequest
	if queued == nil || queued.RunID != "run_bad_brief" {
		t.Fatalf("queued request = %+v, want run_bad_brief", queued)
	}
	if queued.LifecycleEvidenceKind != "" || len(queued.DevelopmentLifecycle) != 0 || len(queued.AgentExecutionPlan) != 0 {
		t.Fatalf("queued lifecycle = kind %q stages %+v plan %+v, want empty on decode error", queued.LifecycleEvidenceKind, queued.DevelopmentLifecycle, queued.AgentExecutionPlan)
	}
	if len(projection.Errors) == 0 || !strings.Contains(projection.Errors[0], "brief lifecycle projection") {
		t.Fatalf("projection errors = %+v, want brief lifecycle projection error", projection.Errors)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceIgnoresEventsWithoutRunStart(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)

	appendEvent(EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "orphan-builder",
		Role:    "implementer",
		Model:   "claude-opus-4-6",
		ActorID: "actor_orphan",
	})
	appendEvent(EventTypeAgentStopped, AgentStoppedContent{
		Name:       "orphan-builder",
		Role:       "implementer",
		StopReason: "orphaned",
		Iterations: 1,
	})
	appendEvent(EventTypeRunCompleted, RunCompletedContent{
		AgentCount: 0,
		DurationMs: 0,
		TotalCost:  0,
	})

	projection := BuildOperatorProjection(s, 50)
	runtimeEvidence := projection.RuntimeEvidence
	if runtimeEvidence.Status != "not_observed" {
		t.Fatalf("runtime status = %q, want not_observed", runtimeEvidence.Status)
	}
	if runtimeEvidence.LastRun != nil {
		t.Fatalf("last run = %+v, want nil without run start", runtimeEvidence.LastRun)
	}
	if runtimeEvidence.AgentEvents.Spawned != 0 || runtimeEvidence.AgentEvents.Stopped != 0 || runtimeEvidence.AgentEvents.ObservedActive != 0 {
		t.Fatalf("agent events = %+v, want no pre-start events counted", runtimeEvidence.AgentEvents)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceKeepsRunAnchorWithAgentChurnLimit(t *testing.T) {
	s, actorID, _ := newOperatorProjectionStore(t)
	convID := types.MustConversationID("conv_runtime_evidence_churn")
	started := appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeRunStarted, RunStartedContent{
		Idea:     "long running",
		RepoPath: "/tmp/churn",
	})
	for i := 0; i < 5; i++ {
		appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeAgentSpawned, AgentSpawnedContent{
			Name:    "builder",
			Role:    "implementer",
			Model:   "claude-opus-4-6",
			ActorID: "actor_churn_builder",
		})
	}

	projection := BuildOperatorProjection(s, 1)
	runtimeEvidence := projection.RuntimeEvidence
	if runtimeEvidence.Status != "running" {
		t.Fatalf("status = %q, want running even when conversation window excludes start", runtimeEvidence.Status)
	}
	if runtimeEvidence.LastRun == nil || runtimeEvidence.LastRun.StartedEventID != started.ID().Value() {
		t.Fatalf("last run = %+v, want anchored start %s", runtimeEvidence.LastRun, started.ID().Value())
	}
	if runtimeEvidence.AgentEvents.Spawned != 5 || runtimeEvidence.AgentEvents.ObservedActive != 1 {
		t.Fatalf("agent events = %+v, want complete lifecycle evidence without evicting run anchor", runtimeEvidence.AgentEvents)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceCombinesQueuedIntentAndRuntimeEvents(t *testing.T) {
	s, actorID, appendEvent := newOperatorProjectionStore(t)
	appendEvent(EventTypeFactoryRunRequested, FactoryRunRequestedContent{
		RunID:      "run_queued",
		IntakeID:   "intake_001",
		OperatorID: "operator_001",
		Title:      "Queued request",
		Status:     "queued",
		Authority: RunLaunchAuthority{
			InitialLevel: event.AuthorityLevelRequired,
		},
		Budget: RunLaunchBudget{
			MaxIterations: 0,
			MaxCostUSD:    0,
		},
		TargetRepos:   []string{"transpara-ai/hive"},
		SourceEventID: newTestEventID(t),
		BriefEventID:  newTestEventID(t),
	})
	convID := types.MustConversationID("conv_runtime_evidence_combined")
	appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeRunStarted, RunStartedContent{
		Idea:     "runtime event",
		RepoPath: "/tmp/runtime",
	})

	projection := BuildOperatorProjection(s, 50)
	runtimeEvidence := projection.RuntimeEvidence
	if runtimeEvidence.LastQueuedRunRequest == nil || runtimeEvidence.LastRun == nil {
		t.Fatalf("runtime evidence = %+v, want queued request and runtime run", runtimeEvidence)
	}
	if runtimeEvidence.LastQueuedRunRequest.RunID != "run_queued" {
		t.Fatalf("queued evidence = %+v", runtimeEvidence.LastQueuedRunRequest)
	}
	if runtimeEvidence.LastQueuedRunRequest.BudgetMaxIterations == nil || *runtimeEvidence.LastQueuedRunRequest.BudgetMaxIterations != 0 {
		t.Fatalf("zero queued budget iterations were not preserved: %+v", runtimeEvidence.LastQueuedRunRequest)
	}
	if runtimeEvidence.LastQueuedRunRequest.BudgetMaxCostUSD == nil || *runtimeEvidence.LastQueuedRunRequest.BudgetMaxCostUSD != 0 {
		t.Fatalf("zero queued budget cost was not preserved: %+v", runtimeEvidence.LastQueuedRunRequest)
	}
	if runtimeEvidence.LastRun.ConversationID != convID.Value() || runtimeEvidence.LastRun.SeedIdea != "runtime event" {
		t.Fatalf("last run evidence = %+v", runtimeEvidence.LastRun)
	}
	if runtimeEvidence.LastQueuedRunRequest.RunID == runtimeEvidence.LastRun.ConversationID {
		t.Fatalf("queued run id was incorrectly joined to runtime conversation: queued=%+v run=%+v", runtimeEvidence.LastQueuedRunRequest, runtimeEvidence.LastRun)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceIgnoresUnrelatedConversationEvents(t *testing.T) {
	s, actorID, _ := newOperatorProjectionStore(t)
	convID := types.MustConversationID("conv_runtime_evidence_progress")
	appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeRunStarted, RunStartedContent{
		Idea:     "runtime with progress",
		RepoPath: "/tmp/runtime",
	})
	appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeProgress, ProgressContent{
		Message: "unrelated progress event for runtime evidence",
	})
	appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "builder",
		Role:    "implementer",
		Model:   "claude-opus-4-6",
		ActorID: "actor_builder",
	})

	projection := BuildOperatorProjection(s, 50)
	if len(projection.Errors) != 0 {
		t.Fatalf("projection errors = %+v, want none for unrelated conversation events", projection.Errors)
	}
	runtimeEvidence := projection.RuntimeEvidence
	if runtimeEvidence.Status != "running" || runtimeEvidence.AgentEvents.Spawned != 1 || runtimeEvidence.AgentEvents.ObservedActive != 1 {
		t.Fatalf("runtime evidence = %+v, want unrelated progress ignored and agent counted", runtimeEvidence)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceKeepsDistinctActiveAgentsWithSameNameRole(t *testing.T) {
	s, actorID, _ := newOperatorProjectionStore(t)
	convID := types.MustConversationID("conv_runtime_evidence_duplicate_agents")
	appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeRunStarted, RunStartedContent{
		Idea:     "duplicate names",
		RepoPath: "/tmp/runtime",
	})
	appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "builder",
		Role:    "implementer",
		Model:   "claude-opus-4-6",
		ActorID: "actor_builder_one",
	})
	appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "builder",
		Role:    "implementer",
		Model:   "claude-opus-4-6",
		ActorID: "actor_builder_two",
	})

	runningProjection := BuildOperatorProjection(s, 50)
	runningEvidence := runningProjection.RuntimeEvidence
	if runningEvidence.AgentEvents.Spawned != 2 || runningEvidence.AgentEvents.ObservedActive != 2 {
		t.Fatalf("running agent events = %+v, want two distinct active agents", runningEvidence.AgentEvents)
	}

	appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeAgentStopped, AgentStoppedContent{
		Name:       "builder",
		Role:       "implementer",
		StopReason: "complete",
		Iterations: 1,
	})
	stoppedProjection := BuildOperatorProjection(s, 50)
	stoppedEvidence := stoppedProjection.RuntimeEvidence
	if stoppedEvidence.AgentEvents.Spawned != 2 || stoppedEvidence.AgentEvents.Stopped != 1 || stoppedEvidence.AgentEvents.ObservedActive != 1 {
		t.Fatalf("stopped agent events = %+v, want one duplicate-name agent remaining", stoppedEvidence.AgentEvents)
	}
	if len(stoppedEvidence.AgentEvents.ActiveAgents) != 1 || stoppedEvidence.AgentEvents.ActiveAgents[0].ActorID != "actor_builder_one" {
		t.Fatalf("active agents after stop = %+v, want first actor remaining", stoppedEvidence.AgentEvents.ActiveAgents)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceFromRunEvents(t *testing.T) {
	s, _, appendEvent := newOperatorProjectionStore(t)

	started := appendEvent(EventTypeRunStarted, RunStartedContent{
		Idea:     "prove runtime evidence",
		RepoPath: "/Transpara/transpara-ai/repos/hive",
	})
	spawned := appendEvent(EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "builder",
		Role:    "implementer",
		Model:   "claude-opus-4-6",
		ActorID: "actor_builder",
	})

	runningProjection := BuildOperatorProjection(s, 50)
	runningEvidence := runningProjection.RuntimeEvidence
	if runningEvidence.Status != "running" {
		t.Fatalf("running status = %q, want running", runningEvidence.Status)
	}
	if runningEvidence.LastRun == nil || runningEvidence.LastRun.StartedEventID != started.ID().Value() {
		t.Fatalf("running last run = %+v, want started event %s", runningEvidence.LastRun, started.ID().Value())
	}
	if runningEvidence.LastRun.ConversationID == "" {
		t.Fatalf("running last run conversation ID is empty: %+v", runningEvidence.LastRun)
	}
	if runningEvidence.AgentEvents.Spawned != 1 || runningEvidence.AgentEvents.ObservedActive != 1 {
		t.Fatalf("running agent events = %+v, want one active spawn", runningEvidence.AgentEvents)
	}
	if len(runningEvidence.AgentEvents.ActiveAgents) != 1 {
		t.Fatalf("active agents = %+v, want one agent", runningEvidence.AgentEvents.ActiveAgents)
	}
	if len(runningEvidence.AgentEvents.ObservedAgents) != 1 {
		t.Fatalf("observed agents = %+v, want one observed agent", runningEvidence.AgentEvents.ObservedAgents)
	}
	active := runningEvidence.AgentEvents.ActiveAgents[0]
	if active.Name != "builder" || active.ActorID != "actor_builder" || active.SpawnedEventID != spawned.ID().Value() {
		t.Fatalf("active agent evidence = %+v", active)
	}
	observed := runningEvidence.AgentEvents.ObservedAgents[0]
	if observed.Name != "builder" || observed.ActorID != "actor_builder" || observed.SpawnedEventID != spawned.ID().Value() {
		t.Fatalf("observed agent evidence = %+v", observed)
	}

	stopped := appendEvent(EventTypeAgentStopped, AgentStoppedContent{
		Name:       "builder",
		Role:       "implementer",
		StopReason: "complete",
		Iterations: 2,
	})
	completed := appendEvent(EventTypeRunCompleted, RunCompletedContent{
		AgentCount: 1,
		DurationMs: 1234,
		TotalCost:  0.25,
	})

	completedProjection := BuildOperatorProjection(s, 50)
	completedEvidence := completedProjection.RuntimeEvidence
	if completedEvidence.Status != "completed" {
		t.Fatalf("completed status = %q, want completed", completedEvidence.Status)
	}
	if completedEvidence.LastRun == nil {
		t.Fatal("completed last run is nil")
	}
	run := completedEvidence.LastRun
	if run.StartedEventID != started.ID().Value() || run.CompletedEventID != completed.ID().Value() {
		t.Fatalf("run evidence = %+v", run)
	}
	if run.SeedIdea != "prove runtime evidence" || run.RepoPath != "/Transpara/transpara-ai/repos/hive" {
		t.Fatalf("run start metadata = %+v", run)
	}
	if run.CompletedAt == nil || run.AgentCount == nil || *run.AgentCount != 1 || run.DurationMs == nil || *run.DurationMs != 1234 || run.TotalCost == nil || *run.TotalCost != 0.25 {
		t.Fatalf("run completion metadata = %+v", run)
	}
	if completedEvidence.AgentEvents.Spawned != 1 || completedEvidence.AgentEvents.Stopped != 1 || completedEvidence.AgentEvents.ObservedActive != 0 {
		t.Fatalf("completed agent events = %+v", completedEvidence.AgentEvents)
	}
	if len(completedEvidence.AgentEvents.ActiveAgents) != 0 {
		t.Fatalf("active agents after completion = %+v, want none", completedEvidence.AgentEvents.ActiveAgents)
	}
	if len(completedEvidence.AgentEvents.ObservedAgents) != 1 {
		t.Fatalf("observed agents after completion = %+v, want completed-run agent retained", completedEvidence.AgentEvents.ObservedAgents)
	}
	observed = completedEvidence.AgentEvents.ObservedAgents[0]
	if observed.Name != "builder" || observed.ActorID != "actor_builder" || observed.SpawnedEventID != spawned.ID().Value() {
		t.Fatalf("completed observed agent evidence = %+v", observed)
	}
	if completedEvidence.AgentEvents.LastAgentEventID != stopped.ID().Value() {
		t.Fatalf("last agent event = %q, want %q", completedEvidence.AgentEvents.LastAgentEventID, stopped.ID().Value())
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceIncludesArtifactsAndCausalGraph(t *testing.T) {
	s, actorID, _ := newOperatorProjectionStore(t)
	convID := types.MustConversationID("conv_runtime_artifact_graph")

	started := appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeRunStarted, RunStartedContent{
		Idea:     "render artifacts",
		RepoPath: "/tmp/runtime",
	})
	spawned := appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "builder",
		Role:    "implementer",
		Model:   "claude-opus-4-6",
		ActorID: "actor_builder",
	})
	artifact := appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeFactoryArtifactCreated, FactoryArtifactCreatedContent{
		RunID:           "run_artifact_graph",
		ArtifactID:      "artifact_brief_001",
		Label:           "factory_brief",
		Title:           "Factory brief",
		MediaType:       "text/markdown",
		URI:             "eventgraph://artifact/artifact_brief_001",
		Summary:         "brief ready for operator inspection",
		ProducerActorID: "actor_builder",
	})
	authorityRequest := appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeAuthorityRequestRecorded, AuthorityRequestRecordedContent{
		RequestID:        newTestEventID(t),
		RequestingActor:  actorID,
		RequestingRole:   "operator",
		ActionName:       "repo.pull_request.create",
		Target:           "transpara-ai/site",
		Environment:      "test",
		RequestedOutcome: "inspect artifact without raw authority payload exposure",
		Justification:    "sensitive rationale must not be exposed through raw run event content",
		RiskSummary:      "raw authority payload exposure",
		Scope:            []string{"repo:transpara-ai/site"},
	})
	runRequest := appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeFactoryRunRequested, FactoryRunRequestedContent{
		RunID:      "run_artifact_graph",
		IntakeID:   "intake_artifact_graph",
		OperatorID: "operator_001",
		Title:      "Launch artifact graph",
		Status:     "queued",
		Authority: RunLaunchAuthority{
			InitialLevel: event.AuthorityLevelRequired,
			Scope:        "operator-launch",
		},
		Budget: RunLaunchBudget{
			MaxIterations: 4,
			MaxCostUSD:    12.5,
		},
		TargetRepos:   []string{"transpara-ai/hive"},
		SourceEventID: newTestEventID(t),
		BriefEventID:  newTestEventID(t),
		Brief:         []byte(`{"goal":"sensitive launch brief"}`),
	})

	projection := BuildOperatorProjection(s, 50)
	runtimeEvidence := projection.RuntimeEvidence
	if runtimeEvidence.LastRun == nil || runtimeEvidence.LastRun.StartedEventID != started.ID().Value() {
		t.Fatalf("last run evidence = %+v, want start %s", runtimeEvidence.LastRun, started.ID().Value())
	}
	if len(runtimeEvidence.Artifacts) != 1 {
		t.Fatalf("artifacts = %+v, want one artifact", runtimeEvidence.Artifacts)
	}
	projectedArtifact := runtimeEvidence.Artifacts[0]
	if projectedArtifact.EventID != artifact.ID().Value() || projectedArtifact.ArtifactID != "artifact_brief_001" {
		t.Fatalf("artifact projection = %+v", projectedArtifact)
	}
	if projectedArtifact.CauseStatus != "caused" {
		t.Fatalf("artifact cause status = %q, want caused", projectedArtifact.CauseStatus)
	}
	if len(projectedArtifact.Causes) != 1 || projectedArtifact.Causes[0].EventID != spawned.ID().Value() || projectedArtifact.Causes[0].EventType != EventTypeAgentSpawned.Value() || projectedArtifact.Causes[0].Scope != "run" {
		t.Fatalf("artifact causes = %+v, want run-scoped spawned event %s", projectedArtifact.Causes, spawned.ID().Value())
	}

	inspectorEvent, ok := findRuntimeEventEvidence(runtimeEvidence.RunEvents, artifact.ID().Value())
	if !ok {
		t.Fatalf("run_events missing artifact inspector payload: %+v", runtimeEvidence.RunEvents)
	}
	if inspectorEvent.InspectorKind != runtimeInspectorKindCuratedEventgraphEvent ||
		!strings.Contains(string(inspectorEvent.Content), `"artifact_id":"artifact_brief_001"`) ||
		!strings.Contains(string(inspectorEvent.Content), `"producer_actor_id":"actor_builder"`) {
		t.Fatalf("artifact inspector event = %+v", inspectorEvent)
	}
	omittedEvent, ok := findRuntimeEventEvidence(runtimeEvidence.RunEvents, authorityRequest.ID().Value())
	if !ok {
		t.Fatalf("run_events missing authority request event: %+v", runtimeEvidence.RunEvents)
	}
	if len(omittedEvent.Content) != 0 || !strings.Contains(omittedEvent.ContentError, "not in the runtime inspector allowlist") {
		t.Fatalf("authority request inspector event = %+v, want curated content omission", omittedEvent)
	}
	omittedRunRequest, ok := findRuntimeEventEvidence(runtimeEvidence.RunEvents, runRequest.ID().Value())
	if !ok {
		t.Fatalf("run_events missing factory run request event: %+v", runtimeEvidence.RunEvents)
	}
	if len(omittedRunRequest.Content) != 0 || !strings.Contains(omittedRunRequest.ContentError, "not in the runtime inspector allowlist") {
		t.Fatalf("factory run request inspector event = %+v, want curated content omission", omittedRunRequest)
	}

	graph := runtimeEvidence.CausalGraph
	if graph.Scope != "latest_run_conversation" || graph.ConversationID != convID.Value() {
		t.Fatalf("causal graph metadata = %+v", graph)
	}
	if len(graph.Nodes) < 3 {
		t.Fatalf("causal graph nodes = %+v, want run start, agent, and artifact nodes", graph.Nodes)
	}
	if !hasRuntimeGraphEdge(graph.Edges, spawned.ID().Value(), artifact.ID().Value(), "run") {
		t.Fatalf("causal graph edges = %+v, want spawned -> artifact edge", graph.Edges)
	}
}

func TestBuildOperatorProjectionRuntimeCausalGraphReportsTruncatedConversation(t *testing.T) {
	s, actorID, _ := newOperatorProjectionStore(t)
	convID := types.MustConversationID("conv_runtime_graph_truncated")
	started := appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeRunStarted, RunStartedContent{
		Idea:     "truncate graph",
		RepoPath: "/tmp/runtime",
	})
	latest := started
	for i := 0; i < 3; i++ {
		latest = appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeAgentSpawned, AgentSpawnedContent{
			Name:    "builder",
			Role:    "implementer",
			Model:   "claude-opus-4-6",
			ActorID: "actor_builder",
		})
	}

	projection := BuildOperatorProjection(s, 1)
	graph := projection.RuntimeEvidence.CausalGraph
	if !graph.Truncated {
		t.Fatalf("causal graph truncated = false, want true for limit 1 with extra conversation events: %+v", graph)
	}
	if graph.Limit != 1 {
		t.Fatalf("causal graph limit = %d, want 1", graph.Limit)
	}
	if !hasRuntimeGraphNode(graph.Nodes, started.ID().Value()) || !hasRuntimeGraphNode(graph.Nodes, latest.ID().Value()) {
		t.Fatalf("causal graph nodes = %+v, want anchored start and latest bounded event", graph.Nodes)
	}
}

func TestBuildOperatorProjectionRuntimeArtifactCauseStatusDistinguishesExternalOnly(t *testing.T) {
	s, actorID, _ := newOperatorProjectionStore(t)
	runConvID := types.MustConversationID("conv_runtime_artifact_external")
	externalConvID := types.MustConversationID("conv_external_artifact_cause")

	appendOperatorProjectionEventWithConversation(t, s, actorID, runConvID, EventTypeRunStarted, RunStartedContent{
		Idea:     "render external artifact causes",
		RepoPath: "/tmp/runtime",
	})
	externalCause := appendOperatorProjectionEventWithConversation(t, s, actorID, externalConvID, EventTypeProgress, ProgressContent{
		Message: "external producer signal",
	})
	artifact := appendOperatorProjectionEventWithConversationAndCauses(t, s, actorID, runConvID, EventTypeFactoryArtifactCreated, FactoryArtifactCreatedContent{
		RunID:      "run_external_artifact",
		ArtifactID: "artifact_external_001",
		Label:      "external_artifact",
		MediaType:  "text/plain",
	}, []types.EventID{externalCause.ID()})
	secondArtifact := appendOperatorProjectionEventWithConversationAndCauses(t, s, actorID, runConvID, EventTypeFactoryArtifactCreated, FactoryArtifactCreatedContent{
		RunID:      "run_external_artifact",
		ArtifactID: "artifact_external_002",
		Label:      "second_external_artifact",
		MediaType:  "text/plain",
	}, []types.EventID{externalCause.ID()})

	projection := BuildOperatorProjection(s, 50)
	runtimeEvidence := projection.RuntimeEvidence
	if len(runtimeEvidence.Artifacts) != 2 {
		t.Fatalf("artifacts = %+v, want two artifacts", runtimeEvidence.Artifacts)
	}
	projectedArtifact, ok := findRuntimeArtifactEvidence(runtimeEvidence.Artifacts, artifact.ID().Value())
	if !ok {
		t.Fatalf("artifacts = %+v, want event %s", runtimeEvidence.Artifacts, artifact.ID().Value())
	}
	if projectedArtifact.CauseStatus != "caused_external_only" {
		t.Fatalf("artifact cause status = %q, want caused_external_only", projectedArtifact.CauseStatus)
	}
	if len(projectedArtifact.Causes) != 1 || projectedArtifact.Causes[0].EventID != externalCause.ID().Value() || projectedArtifact.Causes[0].Scope != "external_or_out_of_window" {
		t.Fatalf("artifact causes = %+v, want external-only cause %s", projectedArtifact.Causes, externalCause.ID().Value())
	}
	if !hasRuntimeGraphEdge(runtimeEvidence.CausalGraph.Edges, externalCause.ID().Value(), artifact.ID().Value(), "external_or_out_of_window") {
		t.Fatalf("causal graph edges = %+v, want external cause -> artifact edge", runtimeEvidence.CausalGraph.Edges)
	}
	if !hasRuntimeGraphEdge(runtimeEvidence.CausalGraph.Edges, externalCause.ID().Value(), secondArtifact.ID().Value(), "external_or_out_of_window") {
		t.Fatalf("causal graph edges = %+v, want repeated external cause -> second artifact edge", runtimeEvidence.CausalGraph.Edges)
	}
	for _, edge := range runtimeEvidence.CausalGraph.Edges {
		if edge.FromEventID == externalCause.ID().Value() && edge.Scope != "external_or_out_of_window" {
			t.Fatalf("edge from external cause mislabeled: %+v", edge)
		}
	}
}

func TestBuildOperatorProjectionRuntimeInspectorOmitsOversizedCuratedContent(t *testing.T) {
	s, actorID, _ := newOperatorProjectionStore(t)
	convID := types.MustConversationID("conv_runtime_inspector_oversized")
	started := appendOperatorProjectionEventWithConversation(t, s, actorID, convID, EventTypeRunStarted, RunStartedContent{
		Idea:     strings.Repeat("x", maxOperatorRuntimeInspectorContentBytes+1),
		RepoPath: "/tmp/runtime",
	})

	projection := BuildOperatorProjection(s, 50)
	if len(projection.Errors) != 0 {
		t.Fatalf("projection errors = %+v, want none for intentionally omitted oversized inspector content", projection.Errors)
	}
	inspectorEvent, ok := findRuntimeEventEvidence(projection.RuntimeEvidence.RunEvents, started.ID().Value())
	if !ok {
		t.Fatalf("run_events missing start event: %+v", projection.RuntimeEvidence.RunEvents)
	}
	if len(inspectorEvent.Content) != 0 || !strings.Contains(inspectorEvent.ContentError, "exceeds") {
		t.Fatalf("start inspector event = %+v, want oversized content omitted", inspectorEvent)
	}
}

func TestRuntimeArtifactCauseStatusValues(t *testing.T) {
	if got := runtimeArtifactCauseStatus(nil); got != "missing_causes" {
		t.Fatalf("empty cause status = %q, want missing_causes", got)
	}
	if got := runtimeArtifactCauseStatus([]OperatorRuntimeCauseEvidence{{EventID: "evt_external", Scope: "external_or_out_of_window"}}); got != "caused_external_only" {
		t.Fatalf("external-only cause status = %q, want caused_external_only", got)
	}
	if got := runtimeArtifactCauseStatus([]OperatorRuntimeCauseEvidence{{EventID: "evt_run", Scope: "run"}}); got != "caused" {
		t.Fatalf("run cause status = %q, want caused", got)
	}
}

func TestBuildOperatorProjectionRuntimeEvidenceReanchorsToLatestConversation(t *testing.T) {
	s, actorID, _ := newOperatorProjectionStore(t)
	firstConversation := types.MustConversationID("conv_runtime_evidence_first")
	secondConversation := types.MustConversationID("conv_runtime_evidence_second")

	appendOperatorProjectionEventWithConversation(t, s, actorID, firstConversation, EventTypeRunStarted, RunStartedContent{
		Idea:     "first run",
		RepoPath: "/tmp/first",
	})
	appendOperatorProjectionEventWithConversation(t, s, actorID, firstConversation, EventTypeRunCompleted, RunCompletedContent{
		AgentCount: 2,
		DurationMs: 100,
		TotalCost:  1.5,
	})
	secondStarted := appendOperatorProjectionEventWithConversation(t, s, actorID, secondConversation, EventTypeRunStarted, RunStartedContent{
		Idea:     "second run",
		RepoPath: "/tmp/second",
	})
	secondSpawned := appendOperatorProjectionEventWithConversation(t, s, actorID, secondConversation, EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "second-builder",
		Role:    "implementer",
		Model:   "claude-opus-4-6",
		ActorID: "actor_second_builder",
	})
	appendOperatorProjectionEventWithConversation(t, s, actorID, firstConversation, EventTypeRunCompleted, RunCompletedContent{
		AgentCount: 9,
		DurationMs: 999,
		TotalCost:  9.99,
	})

	runningProjection := BuildOperatorProjection(s, 50)
	runningEvidence := runningProjection.RuntimeEvidence
	if runningEvidence.Status != "running" {
		t.Fatalf("status after stale completion = %q, want running", runningEvidence.Status)
	}
	if runningEvidence.LastRun == nil {
		t.Fatal("last run is nil")
	}
	if runningEvidence.LastRun.StartedEventID != secondStarted.ID().Value() || runningEvidence.LastRun.ConversationID != secondConversation.Value() {
		t.Fatalf("latest run evidence = %+v, want second conversation", runningEvidence.LastRun)
	}
	if runningEvidence.LastRun.CompletedEventID != "" || runningEvidence.LastRun.AgentCount != nil {
		t.Fatalf("stale completion was attached to latest run: %+v", runningEvidence.LastRun)
	}
	if runningEvidence.AgentEvents.Spawned != 1 || runningEvidence.AgentEvents.ObservedActive != 1 {
		t.Fatalf("running agent events = %+v, want one active second-run spawn", runningEvidence.AgentEvents)
	}
	if len(runningEvidence.AgentEvents.ActiveAgents) != 1 || runningEvidence.AgentEvents.ActiveAgents[0].SpawnedEventID != secondSpawned.ID().Value() {
		t.Fatalf("active agents = %+v, want second-run agent", runningEvidence.AgentEvents.ActiveAgents)
	}
	if runningEvidence.AgentEvents.LastAgentEventID != secondSpawned.ID().Value() {
		t.Fatalf("last agent event = %q, want second-run spawn %q", runningEvidence.AgentEvents.LastAgentEventID, secondSpawned.ID().Value())
	}

	appendOperatorProjectionEventWithConversation(t, s, actorID, secondConversation, EventTypeAgentStopped, AgentStoppedContent{
		Name:       "second-builder",
		Role:       "implementer",
		StopReason: "complete",
		Iterations: 1,
	})
	secondCompleted := appendOperatorProjectionEventWithConversation(t, s, actorID, secondConversation, EventTypeRunCompleted, RunCompletedContent{
		AgentCount: 1,
		DurationMs: 200,
		TotalCost:  0,
	})

	completedProjection := BuildOperatorProjection(s, 50)
	completedEvidence := completedProjection.RuntimeEvidence
	if completedEvidence.Status != "completed" {
		t.Fatalf("status = %q, want completed", completedEvidence.Status)
	}
	if completedEvidence.LastRun == nil || completedEvidence.LastRun.CompletedEventID != secondCompleted.ID().Value() {
		t.Fatalf("completed latest run evidence = %+v", completedEvidence.LastRun)
	}
	if completedEvidence.LastRun.TotalCost == nil || *completedEvidence.LastRun.TotalCost != 0 {
		t.Fatalf("zero total cost was not preserved as observed evidence: %+v", completedEvidence.LastRun)
	}
	if completedEvidence.AgentEvents.Spawned != 1 || completedEvidence.AgentEvents.Stopped != 1 || completedEvidence.AgentEvents.ObservedActive != 0 {
		t.Fatalf("completed agent events = %+v", completedEvidence.AgentEvents)
	}
	if len(completedEvidence.AgentEvents.ObservedAgents) != 1 || completedEvidence.AgentEvents.ObservedAgents[0].SpawnedEventID != secondSpawned.ID().Value() {
		t.Fatalf("completed observed agents = %+v, want second-run agent retained", completedEvidence.AgentEvents.ObservedAgents)
	}

	appendOperatorProjectionEventWithConversation(t, s, actorID, secondConversation, EventTypeAgentSpawned, AgentSpawnedContent{
		Name:    "late-builder",
		Role:    "implementer",
		Model:   "claude-opus-4-6",
		ActorID: "actor_late_builder",
	})
	afterLateSpawnProjection := BuildOperatorProjection(s, 50)
	afterLateSpawnEvidence := afterLateSpawnProjection.RuntimeEvidence
	if afterLateSpawnEvidence.Status != "completed" {
		t.Fatalf("status after late same-conversation spawn = %q, want completed", afterLateSpawnEvidence.Status)
	}
	if afterLateSpawnEvidence.AgentEvents.ObservedActive != 0 || len(afterLateSpawnEvidence.AgentEvents.ActiveAgents) != 0 {
		t.Fatalf("late spawn reopened active state: %+v", afterLateSpawnEvidence.AgentEvents)
	}
	if len(afterLateSpawnEvidence.AgentEvents.ObservedAgents) != 1 || afterLateSpawnEvidence.AgentEvents.ObservedAgents[0].SpawnedEventID != secondSpawned.ID().Value() {
		t.Fatalf("late spawn changed completed-run observed agents: %+v", afterLateSpawnEvidence.AgentEvents.ObservedAgents)
	}
}

func TestBuildOperatorProjectionIncludesStaticModelSelection(t *testing.T) {
	s, _, _ := newOperatorProjectionStore(t)

	projection := BuildOperatorProjection(s, 50)
	models := projection.ModelSelection

	if models.Source != "hive" {
		t.Fatalf("model selection source = %q, want hive", models.Source)
	}
	if models.CatalogSource != operatorModelCatalogSourceEmbedded {
		t.Fatalf("catalog source = %q, want %q", models.CatalogSource, operatorModelCatalogSourceEmbedded)
	}
	if models.ReloadMode != operatorModelCatalogReloadMode || models.HotReload {
		t.Fatalf("reload metadata = mode %q hot_reload %v, want startup-static false", models.ReloadMode, models.HotReload)
	}
	if models.LoadedAt.IsZero() {
		t.Fatal("model catalog loaded_at is zero")
	}
	for _, tier := range []modelconfig.ModelTier{modelconfig.TierJudgment, modelconfig.TierExecution, modelconfig.TierVolume} {
		if models.TierDefaults[string(tier)] == "" {
			t.Fatalf("tier default %q is not projected: %+v", tier, models.TierDefaults)
		}
	}
	if len(models.Models) == 0 {
		t.Fatal("expected projected model catalog entries")
	}
	if len(models.Errors) != 0 {
		t.Fatalf("default role assignments produced errors: %+v", models.Errors)
	}

	implementer := requireModelAssignment(t, models, "implementer")
	if !implementer.CanOperate {
		t.Fatalf("implementer can_operate = false, want true")
	}
	if implementer.Provider != "claude-cli" || implementer.AuthMode != string(modelconfig.AuthSubscription) {
		t.Fatalf("implementer assignment = %+v, want claude-cli subscription", implementer)
	}
	if !containsModelProjectionString(implementer.RequiredCapabilities, string(modelconfig.CapOperate)) {
		t.Fatalf("implementer required capabilities = %+v, want operate", implementer.RequiredCapabilities)
	}

	for _, assignment := range models.Assignments {
		if assignment.Model == "" {
			t.Fatalf("empty model assignment for %s: %+v", assignment.Role, assignment)
		}
		if assignment.AuthMode != string(modelconfig.AuthSubscription) {
			t.Fatalf("default role %s auth_mode = %q, want subscription (no silent API-key default): %+v", assignment.Role, assignment.AuthMode, assignment)
		}
	}
}

func TestBuildOperatorProjectionModelSelectionRequiresExplicitAPIKeyOptIn(t *testing.T) {
	s, _, _ := newOperatorProjectionStore(t)

	defaultProjection := BuildOperatorProjection(s, 50)
	if got := requireModelAssignment(t, defaultProjection.ModelSelection, "guardian").AuthMode; got != string(modelconfig.AuthSubscription) {
		t.Fatalf("default guardian auth_mode = %q, want subscription", got)
	}

	config := testModelSelectionConfigWithRoleDefault("guardian", "api-sonnet")
	projection := BuildOperatorProjection(s, 50, WithOperatorModelSelection(config))
	guardian := requireModelAssignment(t, projection.ModelSelection, "guardian")
	if guardian.Model != "api-claude-sonnet-4-6" || guardian.Provider != "anthropic" || guardian.AuthMode != string(modelconfig.AuthAPIKey) {
		t.Fatalf("guardian explicit API-key opt-in assignment = %+v", guardian)
	}

	implementer := requireModelAssignment(t, projection.ModelSelection, "implementer")
	if implementer.Provider != "claude-cli" || implementer.AuthMode != string(modelconfig.AuthSubscription) {
		t.Fatalf("explicit guardian API-key opt-in leaked to implementer: %+v", implementer)
	}
}

func TestOperatorModelSelectionManagerReloadUpdatesProjection(t *testing.T) {
	catalogPath := writeRoleDefaultCatalog(t, "guardian", "api-sonnet")
	manager, err := NewOperatorModelSelectionManager(catalogPath, time.Unix(1_700_000_000, 0).UTC(), true)
	if err != nil {
		t.Fatalf("NewOperatorModelSelectionManager: %v", err)
	}
	first := BuildOperatorModelSelection(manager.Snapshot())
	firstGuardian := requireModelAssignment(t, first, "guardian")
	if first.ReloadMode != operatorModelCatalogReloadModeHot || !first.HotReload {
		t.Fatalf("reload metadata = %q/%v, want hot-reload true", first.ReloadMode, first.HotReload)
	}
	if firstGuardian.AuthMode != string(modelconfig.AuthAPIKey) {
		t.Fatalf("initial guardian auth = %q, want api-key", firstGuardian.AuthMode)
	}

	writeRoleDefaultCatalogAt(t, catalogPath, "guardian", "haiku")
	if err := manager.Reload(time.Unix(1_700_000_100, 0).UTC()); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	reloaded := BuildOperatorModelSelection(manager.Snapshot())
	reloadedGuardian := requireModelAssignment(t, reloaded, "guardian")
	if reloaded.LastReloadAt == nil {
		t.Fatal("last_reload_at is nil after reload")
	}
	if reloadedGuardian.AuthMode != string(modelconfig.AuthSubscription) {
		t.Fatalf("reloaded guardian auth = %q, want subscription", reloadedGuardian.AuthMode)
	}
	if reloadedGuardian.Model == firstGuardian.Model {
		t.Fatalf("guardian model did not change after reload: %q", reloadedGuardian.Model)
	}
}

func TestOperatorModelSelectionManagerReloadFailureIsVisibleAndKeepsPreviousResolver(t *testing.T) {
	catalogPath := writeRoleDefaultCatalog(t, "guardian", "api-sonnet")
	manager, err := NewOperatorModelSelectionManager(catalogPath, time.Unix(1_700_000_000, 0).UTC(), true)
	if err != nil {
		t.Fatalf("NewOperatorModelSelectionManager: %v", err)
	}
	before := requireModelAssignment(t, BuildOperatorModelSelection(manager.Snapshot()), "guardian")
	if before.AuthMode != string(modelconfig.AuthAPIKey) {
		t.Fatalf("initial guardian auth = %q, want api-key", before.AuthMode)
	}

	if err := os.WriteFile(catalogPath, []byte("models: [\n"), 0o644); err != nil {
		t.Fatalf("write invalid catalog: %v", err)
	}
	if err := manager.Reload(time.Unix(1_700_000_100, 0).UTC()); err == nil {
		t.Fatal("Reload invalid catalog succeeded, want error")
	}
	afterProjection := BuildOperatorModelSelection(manager.Snapshot())
	after := requireModelAssignment(t, afterProjection, "guardian")
	if after.AuthMode != string(modelconfig.AuthAPIKey) || after.Model != before.Model {
		t.Fatalf("reload failure corrupted active assignment: before=%+v after=%+v", before, after)
	}
	if len(afterProjection.Errors) == 0 || !strings.Contains(afterProjection.Errors[0], "catalog reload") {
		t.Fatalf("projection errors = %+v, want catalog reload error", afterProjection.Errors)
	}
}

func newOperatorProjectionStore(t *testing.T) (*store.InMemoryStore, types.ActorID, func(types.EventType, event.EventContent) event.Event) {
	t.Helper()
	RegisterEventTypes()
	registry := event.DefaultRegistry()
	RegisterWithRegistry(registry)
	work.RegisterWithRegistry(registry)

	s := store.NewInMemoryStore()
	actorID := types.MustActorID("actor_00000000000000000000000000000077")
	signer := deriveSignerFromID(actorID)
	bootstrap, err := event.NewBootstrapFactory(registry).Init(actorID, signer)
	if err != nil {
		t.Fatalf("bootstrap init: %v", err)
	}
	if _, err := s.Append(bootstrap); err != nil {
		t.Fatalf("append bootstrap: %v", err)
	}
	factory := event.NewEventFactory(registry)
	convID := types.MustConversationID("conv_00000000000000000000000000000077")

	appendEvent := func(eventType types.EventType, content event.EventContent) event.Event {
		t.Helper()
		head, err := s.Head()
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		ev, err := factory.Create(eventType, actorID, content, []types.EventID{head.Unwrap().ID()}, convID, s, signer)
		if err != nil {
			t.Fatalf("create %s: %v", eventType.Value(), err)
		}
		stored, err := s.Append(ev)
		if err != nil {
			t.Fatalf("append %s: %v", eventType.Value(), err)
		}
		return stored
	}

	return s, actorID, appendEvent
}

func appendOpaqueSharedStoreEvent(t *testing.T, s *store.InMemoryStore, actorID types.ActorID) event.Event {
	t.Helper()
	head, err := s.Head()
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	eventType := types.MustEventType("foreign.event.type")
	content := foreignSharedStoreContent{Source: "shared-store"}
	eventID := newTestEventID(t)
	convID := types.MustConversationID("conv_00000000000000000000000000000088")
	causes := []types.EventID{head.Unwrap().ID()}
	prevHash := head.Unwrap().Hash()
	tmp := event.NewEvent(event.CurrentEventVersion, eventID, eventType, types.Now(), actorID, content, causes, convID, types.ZeroHash(), prevHash, types.Signature{})
	hash, err := event.ComputeHash(event.CanonicalForm(tmp))
	if err != nil {
		t.Fatalf("hash opaque event: %v", err)
	}
	ev := event.NewEvent(event.CurrentEventVersion, eventID, eventType, tmp.Timestamp(), actorID, content, causes, convID, hash, prevHash, types.Signature{})
	stored, err := s.Append(ev)
	if err != nil {
		t.Fatalf("append opaque event: %v", err)
	}
	return stored
}

type foreignSharedStoreContent struct {
	Source string `json:"source"`
}

func (c foreignSharedStoreContent) EventTypeName() string { return "foreign.event.type" }
func (c foreignSharedStoreContent) Accept(event.EventContentVisitor) {
}

type factoryOrderReadFailureStore struct {
	store.Store
}

func (s factoryOrderReadFailureStore) ByType(eventType types.EventType, limit int, after types.Option[types.Cursor]) (types.Page[event.Event], error) {
	if eventType == work.EventTypeTaskCreated {
		return types.NewPage[event.Event](nil, types.None[types.Cursor](), false), errors.New("work task query unavailable")
	}
	return s.Store.ByType(eventType, limit, after)
}

type serializationRoundTripStore struct {
	store.Store
}

func (s serializationRoundTripStore) Head() (types.Option[event.Event], error) {
	head, err := s.Store.Head()
	if err != nil || !head.IsSome() {
		return head, err
	}
	ev, err := roundTripEventContent(head.Unwrap())
	if err != nil {
		return types.None[event.Event](), err
	}
	return types.Some(ev), nil
}

func (s serializationRoundTripStore) Recent(limit int, after types.Option[types.Cursor]) (types.Page[event.Event], error) {
	page, err := s.Store.Recent(limit, after)
	if err != nil {
		return page, err
	}
	return roundTripEventPage(page)
}

func (s serializationRoundTripStore) ByType(eventType types.EventType, limit int, after types.Option[types.Cursor]) (types.Page[event.Event], error) {
	page, err := s.Store.ByType(eventType, limit, after)
	if err != nil {
		return page, err
	}
	return roundTripEventPage(page)
}

func (s serializationRoundTripStore) BySource(source types.ActorID, limit int, after types.Option[types.Cursor]) (types.Page[event.Event], error) {
	page, err := s.Store.BySource(source, limit, after)
	if err != nil {
		return page, err
	}
	return roundTripEventPage(page)
}

func (s serializationRoundTripStore) ByConversation(id types.ConversationID, limit int, after types.Option[types.Cursor]) (types.Page[event.Event], error) {
	page, err := s.Store.ByConversation(id, limit, after)
	if err != nil {
		return page, err
	}
	return roundTripEventPage(page)
}

func (s serializationRoundTripStore) Since(afterID types.EventID, limit int) (types.Page[event.Event], error) {
	page, err := s.Store.Since(afterID, limit)
	if err != nil {
		return page, err
	}
	return roundTripEventPage(page)
}

func (s serializationRoundTripStore) Ancestors(id types.EventID, maxDepth int) ([]event.Event, error) {
	events, err := s.Store.Ancestors(id, maxDepth)
	if err != nil {
		return nil, err
	}
	return roundTripEvents(events)
}

func (s serializationRoundTripStore) Descendants(id types.EventID, maxDepth int) ([]event.Event, error) {
	events, err := s.Store.Descendants(id, maxDepth)
	if err != nil {
		return nil, err
	}
	return roundTripEvents(events)
}

func roundTripEventPage(page types.Page[event.Event]) (types.Page[event.Event], error) {
	events, err := roundTripEvents(page.Items())
	if err != nil {
		return types.Page[event.Event]{}, err
	}
	return types.NewPage(events, page.Cursor(), page.HasMore()), nil
}

func roundTripEvents(events []event.Event) ([]event.Event, error) {
	roundTripped := make([]event.Event, 0, len(events))
	for _, ev := range events {
		next, err := roundTripEventContent(ev)
		if err != nil {
			return nil, err
		}
		roundTripped = append(roundTripped, next)
	}
	return roundTripped, nil
}

func roundTripEventContent(ev event.Event) (event.Event, error) {
	contentJSON, err := json.Marshal(ev.Content())
	if err != nil {
		return event.Event{}, err
	}
	content, err := event.UnmarshalContent(ev.Type().Value(), contentJSON)
	if err != nil {
		return event.Event{}, err
	}
	if ev.IsBootstrap() {
		bootstrap, ok := content.(event.BootstrapContent)
		if !ok {
			return event.Event{}, fmt.Errorf("bootstrap content type = %T, want event.BootstrapContent", content)
		}
		return event.NewBootstrapEvent(ev.Version(), ev.ID(), ev.Type(), ev.Timestamp(), ev.Source(), bootstrap, ev.ConversationID(), ev.Hash(), ev.Signature()), nil
	}
	return event.NewEvent(ev.Version(), ev.ID(), ev.Type(), ev.Timestamp(), ev.Source(), content, ev.Causes(), ev.ConversationID(), ev.Hash(), ev.PrevHash(), ev.Signature()), nil
}

func issueScanRoleContractBodyForProjectionTest(t *testing.T, orderID, stageID string, roles []string) string {
	t.Helper()
	plan := make([]OperatorQueuedRunAgentPlanStep, 0, len(roles))
	for _, role := range roles {
		plan = append(plan, OperatorQueuedRunAgentPlanStep{
			ID:              safeRunLaunchID(stageID) + ":" + role,
			StageID:         stageID,
			Role:            role,
			RequiredOutputs: []string{"projection_contract"},
			EvidenceStatus:  "pending_runtime_evidence",
		})
	}
	payload := struct {
		Kind               string                           `json:"kind"`
		LifecycleVersion   string                           `json:"lifecycle_version"`
		RunID              string                           `json:"run_id"`
		FactoryOrderID     string                           `json:"factory_order_id"`
		StageID            string                           `json:"stage_id"`
		Stage              OperatorQueuedRunLifecycleStage  `json:"stage"`
		AgentExecutionPlan []OperatorQueuedRunAgentPlanStep `json:"agent_execution_plan"`
		EvidenceKind       string                           `json:"evidence_kind"`
		EvidenceStatus     string                           `json:"evidence_status"`
	}{
		Kind:             issueScanStageRoleContractArtifactKind,
		LifecycleVersion: issueScanLifecycleVersion,
		RunID:            "run_issue_scan_role_contracts_001",
		FactoryOrderID:   orderID,
		StageID:          stageID,
		Stage: OperatorQueuedRunLifecycleStage{
			ID:               stageID,
			RequiredRoles:    append([]string(nil), roles...),
			RequiredEvidence: []string{"projection_contract"},
			EvidenceStatus:   "expected_not_observed",
		},
		AgentExecutionPlan: plan,
		EvidenceKind:       "required_role_contract_not_runtime_execution",
		EvidenceStatus:     "pending_runtime_evidence",
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		t.Fatalf("marshal role contract: %v", err)
	}
	return string(encoded)
}

func issueScanOutputContractBodyForProjectionTest(t *testing.T, orderID, stageID string, roles, evidence []string) string {
	t.Helper()
	roleContracts := make([]CivilizationAssemblyRoleOutputContract, 0, len(roles))
	for _, role := range roles {
		roleContracts = append(roleContracts, CivilizationAssemblyRoleOutputContract{
			Role:              role,
			RequiredOutputs:   []string{"projection_contract_" + safeRunLaunchID(role)},
			EvidenceStatus:    "pending_runtime_evidence",
			CompletionGate:    "evidence_refs_recorded",
			AuthorityBoundary: "human_approval_required_no_merge",
		})
	}
	payload := struct {
		Kind                string                                   `json:"kind"`
		LifecycleVersion    string                                   `json:"lifecycle_version"`
		RunID               string                                   `json:"run_id"`
		FactoryOrderID      string                                   `json:"factory_order_id"`
		StageID             string                                   `json:"stage_id"`
		Stage               OperatorQueuedRunLifecycleStage          `json:"stage"`
		RequiredEvidence    []string                                 `json:"required_evidence"`
		ExpectedOutputs     []string                                 `json:"expected_outputs"`
		RoleOutputContracts []CivilizationAssemblyRoleOutputContract `json:"role_output_contracts"`
		EvidenceKind        string                                   `json:"evidence_kind"`
		EvidenceStatus      string                                   `json:"evidence_status"`
	}{
		Kind:             issueScanStageOutputContractArtifactKind,
		LifecycleVersion: issueScanLifecycleVersion,
		RunID:            "run_issue_scan_output_contracts_001",
		FactoryOrderID:   orderID,
		StageID:          stageID,
		Stage: OperatorQueuedRunLifecycleStage{
			ID:               stageID,
			RequiredRoles:    append([]string(nil), roles...),
			RequiredEvidence: append([]string(nil), evidence...),
			EvidenceStatus:   "expected_not_observed",
		},
		RequiredEvidence:    append([]string(nil), evidence...),
		ExpectedOutputs:     append([]string(nil), evidence...),
		RoleOutputContracts: roleContracts,
		EvidenceKind:        "stage_output_contract_not_runtime_execution",
		EvidenceStatus:      "pending_runtime_evidence",
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		t.Fatalf("marshal output contract: %v", err)
	}
	return string(encoded)
}

func civilizationProjectionFailureContains(projection CivilizationAssemblyProjection, want string) bool {
	for _, failure := range projection.FailureReasons {
		if strings.Contains(failure, want) {
			return true
		}
	}
	return false
}

func appendOperatorProjectionEventWithConversation(t *testing.T, s *store.InMemoryStore, actorID types.ActorID, convID types.ConversationID, eventType types.EventType, content event.EventContent) event.Event {
	t.Helper()
	head, err := s.Head()
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	return appendOperatorProjectionEventWithConversationAndCauses(t, s, actorID, convID, eventType, content, []types.EventID{head.Unwrap().ID()})
}

func appendOperatorProjectionEventWithConversationAndCauses(t *testing.T, s *store.InMemoryStore, actorID types.ActorID, convID types.ConversationID, eventType types.EventType, content event.EventContent, causes []types.EventID) event.Event {
	t.Helper()
	RegisterEventTypes()
	registry := event.DefaultRegistry()
	RegisterWithRegistry(registry)
	factory := event.NewEventFactory(registry)
	signer := deriveSignerFromID(actorID)

	ev, err := factory.Create(eventType, actorID, content, causes, convID, s, signer)
	if err != nil {
		t.Fatalf("create %s: %v", eventType.Value(), err)
	}
	stored, err := s.Append(ev)
	if err != nil {
		t.Fatalf("append %s: %v", eventType.Value(), err)
	}
	return stored
}

func testModelSelectionConfigWithRoleDefault(role, model string) OperatorModelSelectionConfig {
	base := modelconfig.DefaultResolver()
	defaults := base.Defaults()
	tierModels := make(map[modelconfig.ModelTier]string, len(defaults.TierModels))
	for tier, model := range defaults.TierModels {
		tierModels[tier] = model
	}
	roleModels := make(map[string]string, len(defaults.RoleModels)+1)
	for role, model := range defaults.RoleModels {
		roleModels[role] = model
	}
	roleModels[role] = model
	defaults.TierModels = tierModels
	defaults.RoleModels = roleModels
	return OperatorModelSelectionConfig{
		Resolver:      modelconfig.NewResolver(base.Catalog(), nil, defaults),
		CatalogSource: "test-explicit-role-default",
		LoadedAt:      time.Unix(1_700_000_000, 0).UTC(),
		ReloadMode:    operatorModelCatalogReloadMode,
		HotReload:     false,
	}
}

func requireModelAssignment(t *testing.T, projection OperatorModelSelection, role string) OperatorModelRoleAssignment {
	t.Helper()
	for _, assignment := range projection.Assignments {
		if assignment.Role == role {
			return assignment
		}
	}
	t.Fatalf("missing model assignment for role %q in %+v", role, projection.Assignments)
	return OperatorModelRoleAssignment{}
}

func containsModelProjectionString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func queuedRunLifecycleStageByID(stages []OperatorQueuedRunLifecycleStage, id string) *OperatorQueuedRunLifecycleStage {
	for i := range stages {
		if stages[i].ID == id {
			return &stages[i]
		}
	}
	return nil
}

func queuedRunAgentPlanStepByRole(steps []OperatorQueuedRunAgentPlanStep, stageID, role string) *OperatorQueuedRunAgentPlanStep {
	for i := range steps {
		if steps[i].StageID == stageID && steps[i].Role == role {
			return &steps[i]
		}
	}
	return nil
}

func queuedRunRolePolicyByActor(policies []OperatorQueuedRunRoleSeparationActorPolicy, actor string) *OperatorQueuedRunRoleSeparationActorPolicy {
	for i := range policies {
		if policies[i].ActorClass == actor {
			return &policies[i]
		}
	}
	return nil
}

func findRuntimeEventEvidence(events []OperatorRuntimeEventEvidence, eventID string) (OperatorRuntimeEventEvidence, bool) {
	for _, ev := range events {
		if ev.EventID == eventID {
			return ev, true
		}
	}
	return OperatorRuntimeEventEvidence{}, false
}

func findRuntimeArtifactEvidence(artifacts []OperatorRuntimeArtifactEvidence, eventID string) (OperatorRuntimeArtifactEvidence, bool) {
	for _, artifact := range artifacts {
		if artifact.EventID == eventID {
			return artifact, true
		}
	}
	return OperatorRuntimeArtifactEvidence{}, false
}

func civilizationProjectionArtifactEvidenceByID(artifacts []CivilizationAssemblyArtifactEvidence, eventID string) *CivilizationAssemblyArtifactEvidence {
	for i := range artifacts {
		if artifacts[i].ID == eventID {
			return &artifacts[i]
		}
	}
	return nil
}

func civilizationProjectionTaskEvidenceByID(tasks []CivilizationAssemblyTaskEvidence, eventID string) *CivilizationAssemblyTaskEvidence {
	for i := range tasks {
		if tasks[i].ID == eventID {
			return &tasks[i]
		}
	}
	return nil
}

func hasRuntimeGraphEdge(edges []OperatorRuntimeCausalEdge, from, to, scope string) bool {
	for _, edge := range edges {
		if edge.FromEventID == from && edge.ToEventID == to && edge.Scope == scope {
			return true
		}
	}
	return false
}

func hasRuntimeGraphNode(nodes []OperatorRuntimeCausalNode, eventID string) bool {
	for _, node := range nodes {
		if node.EventID == eventID {
			return true
		}
	}
	return false
}

func writeRoleDefaultCatalog(t *testing.T, role, model string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog.yaml")
	writeRoleDefaultCatalogAt(t, path, role, model)
	return path
}

func writeRoleDefaultCatalogAt(t *testing.T, path, role, model string) {
	t.Helper()
	body := "role_defaults:\n  " + role + ": " + model + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write catalog: %v", err)
	}
}

func TestCatalogMixedAddsCurrentClaudeModelsAndSurvivesMerge(t *testing.T) {
	catalogPath := filepath.Join("..", "..", "catalog-mixed.yaml")
	// MergeCatalogs replaces same-ID embedded entries WHOLESALE with the user
	// file's copy, so a wrong catalog-mixed price or capability set would
	// silently override eventgraph's correct entry and feed the cost gates
	// (MaxCostPerCallUSD, CheapestWithCapabilities) and CanOperate validation
	// — pin all four pricing numbers and the exact capability list post-merge.
	wantCapabilities := []modelconfig.Capability{
		modelconfig.CapTools, modelconfig.CapReasoning, modelconfig.CapCoding,
		modelconfig.CapVision, modelconfig.CapOperate, modelconfig.CapLargeContext,
		modelconfig.CapStructuredOut,
	}
	wantEntries := map[string]struct {
		aliases []string
		pricing modelconfig.ModelPricing
	}{
		"claude-opus-4-8": {
			aliases: []string{"opus-4-8"},
			pricing: modelconfig.ModelPricing{
				InputPerMillion:      5.00,
				OutputPerMillion:     25.00,
				CacheReadPerMillion:  0.50,
				CacheWritePerMillion: 6.25,
			},
		},
		"claude-fable-5": {
			aliases: []string{"fable", "fable-5"},
			pricing: modelconfig.ModelPricing{
				InputPerMillion:      10.00,
				OutputPerMillion:     50.00,
				CacheReadPerMillion:  1.00,
				CacheWritePerMillion: 12.50,
			},
		},
	}

	// The entries must exist in catalog-mixed.yaml ITSELF, not only in the
	// embedded eventgraph defaults — MergeCatalogs replaces same-ID base
	// entries with the user file's copy, so the user file's copy is what
	// survives the merge (packet D5 / CFADA1-adv3).
	data, err := os.ReadFile(catalogPath)
	if err != nil {
		t.Fatalf("read catalog-mixed.yaml: %v", err)
	}
	userCatalog, _, _, err := modelconfig.ParseCatalogYAML(data)
	if err != nil {
		t.Fatalf("parse catalog-mixed.yaml: %v", err)
	}
	for id := range wantEntries {
		entry, ok := userCatalog.Lookup(id)
		if !ok {
			t.Fatalf("catalog-mixed.yaml missing model entry %q", id)
		}
		if entry.Provider != "claude-cli" || entry.AuthMode != modelconfig.AuthSubscription {
			t.Fatalf("catalog-mixed.yaml entry %q = provider %q auth %q, want claude-cli subscription", id, entry.Provider, entry.AuthMode)
		}
		if entry.Tier != modelconfig.TierJudgment {
			t.Fatalf("catalog-mixed.yaml entry %q tier = %q, want judgment", id, entry.Tier)
		}
		if entry.ContextWindow != 1_000_000 || entry.MaxOutputTokens != 16_384 {
			t.Fatalf("catalog-mixed.yaml entry %q window/output = %d/%d, want 1000000/16384", id, entry.ContextWindow, entry.MaxOutputTokens)
		}
	}

	// Loaded via the same path hive uses for the ops projection
	// (OperatorModelSelectionFromCatalogPath → ResolverFromCatalogFile →
	// MergeCatalogs): both new ids and every new alias must resolve post-merge.
	config, err := OperatorModelSelectionFromCatalogPath(catalogPath, time.Unix(1_700_000_000, 0).UTC())
	if err != nil {
		t.Fatalf("OperatorModelSelectionFromCatalogPath: %v", err)
	}
	merged := config.Resolver.Catalog()
	for id, want := range wantEntries {
		entry, ok := merged.Lookup(id)
		if !ok {
			t.Fatalf("merged catalog missing id %q", id)
		}
		if entry.ID != id {
			t.Fatalf("merged catalog lookup %q resolved to %q", id, entry.ID)
		}
		if entry.Pricing != want.pricing {
			t.Fatalf("merged catalog entry %q pricing = %+v, want %+v", id, entry.Pricing, want.pricing)
		}
		if len(entry.Capabilities) != len(wantCapabilities) {
			t.Fatalf("merged catalog entry %q capabilities = %v, want %v", id, entry.Capabilities, wantCapabilities)
		}
		for i, capability := range wantCapabilities {
			if entry.Capabilities[i] != capability {
				t.Fatalf("merged catalog entry %q capabilities = %v, want %v", id, entry.Capabilities, wantCapabilities)
			}
		}
		for _, alias := range want.aliases {
			aliased, ok := merged.Lookup(alias)
			if !ok {
				t.Fatalf("merged catalog missing alias %q for %q", alias, id)
			}
			if aliased.ID != id {
				t.Fatalf("merged catalog alias %q resolved to %q, want %q", alias, aliased.ID, id)
			}
		}
	}

	// The live alias `opus` must stay bound to claude-opus-4-6 — adding
	// opus-4-8 must not silently rebind it (packet D1 / IADA-4).
	opus, ok := merged.Lookup("opus")
	if !ok {
		t.Fatal("merged catalog missing alias \"opus\"")
	}
	if opus.ID != "claude-opus-4-6" {
		t.Fatalf("merged catalog alias \"opus\" resolved to %q, want claude-opus-4-6", opus.ID)
	}
}

func TestOperatorModelRoleAssignmentSelectionModeRoundTrip(t *testing.T) {
	loadedAt := time.Unix(1_700_000_000, 0).UTC()

	// Every starter role resolves through the embedded role_defaults map
	// (guardian → sonnet, implementer → opus, ...), so the role-default layer
	// wins the token and the starter policy's PreferredTier never fires:
	// system config chose the model → system-default.
	defaultSelection := BuildOperatorModelSelection(DefaultOperatorModelSelectionConfig(loadedAt))
	for _, role := range []string{"guardian", "implementer"} {
		assignment := requireModelAssignment(t, defaultSelection, role)
		if assignment.Error != "" {
			t.Fatalf("%s assignment unexpectedly errored: %q", role, assignment.Error)
		}
		if assignment.SelectionMode != string(modelconfig.SelectionModeSystemDefault) {
			t.Fatalf("%s selection_mode = %q, want %q", role, assignment.SelectionMode, modelconfig.SelectionModeSystemDefault)
		}
	}

	// A stored role policy pinning a concrete catalog alias is an explicit pin.
	pinned := DefaultOperatorModelSelectionConfig(loadedAt)
	pinned.RolePolicies = map[string]OperatorModelRolePolicy{
		"guardian": {Policy: &modelconfig.RoleModelPolicy{Model: "sonnet"}},
	}
	pinnedGuardian := requireModelAssignment(t, BuildOperatorModelSelection(pinned), "guardian")
	if pinnedGuardian.Error != "" {
		t.Fatalf("pinned guardian assignment unexpectedly errored: %q", pinnedGuardian.Error)
	}
	if pinnedGuardian.SelectionMode != string(modelconfig.SelectionModeManualExplicit) {
		t.Fatalf("pinned guardian selection_mode = %q, want %q", pinnedGuardian.SelectionMode, modelconfig.SelectionModeManualExplicit)
	}

	// Without a role default for guardian, the starter policy's PreferredTier
	// selects via tier defaults → auto-tier.
	tierGuardian := requireModelAssignment(t, BuildOperatorModelSelection(testModelSelectionConfigWithoutRoleDefault("guardian", loadedAt)), "guardian")
	if tierGuardian.Error != "" {
		t.Fatalf("tier guardian assignment unexpectedly errored: %q", tierGuardian.Error)
	}
	if tierGuardian.SelectionMode != string(modelconfig.SelectionModeAutoTier) {
		t.Fatalf("tier guardian selection_mode = %q, want %q", tierGuardian.SelectionMode, modelconfig.SelectionModeAutoTier)
	}
}

func TestOperatorModelRoleAssignmentErrorPathsCarryNoSelectionMode(t *testing.T) {
	// operatorModelRoleAssignment has exactly three error-return paths before
	// SelectionMode is set; one row per path completes the input-domain sweep.
	config := DefaultOperatorModelSelectionConfig(time.Unix(1_700_000_000, 0).UTC())
	config.RolePolicies = map[string]OperatorModelRolePolicy{
		// Path 1 — Resolve itself fails: the token is not a catalog id,
		// alias, or tier.
		"guardian": {Policy: &modelconfig.RoleModelPolicy{Model: "no-such-model"}},
		// Path 2 — Resolve succeeds but validateRunLaunchOverrideResolvedConfig
		// fails: an api-key model without the explicit auth_mode opt-in.
		"sysmon": {Policy: &modelconfig.RoleModelPolicy{Model: "api-sonnet"}},
		// Path 3 — Resolve succeeds but validateStoredModelRolePolicyResolution
		// fails: the stored resolved_model no longer matches what the current
		// resolver produces.
		"allocator": {
			Policy:        &modelconfig.RoleModelPolicy{Model: "sonnet"},
			ResolvedModel: "claude-opus-4-6",
		},
	}
	selection := BuildOperatorModelSelection(config)
	wantErrors := map[string]string{
		"guardian":  "not found in catalog",
		"sysmon":    "opt in to metered API-key models",
		"allocator": "stored resolved_model",
	}
	for role, wantError := range wantErrors {
		assignment := requireModelAssignment(t, selection, role)
		if assignment.Error == "" {
			t.Fatalf("%s assignment expected an error, got none: %+v", role, assignment)
		}
		if !strings.Contains(assignment.Error, wantError) {
			t.Fatalf("%s error = %q, want the %q gate (containing %q)", role, assignment.Error, role, wantError)
		}
		if assignment.SelectionMode != "" {
			t.Fatalf("%s error-path selection_mode = %q, want empty — mode is never synthesized", role, assignment.SelectionMode)
		}
	}
}

func TestOperatorModelRoleAssignmentSelectionModeJSONShape(t *testing.T) {
	empty := OperatorModelRoleAssignment{Role: "guardian", Source: "starter-role-definition"}
	data, err := json.Marshal(empty)
	if err != nil {
		t.Fatalf("marshal assignment without mode: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("unmarshal assignment JSON: %v", err)
	}
	if _, ok := fields["selection_mode"]; ok {
		t.Fatalf("selection_mode key present in %s, want omitted when empty", data)
	}

	set := empty
	set.SelectionMode = string(modelconfig.SelectionModeManualExplicit)
	data, err = json.Marshal(set)
	if err != nil {
		t.Fatalf("marshal assignment with mode: %v", err)
	}
	fields = nil
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("unmarshal assignment JSON: %v", err)
	}
	if got, ok := fields["selection_mode"]; !ok || got != string(modelconfig.SelectionModeManualExplicit) {
		t.Fatalf("selection_mode in %s = %v (present=%v), want %q", data, got, ok, modelconfig.SelectionModeManualExplicit)
	}
}

func testModelSelectionConfigWithoutRoleDefault(role string, loadedAt time.Time) OperatorModelSelectionConfig {
	base := modelconfig.DefaultResolver()
	defaults := base.Defaults()
	tierModels := make(map[modelconfig.ModelTier]string, len(defaults.TierModels))
	for tier, model := range defaults.TierModels {
		tierModels[tier] = model
	}
	roleModels := make(map[string]string, len(defaults.RoleModels))
	for name, model := range defaults.RoleModels {
		if name == role {
			continue
		}
		roleModels[name] = model
	}
	defaults.TierModels = tierModels
	defaults.RoleModels = roleModels
	return OperatorModelSelectionConfig{
		Resolver:      modelconfig.NewResolver(base.Catalog(), nil, defaults),
		CatalogSource: "test-no-role-default",
		LoadedAt:      loadedAt,
		ReloadMode:    operatorModelCatalogReloadMode,
		HotReload:     false,
	}
}
