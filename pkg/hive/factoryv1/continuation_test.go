package factoryv1

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func continuationRepository() RepositoryIdentity {
	return RepositoryIdentity{Provider: "github", NumericID: 101, OwnerName: "transpara-ai/repo-x"}
}

func continuationHuman() Principal {
	return Principal{Kind: "human", StableID: "github:MichaelSaucier", SubjectRef: "github:MichaelSaucier"}
}

func continuationAuthentication() Authentication {
	return Authentication{Status: "authenticated", Method: "github_session", Reference: "auth:1", AuthenticatedBy: "hive"}
}

func continuationInvocationFixture(t *testing.T) (ContinuationInvocation, []byte) {
	t.Helper()
	record, err := NewInlineSourceRecord(
		"change-1", "human_request", 0, nil, continuationHuman(), continuationAuthentication(),
		"text/plain", "implement the bounded change", "hive", time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	invocation := ContinuationInvocation{
		DocumentType: "invocation", ContractVersion: TLCContinuationVersion,
		TargetRepository: continuationRepository(),
		SourceChain:      SourceChain{ChainID: record.ChainID, HeadDigest: record.RecordDigest, Records: []SourceRecord{record}},
		RequestedAction:  RequestedAction{Action: "route_only", Subject: record.RecordDigest, RequestedBy: continuationHuman()},
	}
	body, err := json.Marshal(invocation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeContinuationInvocation(body); err != nil {
		t.Fatalf("fixture does not satisfy invocation contract: %v", err)
	}
	return invocation, body
}

func continuationIdentity() TLCWorkflowIdentity {
	return TLCWorkflowIdentity{
		PluginName: TLCGovernancePluginName, PluginVersion: "1.1.0",
		SkillName: TLCChangeWorkflowSkill, ContractVersion: TLCContinuationVersion,
		SchemaSHA256: strings.Repeat("a", 64), SkillSHA256: strings.Repeat("b", 64),
		ContractCoreSHA256: strings.Repeat("c", 64),
		RunnerSHA256:       strings.Repeat("d", 64), RunnerArgvSHA256: strings.Repeat("e", 64),
	}
}

func continuationReportFixture(t *testing.T, invocation ContinuationInvocation) []byte {
	t.Helper()
	report := ContinuationReport{
		DocumentType: "report", ContractVersion: TLCContinuationVersion,
		SourceChainHead: invocation.SourceChain.HeadDigest, SourceValidation: "valid",
		CollaborationPhase: "design", InformationState: "CLASSIFIED",
		RouteRequirements: []RouteRequirement{},
		AuthorRequirements: []AuthorRequirement{{
			RequirementID: "author-1", SourceChainHead: invocation.SourceChain.HeadDigest,
			Phase: "design", Objective: "interpret the exact request", Role: "author",
			InputDigests:           []string{invocation.SourceChain.HeadDigest},
			RequiredIdentityFields: []string{"stable_id", "lineage"},
		}},
		FOCandidateSufficiency: Sufficiency{Status: "not_applicable", Satisfied: []string{}, Missing: []string{}},
		RepositoryAffinity:     RepositoryAffinity{Status: "unknown", AffinityRepository: continuationRepository(), Mismatches: []string{}},
		AdmittedEvidenceIDs:    []string{}, RejectedCandidates: []Disposition{}, ContinuationFrontier: []FrontierPoint{},
		BlockedSlices: []Disposition{}, PartialDispositions: []Disposition{}, ReviewCreditIDs: []string{}, AuthorityDispositions: []Disposition{},
	}
	digest, err := CanonicalSHA256(report)
	if err != nil {
		t.Fatal(err)
	}
	report.ReportSHA256 = digest
	body, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

type fakeTLCChangeWorkflowRunner struct {
	identity TLCWorkflowIdentity
	response []byte
	err      error
	calls    int
	got      []byte
}

func (r *fakeTLCChangeWorkflowRunner) ObservedIdentity(context.Context) (TLCWorkflowIdentity, error) {
	return r.identity, nil
}

func (r *fakeTLCChangeWorkflowRunner) Evaluate(_ context.Context, invocationJSON []byte) ([]byte, error) {
	r.calls++
	r.got = append([]byte(nil), invocationJSON...)
	if r.err != nil {
		return nil, r.err
	}
	return append([]byte(nil), r.response...), nil
}

func TestInvokeTLCChangeWorkflowDelegatesExactBytes(t *testing.T) {
	t.Parallel()
	invocation, body := continuationInvocationFixture(t)
	runner := &fakeTLCChangeWorkflowRunner{identity: continuationIdentity(), response: continuationReportFixture(t, invocation)}
	report, err := InvokeTLCChangeWorkflow(context.Background(), runner, continuationIdentity(), body)
	if err != nil {
		t.Fatal(err)
	}
	if runner.calls != 1 || string(runner.got) != string(body) {
		t.Fatalf("workflow delegation changed call semantics: calls=%d got=%s", runner.calls, runner.got)
	}
	if report.SourceChainHead != invocation.SourceChain.HeadDigest {
		t.Fatalf("report head %s does not match invocation %s", report.SourceChainHead, invocation.SourceChain.HeadDigest)
	}
}

func TestCanonicalSHA256IsIndependentOfGoFieldAndMapOrder(t *testing.T) {
	t.Parallel()
	type reverseFields struct {
		B int `json:"b"`
		A int `json:"a"`
	}
	fromStruct, err := CanonicalSHA256(reverseFields{B: 2, A: 1})
	if err != nil {
		t.Fatal(err)
	}
	fromMap, err := CanonicalSHA256(map[string]int{"a": 1, "b": 2})
	if err != nil {
		t.Fatal(err)
	}
	fromRaw, err := CanonicalSHA256(json.RawMessage(`{"b":2,"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if fromStruct != fromMap || fromMap != fromRaw {
		t.Fatalf("portable digests differ: struct=%s map=%s raw=%s", fromStruct, fromMap, fromRaw)
	}
}

func TestInvokeTLCChangeWorkflowRejectsInputAndIdentityBeforeDispatch(t *testing.T) {
	t.Parallel()
	invocation, body := continuationInvocationFixture(t)
	runner := &fakeTLCChangeWorkflowRunner{identity: continuationIdentity(), response: continuationReportFixture(t, invocation)}
	invalid := append([]byte(nil), body[:len(body)-1]...)
	invalid = append(invalid, []byte(",\"ambient_authority\":true}")...)
	if _, err := InvokeTLCChangeWorkflow(context.Background(), runner, continuationIdentity(), invalid); err == nil {
		t.Fatal("unknown invocation field was accepted")
	}
	if runner.calls != 0 {
		t.Fatal("invalid input reached the workflow runner")
	}

	mismatch := continuationIdentity()
	mismatch.SchemaSHA256 = strings.Repeat("c", 64)
	if _, err := InvokeTLCChangeWorkflow(context.Background(), runner, mismatch, body); err == nil {
		t.Fatal("installed identity mismatch was accepted")
	}
	if runner.calls != 0 {
		t.Fatal("identity mismatch reached the workflow runner")
	}
}

func TestInvokeTLCChangeWorkflowPropagatesDownstreamFailure(t *testing.T) {
	t.Parallel()
	_, body := continuationInvocationFixture(t)
	downstream := errors.New("selected workflow unavailable")
	runner := &fakeTLCChangeWorkflowRunner{identity: continuationIdentity(), err: downstream}
	if _, err := InvokeTLCChangeWorkflow(context.Background(), runner, continuationIdentity(), body); !errors.Is(err, downstream) {
		t.Fatalf("downstream failure was not preserved: %v", err)
	}
	if runner.calls != 1 {
		t.Fatalf("downstream runner calls=%d, want 1", runner.calls)
	}
}

func TestHumanSelectionAndDispatchClaimAreSingleUse(t *testing.T) {
	t.Parallel()
	_, body := continuationInvocationFixture(t)
	invocation, err := DecodeContinuationInvocation(body)
	if err != nil {
		t.Fatal(err)
	}
	selection, err := NewCollaborationSelection(
		continuationHuman(), continuationAuthentication(), invocation.SourceChain.HeadDigest,
		"design", "challenge the consequential design fork", "anthropic-human-design-collaboration",
	)
	if err != nil {
		t.Fatal(err)
	}
	machine := continuationHuman()
	machine.Kind = "machine"
	if _, err := NewCollaborationSelection(machine, continuationAuthentication(), invocation.SourceChain.HeadDigest, "design", "machine default", selection.Profile); err == nil {
		t.Fatal("machine selection was accepted")
	}

	intent, err := NewInvocationIntent(selection, "fable-pass-1")
	if err != nil {
		t.Fatal(err)
	}
	store := NewInMemoryInvocationIntentStore()
	if err := store.CreateInvocationIntent(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	claimed, err := ClaimInvocationDispatch(context.Background(), store, intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ClaimInvocationDispatch(context.Background(), store, intent); !errors.Is(err, ErrInvocationIntentConflict) {
		t.Fatal("claimed invocation produced a second launch claim")
	}
	unknown, err := RecoverClaimedInvocation(context.Background(), store, claimed)
	if err != nil {
		t.Fatal(err)
	}
	if unknown.State != "consumed_unknown" {
		t.Fatalf("recovered state=%s", unknown.State)
	}
	if _, err := ClaimInvocationDispatch(context.Background(), store, unknown); err == nil {
		t.Fatal("consumed_unknown invocation produced a retry claim")
	}

	secondSelection, err := NewCollaborationSelection(
		continuationHuman(), continuationAuthentication(), invocation.SourceChain.HeadDigest,
		"design", "run a separately selected second challenge", "anthropic-human-design-collaboration",
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewInvocationIntent(secondSelection, "fable-pass-2")
	if err != nil {
		t.Fatal(err)
	}
	secondStore := NewInMemoryInvocationIntentStore()
	if err := secondStore.CreateInvocationIntent(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	second, err = ClaimInvocationDispatch(context.Background(), secondStore, second)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := CompleteInvocation(context.Background(), secondStore, second, strings.Repeat("d", 64))
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != "completed" || completed.ResultSHA256 != strings.Repeat("d", 64) {
		t.Fatalf("unexpected completion: %+v", completed)
	}
}

func TestInvocationDispatchClaimIsAtomicUnderContention(t *testing.T) {
	t.Parallel()
	invocation, _ := continuationInvocationFixture(t)
	selection, err := NewCollaborationSelection(
		continuationHuman(), continuationAuthentication(), invocation.SourceChain.HeadDigest,
		"design", "challenge one consequential design fork", "anthropic-human-design-collaboration",
	)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := NewInvocationIntent(selection, "fable-atomic-pass")
	if err != nil {
		t.Fatal(err)
	}
	store := NewInMemoryInvocationIntentStore()
	if err := store.CreateInvocationIntent(context.Background(), intent); err != nil {
		t.Fatal(err)
	}

	const contenders = 16
	results := make(chan error, contenders)
	var wait sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, claimErr := ClaimInvocationDispatch(context.Background(), store, intent)
			results <- claimErr
		}()
	}
	wait.Wait()
	close(results)
	winners := 0
	for claimErr := range results {
		if claimErr == nil {
			winners++
			continue
		}
		if !errors.Is(claimErr, ErrInvocationIntentConflict) {
			t.Fatalf("unexpected claim error: %v", claimErr)
		}
	}
	if winners != 1 {
		t.Fatalf("atomic dispatch winners=%d, want 1", winners)
	}
}

func TestStrictContractShapeRejectsMissingAndNullFields(t *testing.T) {
	t.Parallel()
	invocation, invocationBody := continuationInvocationFixture(t)
	var invocationJSON map[string]any
	if err := json.Unmarshal(invocationBody, &invocationJSON); err != nil {
		t.Fatal(err)
	}
	invocationJSON["collaboration_context"] = nil
	invalidInvocation, err := json.Marshal(invocationJSON)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeContinuationInvocation(invalidInvocation); err == nil {
		t.Fatal("explicit null collaboration_context was accepted")
	}

	var reportJSON map[string]any
	if err := json.Unmarshal(continuationReportFixture(t, invocation), &reportJSON); err != nil {
		t.Fatal(err)
	}
	delete(reportJSON, "blocked_slices")
	invalidReport, err := json.Marshal(reportJSON)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeContinuationReport(invalidReport, invocation.SourceChain.HeadDigest); err == nil {
		t.Fatal("missing required report array was accepted")
	}

	reportJSON["blocked_slices"] = []any{}
	reportJSON["continuation_frontier"] = []any{map[string]any{
		"candidate_id": "candidate-1", "slice_id": "slice-1",
		"subject_digest": invocation.SourceChain.HeadDigest, "next_safe_action": "continue",
	}}
	invalidReport, err = json.Marshal(reportJSON)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeContinuationReport(invalidReport, invocation.SourceChain.HeadDigest); err == nil {
		t.Fatal("missing required continuation_permitted boolean was accepted")
	}
}

func TestAuxiliaryContractDigestRejectsTamperedRecord(t *testing.T) {
	t.Parallel()
	invocation, _ := continuationInvocationFixture(t)
	content := "bounded authored direction"
	author := AuthorResult{
		ResultID: "author-result-1", RequirementID: "author-requirement-1",
		SourceChainHead: invocation.SourceChain.HeadDigest, Mode: "interactive_host",
		Author:        Principal{Kind: "model", StableID: "codex:session", SubjectRef: "codex:session", ModelID: "gpt-5", Lineage: "openai"},
		Content:       ExactContent{MediaType: "text/plain", Encoding: "utf-8", Readability: "readable", DigestVerified: true, Inline: &content},
		ContentSHA256: HashText(content), PromptSHA256: strings.Repeat("d", 64),
		AttestationID: "interactive-host:1", BoundedDirection: true,
	}
	digest, err := CanonicalSHA256(author)
	if err != nil {
		t.Fatal(err)
	}
	author.ResultDigest = digest
	invocation.AuthorResults = []AuthorResult{author}
	body, err := json.Marshal(invocation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeContinuationInvocation(body); err != nil {
		t.Fatalf("exact auxiliary digest was rejected: %v", err)
	}

	invocation.AuthorResults[0].InvalidatesDirection = true
	body, err = json.Marshal(invocation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeContinuationInvocation(body); err == nil || !strings.Contains(err.Error(), "does not bind the exact record") {
		t.Fatalf("tampered author result was not rejected by digest binding: %v", err)
	}
}

type failNextContinuationAttach struct {
	ContinuationWorkStore
	fail bool
}

func (s *failNextContinuationAttach) AttachContinuationArtifact(ctx context.Context, artifact ContinuationWorkArtifact) (string, error) {
	if s.fail {
		s.fail = false
		return "", errors.New("injected Work projection failure")
	}
	return s.ContinuationWorkStore.AttachContinuationArtifact(ctx, artifact)
}

func TestContinuationPersistenceRepairsEventGraphWorkSplit(t *testing.T) {
	t.Parallel()
	invocation, _ := continuationInvocationFixture(t)
	events := NewInMemoryStore(testClock())
	work := NewInMemoryContinuationWorkStore()
	sourceEnvelope, err := NewContinuationRecord(
		ContinuationRecordSource, invocation.SourceChain.ChainID, invocation.SourceChain.HeadDigest,
		invocation.SourceChain.HeadDigest, nil, continuationHuman(), "hive", time.Date(2026, 8, 30, 12, 0, 1, 0, time.UTC), invocation.SourceChain.Records[0],
	)
	if err != nil {
		t.Fatal(err)
	}
	seedEvent, complete, err := SeedContinuationSource(context.Background(), events, work, continuationRepository(), sourceEnvelope)
	if err != nil || !complete {
		t.Fatalf("seed continuation: complete=%v err=%v", complete, err)
	}
	replayed, complete, err := SeedContinuationSource(context.Background(), events, work, continuationRepository(), sourceEnvelope)
	if err != nil || !complete || replayed.ID != seedEvent.ID {
		t.Fatalf("idempotent seed replay: event=%s complete=%v err=%v", replayed.ID, complete, err)
	}

	reportEnvelope, err := NewContinuationRecord(
		ContinuationRecordTLCReport, invocation.SourceChain.ChainID, invocation.SourceChain.HeadDigest,
		invocation.SourceChain.HeadDigest, []string{sourceEnvelope.RecordID}, continuationHuman(), "hive",
		time.Date(2026, 8, 30, 12, 0, 2, 0, time.UTC), map[string]string{"report": "exact"},
	)
	if err != nil {
		t.Fatal(err)
	}
	failing := &failNextContinuationAttach{ContinuationWorkStore: work, fail: true}
	splitEvent, complete, err := PersistContinuationRecord(context.Background(), events, failing, reportEnvelope)
	if err == nil || complete || splitEvent.ID == "" {
		t.Fatalf("split append was not exposed safely: event=%s complete=%v err=%v", splitEvent.ID, complete, err)
	}
	if len(splitEvent.Causes) != 1 || splitEvent.Causes[0] != seedEvent.ID {
		t.Fatalf("declared causal record was not an EventGraph cause: got=%v want=%s", splitEvent.Causes, seedEvent.ID)
	}
	repaired, err := RepairContinuationProjection(context.Background(), work, splitEvent)
	if err != nil || !repaired {
		t.Fatalf("repair EventGraph-only record: repaired=%v err=%v", repaired, err)
	}
	if _, err := work.GetContinuationArtifact(context.Background(), reportEnvelope.ChainID, reportEnvelope.RecordID); err != nil {
		t.Fatalf("repaired Work twin is unreadable: %v", err)
	}
}

func TestEffectContinuationBlocksUnknownAndConflict(t *testing.T) {
	t.Parallel()
	if decision := DecideEffectContinuation(EffectAbsent, true, true, true); !decision.RetrySame || decision.Blocked {
		t.Fatalf("authenticated absence should permit the same operation: %+v", decision)
	}
	for _, state := range []EffectObservationState{EffectUnknown, EffectConflict} {
		if decision := DecideEffectContinuation(state, true, true, true); !decision.Blocked || decision.RetrySame {
			t.Fatalf("state %s did not fail closed: %+v", state, decision)
		}
	}
}

func TestRepositoryContinuationKeepsRepoXEffectsInRepoX(t *testing.T) {
	t.Parallel()
	invocation, _ := continuationInvocationFixture(t)
	var report ContinuationReport
	if err := json.Unmarshal(continuationReportFixture(t, invocation), &report); err != nil {
		t.Fatal(err)
	}
	report.RepositoryAffinity.Status = "match"
	report.ContinuationFrontier = []FrontierPoint{{
		CandidateID: "repo-x-c3", SliceID: "repo-x", SubjectDigest: invocation.SourceChain.HeadDigest,
		NextSafeAction: "create_worktree", ContinuationPermitted: true,
	}}
	report.ReportSHA256 = ""
	digest, err := CanonicalSHA256(report)
	if err != nil {
		t.Fatal(err)
	}
	report.ReportSHA256 = digest
	if err := RequireRepositoryContinuation(report, continuationRepository(), "repo-x-c3", "create_worktree"); err != nil {
		t.Fatalf("RepoX continuation was rejected: %v", err)
	}
	repoY := RepositoryIdentity{Provider: "github", NumericID: 202, OwnerName: "transpara-ai/repo-y"}
	if err := RequireRepositoryContinuation(report, repoY, "repo-x-c3", "create_worktree"); err == nil {
		t.Fatal("RepoX TLC report authorized a RepoY worktree")
	}
}
