package civilization

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/transpara-ai/hive/pkg/hive/tlcbridge"
)

type fakeProvider struct {
	mu                      sync.Mutex
	route                   string
	runs                    map[ProviderOperation]int
	prompts                 map[ProviderOperation][]string
	blockImplementationOnce bool
	blockReviewOnce         bool
}

type crashOnceProvider struct {
	delegate       *fakeProvider
	mu             sync.Mutex
	crashImplement bool
	attempts       []string
}

func (p *crashOnceProvider) Run(ctx context.Context, request ProviderRequest) (ProviderResult, error) {
	if request.Operation == OperationImplement {
		p.mu.Lock()
		p.attempts = append(p.attempts, request.AttemptID)
		crash := p.crashImplement
		p.crashImplement = false
		p.mu.Unlock()
		if crash {
			panic("simulated process loss during provider attempt")
		}
	}
	return p.delegate.Run(ctx, request)
}

func (p *fakeProvider) Run(_ context.Context, request ProviderRequest) (ProviderResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.runs == nil {
		p.runs = map[ProviderOperation]int{}
	}
	if p.prompts == nil {
		p.prompts = map[ProviderOperation][]string{}
	}
	p.runs[request.Operation]++
	p.prompts[request.Operation] = append(p.prompts[request.Operation], request.Prompt)
	switch request.Operation {
	case OperationRoute:
		route := p.route
		if route == "" {
			route = "Routine"
		}
		return ProviderResult{
			Status: "passed", Summary: "routed", NextAction: "implement",
			ChangedFiles: []string{}, Checks: []CheckResult{},
			TLCEnvelope: []byte(fmt.Sprintf(`{
  "schema_version":"tlc-envelope/v1",
  "workflow":{"name":"transpara-tlc","version":"0.1.2"},
  "route":%q,
  "brief":{"outcome":"Produce a useful change","scope":["README.md"],"non_goals":[],"assumptions":[],"constraints":[],"tests":["go test ./..."],"next_action":"Implement"},
  "route_owned_data":{"preserved":true}
}`, route)),
		}, nil
	case OperationImplement:
		if p.blockImplementationOnce {
			p.blockImplementationOnce = false
			return ProviderResult{Status: "blocked", Summary: "need input", Blocker: "Which compatibility mode should be used?", NextAction: "Ask the Human.", ChangedFiles: []string{}, Checks: []CheckResult{}}, nil
		}
		return ProviderResult{
			Status: "passed", Summary: "implemented", NextAction: "review",
			ChangedFiles: []string{"README.md"},
			Checks:       []CheckResult{{Name: "go test ./...", Status: "passed", Summary: "all packages passed"}},
		}, nil
	case OperationReview:
		if p.blockReviewOnce {
			p.blockReviewOnce = false
			return ProviderResult{Status: "blocked", Summary: "review found a gap", Blocker: "Choose the safe compatibility behavior.", NextAction: "Ask the Human.", ChangedFiles: []string{}, Checks: []CheckResult{}}, nil
		}
		return ProviderResult{
			Status: "passed", Summary: "review passed", NextAction: "publish",
			ChangedFiles: []string{}, Checks: []CheckResult{},
			Review: &ReviewResult{Status: "passed", Summary: "no findings", Findings: []string{}},
		}, nil
	default:
		return ProviderResult{}, fmt.Errorf("unexpected operation %q", request.Operation)
	}
}

type fakeEffects struct {
	mu                        sync.Mutex
	root                      string
	mergeHeads                []string
	publishByID               map[string]int
	checksState               string
	merged                    bool
	implementationMissingOnce bool
}

func (e *fakeEffects) RepositoryRoot(_ context.Context, _ string) (string, error) {
	return e.root, nil
}

func (e *fakeEffects) Prepare(_ context.Context, workID string, bound tlcbridge.BoundRequest) (Workspace, error) {
	return Workspace{Root: e.root, Repository: bound.Source.Repository, Branch: "civilization/" + workID, BaseSHA: "base"}, nil
}

func (e *fakeEffects) CaptureImplementation(_ context.Context, _ string, _ tlcbridge.BoundRequest, _ Workspace, _ ProviderResult) (string, error) {
	return strings.Repeat("d", 64), nil
}

func (e *fakeEffects) ImplementationMatches(_ context.Context, _ string, _ tlcbridge.BoundRequest, _ Workspace, _ ProviderResult, expectedDigest string) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.implementationMissingOnce {
		e.implementationMissingOnce = false
		return false, nil
	}
	return expectedDigest == strings.Repeat("d", 64), nil
}

func (e *fakeEffects) Publish(_ context.Context, workID string, bound tlcbridge.BoundRequest, _ Workspace, implementation ProviderResult, implementationDigest string, _ ProviderResult) (PullRequest, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if implementationDigest != strings.Repeat("d", 64) {
		return PullRequest{}, fmt.Errorf("unexpected implementation digest %q", implementationDigest)
	}
	if e.publishByID == nil {
		e.publishByID = map[string]int{}
	}
	e.publishByID[workID]++
	checksState := e.checksState
	if checksState == "" {
		checksState = "passed"
	}
	head := "0123456789abcdef0123456789abcdef01234567"
	return PullRequest{
		Repository: bound.Source.Repository, Number: len(e.publishByID), URL: "https://github.com/" + bound.Source.Repository + "/pull/1",
		HeadSHA: head, ReviewedHeadSHA: head, ValidatedHeadSHA: head,
		Open: !e.merged, Merged: e.merged, Draft: false, ChecksPassing: checksState == "passed", ChecksState: checksState,
		ChangedFiles: append([]string(nil), implementation.ChangedFiles...), ChangedFilesComplete: true, CreatedByCivilization: true,
	}, nil
}

func (e *fakeEffects) ObservePullRequest(_ context.Context, pullRequest PullRequest) (PullRequest, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.checksState != "" {
		pullRequest.ChecksState = e.checksState
		pullRequest.ChecksPassing = e.checksState == "passed"
	}
	if e.merged {
		pullRequest.Merged = true
		pullRequest.Open = false
	}
	return pullRequest, nil
}

func (e *fakeEffects) EnableAutoMerge(_ context.Context, _ PullRequest, expectedHeadSHA string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mergeHeads = append(e.mergeHeads, expectedHeadSHA)
	return nil
}

func newTestEngine(t *testing.T, route string, autoMerge bool) (*Engine, *fakeProvider, *fakeEffects) {
	t.Helper()
	root := t.TempDir()
	if out, err := exec.Command("git", "init", "--quiet", root).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	provider := &fakeProvider{route: route}
	effects := &fakeEffects{root: root}
	policy := AutoMergePolicy{}
	if autoMerge {
		policy = AutoMergePolicy{
			Enabled: true, AuthorityRef: "human:test", ProtectedPaths: DefaultProtectedPaths(),
			Repositories: map[string]struct{}{"transpara-ai/hive": {}},
		}
	}
	engine, err := NewEngine(EngineConfig{
		Store: NewInMemoryStore(nil), Provider: provider, Effects: effects, AutoMergePolicy: policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	return engine, provider, effects
}

func TestEngineNaturalLanguageToHumanReady(t *testing.T) {
	engine, provider, effects := newTestEngine(t, "Routine", false)
	source := tlcbridge.Source{Kind: tlcbridge.SourceHuman, Identity: "idea:1", Repository: "transpara-ai/hive"}
	queued, err := engine.SubmitText(context.Background(), source, "Improve the operator summary.")
	if err != nil {
		t.Fatal(err)
	}
	if queued.State != StateQueued || queued.Bound == nil || queued.Bound.Envelope.Workflow.Version != "0.1.2" {
		t.Fatalf("queued = %+v", queued)
	}
	ready, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if ready.State != StateReady || ready.PullRequest == nil || ready.MergeDecision == nil || ready.MergeDecision.Eligible {
		t.Fatalf("ready = %+v", ready)
	}
	if provider.runs[OperationRoute] != 1 || provider.runs[OperationImplement] != 1 || provider.runs[OperationReview] != 1 {
		t.Fatalf("provider runs = %+v", provider.runs)
	}
	reviewEvidence := provider.prompts[OperationReview][0]
	for _, evidence := range []string{`"changed_files":["README.md"]`, `"name":"go test ./...","status":"passed","summary":"all packages passed"`} {
		if !strings.Contains(reviewEvidence, evidence) {
			t.Fatalf("review did not receive the recorded implementation evidence %q", evidence)
		}
	}
	if len(effects.mergeHeads) != 0 {
		t.Fatalf("unexpected merge effects: %v", effects.mergeHeads)
	}

	// Restarting the engine over the same event store preserves the result and
	// repeating source intake performs no provider call.
	restarted, err := NewEngine(EngineConfig{Store: engine.store, Provider: provider, Effects: effects})
	if err != nil {
		t.Fatal(err)
	}
	again, err := restarted.SubmitText(context.Background(), source, "Improve the operator summary.")
	if err != nil {
		t.Fatal(err)
	}
	if again.State != StateReady || provider.runs[OperationRoute] != 1 {
		t.Fatalf("restart replay = %+v, runs = %+v", again, provider.runs)
	}
}

func TestEngineQueuesEligibleRoutineAutoMerge(t *testing.T) {
	engine, _, effects := newTestEngine(t, "Routine", true)
	queued, err := engine.SubmitText(context.Background(), tlcbridge.Source{
		Kind: tlcbridge.SourceIssue, Identity: "issue:2", Repository: "transpara-ai/hive",
	}, "Repair the README typo.")
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != StateMergeQueued || len(effects.mergeHeads) != 1 {
		t.Fatalf("result = %+v, merge heads = %v", result, effects.mergeHeads)
	}
}

func TestEngineWaitsForPendingChecksThenQueuesAndObservesMerge(t *testing.T) {
	engine, _, effects := newTestEngine(t, "Routine", true)
	effects.checksState = "pending"
	queued, err := engine.SubmitText(context.Background(), tlcbridge.Source{
		Kind: tlcbridge.SourceIssue, Identity: "issue:pending-checks", Repository: "transpara-ai/hive",
	}, "Repair a bounded typo.")
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if waiting.State != StatePublishing || len(waiting.Interventions) != 0 || len(effects.mergeHeads) != 0 {
		t.Fatalf("waiting = %+v, merge heads = %v", waiting, effects.mergeHeads)
	}

	effects.checksState = "passed"
	mergeQueued, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if mergeQueued.State != StateMergeQueued || len(effects.mergeHeads) != 1 {
		t.Fatalf("merge queued = %+v, merge heads = %v", mergeQueued, effects.mergeHeads)
	}

	effects.merged = true
	completed, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != StateCompleted || completed.PullRequest == nil || !completed.PullRequest.Merged {
		t.Fatalf("completed = %+v", completed)
	}
}

func TestEngineReimplementsWhenRecoveredWorktreeLostRecordedDiff(t *testing.T) {
	engine, provider, effects := newTestEngine(t, "Designed", false)
	effects.checksState = "pending"
	queued, err := engine.SubmitText(context.Background(), tlcbridge.Source{
		Kind: tlcbridge.SourceHuman, Identity: "idea:recovery", Repository: "transpara-ai/hive",
	}, "Make a recoverable change.")
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil || waiting.State != StatePublishing {
		t.Fatalf("waiting = %+v, err = %v", waiting, err)
	}
	effects.implementationMissingOnce = true
	effects.checksState = "passed"
	recovered, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil || recovered.State != StateReady {
		t.Fatalf("recovered = %+v, err = %v", recovered, err)
	}
	if provider.runs[OperationImplement] != 2 || provider.runs[OperationReview] != 2 {
		t.Fatalf("provider runs after recovery = %+v", provider.runs)
	}
}

func TestEngineRestartReusesInFlightProviderAttempt(t *testing.T) {
	engine, _, effects := newTestEngine(t, "Routine", false)
	provider := &crashOnceProvider{
		delegate:       &fakeProvider{route: "Routine"},
		crashImplement: true,
	}
	engine.provider = provider
	source := tlcbridge.Source{
		Kind: tlcbridge.SourceHuman, Identity: "idea:in-flight-restart", Repository: "transpara-ai/hive",
	}
	queued, err := engine.SubmitText(context.Background(), source, "Make a restart-safe change.")
	if err != nil {
		t.Fatal(err)
	}

	var processLoss any
	func() {
		defer func() { processLoss = recover() }()
		_, _ = engine.Run(context.Background(), queued.WorkID)
	}()
	if processLoss == nil {
		t.Fatal("provider attempt did not simulate process loss")
	}
	afterLoss, err := engine.mustFind(context.Background(), queued.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if afterLoss.State != StateImplementing || len(afterLoss.ProviderRuns) != 1 {
		t.Fatalf("after process loss = %+v", afterLoss)
	}

	restarted, err := NewEngine(EngineConfig{Store: engine.store, Provider: provider, Effects: effects})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := restarted.Run(context.Background(), queued.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if ready.State != StateReady {
		t.Fatalf("restarted result = %+v", ready)
	}
	provider.mu.Lock()
	attempts := append([]string(nil), provider.attempts...)
	provider.mu.Unlock()
	if len(attempts) != 2 || attempts[0] != attempts[1] {
		t.Fatalf("implementation attempts = %v, want one exact attempt reused", attempts)
	}
	events, err := engine.store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assignments := 0
	for _, event := range events {
		if event.WorkID == queued.WorkID && event.IdempotencyKey == "worker:"+attempts[0] {
			assignments++
		}
	}
	if assignments != 1 {
		t.Fatalf("durable assignment count = %d, want 1", assignments)
	}
}

func TestEngineBlocksOnFailedRequiredCheck(t *testing.T) {
	engine, _, effects := newTestEngine(t, "Routine", true)
	effects.checksState = "failed"
	queued, err := engine.SubmitText(context.Background(), tlcbridge.Source{
		Kind: tlcbridge.SourceIssue, Identity: "issue:failed-check", Repository: "transpara-ai/hive",
	}, "Repair a bounded typo.")
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.State != StateBlocked || len(blocked.Interventions) != 1 || len(effects.mergeHeads) != 0 {
		t.Fatalf("blocked = %+v, merge heads = %v", blocked, effects.mergeHeads)
	}
}

func TestEngineNeverAutoMergesDesignedWork(t *testing.T) {
	engine, _, effects := newTestEngine(t, "Designed", true)
	queued, err := engine.SubmitText(context.Background(), tlcbridge.Source{
		Kind: tlcbridge.SourceIssue, Identity: "issue:3", Repository: "transpara-ai/hive",
	}, "Choose an API shape.")
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != StateReady || result.MergeDecision == nil || result.MergeDecision.Eligible || len(effects.mergeHeads) != 0 {
		t.Fatalf("result = %+v, merge heads = %v", result, effects.mergeHeads)
	}
}

func TestEngineNeverAutoMergesCriticalFactoryOrder(t *testing.T) {
	engine, _, effects := newTestEngine(t, "Critical", true)
	queued, err := engine.SubmitText(context.Background(), tlcbridge.Source{
		Kind: tlcbridge.SourceOrder, Identity: "factory-order:production", Repository: "transpara-ai/hive",
	}, "Execute the approved production Factory Order.")
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != StateReady || result.MergeDecision == nil || result.MergeDecision.Eligible || len(effects.mergeHeads) != 0 {
		t.Fatalf("result = %+v, merge heads = %v", result, effects.mergeHeads)
	}
}

func TestEngineFeedsHumanResolutionBackIntoImplementation(t *testing.T) {
	engine, provider, _ := newTestEngine(t, "Designed", false)
	provider.blockImplementationOnce = true
	queued, err := engine.SubmitText(context.Background(), tlcbridge.Source{
		Kind: tlcbridge.SourceHuman, Identity: "idea:clarified", Repository: "transpara-ai/hive",
	}, "Add a compatibility option.")
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.State != StateBlocked || len(blocked.Interventions) != 1 {
		t.Fatalf("blocked = %+v", blocked)
	}
	resolved, err := engine.ResolveIntervention(context.Background(), queued.WorkID, blocked.Interventions[0].ID, "Use strict mode and reject legacy input.")
	if err != nil || resolved.State != StateImplementing {
		t.Fatalf("resolved = %+v, err = %v", resolved, err)
	}
	ready, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil || ready.State != StateReady {
		t.Fatalf("ready = %+v, err = %v", ready, err)
	}
	if len(provider.prompts[OperationImplement]) != 2 || !strings.Contains(provider.prompts[OperationImplement][1], "Use strict mode and reject legacy input.") {
		t.Fatalf("implementation prompts = %#v", provider.prompts[OperationImplement])
	}
}

func TestEngineReviewQuestionReturnsToImplementation(t *testing.T) {
	engine, provider, _ := newTestEngine(t, "Designed", false)
	provider.blockReviewOnce = true
	queued, err := engine.SubmitText(context.Background(), tlcbridge.Source{
		Kind: tlcbridge.SourceHuman, Identity: "idea:review-remediation", Repository: "transpara-ai/hive",
	}, "Add a bounded behavior.")
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.State != StateBlocked || blocked.ResumeState != StateImplementing || len(blocked.Interventions) != 1 {
		t.Fatalf("blocked = %+v", blocked)
	}
	if _, err := engine.ResolveIntervention(context.Background(), queued.WorkID, blocked.Interventions[0].ID, "Preserve the current API and fail closed."); err != nil {
		t.Fatal(err)
	}
	ready, err := engine.Run(context.Background(), queued.WorkID)
	if err != nil || ready.State != StateReady {
		t.Fatalf("ready = %+v, err = %v", ready, err)
	}
	if provider.runs[OperationImplement] != 2 || provider.runs[OperationReview] != 2 {
		t.Fatalf("provider runs = %+v", provider.runs)
	}
	secondImplementation := provider.prompts[OperationImplement][1]
	if !strings.Contains(secondImplementation, "Preserve the current API and fail closed.") || !strings.Contains(secondImplementation, "review found a gap") {
		t.Fatalf("second implementation prompt = %q", secondImplementation)
	}
}

func TestEngineRunsConcurrentIndependentWork(t *testing.T) {
	engine, _, effects := newTestEngine(t, "Routine", false)
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			source := tlcbridge.Source{Kind: tlcbridge.SourceIssue, Identity: fmt.Sprintf("issue:%d", index+10), Repository: "transpara-ai/hive"}
			queued, err := engine.SubmitText(context.Background(), source, fmt.Sprintf("Bounded request %d", index))
			if err == nil {
				_, err = engine.Run(context.Background(), queued.WorkID)
			}
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	items, err := engine.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("work items = %d", len(items))
	}
	for _, item := range items {
		if item.State != StateReady {
			t.Fatalf("item %s state = %s", item.WorkID, item.State)
		}
	}
	effects.mu.Lock()
	defer effects.mu.Unlock()
	if len(effects.publishByID) != 3 {
		t.Fatalf("published = %+v", effects.publishByID)
	}
}

func TestProjectWorkUsesCausalityWhenTimestampsAndInputOrderDisagree(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	intake := Event{
		ID: "event-z", Type: EventIntakeAccepted, WorkID: "work-order", IdempotencyKey: "intake",
		OccurredAt: now.Add(time.Minute), Payload: mustJSON(t, Intake{Source: tlcbridge.Source{Kind: tlcbridge.SourceHuman, Identity: "human:order", Repository: "transpara-ai/hive"}, Text: "Ship it"}),
	}
	state := Event{
		ID: "event-a", Type: EventStateChanged, WorkID: "work-order", IdempotencyKey: "state",
		Causes: []string{intake.ID}, OccurredAt: now, Payload: mustJSON(t, StateChange{To: StateRouting, Summary: "Routing", NextAction: "Wait"}),
	}
	projection, err := projectWork("work-order", []Event{state, intake})
	if err != nil {
		t.Fatal(err)
	}
	if projection.IntakeText != "Ship it" || projection.State != StateRouting || projection.LatestEventID != state.ID {
		t.Fatalf("causal projection = %#v", projection)
	}
}

func TestProjectWorkRejectsCausalCycle(t *testing.T) {
	_, err := projectWork("work-cycle", []Event{
		{ID: "event-a", Type: EventStateChanged, WorkID: "work-cycle", Causes: []string{"event-b"}, Payload: mustJSON(t, StateChange{To: StateQueued})},
		{ID: "event-b", Type: EventStateChanged, WorkID: "work-cycle", Causes: []string{"event-a"}, Payload: mustJSON(t, StateChange{To: StateReady})},
	})
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cycle error = %v", err)
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
