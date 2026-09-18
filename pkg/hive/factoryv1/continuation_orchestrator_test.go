package factoryv1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDurableInvocationDispatchClaimIsAtomicAndRestartReadable(t *testing.T) {
	t.Parallel()
	invocation, _ := continuationInvocationFixture(t)
	intent, err := NewWorkflowInvocationIntent(invocation.SourceChain.HeadDigest)
	if err != nil {
		t.Fatal(err)
	}
	events := NewInMemoryStore(testClock())
	first, err := NewDurableInvocationIntentStore(events)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.CreateInvocationIntent(context.Background(), intent); err != nil {
		t.Fatal(err)
	}

	const contenders = 16
	results := make(chan error, contenders)
	var wait sync.WaitGroup
	for index := 0; index < contenders; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			store, createErr := NewDurableInvocationIntentStore(events)
			if createErr != nil {
				results <- createErr
				return
			}
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
		t.Fatalf("durable dispatch winners=%d, want 1", winners)
	}
	restarted, err := NewDurableInvocationIntentStore(events)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := restarted.GetInvocationIntent(context.Background(), intent.IntentID)
	if err != nil {
		t.Fatal(err)
	}
	if observed.State != "dispatch_claimed" {
		t.Fatalf("restart state=%s, want dispatch_claimed", observed.State)
	}
}

func TestRunContinuationWorkflowPersistsAndReusesExactReport(t *testing.T) {
	t.Parallel()
	invocation, body := continuationInvocationFixture(t)
	runner := &fakeTLCChangeWorkflowRunner{identity: continuationIdentity(), response: continuationReportFixture(t, invocation)}
	events := NewInMemoryStore(testClock())
	work := NewInMemoryContinuationWorkStore()
	when := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	first, err := RunContinuationWorkflow(
		context.Background(), events, work, runner, continuationIdentity(), body,
		continuationHuman(), "hive", when,
	)
	if err != nil {
		t.Fatal(err)
	}
	if runner.calls != 1 || first.Reused || first.ReportRecordID == "" {
		t.Fatalf("first run=%+v calls=%d", first, runner.calls)
	}
	second, err := RunContinuationWorkflow(
		context.Background(), events, work, runner, continuationIdentity(), body,
		continuationHuman(), "hive", when,
	)
	if err != nil {
		t.Fatal(err)
	}
	if runner.calls != 1 || !second.Reused || second.ReportRecordID != first.ReportRecordID || string(second.ExactReportJSON) != string(first.ExactReportJSON) {
		t.Fatalf("exact report was not reused: first=%+v second=%+v calls=%d", first, second, runner.calls)
	}

	listed, err := events.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	recordEvents := map[string]Event{}
	for _, event := range listed {
		if event.Type != EventContinuationRecorded {
			continue
		}
		var record ContinuationRecord
		if err := json.Unmarshal(event.Payload, &record); err != nil {
			t.Fatal(err)
		}
		recordEvents[record.RecordID] = event
	}
	invocationEvent := recordEvents[first.InvocationRecordID]
	reportEvent := recordEvents[first.ReportRecordID]
	if len(invocationEvent.Causes) != 1 || invocationEvent.Causes[0] != recordEvents[first.SourceRecordID].ID {
		t.Fatalf("invocation causes=%v", invocationEvent.Causes)
	}
	if len(reportEvent.Causes) != 1 || reportEvent.Causes[0] != invocationEvent.ID {
		t.Fatalf("report causes=%v, want %s", reportEvent.Causes, invocationEvent.ID)
	}
}

func TestRunContinuationWorkflowCrashConsumesUnknownWithoutRedispatch(t *testing.T) {
	t.Parallel()
	_, body := continuationInvocationFixture(t)
	runner := &fakeTLCChangeWorkflowRunner{identity: continuationIdentity(), err: errors.New("runner connection lost")}
	events := NewInMemoryStore(testClock())
	work := NewInMemoryContinuationWorkStore()
	when := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	if _, err := RunContinuationWorkflow(context.Background(), events, work, runner, continuationIdentity(), body, continuationHuman(), "hive", when); err == nil {
		t.Fatal("ambiguous runner failure was accepted")
	}
	if runner.calls != 1 {
		t.Fatalf("runner calls=%d, want 1", runner.calls)
	}
	if _, err := RunContinuationWorkflow(context.Background(), events, work, runner, continuationIdentity(), body, continuationHuman(), "hive", when); err == nil || !strings.Contains(err.Error(), "consumed_unknown") {
		t.Fatalf("restart did not consume unknown launch: %v", err)
	}
	if runner.calls != 1 {
		t.Fatalf("restart redispatched ambiguous runner: calls=%d", runner.calls)
	}
	if _, err := RunContinuationWorkflow(context.Background(), events, work, runner, continuationIdentity(), body, continuationHuman(), "hive", when); err == nil || !strings.Contains(err.Error(), "consumed_unknown") {
		t.Fatalf("terminal consumed_unknown state was not retained: %v", err)
	}
	if runner.calls != 1 {
		t.Fatalf("terminal recovery redispatched runner: calls=%d", runner.calls)
	}
}

func TestExecuteContinuationEffectGuardsAffinityAndObserveBeforeRetry(t *testing.T) {
	t.Parallel()
	invocation, _ := continuationInvocationFixture(t)
	repository := continuationRepository()
	var report ContinuationReport
	if err := json.Unmarshal(continuationReportFixture(t, invocation), &report); err != nil {
		t.Fatal(err)
	}
	report.RepositoryAffinity = RepositoryAffinity{Status: "match", AffinityRepository: repository, Mismatches: []string{}}
	report.ContinuationFrontier = []FrontierPoint{{
		CandidateID: "repo-x-c3", SliceID: "repo-x", SubjectDigest: strings.Repeat("a", 64),
		NextSafeAction: "create_worktree", ContinuationPermitted: true,
	}}
	report.ReportSHA256 = ""
	digest, err := CanonicalSHA256(report)
	if err != nil {
		t.Fatal(err)
	}
	report.ReportSHA256 = digest
	calls := 0
	decision, err := ExecuteContinuationEffect(context.Background(), report, repository, "repo-x-c3", "create_worktree", EffectAbsent, true, true, func(context.Context) (EffectObservationState, error) {
		calls++
		return EffectExact, nil
	})
	if err != nil || !decision.ReuseExact || calls != 1 {
		t.Fatalf("admitted effect=%+v calls=%d err=%v", decision, calls, err)
	}
	wrong := repository
	wrong.NumericID++
	if _, err := ExecuteContinuationEffect(context.Background(), report, wrong, "repo-x-c3", "create_worktree", EffectAbsent, true, true, func(context.Context) (EffectObservationState, error) {
		calls++
		return EffectExact, nil
	}); err == nil {
		t.Fatal("RepoY effect passed RepoX continuation guard")
	}
	if calls != 1 {
		t.Fatal("repository mismatch reached effect callback")
	}
	decision, err = ExecuteContinuationEffect(context.Background(), report, repository, "repo-x-c3", "create_worktree", EffectUnknown, true, true, func(context.Context) (EffectObservationState, error) {
		calls++
		return EffectExact, nil
	})
	if err != nil || !decision.Blocked || calls != 1 {
		t.Fatalf("unknown effect was retried: decision=%+v calls=%d err=%v", decision, calls, err)
	}
}

func TestCommandTLCChangeWorkflowRunnerAuthenticatesBoundPluginBytes(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".codex-plugin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, filepath.Dir(tlcSchemaRelativePath)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, filepath.Dir(tlcSkillRelativePath)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, filepath.Dir(tlcCoreRelativePath)), 0o755); err != nil {
		t.Fatal(err)
	}
	schema := []byte("{\"title\":\"schema\"}\n")
	skill := []byte("---\nname: tlc-change-workflow\n---\n")
	core := []byte("#!/usr/bin/env python3\n")
	writeTestFile(t, filepath.Join(root, tlcSchemaRelativePath), schema, 0o644)
	writeTestFile(t, filepath.Join(root, tlcSkillRelativePath), skill, 0o644)
	writeTestFile(t, filepath.Join(root, tlcCoreRelativePath), core, 0o644)
	writeTestFile(t, filepath.Join(root, ".codex-plugin", "plugin.json"), []byte(`{"name":"transpara-governance-gates","version":"1.1.0"}`), 0o644)
	binding := map[string]any{
		"adapter": map[string]any{"name": TLCGovernancePluginName, "version": "1.1.0"},
		"payload": map[string]any{"files": []any{
			map[string]any{"path": tlcSchemaRelativePath, "sha256": testSHA256(schema)},
			map[string]any{"path": tlcSkillRelativePath, "sha256": testSHA256(skill)},
			map[string]any{"path": tlcCoreRelativePath, "sha256": testSHA256(core)},
		}},
	}
	bindingJSON, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(root, "adapter-binding.json"), bindingJSON, 0o644)
	runnerScript := filepath.Join(root, "runner.sh")
	writeTestFile(t, runnerScript, []byte("#!/bin/sh\ncat\n"), 0o755)
	runner, err := NewCommandTLCChangeWorkflowRunner(root, runnerScript, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := runner.ObservedIdentity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	argvJSON, err := json.Marshal([]string{runnerScript})
	if err != nil {
		t.Fatal(err)
	}
	if identity.SchemaSHA256 != testSHA256(schema) || identity.SkillSHA256 != testSHA256(skill) || identity.ContractCoreSHA256 != testSHA256(core) ||
		identity.RunnerSHA256 != testSHA256([]byte("#!/bin/sh\ncat\n")) || identity.RunnerArgvSHA256 != testSHA256(argvJSON) {
		t.Fatalf("observed identity=%+v", identity)
	}
	input := []byte(`{"exact":true}`)
	output, err := runner.Evaluate(context.Background(), input)
	if err != nil || string(output) != string(input) {
		t.Fatalf("runner output=%s err=%v", output, err)
	}
	writeTestFile(t, filepath.Join(root, tlcSchemaRelativePath), []byte("{}\n"), 0o644)
	if _, err := runner.ObservedIdentity(context.Background()); err == nil {
		t.Fatal("tampered installed schema passed binding authentication")
	}
	writeTestFile(t, filepath.Join(root, tlcSchemaRelativePath), schema, 0o644)
	writeTestFile(t, filepath.Join(root, tlcCoreRelativePath), []byte("# tampered\n"), 0o644)
	if _, err := runner.ObservedIdentity(context.Background()); err == nil {
		t.Fatal("tampered installed conformance core passed binding authentication")
	}
	writeTestFile(t, filepath.Join(root, tlcCoreRelativePath), core, 0o644)
	writeTestFile(t, runnerScript, []byte("#!/bin/sh\nexit 1\n"), 0o755)
	changed, err := runner.ObservedIdentity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if changed.RunnerSHA256 == identity.RunnerSHA256 {
		t.Fatal("changed workflow runner retained its authenticated identity")
	}
}

func writeTestFile(t *testing.T, path string, content []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, content, mode); err != nil {
		t.Fatal(err)
	}
}

func testSHA256(content []byte) string {
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:])
}
