package factoryv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// WorkflowRunResult identifies the exact durable source, invocation, and
// report records produced or reused by one Civilization call.
type WorkflowRunResult struct {
	Report             ContinuationReport `json:"report"`
	ExactReportJSON    json.RawMessage    `json:"exact_report_json"`
	SourceRecordID     string             `json:"source_record_id"`
	InvocationRecordID string             `json:"invocation_record_id"`
	ReportRecordID     string             `json:"report_record_id"`
	Reused             bool               `json:"reused"`
}

// RunContinuationWorkflow is the bounded production intake-to-report path.
// EventGraph truth and its Work twin are committed before the external runner
// launches. A durable compare-and-swap is the sole launch claim. Recovery
// reuses an exact persisted report; an unresolved claim becomes
// consumed_unknown and is never blindly dispatched again.
func RunContinuationWorkflow(
	ctx context.Context,
	store Store,
	work ContinuationWorkStore,
	runner TLCChangeWorkflowRunner,
	expected TLCWorkflowIdentity,
	invocationJSON []byte,
	principal Principal,
	capturedBy string,
	observedAt time.Time,
) (WorkflowRunResult, error) {
	if store == nil || work == nil {
		return WorkflowRunResult{}, errors.New("TLC continuation workflow requires EventGraph and Work stores")
	}
	invocation, err := DecodeContinuationInvocation(invocationJSON)
	if err != nil {
		return WorkflowRunResult{}, err
	}
	sourceHead := invocation.SourceChain.HeadDigest
	sourceRecord, err := NewContinuationRecord(
		ContinuationRecordSource, invocation.SourceChain.ChainID, sourceHead, sourceHead,
		nil, principal, capturedBy, observedAt, json.RawMessage(invocationJSON),
	)
	if err != nil {
		return WorkflowRunResult{}, err
	}
	if _, complete, err := SeedContinuationSource(ctx, store, work, invocation.TargetRepository, sourceRecord); err != nil || !complete {
		if err == nil {
			err = errors.New("source Work projection is incomplete")
		}
		return WorkflowRunResult{}, fmt.Errorf("persist TLC continuation source: %w", err)
	}

	invocationPayload := struct {
		ContractVersion  string              `json:"contract_version"`
		ExpectedIdentity TLCWorkflowIdentity `json:"expected_identity"`
		InvocationSHA256 string              `json:"invocation_sha256"`
	}{TLCContinuationVersion, expected, HashText(string(invocationJSON))}
	invocationRecord, err := NewContinuationRecord(
		ContinuationRecordTLCInvocation, invocation.SourceChain.ChainID, sourceHead,
		invocationPayload.InvocationSHA256, []string{sourceRecord.RecordID}, principal,
		capturedBy, observedAt, invocationPayload,
	)
	if err != nil {
		return WorkflowRunResult{}, err
	}
	if _, complete, err := PersistContinuationRecord(ctx, store, work, invocationRecord); err != nil || !complete {
		if err == nil {
			err = errors.New("invocation Work projection is incomplete")
		}
		return WorkflowRunResult{}, fmt.Errorf("persist TLC invocation: %w", err)
	}

	intentStore, err := NewDurableInvocationIntentStore(store)
	if err != nil {
		return WorkflowRunResult{}, err
	}
	intent, err := NewWorkflowInvocationIntent(sourceHead)
	if err != nil {
		return WorkflowRunResult{}, err
	}
	if err := intentStore.CreateInvocationIntent(ctx, intent); err != nil {
		return WorkflowRunResult{}, fmt.Errorf("persist TLC workflow launch intent: %w", err)
	}
	current, err := intentStore.GetInvocationIntent(ctx, intent.IntentID)
	if err != nil {
		return WorkflowRunResult{}, err
	}

	result := WorkflowRunResult{SourceRecordID: sourceRecord.RecordID, InvocationRecordID: invocationRecord.RecordID}
	persisted, found, err := findPersistedContinuationReport(ctx, store, work, invocationRecord)
	if err != nil {
		return WorkflowRunResult{}, err
	}
	if found {
		if current.State == "dispatch_claimed" {
			current, err = CompleteInvocation(ctx, intentStore, current, HashText(string(persisted.ExactReportJSON)))
			if err != nil {
				return WorkflowRunResult{}, fmt.Errorf("complete recovered TLC workflow invocation: %w", err)
			}
		}
		if current.State != "completed" || current.ResultSHA256 != HashText(string(persisted.ExactReportJSON)) {
			return WorkflowRunResult{}, errors.New("persisted TLC report conflicts with durable invocation state")
		}
		persisted.SourceRecordID = sourceRecord.RecordID
		persisted.InvocationRecordID = invocationRecord.RecordID
		persisted.Reused = true
		return persisted, nil
	}

	switch current.State {
	case "dispatch_claimed":
		if _, err := RecoverClaimedInvocation(ctx, intentStore, current); err != nil {
			return WorkflowRunResult{}, fmt.Errorf("consume unresolved TLC workflow invocation: %w", err)
		}
		return WorkflowRunResult{}, errors.New("prior TLC workflow launch has no exact persisted result; consumed_unknown blocks retry")
	case "completed":
		return WorkflowRunResult{}, errors.New("completed TLC workflow invocation has no exact persisted result")
	case "consumed_unknown":
		return WorkflowRunResult{}, errors.New("TLC workflow invocation is consumed_unknown and cannot retry")
	case "not_started":
		current, err = ClaimInvocationDispatch(ctx, intentStore, current)
		if err != nil {
			return WorkflowRunResult{}, fmt.Errorf("claim TLC workflow launch: %w", err)
		}
	default:
		return WorkflowRunResult{}, errors.New("unsupported TLC workflow invocation state")
	}

	report, exactReport, err := InvokeTLCChangeWorkflowExact(ctx, runner, expected, invocationJSON)
	if err != nil {
		// Leave dispatch_claimed durable. Restart recovery consumes it rather
		// than guessing whether the external runner produced an effect.
		return WorkflowRunResult{}, err
	}
	reportRecord, err := NewContinuationRecord(
		ContinuationRecordTLCReport, invocation.SourceChain.ChainID, sourceHead,
		report.ReportSHA256, []string{invocationRecord.RecordID}, principal,
		capturedBy, observedAt, json.RawMessage(exactReport),
	)
	if err != nil {
		return WorkflowRunResult{}, err
	}
	if _, complete, err := PersistContinuationRecord(ctx, store, work, reportRecord); err != nil || !complete {
		if err == nil {
			err = errors.New("report Work projection is incomplete")
		}
		return WorkflowRunResult{}, fmt.Errorf("persist TLC report: %w", err)
	}
	if _, err := CompleteInvocation(ctx, intentStore, current, HashText(string(exactReport))); err != nil {
		return WorkflowRunResult{}, fmt.Errorf("complete TLC workflow invocation: %w", err)
	}
	result.Report = report
	result.ExactReportJSON = append(json.RawMessage(nil), exactReport...)
	result.ReportRecordID = reportRecord.RecordID
	return result, nil
}

func findPersistedContinuationReport(ctx context.Context, store Store, work ContinuationWorkStore, invocationRecord ContinuationRecord) (WorkflowRunResult, bool, error) {
	events, err := store.List(ctx)
	if err != nil {
		return WorkflowRunResult{}, false, err
	}
	var found *WorkflowRunResult
	for _, event := range events {
		if event.Type != EventContinuationRecorded {
			continue
		}
		var record ContinuationRecord
		if err := decodeStrictJSON(event.Payload, &record); err != nil {
			return WorkflowRunResult{}, false, err
		}
		if record.Kind != ContinuationRecordTLCReport || record.ChainID != invocationRecord.ChainID ||
			record.SourceChainHead != invocationRecord.SourceChainHead || !containsString(record.CausalRecordIDs, invocationRecord.RecordID) {
			continue
		}
		if found != nil {
			return WorkflowRunResult{}, false, errors.New("multiple TLC reports claim one invocation")
		}
		if _, err := work.GetContinuationArtifact(ctx, record.ChainID, record.RecordID); err != nil {
			return WorkflowRunResult{}, false, fmt.Errorf("TLC report lacks exact Work twin: %w", err)
		}
		report, err := DecodeContinuationReport(record.Payload, record.SourceChainHead)
		if err != nil {
			return WorkflowRunResult{}, false, err
		}
		value := WorkflowRunResult{
			Report: report, ExactReportJSON: append(json.RawMessage(nil), record.Payload...),
			ReportRecordID: record.RecordID,
		}
		found = &value
	}
	if found == nil {
		return WorkflowRunResult{}, false, nil
	}
	return *found, true, nil
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

// ExecuteContinuationEffect is the only generic effect callback admitted by
// this adapter. It places RepoX/frontier and observe-before-retry checks in the
// call path immediately before the callback. Exact effects are reused; unknown
// or conflicting observations never call the mutator.
func ExecuteContinuationEffect(
	ctx context.Context,
	report ContinuationReport,
	expectedRepository RepositoryIdentity,
	candidateID, action string,
	observation EffectObservationState,
	authorityApplicable, budgetAvailable bool,
	effect func(context.Context) (EffectObservationState, error),
) (EffectDecision, error) {
	if err := RequireRepositoryContinuation(report, expectedRepository, candidateID, action); err != nil {
		return EffectDecision{Blocked: true, Reason: err.Error()}, err
	}
	decision := DecideEffectContinuation(observation, true, authorityApplicable, budgetAvailable)
	if decision.Blocked || decision.ReuseExact {
		return decision, nil
	}
	if !decision.RetrySame || effect == nil {
		return EffectDecision{Blocked: true, Reason: "continuation effect callback is unavailable"}, errors.New("continuation effect callback is unavailable")
	}
	observed, err := effect(ctx)
	if err != nil {
		return EffectDecision{Blocked: true, Reason: "continuation effect outcome is unknown"}, err
	}
	if observed != EffectExact {
		return EffectDecision{Blocked: true, Reason: "continuation effect did not produce an exact authenticated observation"}, errors.New("continuation effect lacks exact post-mutation observation")
	}
	return EffectDecision{ReuseExact: true, Reason: "exact effect observed after one admitted operation"}, nil
}

// PersistPartialAndExecuteRecovery guarantees that quarantined dirty-worktree
// evidence is durable in EventGraph and Work before a clean recovery effect is
// admitted. The callback must create and authenticate a new clean worktree; it
// must never continue in the partial directory.
func PersistPartialAndExecuteRecovery(
	ctx context.Context,
	store Store,
	work ContinuationWorkStore,
	partial PartialWorktreeEvidence,
	causalRecordIDs []string,
	principal Principal,
	capturedBy string,
	observedAt time.Time,
	report ContinuationReport,
	expectedRepository RepositoryIdentity,
	candidateID, action string,
	observation EffectObservationState,
	authorityApplicable, budgetAvailable bool,
	recoverClean func(context.Context) (EffectObservationState, error),
) (EffectDecision, error) {
	if err := ValidatePartialWorktreeEvidence(partial); err != nil {
		return EffectDecision{Blocked: true, Reason: err.Error()}, err
	}
	if len(causalRecordIDs) == 0 {
		return EffectDecision{Blocked: true, Reason: "partial evidence requires a causal attempt record"}, errors.New("partial evidence requires a causal attempt record")
	}
	record, err := NewContinuationRecord(
		ContinuationRecordPartialEvidence, partial.ChainID, partial.SourceChainHead,
		partial.PartialID, causalRecordIDs, principal, capturedBy, observedAt, partial,
	)
	if err != nil {
		return EffectDecision{Blocked: true, Reason: err.Error()}, err
	}
	if _, complete, err := PersistContinuationRecord(ctx, store, work, record); err != nil || !complete {
		if err == nil {
			err = errors.New("partial evidence Work projection is incomplete")
		}
		return EffectDecision{Blocked: true, Reason: "partial evidence persistence is incomplete"}, err
	}
	return ExecuteContinuationEffect(
		ctx, report, expectedRepository, candidateID, action, observation,
		authorityApplicable, budgetAvailable, recoverClean,
	)
}
