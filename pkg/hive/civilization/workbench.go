package civilization

import (
	"context"
	"errors"
	"fmt"

	"github.com/transpara-ai/hive/pkg/hive/tlcbridge"
)

// Confirm queues work only for the exact brief the operator inspected. It is
// not publication, merge, deployment, or authentication authority.
func (e *Engine) Confirm(ctx context.Context, workID, briefID string) (WorkProjection, error) {
	unlock := e.lockWork(workID)
	defer unlock()
	item, err := e.mustFind(ctx, workID)
	if err != nil {
		return item, err
	}
	if item.Bound == nil || item.Bound.IdempotencyKey != briefID {
		return item, fmt.Errorf("%w: brief changed; reload before confirming", ErrIdempotencyConflict)
	}
	if item.State != StateAwaitingConfirmation {
		// Repeated confirmation of this immutable brief is harmless.
		if item.RequireConfirmation && item.State != StateRouting {
			return item, nil
		}
		return item, errors.New("work is not awaiting confirmation")
	}
	if _, err := appendEvent(ctx, e.store, EventStateChanged, item.WorkID, "confirmation:"+briefID, []string{item.LatestEventID}, StateChange{ChangedBy: humanActor(ctx), From: item.State, To: StateQueued, Summary: "Implementation confirmed by the operator.", NextAction: "Hive will start the selected host."}); err != nil {
		return item, err
	}
	return e.mustFind(ctx, workID)
}

type Artifact struct {
	Verification    *RunnerEvidence `json:"verification,omitempty"`
	Repository      string          `json:"repository"`
	Branch          string          `json:"branch"`
	BaseSHA         string          `json:"base_sha"`
	WorkspaceDigest string          `json:"workspace_digest"`
	ChangedFiles    []string        `json:"changed_files"`
	Patch           string          `json:"patch"`
}

func (e *Engine) recordProviderFailure(ctx context.Context, workID string, operation ProviderOperation, attempt string, result ProviderResult, failure error) (WorkProjection, error) {
	if errors.Is(failure, ErrRunnerPending) || errors.Is(failure, context.Canceled) {
		item, _ := e.mustFind(ctx, workID)
		return item, failure
	}
	result.Status, result.Summary = "blocked", "The selected host could not complete this invocation."
	result.Blocker, result.NextAction = failure.Error(), "Repair the selected host or model, then record the repair and retry."
	result.ChangedFiles, result.Checks = []string{}, []CheckResult{}
	result.TLCEnvelope, result.Review = nil, nil
	recorded, err := e.recordProviderAttempt(ctx, workID, operation, attempt, result, "")
	if err != nil {
		return WorkProjection{}, err
	}
	return e.blockAfter(ctx, workID, recorded.ID, result.Blocker, result.NextAction)
}

type PreparedEffects interface {
	PublicationEnabled() bool
	PreparedArtifact(context.Context, string, tlcbridge.BoundRequest, Workspace, ProviderResult, string) (Artifact, error)
}

func (e *Engine) Artifact(ctx context.Context, workID string) (Artifact, error) {
	unlock := e.lockWork(workID)
	defer unlock()
	item, err := e.mustFind(ctx, workID)
	if err != nil {
		return Artifact{}, err
	}
	if (item.State != StatePrepared && item.State != StateApproved && item.State != StateRejected && item.State != StateChangesRequested) || item.Bound == nil {
		return Artifact{}, errors.New("artifact is not prepared; wait for independent verification")
	}
	if item.Artifact == nil {
		return Artifact{}, errors.New("prepared implementation evidence is unavailable")
	}
	return *item.Artifact, nil
}
