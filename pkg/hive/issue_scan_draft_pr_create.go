package hive

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/transpara-ai/eventgraph/go/pkg/types"
	"github.com/transpara-ai/hive/pkg/hive/factoryv1"
	"github.com/transpara-ai/work"
)

const (
	issueScanDraftPRCreationReservationArtifactLabel = "issue_scan_draft_pr_creation_reservation"
	issueScanDraftPRCreationReservationKind          = "issue_scan_draft_pr_creation_reservation"
)

var issueScanDraftPRCreationReservationMu sync.Mutex

// IssueScanDraftPRCreationResult summarizes creation of the draft PR that will
// later be marked ready and reviewed before Human merge approval. This result
// proves draft PR creation and ready-stage receipt recording only.
type IssueScanDraftPRCreationResult struct {
	RunID                    string
	FactoryOrderID           string
	Repository               string
	RequestID                string
	CreationReservationID    types.EventID
	WorkTaskID               types.EventID
	DraftPRReceipt           IssueScanDraftPRReceiptRecordResult
	PRNumber                 int
	PRURL                    string
	HeadSHA                  string
	Created                  bool
	NoReadyReviewMergeDeploy bool
}

type IssueScanDraftPRCreationReservation struct {
	Kind                     string `json:"kind"`
	LifecycleVersion         string `json:"lifecycle_version"`
	RunID                    string `json:"run_id"`
	FactoryOrderID           string `json:"factory_order_id"`
	ReadyStageTaskID         string `json:"ready_stage_task_id"`
	RequestID                string `json:"request_id"`
	Repository               string `json:"repository"`
	BaseRef                  string `json:"base_ref"`
	BaseSHA                  string `json:"base_sha"`
	HeadRef                  string `json:"head_ref"`
	HeadSHA                  string `json:"head_sha"`
	TitleHash                string `json:"title_hash"`
	BodyHash                 string `json:"body_hash"`
	PolicyBundleID           string `json:"policy_bundle_id"`
	PolicyBundleHash         string `json:"policy_bundle_hash"`
	AuthorityNonce           string `json:"authority_nonce"`
	Result                   string `json:"result"`
	ManualReconciliationOn   string `json:"manual_reconciliation_on"`
	NoReadyReviewMergeDeploy bool   `json:"no_ready_review_merge_deploy"`
}

// CreateIssueScanDraftPRFromApprovedContinuation is the continuation-only PR
// entrypoint. Existing legacy issue-scan callers retain their old contract;
// every caller using tlc-change-continuation/v1 must enter here so RepoX,
// frontier, authority, budget, and observe-before-retry checks execute
// immediately before the existing guarded draft-PR path.
func (r *Runtime) CreateIssueScanDraftPRFromApprovedContinuation(
	ctx context.Context,
	report factoryv1.ContinuationReport,
	expectedRepository factoryv1.RepositoryIdentity,
	candidateID string,
	observation factoryv1.EffectObservationState,
	authorityApplicable, budgetAvailable bool,
	runID, requestID string,
	client work.Epic11PullRequestCreator,
) (IssueScanDraftPRCreationResult, factoryv1.EffectDecision, error) {
	var result IssueScanDraftPRCreationResult
	decision, err := factoryv1.ExecuteContinuationEffect(
		ctx, report, expectedRepository, candidateID, "create_pr", observation,
		authorityApplicable, budgetAvailable,
		func(ctx context.Context) (factoryv1.EffectObservationState, error) {
			var createErr error
			result, createErr = r.CreateIssueScanDraftPRFromApprovedRequest(ctx, runID, requestID, client)
			if createErr != nil {
				return factoryv1.EffectUnknown, createErr
			}
			if !result.Created || !strings.EqualFold(result.Repository, expectedRepository.OwnerName) {
				return factoryv1.EffectConflict, fmt.Errorf("created draft PR does not match continuation repository")
			}
			return factoryv1.EffectExact, nil
		},
	)
	return result, decision, err
}

// CreateIssueScanDraftPRFromApprovedRequest consumes an approved draft-PR
// authority request for a completed zero-blocker issue-scan run, creates exactly
// one draft PR through the existing Transpara-AI PR creator, and records the
// resulting receipt on the terminal ready stage. It never marks the PR ready,
// approves, merges, deploys, or completes the ready-for-Human stage.
func (r *Runtime) CreateIssueScanDraftPRFromApprovedRequest(ctx context.Context, runID, requestID string, client work.Epic11PullRequestCreator) (IssueScanDraftPRCreationResult, error) {
	runID = strings.TrimSpace(runID)
	requestID = strings.TrimSpace(requestID)
	result := IssueScanDraftPRCreationResult{RunID: runID, RequestID: requestID}
	if r == nil || r.store == nil || r.tasks == nil {
		return result, fmt.Errorf("runtime store and task store are required")
	}
	if runID == "" {
		return result, fmt.Errorf("run_id is required")
	}
	if requestID == "" {
		return result, fmt.Errorf("request_id is required")
	}
	if client == nil {
		return result, fmt.Errorf("pull request creator is required")
	}

	target, err := LoadApprovedDraftPRTarget(r.store, requestID)
	if err != nil {
		return result, err
	}
	requestContext, err := r.IssueScanDraftPRAuthorityRequestContext(runID, target.BaseRef, target.BaseSHA, target.SingleUseNonce)
	if err != nil {
		return result, err
	}
	if err := validateIssueScanApprovedDraftPRTarget(target, requestContext.DraftPRTarget); err != nil {
		return result, err
	}
	if err := VerifyDraftPRContent(target, requestContext.DraftPRTitle, requestContext.DraftPRBody); err != nil {
		return result, err
	}
	changedFiles, err := issueScanDraftPRChangedFiles(requestContext.ChangedFilesSummary)
	if err != nil {
		return result, err
	}

	requestEventID, err := types.NewEventID(requestID)
	if err != nil {
		return result, fmt.Errorf("parse request id: %w", err)
	}
	reservationID, err := r.reserveIssueScanDraftPRCreation(requestContext, target, requestEventID)
	if err != nil {
		return result, err
	}
	causes := compactEventIDs([]types.EventID{requestEventID})
	convID := runLaunchConversationID(requestContext.RunID, r.convID)
	run, err := CreateTransparaAIDraftPRFromApprovedDecision(ctx, r.tasks, r.humanID, convID, client, DraftPRArtifact{
		Target:         target,
		Title:          requestContext.DraftPRTitle,
		Body:           requestContext.DraftPRBody,
		ChangedFiles:   changedFiles,
		ActorRole:      "guardian",
		DeciderActorID: r.humanID.Value(),
		DeciderRole:    "human",
	}, causes...)
	if err != nil {
		return result, err
	}
	receiptResult, err := r.RecordIssueScanDraftPRReceipt(runID, run.Receipt)
	if err != nil {
		return result, err
	}
	result.FactoryOrderID = requestContext.FactoryOrderID
	result.Repository = requestContext.Repository
	result.CreationReservationID = reservationID
	result.WorkTaskID = run.WorkTask.ID
	result.DraftPRReceipt = receiptResult
	result.PRNumber = receiptResult.PRNumber
	result.PRURL = receiptResult.PRURL
	result.HeadSHA = receiptResult.HeadSHA
	result.Created = true
	result.NoReadyReviewMergeDeploy = true
	return result, nil
}

func validateIssueScanApprovedDraftPRTarget(approved, derived DraftPRTarget) error {
	approved, err := normalizeTransparaAIDraftPRTarget(approved)
	if err != nil {
		return fmt.Errorf("approved draft-PR target: %w", err)
	}
	derived, err = normalizeTransparaAIDraftPRTarget(derived)
	if err != nil {
		return fmt.Errorf("issue-scan run-derived draft-PR target: %w", err)
	}
	for _, check := range []struct {
		field    string
		approved string
		derived  string
	}{
		{"repository", approved.Repository, derived.Repository},
		{"base_ref", approved.BaseRef, derived.BaseRef},
		{"base_sha", approved.BaseSHA, derived.BaseSHA},
		{"head_ref", approved.HeadRef, derived.HeadRef},
		{"head_sha", approved.HeadSHA, derived.HeadSHA},
		{"title_hash", approved.TitleHash, derived.TitleHash},
		{"body_hash", approved.BodyHash, derived.BodyHash},
		{"policy_bundle_id", approved.PolicyBundleID, derived.PolicyBundleID},
		{"policy_bundle_hash", approved.PolicyBundleHash, derived.PolicyBundleHash},
		{"single_use_nonce", approved.SingleUseNonce, derived.SingleUseNonce},
	} {
		if strings.TrimSpace(check.approved) != strings.TrimSpace(check.derived) {
			return fmt.Errorf("approved draft-PR target %s %q does not match issue-scan run-derived %s %q", check.field, check.approved, check.field, check.derived)
		}
	}
	return nil
}

func (r *Runtime) reserveIssueScanDraftPRCreation(requestContext IssueScanDraftPRAuthorityRequestContext, target DraftPRTarget, requestID types.EventID) (types.EventID, error) {
	issueScanDraftPRCreationReservationMu.Lock()
	defer issueScanDraftPRCreationReservationMu.Unlock()

	if r == nil || r.tasks == nil {
		return types.EventID{}, fmt.Errorf("runtime task store is required")
	}
	if requestID.IsZero() {
		return types.EventID{}, fmt.Errorf("request id is required to reserve issue-scan draft PR creation")
	}
	readyStageTaskID, err := types.NewEventID(requestContext.ReadyStageTaskID)
	if err != nil {
		return types.EventID{}, fmt.Errorf("parse ready stage task id: %w", err)
	}
	artifacts, err := r.tasks.ListArtifacts(readyStageTaskID)
	if err != nil {
		return types.EventID{}, fmt.Errorf("list issue-scan ready-stage artifacts before draft PR reservation: %w", err)
	}
	for _, artifact := range artifacts {
		reservation, ok, err := issueScanDraftPRCreationReservationArtifact(artifact.Label, artifact.Body)
		if err != nil {
			return types.EventID{}, fmt.Errorf("parse issue-scan draft PR creation reservation artifact %s: %w", artifact.ID.Value(), err)
		}
		if !ok {
			continue
		}
		if strings.TrimSpace(reservation.RequestID) == requestID.Value() || strings.TrimSpace(reservation.AuthorityNonce) == strings.TrimSpace(target.SingleUseNonce) {
			return types.EventID{}, fmt.Errorf("issue-scan draft PR creation already has reservation artifact %s for request %s at head %s; manual reconciliation is required before retry to avoid duplicate draft PR creation", artifact.ID.Value(), reservation.RequestID, reservation.HeadSHA)
		}
	}
	body, err := issueScanDraftPRCreationReservationBody(issueScanDraftPRCreationReservation(requestContext, target, requestID))
	if err != nil {
		return types.EventID{}, err
	}
	causes := compactEventIDs([]types.EventID{requestID, readyStageTaskID})
	if err := r.tasks.AddArtifact(r.humanID, readyStageTaskID, issueScanDraftPRCreationReservationArtifactLabel, "application/json", body, causes, runLaunchConversationID(requestContext.RunID, r.convID)); err != nil {
		return types.EventID{}, fmt.Errorf("reserve issue-scan draft PR creation: %w", err)
	}
	artifactID, exists, err := r.findIssueScanReadyStageArtifactID(readyStageTaskID, issueScanDraftPRCreationReservationArtifactLabel, body)
	if err != nil {
		return types.EventID{}, err
	}
	if !exists {
		return types.EventID{}, fmt.Errorf("issue-scan draft PR creation reservation artifact was not found after append")
	}
	return artifactID, nil
}

func issueScanDraftPRCreationReservation(requestContext IssueScanDraftPRAuthorityRequestContext, target DraftPRTarget, requestID types.EventID) IssueScanDraftPRCreationReservation {
	return IssueScanDraftPRCreationReservation{
		Kind:                     issueScanDraftPRCreationReservationKind,
		LifecycleVersion:         issueScanLifecycleVersion,
		RunID:                    strings.TrimSpace(requestContext.RunID),
		FactoryOrderID:           strings.TrimSpace(requestContext.FactoryOrderID),
		ReadyStageTaskID:         strings.TrimSpace(requestContext.ReadyStageTaskID),
		RequestID:                requestID.Value(),
		Repository:               strings.TrimSpace(target.Repository),
		BaseRef:                  strings.TrimSpace(target.BaseRef),
		BaseSHA:                  strings.TrimSpace(target.BaseSHA),
		HeadRef:                  strings.TrimSpace(target.HeadRef),
		HeadSHA:                  strings.TrimSpace(target.HeadSHA),
		TitleHash:                strings.TrimSpace(target.TitleHash),
		BodyHash:                 strings.TrimSpace(target.BodyHash),
		PolicyBundleID:           strings.TrimSpace(target.PolicyBundleID),
		PolicyBundleHash:         strings.TrimSpace(target.PolicyBundleHash),
		AuthorityNonce:           strings.TrimSpace(target.SingleUseNonce),
		Result:                   "reserved",
		ManualReconciliationOn:   "reservation_without_" + TransparaAIDraftPRReceiptArtifactLabel,
		NoReadyReviewMergeDeploy: true,
	}
}

func issueScanDraftPRCreationReservationBody(reservation IssueScanDraftPRCreationReservation) (string, error) {
	body, err := json.MarshalIndent(reservation, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal issue-scan draft PR creation reservation: %w", err)
	}
	return string(body), nil
}

func issueScanDraftPRCreationReservationArtifact(label, body string) (IssueScanDraftPRCreationReservation, bool, error) {
	if strings.TrimSpace(label) != issueScanDraftPRCreationReservationArtifactLabel {
		return IssueScanDraftPRCreationReservation{}, false, nil
	}
	raw := strings.TrimSpace(body)
	if raw == "" {
		return IssueScanDraftPRCreationReservation{}, false, fmt.Errorf("label %q has empty artifact body", label)
	}
	var payload IssueScanDraftPRCreationReservation
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return IssueScanDraftPRCreationReservation{}, false, fmt.Errorf("decode artifact body: %w", err)
	}
	if strings.TrimSpace(payload.Kind) != issueScanDraftPRCreationReservationKind {
		return IssueScanDraftPRCreationReservation{}, false, fmt.Errorf("kind %q does not match %q", payload.Kind, issueScanDraftPRCreationReservationKind)
	}
	return payload, true, nil
}

func issueScanDraftPRChangedFiles(summary string) ([]string, error) {
	lines := strings.Split(summary, "\n")
	files := make([]string, 0, len(lines))
	for _, line := range lines {
		before, _, ok := strings.Cut(line, "|")
		if !ok {
			continue
		}
		file := strings.TrimSpace(before)
		if file != "" {
			files = append(files, file)
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("changed_files_summary does not contain repository-relative changed file paths")
	}
	return normalizePRChangedFiles(files)
}
