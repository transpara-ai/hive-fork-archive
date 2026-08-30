package factoryv1

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"
)

// ContinuationSchemaVersion is Hive's durable execution vocabulary. The TLC
// contract version is separate because Hive must never reinterpret a legacy
// factory-v1/tlc-v1 record as a continuation invocation.
const (
	ContinuationSchemaVersion = "civilization-tlc-continuation/v1"
	TLCContinuationVersion    = "tlc-change-continuation/v1"
	TLCGovernancePluginName   = "transpara-governance-gates"
	TLCChangeWorkflowSkill    = "tlc-change-workflow"
)

// TLCWorkflowIdentity is authenticated read-back supplied by the installed
// plugin runner. A configured expectation and an observed identity must match
// exactly before Hive submits source bytes.
type TLCWorkflowIdentity struct {
	PluginName      string `json:"plugin_name"`
	PluginVersion   string `json:"plugin_version"`
	SkillName       string `json:"skill_name"`
	ContractVersion string `json:"contract_version"`
	SchemaSHA256    string `json:"schema_sha256"`
	SkillSHA256     string `json:"skill_sha256"`
}

// TLCChangeWorkflowRunner adapts an installed skill invocation. Hive owns the
// provider process and credentials; the portable TLC contract sees only exact
// JSON input and returns exact JSON output.
type TLCChangeWorkflowRunner interface {
	ObservedIdentity(ctx context.Context) (TLCWorkflowIdentity, error)
	Evaluate(ctx context.Context, invocationJSON []byte) ([]byte, error)
}

type RepositoryIdentity struct {
	Provider  string `json:"provider"`
	NumericID int64  `json:"numeric_id"`
	OwnerName string `json:"owner_name"`
}

type Principal struct {
	Kind       string `json:"kind"`
	StableID   string `json:"stable_id"`
	SubjectRef string `json:"subject_ref"`
	ModelID    string `json:"model_id,omitempty"`
	Lineage    string `json:"lineage,omitempty"`
}

type Authentication struct {
	Status          string `json:"status"`
	Method          string `json:"method"`
	Reference       string `json:"reference"`
	AuthenticatedBy string `json:"authenticated_by"`
}

type ContentReference struct {
	Reference       string `json:"reference"`
	AuthenticatedBy string `json:"authenticated_by"`
}

type ExactContent struct {
	MediaType      string            `json:"media_type"`
	Encoding       string            `json:"encoding"`
	Readability    string            `json:"readability"`
	DigestVerified bool              `json:"digest_verified"`
	Inline         *string           `json:"inline,omitempty"`
	ContentRef     *ContentReference `json:"content_ref,omitempty"`
}

type IssueIdentity struct {
	Repository     RepositoryIdentity `json:"repository"`
	Number         int                `json:"number"`
	NodeID         string             `json:"node_id"`
	TitleSHA256    string             `json:"title_sha256"`
	BodySHA256     string             `json:"body_sha256"`
	CommentIDs     []string           `json:"comment_ids"`
	SnapshotSHA256 string             `json:"snapshot_sha256"`
}

type SourceRecord struct {
	ChainID           string         `json:"chain_id"`
	Ordinal           int            `json:"ordinal"`
	PredecessorDigest *string        `json:"predecessor_digest"`
	Kind              string         `json:"kind"`
	Principal         Principal      `json:"principal"`
	Authentication    Authentication `json:"authentication"`
	Content           ExactContent   `json:"content"`
	ContentSHA256     string         `json:"content_sha256"`
	CapturedBy        string         `json:"captured_by"`
	ObservedTime      string         `json:"observed_time"`
	Issue             *IssueIdentity `json:"issue,omitempty"`
	RecordDigest      string         `json:"record_digest"`
}

type SourceChain struct {
	ChainID    string         `json:"chain_id"`
	HeadDigest string         `json:"head_digest"`
	Records    []SourceRecord `json:"records"`
}

type CollaborationContext struct {
	PriorReportSHA256      string `json:"prior_report_sha256,omitempty"`
	SemanticInventionFound bool   `json:"semantic_invention_detected,omitempty"`
}

type CollaborationSelection struct {
	SelectionID     string         `json:"selection_id"`
	Principal       Principal      `json:"principal"`
	Authentication  Authentication `json:"authentication"`
	SourceChainHead string         `json:"source_chain_head"`
	Phase           string         `json:"phase"`
	Objective       string         `json:"objective"`
	Profile         string         `json:"profile"`
	MaxInvocations  int            `json:"max_invocations"`
	SelectionDigest string         `json:"selection_digest"`
}

type RouteRequirement struct {
	RequirementID   string `json:"requirement_id"`
	Kind            string `json:"kind"`
	SourceChainHead string `json:"source_chain_head"`
	Profile         string `json:"profile"`
	Model           string `json:"model"`
	Effort          string `json:"effort"`
	Lineage         string `json:"lineage"`
	Role            string `json:"role"`
	GateCredit      string `json:"gate_credit"`
	PromptSHA256    string `json:"prompt_sha256"`
}

type AuthorRequirement struct {
	RequirementID          string   `json:"requirement_id"`
	SourceChainHead        string   `json:"source_chain_head"`
	Phase                  string   `json:"phase"`
	Objective              string   `json:"objective"`
	Role                   string   `json:"role"`
	InputDigests           []string `json:"input_digests"`
	RequiredIdentityFields []string `json:"required_identity_fields"`
}

type AuthorResult struct {
	ResultID             string       `json:"result_id"`
	RequirementID        string       `json:"requirement_id"`
	SourceChainHead      string       `json:"source_chain_head"`
	Mode                 string       `json:"mode"`
	Author               Principal    `json:"author"`
	Content              ExactContent `json:"content"`
	ContentSHA256        string       `json:"content_sha256"`
	PromptSHA256         string       `json:"prompt_sha256"`
	AttestationID        string       `json:"attestation_id"`
	BoundedDirection     bool         `json:"bounded_direction"`
	InvalidatesDirection bool         `json:"invalidates_direction"`
	ResultDigest         string       `json:"result_digest"`
}

type CollaborationResult struct {
	ResultID        string       `json:"result_id"`
	RequirementID   string       `json:"requirement_id"`
	SelectionID     string       `json:"selection_id"`
	SourceChainHead string       `json:"source_chain_head"`
	Phase           string       `json:"phase"`
	Collaborator    Principal    `json:"collaborator"`
	Content         ExactContent `json:"content"`
	ContentSHA256   string       `json:"content_sha256"`
	PromptSHA256    string       `json:"prompt_sha256"`
	AttestationID   string       `json:"attestation_id"`
	ResultDigest    string       `json:"result_digest"`
}

type InvocationIntent struct {
	IntentID       string `json:"intent_id"`
	SelectionID    string `json:"selection_id"`
	State          string `json:"state"`
	OperationID    string `json:"operation_id"`
	IdempotencyKey string `json:"idempotency_key"`
	ResultSHA256   string `json:"result_sha256,omitempty"`
	RecordDigest   string `json:"record_digest"`
}

// NewCollaborationSelection captures only a dedicated authenticated Human
// action. Issue prose, labels, machine defaults, and model output never call
// this constructor on their own.
func NewCollaborationSelection(principal Principal, authentication Authentication, sourceHead, phase, objective, profile string) (CollaborationSelection, error) {
	selection := CollaborationSelection{
		Principal: principal, Authentication: authentication,
		SourceChainHead: sourceHead, Phase: phase, Objective: strings.TrimSpace(objective),
		Profile: profile, MaxInvocations: 1,
	}
	selection.SelectionID = "selection-" + HashText(principal.StableID + "\x00" + sourceHead + "\x00" + phase + "\x00" + selection.Objective + "\x00" + profile)[:32]
	digest, err := collaborationSelectionDigest(selection)
	if err != nil {
		return CollaborationSelection{}, err
	}
	selection.SelectionDigest = digest
	if err := validateCapturedCollaborationSelection(selection, sourceHead); err != nil {
		return CollaborationSelection{}, err
	}
	return selection, nil
}

func collaborationSelectionDigest(selection CollaborationSelection) (string, error) {
	copy := selection
	copy.SelectionDigest = ""
	return CanonicalSHA256(copy)
}

func validateCapturedCollaborationSelection(selection CollaborationSelection, sourceHead string) error {
	var fields []string
	validatePrincipal("principal", selection.Principal, &fields)
	validateAuthentication("authentication", selection.Authentication, &fields)
	if selection.Principal.Kind != "human" || selection.Authentication.Status != "authenticated" {
		fields = append(fields, "selection requires an explicitly authenticated Human")
	}
	if selection.SourceChainHead != sourceHead || !hexPattern.MatchString(sourceHead) {
		fields = append(fields, "selection must bind the exact current source head")
	}
	if !oneOf(selection.Phase, "design", "refine", "formalize") || selection.Objective == "" || len(selection.Objective) > 4096 || selection.MaxInvocations != 1 {
		fields = append(fields, "selection has invalid phase, objective, or invocation bound")
	}
	validateIdentifier("selection_id", selection.SelectionID, &fields)
	validateIdentifier("profile", selection.Profile, &fields)
	validateDigest("selection_digest", selection.SelectionDigest, &fields)
	if digest, err := collaborationSelectionDigest(selection); err != nil || digest != selection.SelectionDigest {
		fields = append(fields, "selection_digest does not match the selection")
	}
	if len(fields) != 0 {
		sort.Strings(fields)
		return &ContinuationValidationError{Fields: fields}
	}
	return nil
}

// NewInvocationIntent creates the durable pre-dispatch state for one selected
// collaboration. The provider must not launch from this state.
func NewInvocationIntent(selection CollaborationSelection, operationID string) (InvocationIntent, error) {
	if err := validateCapturedCollaborationSelection(selection, selection.SourceChainHead); err != nil {
		return InvocationIntent{}, err
	}
	intent := InvocationIntent{
		IntentID:    "intent-" + HashText(selection.SelectionDigest + "\x00" + operationID)[:32],
		SelectionID: selection.SelectionID, State: "not_started", OperationID: operationID,
		IdempotencyKey: HashText(selection.SelectionDigest + "\x00" + operationID),
	}
	if strings.TrimSpace(operationID) == "" {
		return InvocationIntent{}, errors.New("operation_id is required")
	}
	return withInvocationIntentDigest(intent)
}

func claimedInvocation(intent InvocationIntent) (InvocationIntent, error) {
	if err := validateInvocationIntent(intent); err != nil {
		return InvocationIntent{}, err
	}
	if intent.State != "not_started" {
		return InvocationIntent{}, errors.New("collaboration invocation has no unclaimed dispatch")
	}
	intent.State = "dispatch_claimed"
	return withInvocationIntentDigest(intent)
}

func completedInvocation(intent InvocationIntent, resultSHA256 string) (InvocationIntent, error) {
	if err := validateInvocationIntent(intent); err != nil {
		return InvocationIntent{}, err
	}
	if intent.State != "dispatch_claimed" || !hexPattern.MatchString(resultSHA256) {
		return InvocationIntent{}, errors.New("only a claimed invocation may complete with an exact result digest")
	}
	intent.State = "completed"
	intent.ResultSHA256 = resultSHA256
	return withInvocationIntentDigest(intent)
}

func consumedUnknownInvocation(intent InvocationIntent) (InvocationIntent, error) {
	if err := validateInvocationIntent(intent); err != nil {
		return InvocationIntent{}, err
	}
	if intent.State != "dispatch_claimed" {
		return InvocationIntent{}, errors.New("only a claimed invocation can become consumed_unknown")
	}
	intent.State = "consumed_unknown"
	return withInvocationIntentDigest(intent)
}

func withInvocationIntentDigest(intent InvocationIntent) (InvocationIntent, error) {
	intent.RecordDigest = ""
	digest, err := CanonicalSHA256(intent)
	if err != nil {
		return InvocationIntent{}, err
	}
	intent.RecordDigest = digest
	return intent, validateInvocationIntent(intent)
}

func validateInvocationIntent(intent InvocationIntent) error {
	var fields []string
	validateIdentifier("intent_id", intent.IntentID, &fields)
	validateIdentifier("selection_id", intent.SelectionID, &fields)
	validateIdentifier("operation_id", intent.OperationID, &fields)
	validateDigest("idempotency_key", intent.IdempotencyKey, &fields)
	validateDigest("record_digest", intent.RecordDigest, &fields)
	if !oneOf(intent.State, "not_started", "dispatch_claimed", "completed", "consumed_unknown") {
		fields = append(fields, "state is invalid")
	}
	if intent.State == "completed" {
		validateDigest("result_sha256", intent.ResultSHA256, &fields)
	} else if intent.ResultSHA256 != "" {
		fields = append(fields, "only completed state may contain result_sha256")
	}
	copy := intent
	copy.RecordDigest = ""
	if digest, err := CanonicalSHA256(copy); err != nil || digest != intent.RecordDigest {
		fields = append(fields, "record_digest does not match the invocation intent")
	}
	if len(fields) != 0 {
		sort.Strings(fields)
		return &ContinuationValidationError{Fields: fields}
	}
	return nil
}

type ProviderAttestation struct {
	AttestationID         string `json:"attestation_id"`
	RequirementID         string `json:"requirement_id"`
	ProviderID            string `json:"provider_id"`
	Model                 string `json:"model"`
	Effort                string `json:"effort"`
	Lineage               string `json:"lineage"`
	ArgvSHA256            string `json:"argv_sha256"`
	PromptSHA256          string `json:"prompt_sha256"`
	SourceTransportSHA256 string `json:"source_transport_sha256"`
	SourceSessionSHA256   string `json:"source_session_sha256"`
	ResultSHA256          string `json:"result_sha256"`
	ExitState             string `json:"exit_state"`
	InvocationOrdinal     int    `json:"invocation_ordinal"`
	DurableReference      string `json:"durable_reference"`
	AttestationDigest     string `json:"attestation_digest"`
}

type ArtifactContract struct {
	ContractID      string   `json:"contract_id"`
	Kind            string   `json:"kind"`
	SourceChainHead string   `json:"source_chain_head"`
	RequiredFields  []string `json:"required_fields"`
	ContractDigest  string   `json:"contract_digest"`
}

type ArtifactCandidate struct {
	CandidateID      string    `json:"candidate_id"`
	ContractID       string    `json:"contract_id"`
	SourceChainHead  string    `json:"source_chain_head"`
	Author           Principal `json:"author"`
	ContentSHA256    string    `json:"content_sha256"`
	DurableReference string    `json:"durable_reference"`
	CandidateDigest  string    `json:"candidate_digest"`
}

type ReviewResult struct {
	ReviewID         string    `json:"review_id"`
	RequirementID    string    `json:"requirement_id"`
	Subject          string    `json:"subject"`
	Reviewer         Principal `json:"reviewer"`
	AttestationID    string    `json:"attestation_id"`
	Status           string    `json:"status"`
	BlockerCount     int       `json:"blocker_count"`
	DurableReference string    `json:"durable_reference"`
	RecordDigest     string    `json:"record_digest"`
}

type RepositoryObservation struct {
	ObservationID   string             `json:"observation_id"`
	Kind            string             `json:"kind"`
	Repository      RepositoryIdentity `json:"repository"`
	Subject         string             `json:"subject"`
	Clean           *bool              `json:"clean,omitempty"`
	ObservedTime    string             `json:"observed_time"`
	AuthenticatedBy string             `json:"authenticated_by"`
	RecordDigest    string             `json:"record_digest"`
}

type EvidenceCandidate struct {
	CandidateID       string   `json:"candidate_id"`
	Class             string   `json:"class"`
	SubjectScope      string   `json:"subject_scope"`
	SliceID           string   `json:"slice_id"`
	SubjectDigest     string   `json:"subject_digest"`
	DurableReference  string   `json:"durable_reference"`
	PrerequisiteIDs   []string `json:"prerequisite_ids"`
	RecordDigest      string   `json:"record_digest"`
	CurrentlyVerified bool     `json:"currently_verified"`
}

type AuthorityEvidence struct {
	AuthorityID    string             `json:"authority_id"`
	Grantor        Principal          `json:"grantor"`
	Authentication Authentication     `json:"authentication"`
	Action         string             `json:"action"`
	Subject        string             `json:"subject"`
	Repository     RepositoryIdentity `json:"repository"`
	ValidFrom      string             `json:"valid_from"`
	ValidUntil     string             `json:"valid_until"`
	Denials        []string           `json:"denials,omitempty"`
	RecordDigest   string             `json:"record_digest"`
}

type PartialEvidence struct {
	PartialID        string `json:"partial_id"`
	Kind             string `json:"kind"`
	ContentSHA256    string `json:"content_sha256"`
	DurableReference string `json:"durable_reference"`
	Authoritative    bool   `json:"authoritative"`
	RecordDigest     string `json:"record_digest"`
}

type RequestedAction struct {
	Action      string    `json:"action"`
	Subject     string    `json:"subject"`
	RequestedBy Principal `json:"requested_by"`
}

type ContinuationInvocation struct {
	DocumentType            string                   `json:"document_type"`
	ContractVersion         string                   `json:"contract_version"`
	TargetRepository        RepositoryIdentity       `json:"target_repository"`
	SourceChain             SourceChain              `json:"source_chain"`
	CollaborationContext    *CollaborationContext    `json:"collaboration_context,omitempty"`
	CollaborationSelections []CollaborationSelection `json:"collaboration_selections,omitempty"`
	AuthorResults           []AuthorResult           `json:"author_results,omitempty"`
	CollaborationResults    []CollaborationResult    `json:"collaboration_results,omitempty"`
	InvocationIntents       []InvocationIntent       `json:"invocation_intents,omitempty"`
	ProviderAttestations    []ProviderAttestation    `json:"provider_attestations,omitempty"`
	ArtifactCandidates      []ArtifactCandidate      `json:"artifact_candidates,omitempty"`
	ReviewResults           []ReviewResult           `json:"review_results,omitempty"`
	RepositoryObservations  []RepositoryObservation  `json:"repository_observations,omitempty"`
	EvidenceCandidates      []EvidenceCandidate      `json:"evidence_candidates,omitempty"`
	AuthorityEvidence       []AuthorityEvidence      `json:"authority_evidence,omitempty"`
	PartialEvidence         []PartialEvidence        `json:"partial_evidence,omitempty"`
	RequestedAction         RequestedAction          `json:"requested_action"`
}

type Disposition struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Reason string `json:"reason"`
}

type FrontierPoint struct {
	CandidateID           string `json:"candidate_id"`
	SliceID               string `json:"slice_id"`
	SubjectDigest         string `json:"subject_digest"`
	NextSafeAction        string `json:"next_safe_action"`
	ContinuationPermitted bool   `json:"continuation_permitted"`
}

type Sufficiency struct {
	Status    string   `json:"status"`
	Satisfied []string `json:"satisfied"`
	Missing   []string `json:"missing"`
}

type RepositoryAffinity struct {
	Status             string             `json:"status"`
	AffinityRepository RepositoryIdentity `json:"affinity_repository"`
	Mismatches         []string           `json:"mismatches"`
}

type ContinuationReport struct {
	DocumentType                    string              `json:"document_type"`
	ContractVersion                 string              `json:"contract_version"`
	SourceChainHead                 string              `json:"source_chain_head"`
	SourceValidation                string              `json:"source_validation"`
	CollaborationPhase              string              `json:"collaboration_phase"`
	InformationState                string              `json:"information_state"`
	Track                           *string             `json:"track"`
	RouteRequirements               []RouteRequirement  `json:"route_requirements"`
	AuthorRequirements              []AuthorRequirement `json:"author_requirements"`
	AuthorResultDispositions        []Disposition       `json:"author_result_dispositions,omitempty"`
	CollaborationResultDispositions []Disposition       `json:"collaboration_result_dispositions,omitempty"`
	SelectionDispositions           []Disposition       `json:"selection_dispositions,omitempty"`
	AttestationDispositions         []Disposition       `json:"attestation_dispositions,omitempty"`
	ArtifactContract                *ArtifactContract   `json:"artifact_contract,omitempty"`
	CandidateDispositions           []Disposition       `json:"candidate_dispositions,omitempty"`
	FOCandidateSufficiency          Sufficiency         `json:"fo_candidate_sufficiency"`
	RepositoryAffinity              RepositoryAffinity  `json:"repository_affinity"`
	AdmittedEvidenceIDs             []string            `json:"admitted_evidence_ids"`
	RejectedCandidates              []Disposition       `json:"rejected_candidates"`
	ContinuationFrontier            []FrontierPoint     `json:"continuation_frontier"`
	BlockedSlices                   []Disposition       `json:"blocked_slices"`
	PartialDispositions             []Disposition       `json:"partial_dispositions"`
	ReviewCreditIDs                 []string            `json:"review_credit_ids"`
	AuthorityDispositions           []Disposition       `json:"authority_dispositions"`
	ReportSHA256                    string              `json:"report_sha256"`
}

type ContinuationValidationError struct{ Fields []string }

func (e *ContinuationValidationError) Error() string {
	return "invalid continuation contract: " + strings.Join(e.Fields, "; ")
}

func DecodeContinuationInvocation(data []byte) (ContinuationInvocation, error) {
	var result ContinuationInvocation
	if err := decodeStrictJSON(data, &result); err != nil {
		return result, err
	}
	return result, ValidateContinuationInvocation(result)
}

func DecodeContinuationReport(data []byte, sourceHead string) (ContinuationReport, error) {
	var result ContinuationReport
	if err := decodeStrictJSON(data, &result); err != nil {
		return result, err
	}
	return result, ValidateContinuationReport(result, sourceHead)
}

// InvokeTLCChangeWorkflow is the thin callable Civilization boundary. It
// verifies the installed plugin identity, strictly validates the input, passes
// the exact JSON bytes through once, and strictly validates the returned report.
// It performs no fallback and preserves downstream failure as an error.
func InvokeTLCChangeWorkflow(ctx context.Context, runner TLCChangeWorkflowRunner, expected TLCWorkflowIdentity, invocationJSON []byte) (ContinuationReport, error) {
	if runner == nil {
		return ContinuationReport{}, errors.New("TLC change workflow runner is required")
	}
	if err := validateTLCWorkflowIdentity(expected); err != nil {
		return ContinuationReport{}, fmt.Errorf("configured TLC workflow identity: %w", err)
	}
	observed, err := runner.ObservedIdentity(ctx)
	if err != nil {
		return ContinuationReport{}, fmt.Errorf("read installed TLC workflow identity: %w", err)
	}
	if err := validateTLCWorkflowIdentity(observed); err != nil {
		return ContinuationReport{}, fmt.Errorf("observed TLC workflow identity: %w", err)
	}
	if observed != expected {
		return ContinuationReport{}, errors.New("installed TLC workflow identity does not match the configured exact identity")
	}
	invocation, err := DecodeContinuationInvocation(invocationJSON)
	if err != nil {
		return ContinuationReport{}, err
	}
	response, err := runner.Evaluate(ctx, append([]byte(nil), invocationJSON...))
	if err != nil {
		return ContinuationReport{}, fmt.Errorf("TLC change workflow failed: %w", err)
	}
	report, err := DecodeContinuationReport(response, invocation.SourceChain.HeadDigest)
	if err != nil {
		return ContinuationReport{}, fmt.Errorf("invalid TLC change workflow response: %w", err)
	}
	return report, nil
}

func validateTLCWorkflowIdentity(identity TLCWorkflowIdentity) error {
	var fields []string
	if identity.PluginName != TLCGovernancePluginName || identity.SkillName != TLCChangeWorkflowSkill || identity.ContractVersion != TLCContinuationVersion {
		fields = append(fields, "plugin, skill, or contract identity is invalid")
	}
	validateIdentifier("plugin_version", identity.PluginVersion, &fields)
	validateDigest("schema_sha256", identity.SchemaSHA256, &fields)
	validateDigest("skill_sha256", identity.SkillSHA256, &fields)
	if len(fields) != 0 {
		sort.Strings(fields)
		return &ContinuationValidationError{Fields: fields}
	}
	return nil
}

func decodeStrictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode strict continuation JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("decode strict continuation JSON: trailing value")
		}
		return fmt.Errorf("decode strict continuation JSON trailing data: %w", err)
	}
	if err := validateRequiredJSONShape(data, reflect.TypeOf(target)); err != nil {
		return err
	}
	return nil
}

// validateRequiredJSONShape closes a gap in encoding/json: absent required
// fields and explicit nulls otherwise decode to Go zero values. Fields without
// omitempty are the contract-required fields; the three nullable fields are
// explicit in tlc-change-continuation/v1.
func validateRequiredJSONShape(data []byte, targetType reflect.Type) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("decode continuation shape: %w", err)
	}
	var fields []string
	validateJSONValueShape(value, targetType, "$", &fields)
	if len(fields) != 0 {
		sort.Strings(fields)
		return &ContinuationValidationError{Fields: fields}
	}
	return nil
}

func validateJSONValueShape(value any, targetType reflect.Type, path string, fields *[]string) {
	for targetType.Kind() == reflect.Pointer {
		targetType = targetType.Elem()
	}
	if targetType == reflect.TypeOf(json.RawMessage{}) {
		return
	}
	switch targetType.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			*fields = append(*fields, path+" must be an object")
			return
		}
		for i := 0; i < targetType.NumField(); i++ {
			field := targetType.Field(i)
			tag := field.Tag.Get("json")
			if tag == "-" {
				continue
			}
			parts := strings.Split(tag, ",")
			name := parts[0]
			if name == "" {
				name = field.Name
			}
			optional := false
			for _, option := range parts[1:] {
				optional = optional || option == "omitempty"
			}
			child, exists := object[name]
			childPath := path + "." + name
			if !exists {
				if !optional {
					*fields = append(*fields, childPath+" is required")
				}
				continue
			}
			if child == nil {
				if !nullableContinuationField(targetType, name) {
					*fields = append(*fields, childPath+" cannot be null")
				}
				continue
			}
			validateJSONValueShape(child, field.Type, childPath, fields)
		}
	case reflect.Slice, reflect.Array:
		array, ok := value.([]any)
		if !ok {
			*fields = append(*fields, path+" must be an array")
			return
		}
		for i, child := range array {
			childPath := fmt.Sprintf("%s[%d]", path, i)
			if child == nil {
				*fields = append(*fields, childPath+" cannot be null")
				continue
			}
			validateJSONValueShape(child, targetType.Elem(), childPath, fields)
		}
	}
}

func nullableContinuationField(parent reflect.Type, name string) bool {
	switch parent.Name() {
	case "SourceRecord":
		return name == "predecessor_digest"
	case "ContinuationReport":
		return name == "track" || name == "artifact_contract"
	default:
		return false
	}
}

func ValidateContinuationInvocation(invocation ContinuationInvocation) error {
	var fields []string
	if invocation.DocumentType != "invocation" || invocation.ContractVersion != TLCContinuationVersion {
		fields = append(fields, "document_type and contract_version must name the invocation contract")
	}
	validateRepository("target_repository", invocation.TargetRepository, &fields)
	validateSourceChain(invocation.SourceChain, &fields)
	if invocation.CollaborationContext != nil && invocation.CollaborationContext.PriorReportSHA256 != "" {
		validateDigest("collaboration_context.prior_report_sha256", invocation.CollaborationContext.PriorReportSHA256, &fields)
	}
	if len(invocation.CollaborationSelections) > 1 {
		fields = append(fields, "at most one collaboration selection may be active")
	}
	selectionIDs := make(map[string]struct{}, len(invocation.CollaborationSelections))
	for i, selection := range invocation.CollaborationSelections {
		prefix := fmt.Sprintf("collaboration_selections[%d]", i)
		if selection.Principal.Kind != "human" || selection.Authentication.Status != "authenticated" {
			fields = append(fields, prefix+" must be explicitly selected by an authenticated Human")
		}
		if selection.SourceChainHead != invocation.SourceChain.HeadDigest {
			fields = append(fields, prefix+" must bind the current source head")
		}
		if !oneOf(selection.Phase, "design", "refine", "formalize") || strings.TrimSpace(selection.Objective) == "" || len(selection.Objective) > 4096 || selection.MaxInvocations != 1 {
			fields = append(fields, prefix+" has an invalid phase, objective, or invocation bound")
		}
		validateIdentifier(prefix+".selection_id", selection.SelectionID, &fields)
		validateIdentifier(prefix+".profile", selection.Profile, &fields)
		validateDigest(prefix+".selection_digest", selection.SelectionDigest, &fields)
		validatePrincipal(prefix+".principal", selection.Principal, &fields)
		validateAuthentication(prefix+".authentication", selection.Authentication, &fields)
		if digest, err := collaborationSelectionDigest(selection); err != nil || digest != selection.SelectionDigest {
			fields = append(fields, prefix+".selection_digest does not match the selection")
		}
		if _, duplicate := selectionIDs[selection.SelectionID]; duplicate {
			fields = append(fields, prefix+" duplicates an active selection_id")
		}
		selectionIDs[selection.SelectionID] = struct{}{}
	}
	for i, result := range invocation.AuthorResults {
		prefix := fmt.Sprintf("author_results[%d]", i)
		if result.SourceChainHead != invocation.SourceChain.HeadDigest || !oneOf(result.Mode, "interactive_host", "configured_provider") {
			fields = append(fields, prefix+" has a stale head or invalid author mode")
		}
		validateContent(prefix+".content", result.Content, result.ContentSHA256, &fields)
		validateDigest(prefix+".prompt_sha256", result.PromptSHA256, &fields)
		validateDigest(prefix+".result_digest", result.ResultDigest, &fields)
		validateIdentifier(prefix+".result_id", result.ResultID, &fields)
		validateIdentifier(prefix+".requirement_id", result.RequirementID, &fields)
		validateIdentifier(prefix+".attestation_id", result.AttestationID, &fields)
		validatePrincipal(prefix+".author", result.Author, &fields)
	}
	for i, result := range invocation.CollaborationResults {
		prefix := fmt.Sprintf("collaboration_results[%d]", i)
		if result.SourceChainHead != invocation.SourceChain.HeadDigest || !oneOf(result.Phase, "design", "refine", "formalize") {
			fields = append(fields, prefix+" has a stale head or invalid collaboration phase")
		}
		if _, exists := selectionIDs[result.SelectionID]; !exists {
			fields = append(fields, prefix+" does not name the active collaboration selection")
		}
		validateIdentifier(prefix+".result_id", result.ResultID, &fields)
		validateIdentifier(prefix+".requirement_id", result.RequirementID, &fields)
		validateIdentifier(prefix+".attestation_id", result.AttestationID, &fields)
		validateContent(prefix+".content", result.Content, result.ContentSHA256, &fields)
		validateDigest(prefix+".prompt_sha256", result.PromptSHA256, &fields)
		validateDigest(prefix+".result_digest", result.ResultDigest, &fields)
		validatePrincipal(prefix+".collaborator", result.Collaborator, &fields)
	}
	intentSelections := make(map[string]struct{}, len(invocation.InvocationIntents))
	for i, intent := range invocation.InvocationIntents {
		prefix := fmt.Sprintf("invocation_intents[%d]", i)
		if err := validateInvocationIntent(intent); err != nil {
			fields = append(fields, prefix+" "+err.Error())
		}
		if _, exists := selectionIDs[intent.SelectionID]; !exists {
			fields = append(fields, prefix+" does not name the active collaboration selection")
		}
		if _, duplicate := intentSelections[intent.SelectionID]; duplicate {
			fields = append(fields, prefix+" duplicates the selection invocation")
		}
		intentSelections[intent.SelectionID] = struct{}{}
	}
	for i, attestation := range invocation.ProviderAttestations {
		prefix := fmt.Sprintf("provider_attestations[%d]", i)
		if attestation.InvocationOrdinal < 1 || !oneOf(attestation.ExitState, "completed", "failed", "interrupted", "unknown") {
			fields = append(fields, prefix+" has invalid invocation or exit state")
		}
		for name, value := range map[string]string{"argv_sha256": attestation.ArgvSHA256, "prompt_sha256": attestation.PromptSHA256, "source_transport_sha256": attestation.SourceTransportSHA256, "source_session_sha256": attestation.SourceSessionSHA256, "result_sha256": attestation.ResultSHA256, "attestation_digest": attestation.AttestationDigest} {
			validateDigest(prefix+"."+name, value, &fields)
		}
		for name, value := range map[string]string{"attestation_id": attestation.AttestationID, "requirement_id": attestation.RequirementID, "provider_id": attestation.ProviderID, "model": attestation.Model, "effort": attestation.Effort, "lineage": attestation.Lineage, "durable_reference": attestation.DurableReference} {
			validateIdentifier(prefix+"."+name, value, &fields)
		}
	}
	for i, candidate := range invocation.ArtifactCandidates {
		prefix := fmt.Sprintf("artifact_candidates[%d]", i)
		validateIdentifier(prefix+".candidate_id", candidate.CandidateID, &fields)
		validateIdentifier(prefix+".contract_id", candidate.ContractID, &fields)
		validateDigest(prefix+".source_chain_head", candidate.SourceChainHead, &fields)
		validatePrincipal(prefix+".author", candidate.Author, &fields)
		validateDigest(prefix+".content_sha256", candidate.ContentSHA256, &fields)
		validateIdentifier(prefix+".durable_reference", candidate.DurableReference, &fields)
		validateDigest(prefix+".candidate_digest", candidate.CandidateDigest, &fields)
	}
	for i, review := range invocation.ReviewResults {
		prefix := fmt.Sprintf("review_results[%d]", i)
		validateIdentifier(prefix+".review_id", review.ReviewID, &fields)
		validateIdentifier(prefix+".requirement_id", review.RequirementID, &fields)
		if !gitHashPattern.MatchString(review.Subject) {
			fields = append(fields, prefix+".subject must be an exact Git subject")
		}
		validatePrincipal(prefix+".reviewer", review.Reviewer, &fields)
		validateIdentifier(prefix+".attestation_id", review.AttestationID, &fields)
		if !oneOf(review.Status, "completed", "blocked", "interrupted") || review.BlockerCount < 0 {
			fields = append(fields, prefix+" has invalid status or blocker count")
		}
		validateIdentifier(prefix+".durable_reference", review.DurableReference, &fields)
		validateDigest(prefix+".record_digest", review.RecordDigest, &fields)
	}
	for i, observation := range invocation.RepositoryObservations {
		prefix := fmt.Sprintf("repository_observations[%d]", i)
		validateIdentifier(prefix+".observation_id", observation.ObservationID, &fields)
		validateRepository(prefix+".repository", observation.Repository, &fields)
		if !oneOf(observation.Kind, "target_checkout", "git_common_repository", "branch_repository", "pr_repository", "pr_head_repository", "pr_base_repository", "repository_head", "artifact_blob") {
			fields = append(fields, prefix+" has invalid kind")
		}
		validateTime(prefix+".observed_time", observation.ObservedTime, &fields)
		validateIdentifier(prefix+".subject", observation.Subject, &fields)
		validateIdentifier(prefix+".authenticated_by", observation.AuthenticatedBy, &fields)
		validateDigest(prefix+".record_digest", observation.RecordDigest, &fields)
	}
	for i, candidate := range invocation.EvidenceCandidates {
		prefix := fmt.Sprintf("evidence_candidates[%d]", i)
		if !oneOf(candidate.Class, "C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7") || !oneOf(candidate.SubjectScope, "chain", "artifact", "repository_slice") {
			fields = append(fields, prefix+" has invalid class or subject scope")
		}
		validateIdentifier(prefix+".candidate_id", candidate.CandidateID, &fields)
		validateIdentifier(prefix+".slice_id", candidate.SliceID, &fields)
		validateIdentifier(prefix+".durable_reference", candidate.DurableReference, &fields)
		validateUniqueIdentifiers(prefix+".prerequisite_ids", candidate.PrerequisiteIDs, false, &fields)
		validateDigest(prefix+".subject_digest", candidate.SubjectDigest, &fields)
		validateDigest(prefix+".record_digest", candidate.RecordDigest, &fields)
	}
	for i, authority := range invocation.AuthorityEvidence {
		prefix := fmt.Sprintf("authority_evidence[%d]", i)
		validateIdentifier(prefix+".authority_id", authority.AuthorityID, &fields)
		validateIdentifier(prefix+".action", authority.Action, &fields)
		validateIdentifier(prefix+".subject", authority.Subject, &fields)
		validateRepository(prefix+".repository", authority.Repository, &fields)
		validatePrincipal(prefix+".grantor", authority.Grantor, &fields)
		validateAuthentication(prefix+".authentication", authority.Authentication, &fields)
		from, fromOK := parseTime(prefix+".valid_from", authority.ValidFrom, &fields)
		until, untilOK := parseTime(prefix+".valid_until", authority.ValidUntil, &fields)
		if fromOK && untilOK && !until.After(from) {
			fields = append(fields, prefix+" validity window must increase")
		}
		validateUniqueIdentifiers(prefix+".denials", authority.Denials, false, &fields)
		validateDigest(prefix+".record_digest", authority.RecordDigest, &fields)
	}
	for i, partial := range invocation.PartialEvidence {
		prefix := fmt.Sprintf("partial_evidence[%d]", i)
		if partial.Authoritative {
			fields = append(fields, prefix+" cannot be authoritative")
		}
		validateIdentifier(prefix+".partial_id", partial.PartialID, &fields)
		validateIdentifier(prefix+".kind", partial.Kind, &fields)
		validateIdentifier(prefix+".durable_reference", partial.DurableReference, &fields)
		validateDigest(prefix+".content_sha256", partial.ContentSHA256, &fields)
		validateDigest(prefix+".record_digest", partial.RecordDigest, &fields)
	}
	validateIdentifier("requested_action.action", invocation.RequestedAction.Action, &fields)
	validateIdentifier("requested_action.subject", invocation.RequestedAction.Subject, &fields)
	validatePrincipal("requested_action.requested_by", invocation.RequestedAction.RequestedBy, &fields)
	if len(fields) > 0 {
		sort.Strings(fields)
		return &ContinuationValidationError{Fields: fields}
	}
	return nil
}

func ValidateContinuationReport(report ContinuationReport, sourceHead string) error {
	var fields []string
	if report.DocumentType != "report" || report.ContractVersion != TLCContinuationVersion {
		fields = append(fields, "document_type and contract_version must name the report contract")
	}
	if report.SourceChainHead != sourceHead || !hexPattern.MatchString(report.SourceChainHead) {
		fields = append(fields, "source_chain_head must match the invocation")
	}
	if !oneOf(report.SourceValidation, "valid", "invalid", "contradictory") || !oneOf(report.CollaborationPhase, "design", "refine", "formalize") || !oneOf(report.InformationState, "UNCLASSIFIED", "BLOCKED_CONTRADICTION", "CLASSIFIED") {
		fields = append(fields, "source validation, collaboration phase, or information state is invalid")
	}
	if report.Track != nil && !oneOf(*report.Track, "M", "I", "D", "H") {
		fields = append(fields, "track is invalid")
	}
	for i, requirement := range report.RouteRequirements {
		prefix := fmt.Sprintf("route_requirements[%d]", i)
		if requirement.SourceChainHead != sourceHead || !oneOf(requirement.Kind, "collaboration", "review") || !oneOf(requirement.GateCredit, "none", "eligible_after_validation") {
			fields = append(fields, prefix+" has a stale head, invalid kind, or invalid gate credit")
		}
		validateIdentifier(prefix+".requirement_id", requirement.RequirementID, &fields)
		validateIdentifier(prefix+".profile", requirement.Profile, &fields)
		validateIdentifier(prefix+".model", requirement.Model, &fields)
		validateIdentifier(prefix+".effort", requirement.Effort, &fields)
		validateIdentifier(prefix+".lineage", requirement.Lineage, &fields)
		validateIdentifier(prefix+".role", requirement.Role, &fields)
		validateDigest(prefix+".prompt_sha256", requirement.PromptSHA256, &fields)
	}
	for i, requirement := range report.AuthorRequirements {
		prefix := fmt.Sprintf("author_requirements[%d]", i)
		if requirement.SourceChainHead != sourceHead || requirement.Role != "author" || !oneOf(requirement.Phase, "design", "refine", "formalize") || strings.TrimSpace(requirement.Objective) == "" {
			fields = append(fields, prefix+" has invalid role, phase, objective, or source head")
		}
		validateIdentifier(prefix+".requirement_id", requirement.RequirementID, &fields)
		if len(requirement.Objective) > 4096 {
			fields = append(fields, prefix+".objective exceeds the contract bound")
		}
		validateUniqueDigests(prefix+".input_digests", requirement.InputDigests, true, &fields)
		validateUniqueIdentifiers(prefix+".required_identity_fields", requirement.RequiredIdentityFields, true, &fields)
	}
	if !oneOf(report.FOCandidateSufficiency.Status, "sufficient", "insufficient", "not_applicable") {
		fields = append(fields, "fo_candidate_sufficiency.status is invalid")
	}
	validateUniqueIdentifiers("fo_candidate_sufficiency.satisfied", report.FOCandidateSufficiency.Satisfied, false, &fields)
	validateUniqueIdentifiers("fo_candidate_sufficiency.missing", report.FOCandidateSufficiency.Missing, false, &fields)
	validateRepository("repository_affinity.affinity_repository", report.RepositoryAffinity.AffinityRepository, &fields)
	if !oneOf(report.RepositoryAffinity.Status, "match", "mismatch", "unknown") {
		fields = append(fields, "repository_affinity.status is invalid")
	}
	validateUniqueIdentifiers("repository_affinity.mismatches", report.RepositoryAffinity.Mismatches, false, &fields)
	if report.ArtifactContract != nil {
		contract := report.ArtifactContract
		validateIdentifier("artifact_contract.contract_id", contract.ContractID, &fields)
		if !oneOf(contract.Kind, "mechanical_record", "change_contract", "factory_order", "design", "implementation_unit") {
			fields = append(fields, "artifact_contract.kind is invalid")
		}
		if contract.SourceChainHead != sourceHead {
			fields = append(fields, "artifact_contract.source_chain_head is stale")
		}
		validateDigest("artifact_contract.source_chain_head", contract.SourceChainHead, &fields)
		validateUniqueIdentifiers("artifact_contract.required_fields", contract.RequiredFields, true, &fields)
		validateDigest("artifact_contract.contract_digest", contract.ContractDigest, &fields)
	}
	validateUniqueIdentifiers("admitted_evidence_ids", report.AdmittedEvidenceIDs, false, &fields)
	validateUniqueIdentifiers("review_credit_ids", report.ReviewCreditIDs, false, &fields)
	for i, point := range report.ContinuationFrontier {
		prefix := fmt.Sprintf("continuation_frontier[%d]", i)
		validateIdentifier(prefix+".candidate_id", point.CandidateID, &fields)
		validateIdentifier(prefix+".slice_id", point.SliceID, &fields)
		validateIdentifier(prefix+".next_safe_action", point.NextSafeAction, &fields)
		validateDigest(prefix+".subject_digest", point.SubjectDigest, &fields)
	}
	for name, dispositions := range map[string][]Disposition{
		"author_result_dispositions":        report.AuthorResultDispositions,
		"collaboration_result_dispositions": report.CollaborationResultDispositions,
		"selection_dispositions":            report.SelectionDispositions,
		"attestation_dispositions":          report.AttestationDispositions,
		"candidate_dispositions":            report.CandidateDispositions,
		"rejected_candidates":               report.RejectedCandidates,
		"blocked_slices":                    report.BlockedSlices,
		"partial_dispositions":              report.PartialDispositions,
		"authority_dispositions":            report.AuthorityDispositions,
	} {
		for i, disposition := range dispositions {
			if !oneOf(disposition.Status, "accepted", "rejected", "applicable", "missing", "expired", "denied", "unknown", "non_authoritative") || strings.TrimSpace(disposition.Reason) == "" {
				fields = append(fields, fmt.Sprintf("%s[%d] is invalid", name, i))
			}
			validateIdentifier(fmt.Sprintf("%s[%d].id", name, i), disposition.ID, &fields)
		}
	}
	validateDigest("report_sha256", report.ReportSHA256, &fields)
	if hexPattern.MatchString(report.ReportSHA256) {
		copy := report
		copy.ReportSHA256 = ""
		if digest, err := CanonicalSHA256(copy); err != nil || digest != report.ReportSHA256 {
			fields = append(fields, "report_sha256 does not match canonical report bytes")
		}
	}
	if len(fields) > 0 {
		sort.Strings(fields)
		return &ContinuationValidationError{Fields: fields}
	}
	return nil
}

// RequireRepositoryContinuation consumes, but never recomputes, TLC's RepoX
// affinity and frontier decision before Hive performs a repository action.
// External action authority and Hive operating policy remain separate checks.
func RequireRepositoryContinuation(report ContinuationReport, expected RepositoryIdentity, candidateID, action string) error {
	if err := ValidateContinuationReport(report, report.SourceChainHead); err != nil {
		return err
	}
	if report.RepositoryAffinity.Status != "match" || !sameRepositoryIdentity(report.RepositoryAffinity.AffinityRepository, expected) {
		return errors.New("TLC report does not affirm the exact target repository affinity")
	}
	for _, point := range report.ContinuationFrontier {
		if point.CandidateID != candidateID {
			continue
		}
		if !point.ContinuationPermitted || point.NextSafeAction != action {
			return errors.New("TLC frontier does not permit the exact requested repository action")
		}
		return nil
	}
	return errors.New("TLC frontier does not contain the requested repository slice")
}

func sameRepositoryIdentity(left, right RepositoryIdentity) bool {
	return left.Provider == right.Provider && left.NumericID == right.NumericID && strings.EqualFold(left.OwnerName, right.OwnerName)
}

// CanonicalSHA256 hashes compact UTF-8 JSON after decoding into JSON values.
// encoding/json sorts object keys lexicographically and preserves array order,
// so structs, maps, and raw messages converge on the TLC contract's portable
// normalization. Callers clear a record's own digest field before hashing.
func CanonicalSHA256(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var normalized any
	if err := decoder.Decode(&normalized); err != nil {
		return "", fmt.Errorf("normalize continuation JSON: %w", err)
	}
	canonical, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("encode normalized continuation JSON: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// NewInlineSourceRecord captures exact source bytes and computes both content
// and record digests. It does not classify the source or create authority.
func NewInlineSourceRecord(chainID, kind string, ordinal int, predecessor *string, principal Principal, authentication Authentication, mediaType, content, capturedBy string, observedAt time.Time, issue *IssueIdentity) (SourceRecord, error) {
	contentDigest := sha256.Sum256([]byte(content))
	record := SourceRecord{
		ChainID: chainID, Ordinal: ordinal, PredecessorDigest: predecessor, Kind: kind,
		Principal: principal, Authentication: authentication,
		Content:       ExactContent{MediaType: mediaType, Encoding: "utf-8", Readability: "readable", DigestVerified: true, Inline: &content},
		ContentSHA256: hex.EncodeToString(contentDigest[:]), CapturedBy: capturedBy,
		ObservedTime: observedAt.UTC().Format(time.RFC3339Nano), Issue: issue,
	}
	digest, err := CanonicalSHA256(record)
	if err != nil {
		return SourceRecord{}, err
	}
	record.RecordDigest = digest
	return record, nil
}

func validateSourceChain(chain SourceChain, fields *[]string) {
	validateIdentifier("source_chain.chain_id", chain.ChainID, fields)
	if len(chain.Records) == 0 {
		*fields = append(*fields, "source_chain.records must not be empty")
		return
	}
	for i, record := range chain.Records {
		prefix := fmt.Sprintf("source_chain.records[%d]", i)
		if record.ChainID != chain.ChainID || record.Ordinal != i {
			*fields = append(*fields, prefix+" chain or ordinal is not contiguous")
		}
		if i == 0 {
			if record.PredecessorDigest != nil {
				*fields = append(*fields, prefix+" initial predecessor must be null")
			}
		} else if record.PredecessorDigest == nil || *record.PredecessorDigest != chain.Records[i-1].RecordDigest {
			*fields = append(*fields, prefix+" predecessor does not bind the prior record")
		}
		if !oneOf(record.Kind, "human_request", "human_reply", "issue_snapshot") {
			*fields = append(*fields, prefix+" has invalid source kind")
		}
		if record.Kind == "issue_snapshot" && record.Issue == nil {
			*fields = append(*fields, prefix+" issue snapshot requires issue identity")
		}
		if record.Kind != "issue_snapshot" && record.Issue != nil {
			*fields = append(*fields, prefix+" only an issue snapshot may carry issue identity")
		}
		if record.Issue != nil {
			validateIssueIdentity(prefix+".issue", *record.Issue, fields)
		}
		validatePrincipal(prefix+".principal", record.Principal, fields)
		validateAuthentication(prefix+".authentication", record.Authentication, fields)
		validateContent(prefix+".content", record.Content, record.ContentSHA256, fields)
		validateIdentifier(prefix+".captured_by", record.CapturedBy, fields)
		validateTime(prefix+".observed_time", record.ObservedTime, fields)
		validateDigest(prefix+".record_digest", record.RecordDigest, fields)
		copy := record
		copy.RecordDigest = ""
		if digest, err := CanonicalSHA256(copy); err != nil || digest != record.RecordDigest {
			*fields = append(*fields, prefix+" record_digest does not match canonical record bytes")
		}
	}
	if chain.HeadDigest != chain.Records[len(chain.Records)-1].RecordDigest {
		*fields = append(*fields, "source_chain.head_digest does not match the final record")
	}
}

func validateContent(prefix string, content ExactContent, expectedDigest string, fields *[]string) {
	if strings.TrimSpace(content.MediaType) == "" || !oneOf(content.Encoding, "utf-8", "base64") || content.Readability != "readable" || !content.DigestVerified {
		*fields = append(*fields, prefix+" must be readable, media-typed, encoding-bound, and digest-verified")
	}
	if (content.Inline == nil) == (content.ContentRef == nil) {
		*fields = append(*fields, prefix+" must contain exactly one of inline or content_ref")
	}
	validateDigest(prefix+"_sha256", expectedDigest, fields)
	if content.Inline != nil {
		decoded := []byte(*content.Inline)
		if content.Encoding == "base64" {
			var err error
			decoded, err = base64.StdEncoding.DecodeString(*content.Inline)
			if err != nil {
				*fields = append(*fields, prefix+" inline base64 is invalid")
				return
			}
		}
		sum := sha256.Sum256(decoded)
		if hex.EncodeToString(sum[:]) != expectedDigest {
			*fields = append(*fields, prefix+" inline bytes do not match the declared digest")
		}
	}
	if content.ContentRef != nil && (strings.TrimSpace(content.ContentRef.Reference) == "" || strings.TrimSpace(content.ContentRef.AuthenticatedBy) == "") {
		*fields = append(*fields, prefix+" durable reference is incomplete")
	}
}

func validateIssueIdentity(prefix string, issue IssueIdentity, fields *[]string) {
	validateRepository(prefix+".repository", issue.Repository, fields)
	if issue.Number < 1 {
		*fields = append(*fields, prefix+".number must be positive")
	}
	validateIdentifier(prefix+".node_id", issue.NodeID, fields)
	validateDigest(prefix+".title_sha256", issue.TitleSHA256, fields)
	validateDigest(prefix+".body_sha256", issue.BodySHA256, fields)
	validateUniqueIdentifiers(prefix+".comment_ids", issue.CommentIDs, false, fields)
	validateDigest(prefix+".snapshot_sha256", issue.SnapshotSHA256, fields)
}

func validateRepository(prefix string, repository RepositoryIdentity, fields *[]string) {
	if repository.Provider != "github" || repository.NumericID < 1 || !repoPattern.MatchString(repository.OwnerName) {
		*fields = append(*fields, prefix+" must contain GitHub numeric and owner/name identity")
	}
}

func validatePrincipal(prefix string, principal Principal, fields *[]string) {
	if !oneOf(principal.Kind, "human", "model", "machine", "unknown") || strings.TrimSpace(principal.StableID) == "" || strings.TrimSpace(principal.SubjectRef) == "" {
		*fields = append(*fields, prefix+" is incomplete or has invalid kind")
	}
	if principal.Kind == "model" && (strings.TrimSpace(principal.ModelID) == "" || strings.TrimSpace(principal.Lineage) == "") {
		*fields = append(*fields, prefix+" model identity requires model_id and lineage")
	}
}

func validateAuthentication(prefix string, authentication Authentication, fields *[]string) {
	if !oneOf(authentication.Status, "authenticated", "unauthenticated", "unknown") || strings.TrimSpace(authentication.Method) == "" || strings.TrimSpace(authentication.Reference) == "" || strings.TrimSpace(authentication.AuthenticatedBy) == "" {
		*fields = append(*fields, prefix+" is incomplete or invalid")
	}
}

func validateDigest(prefix, value string, fields *[]string) {
	if !hexPattern.MatchString(value) {
		*fields = append(*fields, prefix+" must be 64 lowercase hexadecimal characters")
	}
}

func validateIdentifier(prefix, value string, fields *[]string) {
	if strings.TrimSpace(value) == "" || len(value) > 512 {
		*fields = append(*fields, prefix+" must be a bounded non-empty identifier")
	}
}

func validateUniqueIdentifiers(prefix string, values []string, required bool, fields *[]string) {
	if required && len(values) == 0 {
		*fields = append(*fields, prefix+" must not be empty")
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		validateIdentifier(prefix, value, fields)
		if _, exists := seen[value]; exists {
			*fields = append(*fields, prefix+" must be unique")
		}
		seen[value] = struct{}{}
	}
}

func validateUniqueDigests(prefix string, values []string, required bool, fields *[]string) {
	if required && len(values) == 0 {
		*fields = append(*fields, prefix+" must not be empty")
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		validateDigest(prefix, value, fields)
		if _, exists := seen[value]; exists {
			*fields = append(*fields, prefix+" must be unique")
		}
		seen[value] = struct{}{}
	}
}

func validateTime(prefix, value string, fields *[]string) { _, _ = parseTime(prefix, value, fields) }

func parseTime(prefix, value string, fields *[]string) (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		*fields = append(*fields, prefix+" must be an RFC3339 timestamp")
		return time.Time{}, false
	}
	return parsed, true
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}
