package factoryv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var ErrInvocationIntentConflict = errors.New("continuation invocation intent conflict")

// InvocationIntentStore must implement durable compare-and-swap. A provider
// launch is permitted only to the caller whose not_started -> dispatch_claimed
// swap succeeds. A read followed by an unconditional write is not conforming.
type InvocationIntentStore interface {
	CreateInvocationIntent(ctx context.Context, intent InvocationIntent) error
	GetInvocationIntent(ctx context.Context, intentID string) (InvocationIntent, error)
	CompareAndSwapInvocationIntent(ctx context.Context, expected, replacement InvocationIntent) (bool, error)
}

type InMemoryInvocationIntentStore struct {
	mu      sync.RWMutex
	intents map[string]InvocationIntent
}

func NewInMemoryInvocationIntentStore() *InMemoryInvocationIntentStore {
	return &InMemoryInvocationIntentStore{intents: make(map[string]InvocationIntent)}
}

func (s *InMemoryInvocationIntentStore) CreateInvocationIntent(ctx context.Context, intent InvocationIntent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateInvocationIntent(intent); err != nil {
		return err
	}
	if intent.State != "not_started" {
		return errors.New("new invocation intent must be not_started")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.intents[intent.IntentID]; ok {
		if existing.RecordDigest == intent.RecordDigest {
			return nil
		}
		return ErrInvocationIntentConflict
	}
	s.intents[intent.IntentID] = intent
	return nil
}

func (s *InMemoryInvocationIntentStore) GetInvocationIntent(ctx context.Context, intentID string) (InvocationIntent, error) {
	if err := ctx.Err(); err != nil {
		return InvocationIntent{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	intent, ok := s.intents[intentID]
	if !ok {
		return InvocationIntent{}, ErrWorkNotFound
	}
	return intent, nil
}

func (s *InMemoryInvocationIntentStore) CompareAndSwapInvocationIntent(ctx context.Context, expected, replacement InvocationIntent) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := validateInvocationIntent(expected); err != nil {
		return false, err
	}
	if err := validateInvocationIntent(replacement); err != nil {
		return false, err
	}
	if !sameInvocationIdentity(expected, replacement) || !validInvocationTransition(expected, replacement) {
		return false, errors.New("invalid invocation intent transition")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.intents[expected.IntentID]
	if !ok || current.RecordDigest != expected.RecordDigest {
		return false, nil
	}
	s.intents[expected.IntentID] = replacement
	return true, nil
}

func sameInvocationIdentity(left, right InvocationIntent) bool {
	return left.IntentID == right.IntentID && left.SelectionID == right.SelectionID &&
		left.OperationID == right.OperationID && left.IdempotencyKey == right.IdempotencyKey
}

func validInvocationTransition(left, right InvocationIntent) bool {
	if left.State == "not_started" && right.State == "dispatch_claimed" {
		return true
	}
	return left.State == "dispatch_claimed" && (right.State == "completed" || right.State == "consumed_unknown")
}

// ClaimInvocationDispatch returns the sole launch claim only after the durable
// compare-and-swap succeeds. A competing or replayed caller receives a
// conflict and must not launch the provider.
func ClaimInvocationDispatch(ctx context.Context, store InvocationIntentStore, intent InvocationIntent) (InvocationIntent, error) {
	if store == nil {
		return InvocationIntent{}, errors.New("invocation intent store is required")
	}
	claimed, err := claimedInvocation(intent)
	if err != nil {
		return InvocationIntent{}, err
	}
	acquired, err := store.CompareAndSwapInvocationIntent(ctx, intent, claimed)
	if err != nil {
		return InvocationIntent{}, err
	}
	if !acquired {
		return InvocationIntent{}, ErrInvocationIntentConflict
	}
	return claimed, nil
}

// CompleteInvocation durably records an exact completed result.
func CompleteInvocation(ctx context.Context, store InvocationIntentStore, intent InvocationIntent, resultSHA256 string) (InvocationIntent, error) {
	if store == nil {
		return InvocationIntent{}, errors.New("invocation intent store is required")
	}
	completed, err := completedInvocation(intent, resultSHA256)
	if err != nil {
		return InvocationIntent{}, err
	}
	updated, err := store.CompareAndSwapInvocationIntent(ctx, intent, completed)
	if err != nil {
		return InvocationIntent{}, err
	}
	if !updated {
		return InvocationIntent{}, ErrInvocationIntentConflict
	}
	return completed, nil
}

// RecoverClaimedInvocation durably consumes an unresolved launch claim. The
// terminal state can never claim another dispatch.
func RecoverClaimedInvocation(ctx context.Context, store InvocationIntentStore, intent InvocationIntent) (InvocationIntent, error) {
	if store == nil {
		return InvocationIntent{}, errors.New("invocation intent store is required")
	}
	unknown, err := consumedUnknownInvocation(intent)
	if err != nil {
		return InvocationIntent{}, err
	}
	updated, err := store.CompareAndSwapInvocationIntent(ctx, intent, unknown)
	if err != nil {
		return InvocationIntent{}, err
	}
	if !updated {
		return InvocationIntent{}, ErrInvocationIntentConflict
	}
	return unknown, nil
}

type ContinuationRecordKind string

const (
	ContinuationRecordSource                ContinuationRecordKind = "source_record"
	ContinuationRecordSelection             ContinuationRecordKind = "collaboration_selection"
	ContinuationRecordTLCInvocation         ContinuationRecordKind = "tlc_invocation"
	ContinuationRecordTLCReport             ContinuationRecordKind = "tlc_report"
	ContinuationRecordRouteRequirement      ContinuationRecordKind = "route_requirement"
	ContinuationRecordProviderInvocation    ContinuationRecordKind = "provider_invocation"
	ContinuationRecordArtifactCandidate     ContinuationRecordKind = "artifact_candidate"
	ContinuationRecordRepositoryObservation ContinuationRecordKind = "repository_observation"
	ContinuationRecordEvidenceCandidate     ContinuationRecordKind = "evidence_candidate"
	ContinuationRecordAuthorityEvidence     ContinuationRecordKind = "authority_evidence"
	ContinuationRecordEffectIntent          ContinuationRecordKind = "effect_intent"
	ContinuationRecordEffectObservation     ContinuationRecordKind = "effect_observation"
	ContinuationRecordPartialEvidence       ContinuationRecordKind = "partial_evidence"
	ContinuationRecordProjectionComplete    ContinuationRecordKind = "projection_complete"
	ContinuationRecordIntervention          ContinuationRecordKind = "intervention_required"
)

func (kind ContinuationRecordKind) valid() bool {
	switch kind {
	case ContinuationRecordSource, ContinuationRecordSelection, ContinuationRecordTLCInvocation,
		ContinuationRecordTLCReport, ContinuationRecordRouteRequirement, ContinuationRecordProviderInvocation,
		ContinuationRecordArtifactCandidate, ContinuationRecordRepositoryObservation,
		ContinuationRecordEvidenceCandidate, ContinuationRecordAuthorityEvidence,
		ContinuationRecordEffectIntent, ContinuationRecordEffectObservation,
		ContinuationRecordPartialEvidence, ContinuationRecordProjectionComplete,
		ContinuationRecordIntervention:
		return true
	default:
		return false
	}
}

// ContinuationRecord is the adapter-neutral durable envelope. PayloadSHA256
// binds the exact domain payload; RecordDigest binds its provenance and causal
// metadata. EventGraph remains causal truth.
type ContinuationRecord struct {
	SchemaVersion   string                 `json:"schema_version"`
	RecordID        string                 `json:"record_id"`
	Kind            ContinuationRecordKind `json:"kind"`
	ChainID         string                 `json:"chain_id"`
	SourceChainHead string                 `json:"source_chain_head"`
	Subject         string                 `json:"subject"`
	CausalRecordIDs []string               `json:"causal_record_ids"`
	Principal       Principal              `json:"principal"`
	CapturedBy      string                 `json:"captured_by"`
	ObservedTime    string                 `json:"observed_time"`
	PayloadSHA256   string                 `json:"payload_sha256"`
	Payload         json.RawMessage        `json:"payload"`
	RecordDigest    string                 `json:"record_digest"`
}

func NewContinuationRecord(kind ContinuationRecordKind, chainID, sourceHead, subject string, causes []string, principal Principal, capturedBy string, observedAt time.Time, payload any) (ContinuationRecord, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return ContinuationRecord{}, fmt.Errorf("marshal continuation payload: %w", err)
	}
	payloadDigest, err := CanonicalSHA256(json.RawMessage(encoded))
	if err != nil {
		return ContinuationRecord{}, err
	}
	record := ContinuationRecord{
		SchemaVersion: ContinuationSchemaVersion, Kind: kind, ChainID: chainID,
		SourceChainHead: sourceHead, Subject: subject, CausalRecordIDs: append([]string(nil), causes...),
		Principal: principal, CapturedBy: capturedBy, ObservedTime: observedAt.UTC().Format(time.RFC3339Nano),
		PayloadSHA256: payloadDigest, Payload: encoded,
	}
	record.RecordID = "ctc-" + HashText(chainID + "\x00" + string(kind) + "\x00" + subject + "\x00" + payloadDigest)[:32]
	digest, err := continuationRecordDigest(record)
	if err != nil {
		return ContinuationRecord{}, err
	}
	record.RecordDigest = digest
	return record, ValidateContinuationRecord(record)
}

func ValidateContinuationRecord(record ContinuationRecord) error {
	var fields []string
	if record.SchemaVersion != ContinuationSchemaVersion || !record.Kind.valid() {
		fields = append(fields, "schema_version or kind is invalid")
	}
	validateIdentifier("record_id", record.RecordID, &fields)
	validateIdentifier("chain_id", record.ChainID, &fields)
	validateDigest("source_chain_head", record.SourceChainHead, &fields)
	validateIdentifier("subject", record.Subject, &fields)
	validatePrincipal("principal", record.Principal, &fields)
	validateIdentifier("captured_by", record.CapturedBy, &fields)
	validateTime("observed_time", record.ObservedTime, &fields)
	validateUniqueIdentifiers("causal_record_ids", record.CausalRecordIDs, false, &fields)
	validateDigest("payload_sha256", record.PayloadSHA256, &fields)
	validateDigest("record_digest", record.RecordDigest, &fields)
	if len(record.Payload) == 0 || !json.Valid(record.Payload) {
		fields = append(fields, "payload must be exact valid JSON")
	} else if digest, err := CanonicalSHA256(record.Payload); err != nil || digest != record.PayloadSHA256 {
		fields = append(fields, "payload_sha256 does not match payload bytes")
	}
	if digest, err := continuationRecordDigest(record); err != nil || digest != record.RecordDigest {
		fields = append(fields, "record_digest does not match the durable envelope")
	}
	if len(fields) > 0 {
		sort.Strings(fields)
		return &ContinuationValidationError{Fields: fields}
	}
	return nil
}

func continuationRecordDigest(record ContinuationRecord) (string, error) {
	copy := record
	copy.RecordDigest = ""
	return CanonicalSHA256(copy)
}

type ContinuationWorkSeed struct {
	ChainID          string             `json:"chain_id"`
	SourceChainHead  string             `json:"source_chain_head"`
	TargetRepository RepositoryIdentity `json:"target_repository"`
	SourceEventID    string             `json:"source_event_id"`
	IdempotencyKey   string             `json:"idempotency_key"`
}

type ContinuationWorkLink struct {
	TaskID           string             `json:"task_id"`
	ArtifactID       string             `json:"artifact_id"`
	ChainID          string             `json:"chain_id"`
	SourceChainHead  string             `json:"source_chain_head"`
	TargetRepository RepositoryIdentity `json:"target_repository"`
	SourceEventID    string             `json:"source_event_id"`
	Quarantined      bool               `json:"quarantined"`
}

type ContinuationWorkArtifact struct {
	ArtifactID    string                 `json:"artifact_id"`
	ChainID       string                 `json:"chain_id"`
	RecordID      string                 `json:"record_id"`
	EventID       string                 `json:"event_id"`
	Kind          ContinuationRecordKind `json:"kind"`
	PayloadSHA256 string                 `json:"payload_sha256"`
	Payload       json.RawMessage        `json:"payload"`
}

type ContinuationWorkStore interface {
	SeedContinuation(ctx context.Context, seed ContinuationWorkSeed) (ContinuationWorkLink, error)
	GetContinuation(ctx context.Context, chainID string) (ContinuationWorkLink, error)
	AttachContinuationArtifact(ctx context.Context, artifact ContinuationWorkArtifact) (string, error)
	GetContinuationArtifact(ctx context.Context, chainID, recordID string) (ContinuationWorkArtifact, error)
	QuarantineContinuation(ctx context.Context, chainID, reason string) error
}

type InMemoryContinuationWorkStore struct {
	mu        sync.RWMutex
	links     map[string]ContinuationWorkLink
	artifacts map[string]ContinuationWorkArtifact
}

func NewInMemoryContinuationWorkStore() *InMemoryContinuationWorkStore {
	return &InMemoryContinuationWorkStore{links: make(map[string]ContinuationWorkLink), artifacts: make(map[string]ContinuationWorkArtifact)}
}

func (s *InMemoryContinuationWorkStore) SeedContinuation(ctx context.Context, seed ContinuationWorkSeed) (ContinuationWorkLink, error) {
	if err := ctx.Err(); err != nil {
		return ContinuationWorkLink{}, err
	}
	if strings.TrimSpace(seed.ChainID) == "" || !hexPattern.MatchString(seed.SourceChainHead) || seed.SourceEventID == "" || seed.IdempotencyKey == "" {
		return ContinuationWorkLink{}, errors.New("invalid continuation Work seed")
	}
	var fields []string
	validateRepository("target_repository", seed.TargetRepository, &fields)
	if len(fields) != 0 {
		return ContinuationWorkLink{}, &ContinuationValidationError{Fields: fields}
	}
	link := ContinuationWorkLink{
		TaskID: "work-continuation-" + HashText(seed.ChainID)[:24], ArtifactID: "work-continuation-seed-" + HashText(seed.IdempotencyKey)[:24],
		ChainID: seed.ChainID, SourceChainHead: seed.SourceChainHead, TargetRepository: seed.TargetRepository, SourceEventID: seed.SourceEventID,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.links[seed.ChainID]; ok {
		if existing.SourceChainHead != seed.SourceChainHead || existing.SourceEventID != seed.SourceEventID || existing.TargetRepository != seed.TargetRepository || existing.Quarantined {
			return ContinuationWorkLink{}, ErrAcceptedTupleConflict
		}
		return existing, nil
	}
	s.links[seed.ChainID] = link
	return link, nil
}

func (s *InMemoryContinuationWorkStore) GetContinuation(ctx context.Context, chainID string) (ContinuationWorkLink, error) {
	if err := ctx.Err(); err != nil {
		return ContinuationWorkLink{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	link, ok := s.links[chainID]
	if !ok {
		return ContinuationWorkLink{}, ErrWorkNotFound
	}
	return link, nil
}

func (s *InMemoryContinuationWorkStore) AttachContinuationArtifact(ctx context.Context, artifact ContinuationWorkArtifact) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if artifact.ChainID == "" || artifact.RecordID == "" || artifact.EventID == "" || !artifact.Kind.valid() || !hexPattern.MatchString(artifact.PayloadSHA256) || len(artifact.Payload) == 0 {
		return "", errors.New("invalid continuation Work artifact")
	}
	if digest, err := CanonicalSHA256(artifact.Payload); err != nil || digest != artifact.PayloadSHA256 {
		return "", errors.New("continuation Work payload digest mismatch")
	}
	if artifact.ArtifactID == "" {
		artifact.ArtifactID = "work-continuation-record-" + HashText(artifact.ChainID + "\x00" + artifact.RecordID)[:24]
	}
	key := artifact.ChainID + "\x00" + artifact.RecordID
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.artifacts[key]; ok {
		if existing.EventID != artifact.EventID || existing.PayloadSHA256 != artifact.PayloadSHA256 || string(existing.Payload) != string(artifact.Payload) {
			return "", ErrIdempotencyConflict
		}
		return existing.ArtifactID, nil
	}
	s.artifacts[key] = artifact
	return artifact.ArtifactID, nil
}

func (s *InMemoryContinuationWorkStore) GetContinuationArtifact(ctx context.Context, chainID, recordID string) (ContinuationWorkArtifact, error) {
	if err := ctx.Err(); err != nil {
		return ContinuationWorkArtifact{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	artifact, ok := s.artifacts[chainID+"\x00"+recordID]
	if !ok {
		return ContinuationWorkArtifact{}, ErrWorkNotFound
	}
	artifact.Payload = append(json.RawMessage(nil), artifact.Payload...)
	return artifact, nil
}

func (s *InMemoryContinuationWorkStore) QuarantineContinuation(ctx context.Context, chainID, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(reason) == "" {
		return errors.New("continuation quarantine reason is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	link, ok := s.links[chainID]
	if !ok {
		return ErrWorkNotFound
	}
	link.Quarantined = true
	s.links[chainID] = link
	return nil
}

// SeedContinuationSource writes causal truth before creating its Work
// projection. Replaying after either append boundary is idempotent.
func SeedContinuationSource(ctx context.Context, store Store, work ContinuationWorkStore, target RepositoryIdentity, record ContinuationRecord) (event Event, complete bool, err error) {
	if store == nil || work == nil {
		return Event{}, false, errors.New("continuation persistence requires EventGraph and Work stores")
	}
	if err := ValidateContinuationRecord(record); err != nil {
		return Event{}, false, err
	}
	if record.Kind != ContinuationRecordSource {
		return Event{}, false, errors.New("continuation seed must be a source record")
	}
	event, err = AppendTyped(ctx, store, EventContinuationRecorded, record.ChainID,
		"continuation:"+record.RecordID+":"+record.RecordDigest, nil, record)
	if err != nil {
		return Event{}, false, err
	}
	if _, err := work.SeedContinuation(ctx, ContinuationWorkSeed{
		ChainID: record.ChainID, SourceChainHead: record.SourceChainHead,
		TargetRepository: target, SourceEventID: event.ID,
		IdempotencyKey: record.RecordDigest,
	}); err != nil {
		return event, false, fmt.Errorf("seed continuation Work projection: %w", err)
	}
	return projectContinuationRecord(ctx, work, record, event)
}

// PersistContinuationRecord implements the EventGraph-first dual-store
// protocol after the source has seeded Work. A Work failure leaves replayable
// causal truth; only an exact read-back match returns complete=true.
func PersistContinuationRecord(ctx context.Context, store Store, work ContinuationWorkStore, record ContinuationRecord) (event Event, complete bool, err error) {
	if store == nil || work == nil {
		return Event{}, false, errors.New("continuation persistence requires EventGraph and Work stores")
	}
	if err := ValidateContinuationRecord(record); err != nil {
		return Event{}, false, err
	}
	event, err = AppendTyped(ctx, store, EventContinuationRecorded, record.ChainID,
		"continuation:"+record.RecordID+":"+record.RecordDigest, nil, record)
	if err != nil {
		return Event{}, false, err
	}
	return projectContinuationRecord(ctx, work, record, event)
}

func projectContinuationRecord(ctx context.Context, work ContinuationWorkStore, record ContinuationRecord, event Event) (Event, bool, error) {
	artifact := ContinuationWorkArtifact{
		ChainID: record.ChainID, RecordID: record.RecordID, EventID: event.ID,
		Kind: record.Kind, PayloadSHA256: record.PayloadSHA256, Payload: append(json.RawMessage(nil), record.Payload...),
	}
	if _, err := work.AttachContinuationArtifact(ctx, artifact); err != nil {
		return event, false, fmt.Errorf("attach continuation Work twin: %w", err)
	}
	observed, err := work.GetContinuationArtifact(ctx, record.ChainID, record.RecordID)
	if err != nil {
		return event, false, fmt.Errorf("read continuation Work twin: %w", err)
	}
	if observed.EventID != event.ID || observed.PayloadSHA256 != record.PayloadSHA256 || string(observed.Payload) != string(record.Payload) {
		_ = work.QuarantineContinuation(ctx, record.ChainID, "EventGraph and Work continuation twins conflict")
		return event, false, errors.New("continuation EventGraph and Work twins conflict")
	}
	return event, true, nil
}

// RepairContinuationProjection reconstructs only a missing Work twin from
// exact EventGraph truth. Work-only/conflicting data is never promoted.
func RepairContinuationProjection(ctx context.Context, work ContinuationWorkStore, event Event) (bool, error) {
	if event.Type != EventContinuationRecorded {
		return false, errors.New("repair requires a continuation EventGraph event")
	}
	var record ContinuationRecord
	if err := decodeStrictJSON(event.Payload, &record); err != nil {
		return false, err
	}
	if err := ValidateContinuationRecord(record); err != nil {
		return false, err
	}
	if existing, err := work.GetContinuationArtifact(ctx, record.ChainID, record.RecordID); err == nil {
		if existing.EventID != event.ID || existing.PayloadSHA256 != record.PayloadSHA256 || string(existing.Payload) != string(record.Payload) {
			_ = work.QuarantineContinuation(ctx, record.ChainID, "conflicting continuation Work twin")
			return false, errors.New("conflicting continuation Work twin")
		}
		return false, nil
	} else if !errors.Is(err, ErrWorkNotFound) {
		return false, err
	}
	_, err := work.AttachContinuationArtifact(ctx, ContinuationWorkArtifact{
		ChainID: record.ChainID, RecordID: record.RecordID, EventID: event.ID,
		Kind: record.Kind, PayloadSHA256: record.PayloadSHA256, Payload: append(json.RawMessage(nil), record.Payload...),
	})
	return err == nil, err
}

type EffectObservationState string

const (
	EffectExact    EffectObservationState = "exact"
	EffectAbsent   EffectObservationState = "absent"
	EffectConflict EffectObservationState = "conflict"
	EffectUnknown  EffectObservationState = "unknown"
)

type EffectIntent struct {
	SchemaVersion   string `json:"schema_version"`
	OperationID     string `json:"operation_id"`
	IdempotencyKey  string `json:"idempotency_key"`
	ChainID         string `json:"chain_id"`
	SourceChainHead string `json:"source_chain_head"`
	SliceID         string `json:"slice_id"`
	EffectKind      string `json:"effect_kind"`
	Subject         string `json:"subject"`
	Ordinal         int    `json:"ordinal"`
	AuthorityID     string `json:"authority_id"`
	RecordDigest    string `json:"record_digest"`
}

type EffectObservation struct {
	SchemaVersion   string                 `json:"schema_version"`
	OperationID     string                 `json:"operation_id"`
	State           EffectObservationState `json:"state"`
	ObservedSubject string                 `json:"observed_subject,omitempty"`
	ObservedTime    string                 `json:"observed_time"`
	AuthenticatedBy string                 `json:"authenticated_by"`
	Detail          string                 `json:"detail"`
	RecordDigest    string                 `json:"record_digest"`
}

func NewEffectIntent(chainID, sourceHead, sliceID, effectKind, subject string, ordinal int, authorityID string) (EffectIntent, error) {
	if ordinal < 1 {
		return EffectIntent{}, errors.New("effect ordinal must be positive")
	}
	intent := EffectIntent{
		SchemaVersion: ContinuationSchemaVersion, ChainID: chainID, SourceChainHead: sourceHead,
		SliceID: sliceID, EffectKind: effectKind, Subject: subject, Ordinal: ordinal, AuthorityID: authorityID,
	}
	intent.OperationID = "op-" + HashText(sliceID + "\x00" + effectKind + "\x00" + fmt.Sprint(ordinal))[:32]
	intent.IdempotencyKey = HashText(sourceHead + "\x00" + effectKind + "\x00" + subject)
	digest, err := CanonicalSHA256(intent)
	if err != nil {
		return EffectIntent{}, err
	}
	intent.RecordDigest = digest
	return intent, nil
}

type EffectDecision struct {
	ReuseExact bool   `json:"reuse_exact"`
	RetrySame  bool   `json:"retry_same"`
	Blocked    bool   `json:"blocked"`
	Reason     string `json:"reason"`
}

// DecideEffectContinuation applies execution safety only. TLC policy and
// external authority are supplied facts; Hive does not manufacture either.
func DecideEffectContinuation(observation EffectObservationState, tlcPermitted, authorityApplicable, budgetAvailable bool) EffectDecision {
	switch observation {
	case EffectExact:
		return EffectDecision{ReuseExact: true, Reason: "exact effect already observed"}
	case EffectAbsent:
		if tlcPermitted && authorityApplicable && budgetAvailable {
			return EffectDecision{RetrySame: true, Reason: "authenticated absence permits the same idempotent operation"}
		}
		return EffectDecision{Blocked: true, Reason: "retry lacks TLC continuation, exact external authority, or budget"}
	case EffectConflict:
		return EffectDecision{Blocked: true, Reason: "conflicting external effect requires Human disposition"}
	default:
		return EffectDecision{Blocked: true, Reason: "external effect is unknown; absence was not proven"}
	}
}

type PartialWorktreeEvidence struct {
	SchemaVersion      string            `json:"schema_version"`
	PartialID          string            `json:"partial_id"`
	ChainID            string            `json:"chain_id"`
	AttemptID          string            `json:"attempt_id"`
	GitCommonDirectory string            `json:"git_common_directory"`
	BaseCommit         string            `json:"base_commit"`
	CurrentHead        string            `json:"current_head"`
	Branch             string            `json:"branch"`
	TrackedDiffSHA256  string            `json:"tracked_diff_sha256"`
	PatchReference     string            `json:"patch_reference"`
	UntrackedDigests   map[string]string `json:"untracked_digests"`
	ProviderOutcome    string            `json:"provider_outcome"`
	AuthorLineage      string            `json:"author_lineage"`
	CaptureFailure     string            `json:"capture_failure,omitempty"`
	Authoritative      bool              `json:"authoritative"`
	Quarantined        bool              `json:"quarantined"`
	RecordDigest       string            `json:"record_digest"`
}
