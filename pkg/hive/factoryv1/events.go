package factoryv1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type EventType string

const (
	EventIdeaRecorded           EventType = "factory.v1.idea.recorded"
	EventIdeaRefined            EventType = "factory.v1.idea.refined"
	EventOrderAccepted          EventType = "factory.v1.order.accepted"
	EventOrderSubmitted         EventType = "factory.v1.order.submitted"
	EventStageTransitioned      EventType = "factory.v1.stage.transitioned"
	EventInterventionRequested  EventType = "factory.v1.intervention.requested"
	EventInterventionResolved   EventType = "factory.v1.intervention.resolved"
	EventRecoveryRecorded       EventType = "factory.v1.recovery.recorded"
	EventIssueAmendmentRecorded EventType = "factory.v1.issue.amendment.blocked"
	EventContinuationRecorded   EventType = "civilization.tlc.continuation.v1.recorded"
	EventInvocationIntent       EventType = "civilization.tlc.continuation.v1.invocation.intent"
)

func (t EventType) valid() bool {
	switch t {
	case EventIdeaRecorded, EventIdeaRefined, EventOrderAccepted, EventOrderSubmitted, EventStageTransitioned,
		EventInterventionRequested, EventInterventionResolved, EventRecoveryRecorded,
		EventIssueAmendmentRecorded, EventContinuationRecorded, EventInvocationIntent:
		return true
	default:
		return false
	}
}

type Event struct {
	ID             string          `json:"id"`
	Type           EventType       `json:"type"`
	OrderID        string          `json:"order_id,omitempty"`
	Causes         []string        `json:"causes"`
	IdempotencyKey string          `json:"idempotency_key"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Payload        json.RawMessage `json:"payload"`
}

type NewEvent struct {
	Type           EventType
	OrderID        string
	Causes         []string
	IdempotencyKey string
	OccurredAt     time.Time
	Payload        any
}

type Store interface {
	Append(ctx context.Context, event NewEvent) (Event, error)
	List(ctx context.Context) ([]Event, error)
}

var ErrIdempotencyConflict = errors.New("factory v1 idempotency conflict")

type InMemoryStore struct {
	mu     sync.RWMutex
	events []Event
	keys   map[string]Event
	clock  Clock
}

func NewInMemoryStore(clock Clock) *InMemoryStore {
	if clock == nil {
		clock = WallClock{}
	}
	return &InMemoryStore{keys: make(map[string]Event), clock: clock}
}

func (s *InMemoryStore) Append(ctx context.Context, input NewEvent) (Event, error) {
	if err := ctx.Err(); err != nil {
		return Event{}, err
	}
	if !input.Type.valid() {
		return Event{}, fmt.Errorf("unknown factory v1 event type %q", input.Type)
	}
	if input.IdempotencyKey == "" {
		return Event{}, errors.New("factory v1 event idempotency key is required")
	}
	payload, err := json.Marshal(input.Payload)
	if err != nil {
		return Event{}, fmt.Errorf("marshal factory v1 event: %w", err)
	}
	when := input.OccurredAt.UTC()
	if when.IsZero() {
		when = s.clock.Now().UTC()
	}
	event := Event{
		Type:           input.Type,
		OrderID:        input.OrderID,
		Causes:         append([]string(nil), input.Causes...),
		IdempotencyKey: input.IdempotencyKey,
		OccurredAt:     when,
		Payload:        payload,
	}
	event.ID = eventIdentity(event)

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, exists := s.keys[input.IdempotencyKey]; exists {
		if sameEvent(existing, event) {
			return cloneEvent(existing), nil
		}
		return Event{}, fmt.Errorf("%w: key %q already names event %s", ErrIdempotencyConflict, input.IdempotencyKey, existing.ID)
	}
	s.events = append(s.events, event)
	s.keys[input.IdempotencyKey] = event
	return cloneEvent(event), nil
}

func (s *InMemoryStore) List(ctx context.Context) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Event, len(s.events))
	for i, event := range s.events {
		result[i] = cloneEvent(event)
	}
	return result, nil
}

func eventIdentity(event Event) string {
	input := string(event.Type) + "\x00" + event.OrderID + "\x00" + event.IdempotencyKey + "\x00" + string(event.Payload)
	sum := sha256.Sum256([]byte(input))
	return "fv1-" + hex.EncodeToString(sum[:16])
}

func sameEvent(left, right Event) bool {
	return left.Type == right.Type && left.OrderID == right.OrderID &&
		string(left.Payload) == string(right.Payload) && equalStrings(left.Causes, right.Causes)
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func cloneEvent(event Event) Event {
	event.Causes = append([]string(nil), event.Causes...)
	event.Payload = append(json.RawMessage(nil), event.Payload...)
	return event
}

func decodeEvent[T any](event Event) (T, error) {
	var result T
	if err := json.Unmarshal(event.Payload, &result); err != nil {
		return result, fmt.Errorf("decode %s event %s: %w", event.Type, event.ID, err)
	}
	return result, nil
}

func eventsByTime(events []Event) []Event {
	result := append([]Event(nil), events...)
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].OccurredAt.Before(result[j].OccurredAt)
	})
	return result
}

type IdeaRevisionPayload struct {
	IdeaID           string       `json:"idea_id"`
	Revision         int          `json:"revision"`
	Note             string       `json:"note"`
	Candidate        FactoryOrder `json:"candidate"`
	CandidateSHA256  string       `json:"candidate_sha256,omitempty"`
	ValidationErrors []string     `json:"validation_errors"`
	ActorID          string       `json:"actor_id"`
}

type OrderAcceptedPayload struct {
	Document              CanonicalDocument     `json:"document"`
	SourceIdentity        string                `json:"source_identity"`
	SourceEventIDs        []string              `json:"source_event_ids"`
	HumanApprovalBasis    ApprovalBasis         `json:"human_approval_basis,omitempty"`
	HumanApprovalReceipt  *HumanApprovalReceipt `json:"human_approval_receipt,omitempty"`
	AcceptedByActorID     string                `json:"accepted_by_actor_id"`
	CredentialKeyID       string                `json:"credential_key_id,omitempty"`
	WorkSeedIdempotencyID string                `json:"work_seed_idempotency_id"`
}

type CompletedOrderSubmittedPayload struct {
	Document        CanonicalDocument `json:"document"`
	ActorID         string            `json:"actor_id"`
	CredentialKeyID string            `json:"credential_key_id"`
}

type IssueAmendmentPayload struct {
	SourceIdentity    string `json:"source_identity"`
	ActiveOrderID     string `json:"active_order_id"`
	PriorSourceSHA256 string `json:"prior_source_sha256"`
	NewSourceSHA256   string `json:"new_source_sha256"`
	Reason            string `json:"reason"`
}

type RecoveryPayload struct {
	OrderID       string       `json:"order_id"`
	Stage         Stage        `json:"stage"`
	AttemptID     string       `json:"attempt_id"`
	Observation   int          `json:"observation"`
	EffectFound   bool         `json:"effect_found"`
	Conflict      bool         `json:"conflict"`
	Evidence      []Evidence   `json:"evidence"`
	RecoveredFrom string       `json:"recovered_from"`
	Result        RunnerStatus `json:"result"`
}

func AppendTyped(ctx context.Context, store Store, eventType EventType, orderID, key string, causes []string, payload any) (Event, error) {
	return store.Append(ctx, NewEvent{
		Type:           eventType,
		OrderID:        orderID,
		Causes:         append([]string(nil), causes...),
		IdempotencyKey: key,
		Payload:        payload,
	})
}
