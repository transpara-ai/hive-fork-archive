package civilization

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type EventType string

const (
	EventWorkerAssigned        EventType = "civilization.worker.assigned"
	EventResultReviewed        EventType = "civilization.result.reviewed"
	EventIntakeAccepted        EventType = "civilization.intake.accepted"
	EventTLCRouted             EventType = "civilization.tlc.routed"
	EventWorkAccepted          EventType = "civilization.work.accepted"
	EventStateChanged          EventType = "civilization.state.changed"
	EventProviderResult        EventType = "civilization.provider.result.recorded"
	EventPullRequestObserved   EventType = "civilization.pr.observed"
	EventInterventionRequested EventType = "civilization.intervention.requested"
	EventInterventionResolved  EventType = "civilization.intervention.resolved"
	EventHumanOwnerAssigned    EventType = "civilization.human.owner.assigned"
	EventMergeDecision         EventType = "civilization.merge.decision.recorded"
	EventMergeQueued           EventType = "civilization.merge.queued"
)

func (t EventType) valid() bool {
	switch t {
	case EventWorkerAssigned, EventResultReviewed, EventIntakeAccepted, EventTLCRouted, EventWorkAccepted, EventStateChanged,
		EventProviderResult, EventPullRequestObserved, EventInterventionRequested,
		EventInterventionResolved, EventHumanOwnerAssigned, EventMergeDecision, EventMergeQueued:
		return true
	default:
		return false
	}
}

type Event struct {
	ID             string          `json:"id"`
	Type           EventType       `json:"type"`
	WorkID         string          `json:"work_id"`
	Causes         []string        `json:"causes"`
	IdempotencyKey string          `json:"idempotency_key"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Payload        json.RawMessage `json:"payload"`
}

type NewEvent struct {
	Type           EventType
	WorkID         string
	Causes         []string
	IdempotencyKey string
	OccurredAt     time.Time
	Payload        any
}

type Store interface {
	Append(ctx context.Context, event NewEvent) (Event, error)
	List(ctx context.Context) ([]Event, error)
}

var ErrIdempotencyConflict = errors.New("civilization idempotency conflict")

type InMemoryStore struct {
	mu     sync.RWMutex
	events []Event
	keys   map[string]Event
	clock  func() time.Time
}

func NewInMemoryStore(clock func() time.Time) *InMemoryStore {
	if clock == nil {
		clock = time.Now
	}
	return &InMemoryStore{keys: map[string]Event{}, clock: clock}
}

func (s *InMemoryStore) Append(ctx context.Context, input NewEvent) (Event, error) {
	if err := ctx.Err(); err != nil {
		return Event{}, err
	}
	if !input.Type.valid() || input.WorkID == "" || input.IdempotencyKey == "" {
		return Event{}, errors.New("event type, work id, and idempotency key are required")
	}
	payload, err := json.Marshal(input.Payload)
	if err != nil {
		return Event{}, fmt.Errorf("marshal event payload: %w", err)
	}
	when := input.OccurredAt.UTC()
	if when.IsZero() {
		when = s.clock().UTC()
	}
	event := Event{
		Type: input.Type, WorkID: input.WorkID, Causes: append([]string(nil), input.Causes...),
		IdempotencyKey: input.IdempotencyKey, OccurredAt: when, Payload: payload,
	}
	event.ID = eventID(event)

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.keys[event.IdempotencyKey]; ok {
		if sameEvent(existing, event) {
			return cloneEvent(existing), nil
		}
		return Event{}, fmt.Errorf("%w: %s", ErrIdempotencyConflict, event.IdempotencyKey)
	}
	s.events = append(s.events, event)
	s.keys[event.IdempotencyKey] = event
	return cloneEvent(event), nil
}

func (s *InMemoryStore) List(ctx context.Context) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Event, len(s.events))
	for i := range s.events {
		result[i] = cloneEvent(s.events[i])
	}
	return result, nil
}

func appendEvent(ctx context.Context, store Store, eventType EventType, workID, key string, causes []string, payload any) (Event, error) {
	return store.Append(ctx, NewEvent{
		Type: eventType, WorkID: workID, IdempotencyKey: key,
		Causes: append([]string(nil), causes...), Payload: payload,
	})
}

func eventID(event Event) string {
	hash := sha256.New()
	for _, value := range []string{string(event.Type), event.WorkID, event.IdempotencyKey, string(event.Payload)} {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return "civ-" + hex.EncodeToString(hash.Sum(nil)[:16])
}

func sameEvent(left, right Event) bool {
	if left.Type != right.Type || left.WorkID != right.WorkID ||
		left.IdempotencyKey != right.IdempotencyKey ||
		!bytes.Equal(left.Payload, right.Payload) || len(left.Causes) != len(right.Causes) {
		return false
	}
	for i := range left.Causes {
		if left.Causes[i] != right.Causes[i] {
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

func decodePayload[T any](event Event) (T, error) {
	var result T
	if err := json.Unmarshal(event.Payload, &result); err != nil {
		return result, fmt.Errorf("decode %s event %s: %w", event.Type, event.ID, err)
	}
	return result, nil
}
