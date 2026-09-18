package hive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/transpara-ai/eventgraph/go/pkg/event"
	"github.com/transpara-ai/eventgraph/go/pkg/store"
	"github.com/transpara-ai/eventgraph/go/pkg/types"
	"github.com/transpara-ai/hive/pkg/hive/factoryv1"
)

var (
	EventTypeFactoryV1IdeaRecorded          = types.MustEventType(string(factoryv1.EventIdeaRecorded))
	EventTypeFactoryV1IdeaRefined           = types.MustEventType(string(factoryv1.EventIdeaRefined))
	EventTypeFactoryV1OrderAccepted         = types.MustEventType(string(factoryv1.EventOrderAccepted))
	EventTypeFactoryV1OrderSubmitted        = types.MustEventType(string(factoryv1.EventOrderSubmitted))
	EventTypeFactoryV1StageTransitioned     = types.MustEventType(string(factoryv1.EventStageTransitioned))
	EventTypeFactoryV1InterventionRequested = types.MustEventType(string(factoryv1.EventInterventionRequested))
	EventTypeFactoryV1InterventionResolved  = types.MustEventType(string(factoryv1.EventInterventionResolved))
	EventTypeFactoryV1RecoveryRecorded      = types.MustEventType(string(factoryv1.EventRecoveryRecorded))
	EventTypeFactoryV1IssueAmendment        = types.MustEventType(string(factoryv1.EventIssueAmendmentRecorded))
	EventTypeFactoryV1ContinuationRecorded  = types.MustEventType(string(factoryv1.EventContinuationRecorded))
	EventTypeFactoryV1InvocationIntent      = types.MustEventType(string(factoryv1.EventInvocationIntent))
)

func factoryV1EventTypes() []types.EventType {
	return []types.EventType{
		EventTypeFactoryV1IdeaRecorded,
		EventTypeFactoryV1IdeaRefined,
		EventTypeFactoryV1OrderAccepted,
		EventTypeFactoryV1OrderSubmitted,
		EventTypeFactoryV1StageTransitioned,
		EventTypeFactoryV1InterventionRequested,
		EventTypeFactoryV1InterventionResolved,
		EventTypeFactoryV1RecoveryRecorded,
		EventTypeFactoryV1IssueAmendment,
		EventTypeFactoryV1ContinuationRecorded,
		EventTypeFactoryV1InvocationIntent,
	}
}

// FactoryV1EventContent is the stable EventGraph envelope for the adapter-neutral
// v1 core. EventGraph supplies causal ordering, identity, signing and hash-chain
// integrity; the core payload remains strict JSON owned by factoryv1.
type FactoryV1EventContent struct {
	hiveContent
	Type           factoryv1.EventType `json:"type"`
	OrderID        string              `json:"order_id,omitempty"`
	Causes         []string            `json:"causes"`
	IdempotencyKey string              `json:"idempotency_key"`
	OccurredAt     time.Time           `json:"occurred_at"`
	Payload        json.RawMessage     `json:"payload"`
}

func (c FactoryV1EventContent) EventTypeName() string { return string(c.Type) }

func registerFactoryV1ContentUnmarshalers() {
	for _, eventType := range factoryV1EventTypes() {
		event.RegisterContentUnmarshaler(eventType.Value(), event.Unmarshal[FactoryV1EventContent])
	}
}

// FactoryV1EventGraphStore implements the core Store on the shared append-only
// EventGraph. An idempotency key is globally unique across v1 event types.
type FactoryV1EventGraphStore struct {
	store   store.Store
	factory *event.EventFactory
	signer  event.Signer
	actor   types.ActorID
	conv    types.ConversationID
}

func NewFactoryV1EventGraphStore(s store.Store, factory *event.EventFactory, signer event.Signer, actor types.ActorID, conv types.ConversationID) (*FactoryV1EventGraphStore, error) {
	if s == nil || factory == nil || signer == nil || actor.Value() == "" || conv.Value() == "" {
		return nil, errors.New("factory v1 EventGraph store requires store, factory, signer, actor, and conversation")
	}
	return &FactoryV1EventGraphStore{store: s, factory: factory, signer: signer, actor: actor, conv: conv}, nil
}

func (s *FactoryV1EventGraphStore) Append(ctx context.Context, input factoryv1.NewEvent) (factoryv1.Event, error) {
	if err := ctx.Err(); err != nil {
		return factoryv1.Event{}, err
	}
	if input.IdempotencyKey == "" {
		return factoryv1.Event{}, errors.New("factory v1 idempotency key is required")
	}
	payload, err := json.Marshal(input.Payload)
	if err != nil {
		return factoryv1.Event{}, fmt.Errorf("marshal factory v1 payload: %w", err)
	}
	wanted := FactoryV1EventContent{
		Type:           input.Type,
		OrderID:        input.OrderID,
		Causes:         append([]string(nil), input.Causes...),
		IdempotencyKey: input.IdempotencyKey,
		OccurredAt:     input.OccurredAt.UTC(),
		Payload:        payload,
	}
	if wanted.OccurredAt.IsZero() {
		wanted.OccurredAt = time.Now().UTC()
	}
	if existing, found, err := s.byIdempotencyKey(ctx, input.IdempotencyKey); err != nil {
		return factoryv1.Event{}, err
	} else if found {
		if factoryV1ContentsEqual(existing.content, wanted) {
			return coreFactoryV1Event(existing.event, existing.content), nil
		}
		return factoryv1.Event{}, fmt.Errorf("%w: key %q already names event %s", factoryv1.ErrIdempotencyConflict, input.IdempotencyKey, existing.event.ID().Value())
	}

	eventType, err := types.NewEventType(string(input.Type))
	if err != nil {
		return factoryv1.Event{}, fmt.Errorf("factory v1 event type: %w", err)
	}
	const maxChainAttempts = 8
	for attempt := 0; attempt < maxChainAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return factoryv1.Event{}, err
		}
		// A concurrent writer may have committed this idempotency key while a
		// prior append raced on the global EventGraph head.
		if attempt > 0 {
			if existing, found, lookupErr := s.byIdempotencyKey(ctx, input.IdempotencyKey); lookupErr != nil {
				return factoryv1.Event{}, lookupErr
			} else if found {
				if factoryV1ContentsEqual(existing.content, wanted) {
					return coreFactoryV1Event(existing.event, existing.content), nil
				}
				return factoryv1.Event{}, fmt.Errorf("%w: key %q already names event %s", factoryv1.ErrIdempotencyConflict, input.IdempotencyKey, existing.event.ID().Value())
			}
		}
		causes, causeErr := s.eventCauses(input.Causes)
		if causeErr != nil {
			return factoryv1.Event{}, causeErr
		}
		ev, createErr := s.factory.Create(eventType, s.actor, wanted, causes, s.conv, s.store, s.signer)
		if createErr != nil {
			return factoryv1.Event{}, fmt.Errorf("create factory v1 EventGraph event: %w", createErr)
		}
		appended, appendErr := s.store.Append(ev)
		if appendErr == nil {
			return coreFactoryV1Event(appended, wanted), nil
		}
		if !strings.Contains(appendErr.Error(), "chain integrity violation") || attempt == maxChainAttempts-1 {
			return factoryv1.Event{}, fmt.Errorf("append factory v1 EventGraph event: %w", appendErr)
		}
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}
	return factoryv1.Event{}, errors.New("append factory v1 EventGraph event: exhausted chain retries")
}

func (s *FactoryV1EventGraphStore) List(ctx context.Context) ([]factoryv1.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var result []factoryv1.Event
	for _, eventType := range factoryV1EventTypes() {
		cursor := types.None[types.Cursor]()
		for {
			page, err := s.store.ByType(eventType, 200, cursor)
			if err != nil {
				return nil, fmt.Errorf("list %s: %w", eventType.Value(), err)
			}
			for _, ev := range page.Items() {
				content, ok := ev.Content().(FactoryV1EventContent)
				if !ok {
					return nil, fmt.Errorf("event %s content is %T, want FactoryV1EventContent", ev.ID().Value(), ev.Content())
				}
				result = append(result, coreFactoryV1Event(ev, content))
			}
			if !page.HasMore() {
				break
			}
			cursor = page.Cursor()
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].OccurredAt.Equal(result[j].OccurredAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].OccurredAt.Before(result[j].OccurredAt)
	})
	return result, nil
}

type factoryV1StoredEvent struct {
	event   event.Event
	content FactoryV1EventContent
}

func (s *FactoryV1EventGraphStore) byIdempotencyKey(ctx context.Context, key string) (factoryV1StoredEvent, bool, error) {
	for _, eventType := range factoryV1EventTypes() {
		cursor := types.None[types.Cursor]()
		for {
			if err := ctx.Err(); err != nil {
				return factoryV1StoredEvent{}, false, err
			}
			page, err := s.store.ByType(eventType, 200, cursor)
			if err != nil {
				return factoryV1StoredEvent{}, false, err
			}
			for _, ev := range page.Items() {
				content, ok := ev.Content().(FactoryV1EventContent)
				if !ok {
					return factoryV1StoredEvent{}, false, fmt.Errorf("event %s content is %T", ev.ID().Value(), ev.Content())
				}
				if content.IdempotencyKey == key {
					return factoryV1StoredEvent{event: ev, content: content}, true, nil
				}
			}
			if !page.HasMore() {
				break
			}
			cursor = page.Cursor()
		}
	}
	return factoryV1StoredEvent{}, false, nil
}

func (s *FactoryV1EventGraphStore) eventCauses(refs []string) ([]types.EventID, error) {
	causes := make([]types.EventID, 0, len(refs))
	for _, ref := range refs {
		id, err := types.NewEventID(ref)
		if err != nil {
			return nil, fmt.Errorf("factory v1 cause %q: %w", ref, err)
		}
		causes = append(causes, id)
	}
	if len(causes) != 0 {
		return causes, nil
	}
	head, err := s.store.Head()
	if err != nil {
		return nil, fmt.Errorf("factory v1 EventGraph head: %w", err)
	}
	if head.IsNone() {
		return nil, errors.New("factory v1 EventGraph requires a bootstrap event")
	}
	return []types.EventID{head.Unwrap().ID()}, nil
}

func factoryV1ContentsEqual(left, right FactoryV1EventContent) bool {
	if left.Type != right.Type || left.OrderID != right.OrderID || left.IdempotencyKey != right.IdempotencyKey || !bytes.Equal(left.Payload, right.Payload) || len(left.Causes) != len(right.Causes) {
		return false
	}
	for i := range left.Causes {
		if left.Causes[i] != right.Causes[i] {
			return false
		}
	}
	return true
}

func coreFactoryV1Event(ev event.Event, content FactoryV1EventContent) factoryv1.Event {
	return factoryv1.Event{
		ID:             ev.ID().Value(),
		Type:           content.Type,
		OrderID:        content.OrderID,
		Causes:         append([]string(nil), content.Causes...),
		IdempotencyKey: content.IdempotencyKey,
		OccurredAt:     content.OccurredAt,
		Payload:        append(json.RawMessage(nil), content.Payload...),
	}
}
