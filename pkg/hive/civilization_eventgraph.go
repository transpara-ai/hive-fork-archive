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
	civ "github.com/transpara-ai/hive/pkg/hive/civilization"
)

var civilizationEventTypeByCore = map[civ.EventType]types.EventType{
	civ.EventWorkerAssigned:        types.MustEventType(string(civ.EventWorkerAssigned)),
	civ.EventIntakeAccepted:        types.MustEventType(string(civ.EventIntakeAccepted)),
	civ.EventTLCRouted:             types.MustEventType(string(civ.EventTLCRouted)),
	civ.EventWorkAccepted:          types.MustEventType(string(civ.EventWorkAccepted)),
	civ.EventStateChanged:          types.MustEventType(string(civ.EventStateChanged)),
	civ.EventProviderResult:        types.MustEventType(string(civ.EventProviderResult)),
	civ.EventPullRequestObserved:   types.MustEventType(string(civ.EventPullRequestObserved)),
	civ.EventInterventionRequested: types.MustEventType(string(civ.EventInterventionRequested)),
	civ.EventInterventionResolved:  types.MustEventType(string(civ.EventInterventionResolved)),
	civ.EventResultReviewed:        types.MustEventType(string(civ.EventResultReviewed)),
	civ.EventHumanOwnerAssigned:    types.MustEventType(string(civ.EventHumanOwnerAssigned)),
	civ.EventMergeDecision:         types.MustEventType(string(civ.EventMergeDecision)),
	civ.EventMergeQueued:           types.MustEventType(string(civ.EventMergeQueued)),
}

func civilizationEventTypes() []types.EventType {
	result := make([]types.EventType, 0, len(civilizationEventTypeByCore))
	for _, eventType := range civilizationEventTypeByCore {
		result = append(result, eventType)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Value() < result[j].Value() })
	return result
}

type CivilizationEventContent struct {
	hiveContent
	Type           civ.EventType   `json:"type"`
	WorkID         string          `json:"work_id"`
	Causes         []string        `json:"causes"`
	IdempotencyKey string          `json:"idempotency_key"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Payload        json.RawMessage `json:"payload"`
}

func (c CivilizationEventContent) EventTypeName() string { return string(c.Type) }

func registerCivilizationContentUnmarshalers() {
	for _, eventType := range civilizationEventTypes() {
		event.RegisterContentUnmarshaler(eventType.Value(), event.Unmarshal[CivilizationEventContent])
	}
}

// CivilizationEventGraphStore persists the production operational lifecycle
// on the shared signed, append-only EventGraph.
type CivilizationEventGraphStore struct {
	store   store.Store
	factory *event.EventFactory
	signer  event.Signer
	actor   types.ActorID
	conv    types.ConversationID
}

func NewCivilizationEventGraphStore(s store.Store, factory *event.EventFactory, signer event.Signer, actor types.ActorID, conv types.ConversationID) (*CivilizationEventGraphStore, error) {
	if s == nil || factory == nil || signer == nil || actor.Value() == "" || conv.Value() == "" {
		return nil, errors.New("Civilization EventGraph store requires store, factory, signer, actor, and conversation")
	}
	return &CivilizationEventGraphStore{store: s, factory: factory, signer: signer, actor: actor, conv: conv}, nil
}

func (s *CivilizationEventGraphStore) Append(ctx context.Context, input civ.NewEvent) (civ.Event, error) {
	if err := ctx.Err(); err != nil {
		return civ.Event{}, err
	}
	eventType, known := civilizationEventTypeByCore[input.Type]
	if !known || input.WorkID == "" || input.IdempotencyKey == "" {
		return civ.Event{}, errors.New("Civilization event type, work id, and idempotency key are required")
	}
	payload, err := json.Marshal(input.Payload)
	if err != nil {
		return civ.Event{}, fmt.Errorf("marshal Civilization payload: %w", err)
	}
	wanted := CivilizationEventContent{
		Type: input.Type, WorkID: input.WorkID, Causes: append([]string(nil), input.Causes...),
		IdempotencyKey: input.IdempotencyKey, OccurredAt: input.OccurredAt.UTC(), Payload: payload,
	}
	if wanted.OccurredAt.IsZero() {
		wanted.OccurredAt = time.Now().UTC()
	}
	if existing, found, err := s.byIdempotencyKey(ctx, input.IdempotencyKey); err != nil {
		return civ.Event{}, err
	} else if found {
		if civilizationContentsEqual(existing.content, wanted) {
			return coreCivilizationEvent(existing.event, existing.content), nil
		}
		return civ.Event{}, fmt.Errorf("%w: key %q already exists", civ.ErrIdempotencyConflict, input.IdempotencyKey)
	}

	const maxChainAttempts = 8
	for attempt := 0; attempt < maxChainAttempts; attempt++ {
		if attempt > 0 {
			if existing, found, lookupErr := s.byIdempotencyKey(ctx, input.IdempotencyKey); lookupErr != nil {
				return civ.Event{}, lookupErr
			} else if found {
				if civilizationContentsEqual(existing.content, wanted) {
					return coreCivilizationEvent(existing.event, existing.content), nil
				}
				return civ.Event{}, fmt.Errorf("%w: key %q already exists", civ.ErrIdempotencyConflict, input.IdempotencyKey)
			}
		}
		causes, err := s.eventCauses(input.Causes)
		if err != nil {
			return civ.Event{}, err
		}
		ev, err := s.factory.Create(eventType, s.actor, wanted, causes, s.conv, s.store, s.signer)
		if err != nil {
			return civ.Event{}, fmt.Errorf("create Civilization EventGraph event: %w", err)
		}
		appended, err := s.store.Append(ev)
		if err == nil {
			return coreCivilizationEvent(appended, wanted), nil
		}
		if !strings.Contains(err.Error(), "chain integrity violation") || attempt == maxChainAttempts-1 {
			return civ.Event{}, fmt.Errorf("append Civilization EventGraph event: %w", err)
		}
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}
	return civ.Event{}, errors.New("append Civilization EventGraph event exhausted retries")
}

func (s *CivilizationEventGraphStore) List(ctx context.Context) ([]civ.Event, error) {
	var result []civ.Event
	for _, eventType := range civilizationEventTypes() {
		cursor := types.None[types.Cursor]()
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			page, err := s.store.ByType(eventType, 200, cursor)
			if err != nil {
				return nil, fmt.Errorf("list %s: %w", eventType.Value(), err)
			}
			for _, ev := range page.Items() {
				content, ok := ev.Content().(CivilizationEventContent)
				if !ok {
					return nil, fmt.Errorf("event %s content is %T", ev.ID().Value(), ev.Content())
				}
				result = append(result, coreCivilizationEvent(ev, content))
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

type civilizationStoredEvent struct {
	event   event.Event
	content CivilizationEventContent
}

func (s *CivilizationEventGraphStore) byIdempotencyKey(ctx context.Context, key string) (civilizationStoredEvent, bool, error) {
	for _, eventType := range civilizationEventTypes() {
		cursor := types.None[types.Cursor]()
		for {
			if err := ctx.Err(); err != nil {
				return civilizationStoredEvent{}, false, err
			}
			page, err := s.store.ByType(eventType, 200, cursor)
			if err != nil {
				return civilizationStoredEvent{}, false, err
			}
			for _, ev := range page.Items() {
				content, ok := ev.Content().(CivilizationEventContent)
				if !ok {
					return civilizationStoredEvent{}, false, fmt.Errorf("event %s content is %T", ev.ID().Value(), ev.Content())
				}
				if content.IdempotencyKey == key {
					return civilizationStoredEvent{event: ev, content: content}, true, nil
				}
			}
			if !page.HasMore() {
				break
			}
			cursor = page.Cursor()
		}
	}
	return civilizationStoredEvent{}, false, nil
}

func (s *CivilizationEventGraphStore) eventCauses(refs []string) ([]types.EventID, error) {
	causes := make([]types.EventID, 0, len(refs))
	for _, ref := range refs {
		id, err := types.NewEventID(ref)
		if err != nil {
			return nil, fmt.Errorf("Civilization cause %q: %w", ref, err)
		}
		causes = append(causes, id)
	}
	if len(causes) > 0 {
		return causes, nil
	}
	head, err := s.store.Head()
	if err != nil {
		return nil, fmt.Errorf("Civilization EventGraph head: %w", err)
	}
	if head.IsNone() {
		return nil, errors.New("Civilization EventGraph requires a bootstrap event")
	}
	return []types.EventID{head.Unwrap().ID()}, nil
}

func civilizationContentsEqual(left, right CivilizationEventContent) bool {
	if left.Type != right.Type || left.WorkID != right.WorkID || left.IdempotencyKey != right.IdempotencyKey ||
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

func coreCivilizationEvent(ev event.Event, content CivilizationEventContent) civ.Event {
	return civ.Event{
		ID: ev.ID().Value(), Type: content.Type, WorkID: content.WorkID,
		Causes: append([]string(nil), content.Causes...), IdempotencyKey: content.IdempotencyKey,
		OccurredAt: content.OccurredAt, Payload: append(json.RawMessage(nil), content.Payload...),
	}
}
