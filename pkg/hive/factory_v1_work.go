package hive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/transpara-ai/eventgraph/go/pkg/event"
	"github.com/transpara-ai/eventgraph/go/pkg/store"
	"github.com/transpara-ai/eventgraph/go/pkg/types"
	"github.com/transpara-ai/hive/pkg/hive/factoryv1"
	"github.com/transpara-ai/work"
)

const (
	factoryV1WorkMetadataLabel           = "factory_v1_order"
	factoryV1WorkQuarantineLabel         = "factory_v1_quarantine"
	factoryV1ContinuationSeedLabel       = "civilization_tlc_continuation_v1"
	factoryV1ContinuationRecordPrefix    = "civilization_tlc_continuation_v1_record:"
	factoryV1ContinuationQuarantineLabel = "civilization_tlc_continuation_v1_quarantine"
	factoryV1WorkPageSize                = 256
)

type factoryV1WorkMetadata struct {
	SchemaVersion   string            `json:"schema_version"`
	OrderID         string            `json:"order_id"`
	Version         string            `json:"version"`
	DocumentSHA256  string            `json:"document_sha256"`
	Markdown        string            `json:"markdown"`
	SourceSHA256    string            `json:"source_sha256"`
	AcceptedEventID string            `json:"accepted_event_id"`
	IdempotencyKey  string            `json:"idempotency_key"`
	Metadata        map[string]string `json:"metadata"`
}

type factoryV1WorkQuarantine struct {
	SchemaVersion string `json:"schema_version"`
	OrderID       string `json:"order_id"`
	Version       string `json:"version"`
	Reason        string `json:"reason"`
}

type factoryV1ContinuationWorkSeed struct {
	SchemaVersion    string                       `json:"schema_version"`
	ChainID          string                       `json:"chain_id"`
	SourceChainHead  string                       `json:"source_chain_head"`
	TargetRepository factoryv1.RepositoryIdentity `json:"target_repository"`
	SourceEventID    string                       `json:"source_event_id"`
	IdempotencyKey   string                       `json:"idempotency_key"`
}

type factoryV1ContinuationWorkQuarantine struct {
	SchemaVersion string `json:"schema_version"`
	ChainID       string `json:"chain_id"`
	Reason        string `json:"reason"`
}

// FactoryV1WorkStore projects accepted FactoryOrders and stage evidence onto
// the existing Work TaskStore. EventGraph remains canonical: every Work task
// is causally descended from its accepted-order event.
type FactoryV1WorkStore struct {
	store          store.Store
	tasks          *work.TaskStore
	actor          types.ActorID
	conv           types.ConversationID
	continuationMu sync.Mutex
}

func NewFactoryV1WorkStore(s store.Store, factory *event.EventFactory, signer event.Signer, actor types.ActorID, conv types.ConversationID) (*FactoryV1WorkStore, error) {
	if s == nil || factory == nil || signer == nil || actor.Value() == "" || conv.Value() == "" {
		return nil, errors.New("factory v1 Work store requires store, factory, signer, actor, and conversation")
	}
	return &FactoryV1WorkStore{store: s, tasks: work.NewTaskStore(s, factory, signer), actor: actor, conv: conv}, nil
}

func (s *FactoryV1WorkStore) GetFactoryOrder(ctx context.Context, orderID, version string) (factoryv1.WorkLink, error) {
	if err := ctx.Err(); err != nil {
		return factoryv1.WorkLink{}, err
	}
	links, err := s.ListFactoryOrders(ctx)
	if err != nil {
		return factoryv1.WorkLink{}, err
	}
	for _, link := range links {
		if link.OrderID == orderID && link.Version == version {
			return link, nil
		}
	}
	return factoryv1.WorkLink{}, factoryv1.ErrWorkNotFound
}

func (s *FactoryV1WorkStore) SeedFactoryOrder(ctx context.Context, seed factoryv1.WorkSeed) (factoryv1.WorkLink, error) {
	if err := ctx.Err(); err != nil {
		return factoryv1.WorkLink{}, err
	}
	if existing, err := s.GetFactoryOrder(ctx, seed.OrderID, seed.Version); err == nil {
		if existing.DocumentSHA256 != seed.DocumentSHA256 || existing.AcceptedEventID != seed.AcceptedEventID || existing.Quarantined {
			return factoryv1.WorkLink{}, factoryv1.ErrAcceptedTupleConflict
		}
		return existing, nil
	} else if !errors.Is(err, factoryv1.ErrWorkNotFound) {
		return factoryv1.WorkLink{}, err
	}
	acceptedID, err := types.NewEventID(seed.AcceptedEventID)
	if err != nil {
		return factoryv1.WorkLink{}, fmt.Errorf("accepted EventGraph event id: %w", err)
	}
	workID := "fo_" + factoryv1.HashText(seed.OrderID + "\x00" + seed.Version)[:24]
	task, err := work.SeedFactoryOrder(s.tasks, s.actor, work.FactoryOrder{
		Kind:                   work.OrderSoftwarePR,
		ID:                     workID,
		Title:                  "Factory v1: " + seed.OrderID,
		Intent:                 seed.Markdown,
		Cell:                   "implementation",
		RiskClass:              "high",
		DefinitionOfDone:       "The exact FactoryOrder reaches Human Review with an inspectable ready PR.",
		AcceptanceCriteria:     "Accepted EventGraph event: " + seed.AcceptedEventID + "\nDocument SHA-256: " + seed.DocumentSHA256,
		TestPlan:               "Execute the named FactoryOrder tests and retain exact evidence.",
		RequirementIDs:         []string{"req_" + factoryv1.HashText(seed.OrderID + ":requirements")[:24]},
		AcceptanceCriterionIDs: []string{"ac_" + factoryv1.HashText(seed.OrderID + ":acceptance")[:24]},
		ExpectedOutputs:        []string{"ready-to-approve pull request", "TLC evidence ledger"},
	}, []types.EventID{acceptedID}, s.conv)
	if err != nil {
		return factoryv1.WorkLink{}, fmt.Errorf("seed Work FactoryOrder: %w", err)
	}
	metadata := factoryV1WorkMetadata{
		SchemaVersion: factoryv1.SchemaVersion, OrderID: seed.OrderID, Version: seed.Version,
		DocumentSHA256: seed.DocumentSHA256, Markdown: seed.Markdown, SourceSHA256: seed.SourceSHA256,
		AcceptedEventID: seed.AcceptedEventID, IdempotencyKey: seed.IdempotencyKey,
		Metadata: cloneFactoryV1StringMap(seed.Metadata),
	}
	body, err := json.Marshal(metadata)
	if err != nil {
		return factoryv1.WorkLink{}, fmt.Errorf("marshal FactoryOrder Work artifact: %w", err)
	}
	if err := s.tasks.AddArtifact(s.actor, task.ID, factoryV1WorkMetadataLabel, "application/json", string(body), []types.EventID{acceptedID, task.ID}, s.conv); err != nil {
		return factoryv1.WorkLink{}, fmt.Errorf("attach FactoryOrder Work artifact: %w", err)
	}
	return s.GetFactoryOrder(ctx, seed.OrderID, seed.Version)
}

func (s *FactoryV1WorkStore) ListFactoryOrders(ctx context.Context) ([]factoryv1.WorkLink, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type artifactRecord struct {
		id    string
		label string
		body  string
	}
	artifactsByTask := make(map[types.EventID][]artifactRecord)
	if err := s.forEachWorkEvent(ctx, work.EventTypeTaskArtifact, func(item event.Event) error {
		content, ok := item.Content().(work.TaskArtifactContent)
		if !ok {
			return nil
		}
		artifactsByTask[content.TaskID] = append(artifactsByTask[content.TaskID], artifactRecord{
			id: item.ID().Value(), label: content.Label, body: content.Body,
		})
		return nil
	}); err != nil {
		return nil, fmt.Errorf("list Work artifacts: %w", err)
	}
	links := make([]factoryv1.WorkLink, 0)
	if err := s.forEachWorkEvent(ctx, work.EventTypeTaskCreated, func(item event.Event) error {
		if _, ok := item.Content().(work.TaskCreatedContent); !ok {
			return nil
		}
		taskID := item.ID()
		artifacts := artifactsByTask[taskID]
		var metadata *factoryV1WorkMetadata
		var artifactID string
		quarantined := false
		for _, artifact := range artifacts {
			switch artifact.label {
			case factoryV1WorkMetadataLabel:
				var candidate factoryV1WorkMetadata
				if err := json.Unmarshal([]byte(artifact.body), &candidate); err != nil {
					return fmt.Errorf("decode FactoryOrder Work artifact %s: %w", artifact.id, err)
				}
				if candidate.SchemaVersion != factoryv1.SchemaVersion {
					return fmt.Errorf("unsupported FactoryOrder Work artifact schema %q", candidate.SchemaVersion)
				}
				if metadata != nil {
					if !reflect.DeepEqual(*metadata, candidate) {
						return fmt.Errorf("conflicting duplicate FactoryOrder Work artifact %s", artifact.id)
					}
					continue
				}
				copy := candidate
				metadata = &copy
				artifactID = artifact.id
			case factoryV1WorkQuarantineLabel:
				quarantined = true
			}
		}
		if metadata == nil {
			return nil
		}
		links = append(links, factoryv1.WorkLink{
			TaskID: taskID.Value(), ArtifactID: artifactID, OrderID: metadata.OrderID,
			Version: metadata.Version, DocumentSHA256: metadata.DocumentSHA256,
			AcceptedEventID: metadata.AcceptedEventID, Quarantined: quarantined,
			Metadata: cloneFactoryV1StringMap(metadata.Metadata),
		})
		return nil
	}); err != nil {
		return nil, fmt.Errorf("list work.task.created events: %w", err)
	}
	return links, nil
}

func (s *FactoryV1WorkStore) forEachWorkEvent(ctx context.Context, eventType types.EventType, visit func(event.Event) error) error {
	cursor := types.None[types.Cursor]()
	seenCursors := make(map[string]struct{})
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := s.store.ByType(eventType, factoryV1WorkPageSize, cursor)
		if err != nil {
			return err
		}
		for _, item := range page.Items() {
			if err := visit(item); err != nil {
				return err
			}
		}
		if !page.HasMore() {
			return nil
		}
		next := page.Cursor()
		if next.IsNone() || strings.TrimSpace(next.Unwrap().Value()) == "" {
			return errors.New("Work event page reports more records without a cursor")
		}
		value := next.Unwrap().Value()
		if _, exists := seenCursors[value]; exists {
			return fmt.Errorf("Work event cursor did not advance: %s", value)
		}
		seenCursors[value] = struct{}{}
		cursor = next
	}
}

func (s *FactoryV1WorkStore) QuarantineFactoryOrder(ctx context.Context, link factoryv1.WorkLink, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(reason) == "" {
		return errors.New("Work quarantine reason is required")
	}
	taskID, err := types.NewEventID(link.TaskID)
	if err != nil {
		return err
	}
	body, err := json.Marshal(factoryV1WorkQuarantine{SchemaVersion: factoryv1.SchemaVersion, OrderID: link.OrderID, Version: link.Version, Reason: reason})
	if err != nil {
		return err
	}
	artifacts, err := s.tasks.ListArtifacts(taskID)
	if err != nil {
		return err
	}
	for _, artifact := range artifacts {
		if artifact.Label != factoryV1WorkQuarantineLabel {
			continue
		}
		if artifact.Body != string(body) {
			return factoryv1.ErrIdempotencyConflict
		}
		return nil
	}
	causes, err := s.headCauses()
	if err != nil {
		return err
	}
	return s.tasks.AddArtifact(s.actor, taskID, factoryV1WorkQuarantineLabel, "application/json", string(body), causes, s.conv)
}

func (s *FactoryV1WorkStore) AttachStageArtifact(ctx context.Context, artifact factoryv1.WorkArtifact) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	links, err := s.ListFactoryOrders(ctx)
	if err != nil {
		return "", err
	}
	var link factoryv1.WorkLink
	for _, candidate := range links {
		if candidate.OrderID == artifact.OrderID {
			link = candidate
			break
		}
	}
	if link.TaskID == "" {
		return "", factoryv1.ErrWorkNotFound
	}
	taskID, err := types.NewEventID(link.TaskID)
	if err != nil {
		return "", err
	}
	if artifact.ArtifactID == "" {
		artifact.ArtifactID = "factory-v1-stage-" + factoryv1.HashText(artifact.OrderID + "\x00" + string(artifact.Stage) + "\x00" + artifact.AttemptID)[:24]
	}
	body, err := json.Marshal(artifact)
	if err != nil {
		return "", err
	}
	label := "factory_v1_stage:" + string(artifact.Stage) + ":" + artifact.AttemptID
	existing, err := s.tasks.ListArtifacts(taskID)
	if err != nil {
		return "", err
	}
	for _, candidate := range existing {
		if candidate.Label != label {
			continue
		}
		if candidate.Body != string(body) {
			return "", factoryv1.ErrIdempotencyConflict
		}
		return candidate.ID.Value(), nil
	}
	stageEventID, err := types.NewEventID(artifact.StageEventID)
	if err != nil {
		return "", fmt.Errorf("stage EventGraph event id: %w", err)
	}
	causes := []types.EventID{stageEventID}
	if err := s.tasks.AddArtifact(s.actor, taskID, label, "application/json", string(body), causes, s.conv); err != nil {
		return "", err
	}
	latest, err := s.tasks.ListArtifacts(taskID)
	if err != nil {
		return "", err
	}
	for _, candidate := range latest {
		if candidate.Label == label && candidate.Body == string(body) {
			return candidate.ID.Value(), nil
		}
	}
	return "", errors.New("Work stage artifact append was not observable")
}

func (s *FactoryV1WorkStore) SeedContinuation(ctx context.Context, seed factoryv1.ContinuationWorkSeed) (factoryv1.ContinuationWorkLink, error) {
	if err := ctx.Err(); err != nil {
		return factoryv1.ContinuationWorkLink{}, err
	}
	s.continuationMu.Lock()
	defer s.continuationMu.Unlock()
	if existing, err := s.GetContinuation(ctx, seed.ChainID); err == nil {
		if existing.SourceChainHead != seed.SourceChainHead || existing.SourceEventID != seed.SourceEventID || existing.TargetRepository != seed.TargetRepository || existing.Quarantined {
			return factoryv1.ContinuationWorkLink{}, factoryv1.ErrAcceptedTupleConflict
		}
		return existing, nil
	} else if !errors.Is(err, factoryv1.ErrWorkNotFound) {
		return factoryv1.ContinuationWorkLink{}, err
	}
	sourceEventID, err := types.NewEventID(seed.SourceEventID)
	if err != nil {
		return factoryv1.ContinuationWorkLink{}, fmt.Errorf("continuation source EventGraph event id: %w", err)
	}
	taskWorkspace := continuationTaskWorkspace(seed.ChainID)
	taskID, found, err := s.findContinuationTask(ctx, taskWorkspace, sourceEventID)
	if err != nil {
		return factoryv1.ContinuationWorkLink{}, err
	}
	if !found {
		task, err := s.tasks.CreateInWorkspace(
			s.actor,
			"TLC continuation: "+seed.ChainID,
			"Versioned TLC change workflow continuation for "+seed.TargetRepository.OwnerName,
			taskWorkspace,
			[]types.EventID{sourceEventID},
			s.conv,
			work.PriorityHigh,
		)
		if err != nil {
			return factoryv1.ContinuationWorkLink{}, fmt.Errorf("seed continuation Work task: %w", err)
		}
		taskID = task.ID
	}
	metadata := factoryV1ContinuationWorkSeed{
		SchemaVersion: factoryv1.ContinuationSchemaVersion, ChainID: seed.ChainID,
		SourceChainHead: seed.SourceChainHead, TargetRepository: seed.TargetRepository,
		SourceEventID: seed.SourceEventID, IdempotencyKey: seed.IdempotencyKey,
	}
	body, err := json.Marshal(metadata)
	if err != nil {
		return factoryv1.ContinuationWorkLink{}, err
	}
	if err := s.tasks.AddArtifact(s.actor, taskID, factoryV1ContinuationSeedLabel, "application/json", string(body), []types.EventID{sourceEventID, taskID}, s.conv); err != nil {
		return factoryv1.ContinuationWorkLink{}, fmt.Errorf("attach continuation Work seed: %w", err)
	}
	return s.GetContinuation(ctx, seed.ChainID)
}

func continuationTaskWorkspace(chainID string) string {
	return "tlc-continuation:" + factoryv1.HashText(chainID)[:32]
}

func (s *FactoryV1WorkStore) findContinuationTask(ctx context.Context, taskWorkspace string, sourceEventID types.EventID) (types.EventID, bool, error) {
	if err := ctx.Err(); err != nil {
		return types.EventID{}, false, err
	}
	var found types.EventID
	if err := s.forEachWorkEvent(ctx, work.EventTypeTaskCreated, func(item event.Event) error {
		content, ok := item.Content().(work.TaskCreatedContent)
		if !ok || content.Workspace != taskWorkspace {
			return nil
		}
		causes := item.Causes()
		if len(causes) != 1 || causes[0] != sourceEventID {
			return factoryv1.ErrAcceptedTupleConflict
		}
		if !found.IsZero() && found != item.ID() {
			return factoryv1.ErrAcceptedTupleConflict
		}
		found = item.ID()
		return nil
	}); err != nil {
		return types.EventID{}, false, fmt.Errorf("find continuation Work task: %w", err)
	}
	return found, !found.IsZero(), nil
}

func (s *FactoryV1WorkStore) GetContinuation(ctx context.Context, chainID string) (factoryv1.ContinuationWorkLink, error) {
	if err := ctx.Err(); err != nil {
		return factoryv1.ContinuationWorkLink{}, err
	}
	type artifactRecord struct{ id, label, body string }
	artifactsByTask := make(map[types.EventID][]artifactRecord)
	if err := s.forEachWorkEvent(ctx, work.EventTypeTaskArtifact, func(item event.Event) error {
		content, ok := item.Content().(work.TaskArtifactContent)
		if ok {
			artifactsByTask[content.TaskID] = append(artifactsByTask[content.TaskID], artifactRecord{id: item.ID().Value(), label: content.Label, body: content.Body})
		}
		return nil
	}); err != nil {
		return factoryv1.ContinuationWorkLink{}, err
	}
	var found *factoryv1.ContinuationWorkLink
	if err := s.forEachWorkEvent(ctx, work.EventTypeTaskCreated, func(item event.Event) error {
		if _, ok := item.Content().(work.TaskCreatedContent); !ok {
			return nil
		}
		var seed *factoryV1ContinuationWorkSeed
		var artifactID string
		quarantined := false
		for _, artifact := range artifactsByTask[item.ID()] {
			switch artifact.label {
			case factoryV1ContinuationSeedLabel:
				var candidate factoryV1ContinuationWorkSeed
				if err := json.Unmarshal([]byte(artifact.body), &candidate); err != nil {
					return fmt.Errorf("decode continuation Work seed %s: %w", artifact.id, err)
				}
				if candidate.SchemaVersion != factoryv1.ContinuationSchemaVersion {
					return fmt.Errorf("unsupported continuation Work schema %q", candidate.SchemaVersion)
				}
				if seed != nil && !reflect.DeepEqual(*seed, candidate) {
					return errors.New("conflicting duplicate continuation Work seed")
				}
				copy := candidate
				seed, artifactID = &copy, artifact.id
			case factoryV1ContinuationQuarantineLabel:
				quarantined = true
			}
		}
		if seed == nil || seed.ChainID != chainID {
			return nil
		}
		candidate := factoryv1.ContinuationWorkLink{
			TaskID: item.ID().Value(), ArtifactID: artifactID, ChainID: seed.ChainID,
			SourceChainHead: seed.SourceChainHead, TargetRepository: seed.TargetRepository,
			SourceEventID: seed.SourceEventID, Quarantined: quarantined,
		}
		if found != nil && !reflect.DeepEqual(*found, candidate) {
			return errors.New("conflicting continuation Work tasks")
		}
		found = &candidate
		return nil
	}); err != nil {
		return factoryv1.ContinuationWorkLink{}, err
	}
	if found == nil {
		return factoryv1.ContinuationWorkLink{}, factoryv1.ErrWorkNotFound
	}
	return *found, nil
}

func (s *FactoryV1WorkStore) AttachContinuationArtifact(ctx context.Context, artifact factoryv1.ContinuationWorkArtifact) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.continuationMu.Lock()
	defer s.continuationMu.Unlock()
	link, err := s.GetContinuation(ctx, artifact.ChainID)
	if err != nil {
		return "", err
	}
	if link.Quarantined {
		return "", errors.New("continuation Work task is quarantined")
	}
	taskID, err := types.NewEventID(link.TaskID)
	if err != nil {
		return "", err
	}
	if artifact.ArtifactID == "" {
		artifact.ArtifactID = "work-continuation-record-" + factoryv1.HashText(artifact.ChainID + "\x00" + artifact.RecordID)[:24]
	}
	body, err := json.Marshal(artifact)
	if err != nil {
		return "", err
	}
	label := factoryV1ContinuationRecordPrefix + artifact.RecordID
	artifacts, err := s.tasks.ListArtifacts(taskID)
	if err != nil {
		return "", err
	}
	for _, existing := range artifacts {
		if existing.Label != label {
			continue
		}
		if existing.Body != string(body) {
			return "", factoryv1.ErrIdempotencyConflict
		}
		return existing.ID.Value(), nil
	}
	eventID, err := types.NewEventID(artifact.EventID)
	if err != nil {
		return "", fmt.Errorf("continuation EventGraph event id: %w", err)
	}
	if err := s.tasks.AddArtifact(s.actor, taskID, label, "application/json", string(body), []types.EventID{eventID}, s.conv); err != nil {
		return "", err
	}
	return s.continuationArtifactID(taskID, label, string(body))
}

func (s *FactoryV1WorkStore) GetContinuationArtifact(ctx context.Context, chainID, recordID string) (factoryv1.ContinuationWorkArtifact, error) {
	if err := ctx.Err(); err != nil {
		return factoryv1.ContinuationWorkArtifact{}, err
	}
	link, err := s.GetContinuation(ctx, chainID)
	if err != nil {
		return factoryv1.ContinuationWorkArtifact{}, err
	}
	taskID, err := types.NewEventID(link.TaskID)
	if err != nil {
		return factoryv1.ContinuationWorkArtifact{}, err
	}
	label := factoryV1ContinuationRecordPrefix + recordID
	artifacts, err := s.tasks.ListArtifacts(taskID)
	if err != nil {
		return factoryv1.ContinuationWorkArtifact{}, err
	}
	var found *factoryv1.ContinuationWorkArtifact
	for _, artifact := range artifacts {
		if artifact.Label != label {
			continue
		}
		var candidate factoryv1.ContinuationWorkArtifact
		if err := json.Unmarshal([]byte(artifact.Body), &candidate); err != nil {
			return factoryv1.ContinuationWorkArtifact{}, err
		}
		if found != nil && !reflect.DeepEqual(*found, candidate) {
			return factoryv1.ContinuationWorkArtifact{}, errors.New("conflicting continuation Work record artifacts")
		}
		copy := candidate
		found = &copy
	}
	if found == nil {
		return factoryv1.ContinuationWorkArtifact{}, factoryv1.ErrWorkNotFound
	}
	return *found, nil
}

func (s *FactoryV1WorkStore) QuarantineContinuation(ctx context.Context, chainID, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.continuationMu.Lock()
	defer s.continuationMu.Unlock()
	if strings.TrimSpace(reason) == "" {
		return errors.New("continuation quarantine reason is required")
	}
	link, err := s.GetContinuation(ctx, chainID)
	if err != nil {
		return err
	}
	taskID, err := types.NewEventID(link.TaskID)
	if err != nil {
		return err
	}
	body, err := json.Marshal(factoryV1ContinuationWorkQuarantine{SchemaVersion: factoryv1.ContinuationSchemaVersion, ChainID: chainID, Reason: reason})
	if err != nil {
		return err
	}
	artifacts, err := s.tasks.ListArtifacts(taskID)
	if err != nil {
		return err
	}
	for _, artifact := range artifacts {
		if artifact.Label == factoryV1ContinuationQuarantineLabel {
			if artifact.Body != string(body) {
				return factoryv1.ErrIdempotencyConflict
			}
			return nil
		}
	}
	causes, err := s.headCauses()
	if err != nil {
		return err
	}
	return s.tasks.AddArtifact(s.actor, taskID, factoryV1ContinuationQuarantineLabel, "application/json", string(body), causes, s.conv)
}

func (s *FactoryV1WorkStore) continuationArtifactID(taskID types.EventID, label, body string) (string, error) {
	artifacts, err := s.tasks.ListArtifacts(taskID)
	if err != nil {
		return "", err
	}
	for _, artifact := range artifacts {
		if artifact.Label == label && artifact.Body == body {
			return artifact.ID.Value(), nil
		}
	}
	return "", errors.New("continuation Work artifact append was not observable")
}

func (s *FactoryV1WorkStore) headCauses() ([]types.EventID, error) {
	head, err := s.store.Head()
	if err != nil {
		return nil, err
	}
	if head.IsNone() {
		return nil, errors.New("Work adapter requires a bootstrap event")
	}
	return []types.EventID{head.Unwrap().ID()}, nil
}

func cloneFactoryV1StringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
