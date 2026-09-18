package hive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/transpara-ai/eventgraph/go/pkg/event"
	"github.com/transpara-ai/eventgraph/go/pkg/store"
	"github.com/transpara-ai/eventgraph/go/pkg/types"
	"github.com/transpara-ai/hive/pkg/hive/factoryv1"
	workpkg "github.com/transpara-ai/work"
)

type factoryV1FailOnceChainStore struct {
	store.Store
	failed bool
}

type factoryV1RepeatingCursorStore struct {
	store.Store
}

func (s *factoryV1RepeatingCursorStore) ByType(types.EventType, int, types.Option[types.Cursor]) (types.Page[event.Event], error) {
	return types.NewPage([]event.Event{}, types.Some(types.MustCursor("repeated-cursor")), true), nil
}

func (s *factoryV1FailOnceChainStore) Append(ev event.Event) (event.Event, error) {
	if !s.failed {
		s.failed = true
		return event.Event{}, errors.New("chain integrity violation: deterministic concurrent-head test")
	}
	return s.Store.Append(ev)
}

func TestFactoryV1EventGraphAppendRetriesConcurrentHeadRace(t *testing.T) {
	ctx := context.Background()
	base, factory, signer, actor, conversation := newDecisionTestStore(t)
	wrapped := &factoryV1FailOnceChainStore{Store: base}
	graph, err := NewFactoryV1EventGraphStore(wrapped, factory, signer, actor, conversation)
	if err != nil {
		t.Fatal(err)
	}
	input := factoryv1.NewEvent{
		Type: factoryv1.EventIssueAmendmentRecorded, OrderID: "FO-CONCURRENT-HEAD",
		IdempotencyKey: "concurrent-head-retry", OccurredAt: factoryv1.WallClock{}.Now(),
		Payload: map[string]string{"result": "converged"},
	}
	first, err := graph.Append(ctx, input)
	if err != nil {
		t.Fatalf("append after head race: %v", err)
	}
	second, err := graph.Append(ctx, input)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if first.ID == "" || second.ID != first.ID {
		t.Fatalf("retry/replay identities = (%q,%q)", first.ID, second.ID)
	}
}

func TestFactoryV1ContinuationAdaptersPersistAndReadBackTwins(t *testing.T) {
	ctx := context.Background()
	eventStore, factory, signer, actor, conversation := newDecisionTestStore(t)
	workpkg.RegisterWithRegistry(factory.Registry)
	graph, err := NewFactoryV1EventGraphStore(eventStore, factory, signer, actor, conversation)
	if err != nil {
		t.Fatal(err)
	}
	workStore, err := NewFactoryV1WorkStore(eventStore, factory, signer, actor, conversation)
	if err != nil {
		t.Fatal(err)
	}
	principal := factoryv1.Principal{Kind: "human", StableID: actor.Value(), SubjectRef: actor.Value()}
	authentication := factoryv1.Authentication{Status: "authenticated", Method: "eventgraph_actor", Reference: actor.Value(), AuthenticatedBy: "hive"}
	source, err := factoryv1.NewInlineSourceRecord(
		"adapter-continuation-1", "human_request", 0, nil, principal, authentication,
		"text/plain", "evaluate the exact change", "hive", time.Date(2026, 8, 30, 13, 0, 0, 0, time.UTC), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := factoryv1.NewContinuationRecord(
		factoryv1.ContinuationRecordSource, source.ChainID, source.RecordDigest, source.RecordDigest,
		nil, principal, "hive", time.Date(2026, 8, 30, 13, 0, 1, 0, time.UTC), source,
	)
	if err != nil {
		t.Fatal(err)
	}
	repository := factoryv1.RepositoryIdentity{Provider: "github", NumericID: 101, OwnerName: "transpara-ai/hive"}
	eventRecord, complete, err := factoryv1.SeedContinuationSource(ctx, graph, workStore, repository, envelope)
	if err != nil || !complete {
		t.Fatalf("seed continuation: complete=%v err=%v", complete, err)
	}
	link, err := workStore.GetContinuation(ctx, source.ChainID)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := workStore.GetContinuationArtifact(ctx, source.ChainID, envelope.RecordID)
	if err != nil {
		t.Fatal(err)
	}
	if link.SourceEventID != eventRecord.ID || artifact.EventID != eventRecord.ID || string(artifact.Payload) != string(envelope.Payload) {
		t.Fatalf("continuation twins differ: link=%+v artifact=%+v event=%s", link, artifact, eventRecord.ID)
	}

	replayed, complete, err := factoryv1.SeedContinuationSource(ctx, graph, workStore, repository, envelope)
	if err != nil || !complete || replayed.ID != eventRecord.ID {
		t.Fatalf("continuation replay was not idempotent: event=%s complete=%v err=%v", replayed.ID, complete, err)
	}
}

func TestFactoryV1ContinuationWorkSeedConvergesUnderContention(t *testing.T) {
	ctx := context.Background()
	eventStore, factory, signer, actor, conversation := newDecisionTestStore(t)
	workpkg.RegisterWithRegistry(factory.Registry)
	graph, err := NewFactoryV1EventGraphStore(eventStore, factory, signer, actor, conversation)
	if err != nil {
		t.Fatal(err)
	}
	workStore, err := NewFactoryV1WorkStore(eventStore, factory, signer, actor, conversation)
	if err != nil {
		t.Fatal(err)
	}
	principal := factoryv1.Principal{Kind: "human", StableID: actor.Value(), SubjectRef: actor.Value()}
	source, err := factoryv1.NewContinuationRecord(
		factoryv1.ContinuationRecordSource, "adapter-contention", strings.Repeat("a", 64), strings.Repeat("a", 64),
		nil, principal, "hive", time.Date(2026, 8, 30, 13, 0, 0, 0, time.UTC), map[string]string{"source": "exact"},
	)
	if err != nil {
		t.Fatal(err)
	}
	repository := factoryv1.RepositoryIdentity{Provider: "github", NumericID: 101, OwnerName: "transpara-ai/hive"}
	const contenders = 12
	errorsByCaller := make(chan error, contenders)
	var wait sync.WaitGroup
	for index := 0; index < contenders; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, complete, seedErr := factoryv1.SeedContinuationSource(ctx, graph, workStore, repository, source)
			if seedErr == nil && !complete {
				seedErr = errors.New("incomplete continuation seed")
			}
			errorsByCaller <- seedErr
		}()
	}
	wait.Wait()
	close(errorsByCaller)
	for seedErr := range errorsByCaller {
		if seedErr != nil {
			t.Fatalf("concurrent continuation seed: %v", seedErr)
		}
	}
	link, err := workStore.GetContinuation(ctx, source.ChainID)
	if err != nil || link.Quarantined {
		t.Fatalf("converged continuation link=%+v err=%v", link, err)
	}
	workspace := continuationTaskWorkspace(source.ChainID)
	taskID, found, err := workStore.findContinuationTask(ctx, workspace, types.MustEventID(link.SourceEventID))
	if err != nil || !found || taskID.Value() != link.TaskID {
		t.Fatalf("canonical continuation task=%s found=%v link=%s err=%v", taskID.Value(), found, link.TaskID, err)
	}
}

func TestFactoryV1DurableAdaptersRestartIdempotency(t *testing.T) {
	ctx := context.Background()
	eventStore, factory, signer, actor, conversation := newDecisionTestStore(t)
	workpkg.RegisterWithRegistry(factory.Registry)

	newBoundaries := func() (*FactoryV1EventGraphStore, *FactoryV1WorkStore, *factoryv1.Intake) {
		t.Helper()
		graph, err := NewFactoryV1EventGraphStore(eventStore, factory, signer, actor, conversation)
		if err != nil {
			t.Fatal(err)
		}
		workStore, err := NewFactoryV1WorkStore(eventStore, factory, signer, actor, conversation)
		if err != nil {
			t.Fatal(err)
		}
		intake, err := factoryv1.NewIntake(graph, workStore, factoryv1.WallClock{})
		if err != nil {
			t.Fatal(err)
		}
		return graph, workStore, intake
	}

	graph, _, intake := newBoundaries()
	order := validFactoryV1APIOrder(factoryv1.ChannelCompletedOrder, "FO-RESTART-IDEMPOTENCY", "transpara-ai/hive")
	first, err := intake.SubmitCompleted(ctx, order, actor.Value(), "credential-test")
	if err != nil {
		t.Fatal(err)
	}
	before, err := graph.List(ctx)
	if err != nil {
		t.Fatal(err)
	}

	restartedGraph, restartedWork, restartedIntake := newBoundaries()
	if err := restartedIntake.ReplayAndRepair(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := restartedIntake.SubmitCompleted(ctx, order, actor.Value(), "credential-test")
	if err != nil {
		t.Fatal(err)
	}
	after, err := restartedGraph.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	links, err := restartedWork.ListFactoryOrders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.AcceptedEventID != second.AcceptedEventID || len(after) != len(before) || len(links) != 1 {
		t.Fatalf("restart duplicated durable state: accepted=(%s,%s) events=(%d,%d) links=%d", first.AcceptedEventID, second.AcceptedEventID, len(before), len(after), len(links))
	}
}

func TestFactoryV1ReplayQuarantinesOrphanWorkOnce(t *testing.T) {
	ctx := context.Background()
	eventStore, factory, signer, actor, conversation := newDecisionTestStore(t)
	workpkg.RegisterWithRegistry(factory.Registry)
	graph, err := NewFactoryV1EventGraphStore(eventStore, factory, signer, actor, conversation)
	if err != nil {
		t.Fatal(err)
	}
	workStore, err := NewFactoryV1WorkStore(eventStore, factory, signer, actor, conversation)
	if err != nil {
		t.Fatal(err)
	}
	head, err := eventStore.Head()
	if err != nil || head.IsNone() {
		t.Fatalf("bootstrap head: %v", err)
	}
	document, err := factoryv1.Canonicalize(validFactoryV1APIOrder(factoryv1.ChannelCompletedOrder, "FO-ORPHAN", "transpara-ai/hive"))
	if err != nil {
		t.Fatal(err)
	}
	link, err := workStore.SeedFactoryOrder(ctx, factoryv1.WorkSeed{
		OrderID: document.Order.DocID, Version: document.Order.Version,
		DocumentSHA256: document.SHA256, Markdown: document.Markdown,
		SourceSHA256:    document.Order.SourceReferences[0].SHA256,
		AcceptedEventID: head.Unwrap().ID().Value(), IdempotencyKey: "orphan-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	intake, err := factoryv1.NewIntake(graph, workStore, factoryv1.WallClock{})
	if err != nil {
		t.Fatal(err)
	}
	for run := 0; run < 2; run++ {
		if err := intake.ReplayAndRepair(ctx); err != nil {
			t.Fatalf("replay %d: %v", run+1, err)
		}
	}
	quarantined, err := workStore.GetFactoryOrder(ctx, document.Order.DocID, document.Order.Version)
	if err != nil {
		t.Fatal(err)
	}
	taskID, err := types.NewEventID(link.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := workStore.tasks.ListArtifacts(taskID)
	if err != nil {
		t.Fatal(err)
	}
	quarantineCount := 0
	for _, artifact := range artifacts {
		if artifact.Label == factoryV1WorkQuarantineLabel {
			quarantineCount++
		}
	}
	if !quarantined.Quarantined || quarantineCount != 1 {
		t.Fatalf("orphan quarantine projection=%t artifacts=%d, want true and 1", quarantined.Quarantined, quarantineCount)
	}
}

func TestFactoryV1ReplayReconcilesAcceptedConflictAfterOrphanQuarantine(t *testing.T) {
	ctx := context.Background()
	eventStore, factory, signer, actor, conversation := newDecisionTestStore(t)
	workpkg.RegisterWithRegistry(factory.Registry)
	graph, err := NewFactoryV1EventGraphStore(eventStore, factory, signer, actor, conversation)
	if err != nil {
		t.Fatal(err)
	}
	workStore, err := NewFactoryV1WorkStore(eventStore, factory, signer, actor, conversation)
	if err != nil {
		t.Fatal(err)
	}
	head, err := eventStore.Head()
	if err != nil || head.IsNone() {
		t.Fatalf("bootstrap head: %v", err)
	}
	order := validFactoryV1APIOrder(factoryv1.ChannelCompletedOrder, "FO-ORPHAN-THEN-ACCEPTED", "transpara-ai/hive")
	document, err := factoryv1.Canonicalize(order)
	if err != nil {
		t.Fatal(err)
	}
	link, err := workStore.SeedFactoryOrder(ctx, factoryv1.WorkSeed{
		OrderID: document.Order.DocID, Version: document.Order.Version,
		DocumentSHA256: document.SHA256, Markdown: document.Markdown,
		SourceSHA256:    document.Order.SourceReferences[0].SHA256,
		AcceptedEventID: head.Unwrap().ID().Value(), IdempotencyKey: "orphan-then-accepted",
	})
	if err != nil {
		t.Fatal(err)
	}
	intake, err := factoryv1.NewIntake(graph, workStore, factoryv1.WallClock{})
	if err != nil {
		t.Fatal(err)
	}
	if err := intake.ReplayAndRepair(ctx); err != nil {
		t.Fatalf("quarantine orphan: %v", err)
	}
	accepted, err := graph.Append(ctx, factoryv1.NewEvent{
		Type: factoryv1.EventOrderAccepted, OrderID: document.Order.DocID,
		Causes: []string{head.Unwrap().ID().Value()}, IdempotencyKey: "accepted:" + document.Order.DocID + "@" + document.Order.Version,
		Payload: factoryv1.OrderAcceptedPayload{
			Document: document, SourceIdentity: "test:late-acceptance",
			SourceEventIDs: []string{head.Unwrap().ID().Value()}, AcceptedByActorID: actor.Value(),
			WorkSeedIdempotencyID: "factory-v1-work:" + document.Order.DocID + "@" + document.Order.Version + ":" + document.SHA256,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for replay := 1; replay <= 2; replay++ {
		if err := intake.ReplayAndRepair(ctx); err != nil {
			t.Fatalf("accepted-conflict replay %d: %v", replay, err)
		}
	}
	taskID, err := types.NewEventID(link.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := workStore.tasks.ListArtifacts(taskID)
	if err != nil {
		t.Fatal(err)
	}
	quarantines := 0
	for _, artifact := range artifacts {
		if artifact.Label == factoryV1WorkQuarantineLabel {
			quarantines++
		}
	}
	listed, err := graph.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conflictRequests := 0
	for _, candidate := range listed {
		if candidate.Type != factoryv1.EventInterventionRequested {
			continue
		}
		var payload factoryv1.InterventionRequestedPayload
		if err := json.Unmarshal(candidate.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.OrderID == document.Order.DocID && payload.Kind == "accepted_tuple_conflict" {
			conflictRequests++
			if len(candidate.Causes) != 1 || candidate.Causes[0] != accepted.ID {
				t.Fatalf("accepted-conflict causes = %+v, want %s", candidate.Causes, accepted.ID)
			}
		}
	}
	if quarantines != 1 || conflictRequests != 1 {
		t.Fatalf("quarantines=%d conflict interventions=%d, want one each", quarantines, conflictRequests)
	}
}

func TestFactoryV1WorkEnumerationRejectsMalformedDuplicateMetadata(t *testing.T) {
	ctx := context.Background()
	eventStore, factory, signer, actor, conversation := newDecisionTestStore(t)
	workpkg.RegisterWithRegistry(factory.Registry)
	workStore, err := NewFactoryV1WorkStore(eventStore, factory, signer, actor, conversation)
	if err != nil {
		t.Fatal(err)
	}
	head, err := eventStore.Head()
	if err != nil || head.IsNone() {
		t.Fatalf("bootstrap head: %v", err)
	}
	link, err := workStore.SeedFactoryOrder(ctx, factoryv1.WorkSeed{
		OrderID: "FO-DUPLICATE-METADATA", Version: "1.0.0", DocumentSHA256: factoryv1.HashText("duplicate-metadata"),
		Markdown: "# duplicate metadata\n", SourceSHA256: factoryv1.HashText("duplicate-source"),
		AcceptedEventID: head.Unwrap().ID().Value(), IdempotencyKey: "duplicate-metadata",
	})
	if err != nil {
		t.Fatal(err)
	}
	taskID, err := types.NewEventID(link.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if err := workStore.tasks.AddArtifact(actor, taskID, factoryV1WorkMetadataLabel, "application/json", "{", []types.EventID{taskID}, conversation); err != nil {
		t.Fatal(err)
	}
	if _, err := workStore.ListFactoryOrders(ctx); err == nil || !strings.Contains(err.Error(), "decode FactoryOrder Work artifact") {
		t.Fatalf("duplicate metadata error = %v, want decode failure", err)
	}
}

func TestFactoryV1WorkEnumerationRejectsDivergentDuplicateMetadata(t *testing.T) {
	workStore, actor, conversation, link, originalBody := newFactoryV1DuplicateMetadataFixture(t)
	var divergent factoryV1WorkMetadata
	if err := json.Unmarshal([]byte(originalBody), &divergent); err != nil {
		t.Fatal(err)
	}
	divergent.DocumentSHA256 = factoryv1.HashText("forged-duplicate-metadata")
	divergentBody, err := json.Marshal(divergent)
	if err != nil {
		t.Fatal(err)
	}
	taskID, err := types.NewEventID(link.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if err := workStore.tasks.AddArtifact(actor, taskID, factoryV1WorkMetadataLabel, "application/json", string(divergentBody), []types.EventID{taskID}, conversation); err != nil {
		t.Fatal(err)
	}
	if _, err := workStore.ListFactoryOrders(context.Background()); err == nil || !strings.Contains(err.Error(), "conflicting duplicate FactoryOrder Work artifact") {
		t.Fatalf("divergent duplicate error = %v, want fail-closed conflict", err)
	}
}

func TestFactoryV1WorkEnumerationAllowsIdenticalDuplicateMetadata(t *testing.T) {
	workStore, actor, conversation, link, originalBody := newFactoryV1DuplicateMetadataFixture(t)
	taskID, err := types.NewEventID(link.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if err := workStore.tasks.AddArtifact(actor, taskID, factoryV1WorkMetadataLabel, "application/json", originalBody, []types.EventID{taskID}, conversation); err != nil {
		t.Fatal(err)
	}
	links, err := workStore.ListFactoryOrders(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 1 || links[0].OrderID != link.OrderID || links[0].DocumentSHA256 != link.DocumentSHA256 {
		t.Fatalf("identical duplicate links = %+v, want original link %+v", links, link)
	}
}

func newFactoryV1DuplicateMetadataFixture(t *testing.T) (*FactoryV1WorkStore, types.ActorID, types.ConversationID, factoryv1.WorkLink, string) {
	t.Helper()
	ctx := context.Background()
	eventStore, factory, signer, actor, conversation := newDecisionTestStore(t)
	workpkg.RegisterWithRegistry(factory.Registry)
	workStore, err := NewFactoryV1WorkStore(eventStore, factory, signer, actor, conversation)
	if err != nil {
		t.Fatal(err)
	}
	head, err := eventStore.Head()
	if err != nil || head.IsNone() {
		t.Fatalf("bootstrap head: %v", err)
	}
	link, err := workStore.SeedFactoryOrder(ctx, factoryv1.WorkSeed{
		OrderID: "FO-DUPLICATE-VALIDATION", Version: "1.0.0", DocumentSHA256: factoryv1.HashText("duplicate-validation"),
		Markdown: "# duplicate validation\n", SourceSHA256: factoryv1.HashText("duplicate-validation-source"),
		AcceptedEventID: head.Unwrap().ID().Value(), IdempotencyKey: "duplicate-validation",
	})
	if err != nil {
		t.Fatal(err)
	}
	taskID, err := types.NewEventID(link.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := workStore.tasks.ListArtifacts(taskID)
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range artifacts {
		if artifact.Label == factoryV1WorkMetadataLabel {
			return workStore, actor, conversation, link, artifact.Body
		}
	}
	t.Fatal("seeded FactoryOrder metadata artifact not found")
	return workStore, actor, conversation, link, ""
}

func TestFactoryV1WorkEnumerationPagesBeyondLegacyCeiling(t *testing.T) {
	ctx := context.Background()
	eventStore, factory, signer, actor, conversation := newDecisionTestStore(t)
	workpkg.RegisterWithRegistry(factory.Registry)
	workStore, err := NewFactoryV1WorkStore(eventStore, factory, signer, actor, conversation)
	if err != nil {
		t.Fatal(err)
	}
	head, err := eventStore.Head()
	if err != nil || head.IsNone() {
		t.Fatalf("bootstrap head: %v", err)
	}
	acceptedEventID := head.Unwrap().ID().Value()
	want := factoryV1WorkPageSize + 1
	for index := 0; index < want; index++ {
		orderID := fmt.Sprintf("FO-PAGED-%04d", index)
		documentSHA := factoryv1.HashText(orderID)
		if _, err := workStore.SeedFactoryOrder(ctx, factoryv1.WorkSeed{
			OrderID: orderID, Version: "1.0.0", DocumentSHA256: documentSHA,
			Markdown: "# " + orderID + "\n", SourceSHA256: factoryv1.HashText("source:" + orderID),
			AcceptedEventID: acceptedEventID, IdempotencyKey: "paged:" + orderID,
		}); err != nil {
			t.Fatalf("seed %s: %v", orderID, err)
		}
	}
	links, err := workStore.ListFactoryOrders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != want {
		t.Fatalf("FactoryOrder links = %d, want %d", len(links), want)
	}
	seen := make(map[string]factoryv1.WorkLink, len(links))
	for _, link := range links {
		seen[link.OrderID] = link
	}
	for _, orderID := range []string{"FO-PAGED-0000", fmt.Sprintf("FO-PAGED-%04d", want-1)} {
		link, ok := seen[orderID]
		if !ok || link.ArtifactID == "" || link.TaskID == "" || link.DocumentSHA256 == "" {
			t.Fatalf("paged link %s = %+v, present=%v", orderID, link, ok)
		}
	}
}

func TestFactoryV1WorkEnumerationRejectsNonAdvancingCursor(t *testing.T) {
	eventStore, factory, signer, actor, conversation := newDecisionTestStore(t)
	workpkg.RegisterWithRegistry(factory.Registry)
	workStore, err := NewFactoryV1WorkStore(&factoryV1RepeatingCursorStore{Store: eventStore}, factory, signer, actor, conversation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workStore.ListFactoryOrders(context.Background()); err == nil || !strings.Contains(err.Error(), "did not advance") {
		t.Fatalf("cursor error = %v, want non-advancing failure", err)
	}
}
