// Command civilization-api runs the production Civilization control plane.
// Repository publication and Routine auto-merge are independently default-off.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/transpara-ai/eventgraph/go/pkg/actor/pgactor"
	"github.com/transpara-ai/eventgraph/go/pkg/event"
	"github.com/transpara-ai/eventgraph/go/pkg/protocol/egip"
	"github.com/transpara-ai/eventgraph/go/pkg/store/pgstore"
	"github.com/transpara-ai/eventgraph/go/pkg/types"
	"github.com/transpara-ai/hive/pkg/hive"
	"github.com/transpara-ai/hive/pkg/hive/civilization"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("civilization-api: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn, err := requiredEnvOrFile("DATABASE_URL")
	if err != nil {
		return err
	}
	requireCleanDatabase, err := boolEnv("CIVILIZATION_REQUIRE_CLEAN_DATABASE", true)
	if err != nil {
		return err
	}
	publishEnabled, err := boolEnv("CIVILIZATION_PUBLISH_ENABLED", false)
	if err != nil {
		return err
	}
	autoMergeEnabled, err := boolEnv("CIVILIZATION_AUTO_MERGE_ENABLED", false)
	if err != nil {
		return err
	}
	apiKey, err := readSecretFileEnv("CIVILIZATION_API_KEY_FILE", 32)
	if err != nil {
		return err
	}
	privateKey, err := readSigningKeyFileEnv("CIVILIZATION_SIGNING_KEY_FILE")
	if err != nil {
		return err
	}
	repositories, err := readRepositoriesFile()
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open Postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping Postgres: %w", err)
	}
	hive.RegisterEventTypes()
	event.SetFallbackUnmarshaler(event.RawFallback)
	graph, err := pgstore.NewPostgresStoreFromPool(ctx, pool)
	if err != nil {
		return fmt.Errorf("open EventGraph: %w", err)
	}
	defer graph.Close()
	actors, err := pgactor.NewPostgresActorStoreFromPool(ctx, pool)
	if err != nil {
		return fmt.Errorf("open actor store: %w", err)
	}
	publicKey, err := types.NewPublicKey(privateKey.Public().(ed25519.PublicKey))
	if err != nil {
		return fmt.Errorf("load signing public key: %w", err)
	}
	serviceActor, err := actors.Register(publicKey, "Civilization production service", event.ActorTypeSystem)
	if err != nil {
		return fmt.Errorf("register service actor: %w", err)
	}
	identity, err := egip.NewIdentityFromKey(types.MustSystemURI("system://transpara-ai/civilization"), privateKey)
	if err != nil {
		return fmt.Errorf("load signing identity: %w", err)
	}
	registry := event.DefaultRegistry()
	hive.RegisterWithRegistry(registry)
	if err := bootstrap(ctx, graph, registry, serviceActor.ID(), identity); err != nil {
		return err
	}
	chain, err := graph.VerifyChain()
	if err != nil {
		return fmt.Errorf("verify production EventGraph chain: %w", err)
	}
	if !chain.Valid {
		return fmt.Errorf("production EventGraph chain is invalid after %d verified events", chain.Length)
	}
	factory := event.NewEventFactory(registry)
	eventStore, err := hive.NewCivilizationEventGraphStore(graph, factory, identity, serviceActor.ID(), types.MustConversationID("conv_civilization_production_v1"))
	if err != nil {
		return err
	}
	if requireCleanDatabase {
		allCount, countErr := graph.Count()
		civilizationEvents, listErr := eventStore.List(ctx)
		if countErr != nil || listErr != nil {
			return errors.New("verify clean production EventGraph")
		}
		if allCount != len(civilizationEvents)+1 {
			return fmt.Errorf("production EventGraph contains %d non-Civilization events; use a clean database and expose history read-only through its old service", allCount-len(civilizationEvents)-1)
		}
	}

	codexPath, _ := requiredEnv("CIVILIZATION_CODEX_PATH")
	codexDigest, _ := requiredEnv("CIVILIZATION_CODEX_SHA256")
	provider, err := configuredProviders(codexPath, codexDigest)
	if err != nil {
		return err
	}
	routingContext, err := loadRoutingContext(envOr("CIVILIZATION_TLC_LOCK_FILE", "/etc/civilization/tlc.lock.json"), envOr("CIVILIZATION_TLC_SKILL_FILE", "/var/lib/civilization/codex/skills/tlc/SKILL.md"))
	if err != nil {
		return err
	}
	runner, err := configuredRunner()
	if err != nil {
		return err
	}
	effects, err := civilization.NewGitHubEffects(civilization.GitHubEffectsConfig{
		Runner:       runner,
		Repositories: repositories, WorktreeRoot: requiredEnvValue("CIVILIZATION_WORKTREE_DIR"),
		GitExecutable: requiredEnvValue("CIVILIZATION_GIT_PATH"), GitSHA256: requiredEnvValue("CIVILIZATION_GIT_SHA256"),
		GitHubExecutable: requiredEnvValue("CIVILIZATION_GH_PATH"), GitHubSHA256: requiredEnvValue("CIVILIZATION_GH_SHA256"),
		VerificationSandboxExecutable: codexPath, VerificationSandboxSHA256: codexDigest,
		VerificationSandboxProfile: "civilization-verification", VerificationCodexHome: requiredEnvValue("CIVILIZATION_VERIFICATION_CODEX_HOME"),
		VerificationConfigSHA256: requiredEnvValue("CIVILIZATION_VERIFICATION_CONFIG_SHA256"),
		VerificationScratchRoot:  requiredEnvValue("CIVILIZATION_VERIFICATION_SCRATCH_DIR"),
		VerificationModuleCache:  requiredEnvValue("CIVILIZATION_VERIFICATION_MODULE_CACHE"),
		EnvironmentKeys:          []string{"PATH", "HOME", "GH_TOKEN", "GITHUB_TOKEN", "GH_CONFIG_DIR", "SSH_AUTH_SOCK", "SSL_CERT_FILE", "SSL_CERT_DIR"},
		OutputLimitBytes:         intEnv("CIVILIZATION_COMMAND_OUTPUT_LIMIT", 2*1024*1024),
		CommitUserName:           envOr("CIVILIZATION_COMMIT_USER_NAME", "Civilization"), CommitUserEmail: envOr("CIVILIZATION_COMMIT_USER_EMAIL", "civilization@transpara.ai"),
		PublishEnabled: publishEnabled, PublishAuthority: os.Getenv("CIVILIZATION_PUBLISH_AUTHORITY_REF"),
		AutoMergeEnabled: autoMergeEnabled, AutoMergeAuthority: os.Getenv("CIVILIZATION_AUTO_MERGE_AUTHORITY_REF"),
	})
	if err != nil {
		return fmt.Errorf("configure repository effects: %w", err)
	}
	repositoryAllowlist := make(map[string]struct{}, len(repositories))
	for repository := range repositories {
		repositoryAllowlist[repository] = struct{}{}
	}
	engine, err := civilization.NewEngine(civilization.EngineConfig{
		RoutingContext: routingContext,
		Store:          eventStore, Provider: provider, Effects: effects,
		AutoMergePolicy: civilization.AutoMergePolicy{
			Enabled: autoMergeEnabled, AuthorityRef: os.Getenv("CIVILIZATION_AUTO_MERGE_AUTHORITY_REF"),
			Repositories: repositoryAllowlist, ProtectedPaths: civilization.DefaultProtectedPaths(),
		},
	})
	if err != nil {
		return err
	}
	requireHumanIdentity, err := boolEnv("CIVILIZATION_REQUIRE_HUMAN_IDENTITY", false)
	if err != nil {
		return err
	}
	handler, err := civilization.NewHTTPHandler(civilization.HTTPConfig{RequireHumanIdentity: requireHumanIdentity, Engine: engine, APIKey: apiKey, MaxBodyBytes: int64(intEnv("CIVILIZATION_MAX_BODY_BYTES", 256*1024))})
	if err != nil {
		return err
	}
	go reconciliationLoop(ctx, engine, autoMergeEnabled, durationEnv("CIVILIZATION_RECONCILE_INTERVAL", 30*time.Second), intEnv("CIVILIZATION_RECONCILE_CONCURRENCY", 3))
	server := &http.Server{
		Addr: envOr("CIVILIZATION_ADDR", "127.0.0.1:8084"), Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 35 * time.Minute, IdleTimeout: 60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("listening on %s (publication=%t auto_merge=%t repositories=%d)", server.Addr, publishEnabled, autoMergeEnabled, len(repositories))
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func reconciliationLoop(ctx context.Context, engine *civilization.Engine, autoMerge bool, interval time.Duration, concurrency int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconcileOnce(ctx, engine, autoMerge, concurrency)
		}
	}
}

func reconcileOnce(ctx context.Context, engine *civilization.Engine, autoMerge bool, concurrency int) {
	items, err := engine.List(ctx)
	if err != nil {
		log.Printf("reconcile: list work failed: %v", err)
		return
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	limit := make(chan struct{}, concurrency)
	var wait sync.WaitGroup
	known := map[string]bool{}
	for _, item := range items {
		known[item.WorkID] = true
	}
	for _, item := range items {
		pendingRevision := item.State == civilization.StateChangesRequested && item.ResultReview != nil && !known[item.ResultReview.RevisionWorkID]
		if !pendingRevision && item.State != civilization.StateRouting && item.State != civilization.StateQueued && item.State != civilization.StateImplementing &&
			item.State != civilization.StateValidating && item.State != civilization.StateReviewing && item.State != civilization.StatePublishing &&
			item.State != civilization.StateMergeQueued && !(autoMerge && item.State == civilization.StateReady) {
			continue
		}
		workID := item.WorkID
		wait.Add(1)
		go func() {
			defer wait.Done()
			select {
			case limit <- struct{}{}:
				defer func() { <-limit }()
			case <-ctx.Done():
				return
			}
			if _, runErr := engine.Advance(ctx, workID); runErr != nil && ctx.Err() == nil {
				log.Printf("reconcile: work %s: %v", workID, runErr)
			}
		}()
	}
	wait.Wait()
}

func bootstrap(ctx context.Context, graph *pgstore.PostgresStore, registry *event.EventTypeRegistry, actorID types.ActorID, signer event.Signer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	head, err := graph.Head()
	if err != nil {
		return fmt.Errorf("read EventGraph head: %w", err)
	}
	if head.IsSome() {
		return nil
	}
	genesis, err := event.NewBootstrapFactory(registry).Init(actorID, signer)
	if err != nil {
		return fmt.Errorf("create EventGraph genesis: %w", err)
	}
	if _, err := graph.Append(genesis); err != nil {
		return fmt.Errorf("append EventGraph genesis: %w", err)
	}
	return nil
}

func readRepositoriesFile() (map[string]civilization.RepositorySpec, error) {
	path, err := requiredEnv("CIVILIZATION_REPOSITORIES_FILE")
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read repository configuration: %w", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	var repositories map[string]civilization.RepositorySpec
	if err := decoder.Decode(&repositories); err != nil {
		return nil, fmt.Errorf("decode repository configuration: %w", err)
	}
	if len(repositories) == 0 {
		return nil, errors.New("repository configuration is empty")
	}
	return repositories, nil
}

func readSigningKeyFileEnv(name string) (ed25519.PrivateKey, error) {
	encoded, err := readSecretFileEnv(name, 32)
	if err != nil {
		return nil, err
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("%s must contain base64: %w", name, err)
	}
	switch len(raw) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(raw), nil
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(append([]byte(nil), raw...)), nil
	default:
		return nil, fmt.Errorf("%s must decode to an Ed25519 seed or private key", name)
	}
}

func readSecretFileEnv(name string, minimum int) (string, error) {
	path, err := requiredEnv(name)
	if err != nil {
		return "", err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", name, err)
	}
	value := strings.TrimSpace(string(raw))
	if len(value) < minimum {
		return "", fmt.Errorf("%s secret is too short", name)
	}
	return value, nil
}

func requiredEnv(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func requiredEnvOrFile(name string) (string, error) {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value, nil
	}
	path := strings.TrimSpace(os.Getenv(name + "_FILE"))
	if path == "" {
		return "", fmt.Errorf("%s or %s_FILE is required", name, name)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s_FILE: %w", name, err)
	}
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return "", fmt.Errorf("%s_FILE is empty", name)
	}
	return value, nil
}

func requiredEnvValue(name string) string {
	return strings.TrimSpace(os.Getenv(name))
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func boolEnv(name string, fallback bool) (bool, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return parsed, nil
}

func intEnv(name string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(strings.TrimSpace(os.Getenv(name)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
