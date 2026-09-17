// Command hive runs hive agents.
//
// Usage:
//
//	hive <verb> [subverb] [flags]
//
// Verbs:
//
//	hive civilization run         Multi-agent runtime, one-shot
//	hive civilization daemon      Multi-agent runtime, long-running with governed issue-scan controls
//	hive pipeline run             Scout→Builder→Critic, one cycle
//	hive pipeline daemon          Scout→Builder→Critic, looping
//	hive role <name> run          Single agent (builder|scout|critic|monitor), one task
//	hive role <name> daemon       Single agent, continuous
//	hive ingest <file>            Post a markdown spec as a task
//	hive council [--topic ...]    Convene one deliberation
//
// Run 'hive <verb> --help' for verb-specific flags.
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/transpara-ai/eventgraph/go/pkg/actor"
	"github.com/transpara-ai/eventgraph/go/pkg/actor/pgactor"
	"github.com/transpara-ai/eventgraph/go/pkg/event"
	"github.com/transpara-ai/eventgraph/go/pkg/intelligence"
	"github.com/transpara-ai/eventgraph/go/pkg/modelconfig"
	"github.com/transpara-ai/eventgraph/go/pkg/store"
	"github.com/transpara-ai/eventgraph/go/pkg/store/pgstore"
	"github.com/transpara-ai/eventgraph/go/pkg/types"

	"github.com/transpara-ai/hive/pkg/api"
	"github.com/transpara-ai/hive/pkg/hive"
	"github.com/transpara-ai/hive/pkg/reconciliation"
	"github.com/transpara-ai/hive/pkg/registry"
	"github.com/transpara-ai/hive/pkg/runner"
	"github.com/transpara-ai/hive/pkg/safety"
	"github.com/transpara-ai/hive/pkg/telemetry"
	"github.com/transpara-ai/work"
)

func main() {
	if err := run(); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		if errors.Is(err, errUsage) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	return routeAndDispatch(os.Args[1:])
}

// ─── Ingest mode ─────────────────────────────────────────────────────

const (
	ingestTitlePrefix = "[SPEC] "
	ingestOp          = "intend"
	ingestKind        = "task"
	ingestTimeout     = 30 * time.Second
	ingestDefaultOrg  = "transpara-ai"
	ingestDefaultLang = "go"
)

// runIngest reads a markdown spec file, creates a repo for it (if needed),
// registers it in repos.json, and posts it as a task to the local API.
func runIngest(specPath, space, apiBase, priority string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	data, err := os.ReadFile(specPath)
	if err != nil {
		return fmt.Errorf("read spec: %w", err)
	}
	body := string(data)

	// Derive title from first H1, or fall back to filename.
	title := strings.TrimSuffix(filepath.Base(specPath), filepath.Ext(specPath))
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "# ") {
			title = strings.TrimSpace(strings.TrimPrefix(line, "# "))
			break
		}
	}
	title = ingestTitlePrefix + title

	// Derive repo slug and ensure it exists (skip in test mode).
	slug := slugify(title)
	var repoPath string
	if os.Getenv("HIVE_INGEST_SKIP_REPO") == "" {
		var err error
		auditEmitter, closeAudit, auditErr := newAuthorityAuditEmitter(ctx, os.Getenv("DATABASE_URL"))
		if auditErr != nil {
			log.Printf("[ingest] authority audit unavailable: %v", auditErr)
		}
		if closeAudit != nil {
			defer closeAudit()
		}

		repoPath, err = ensureSpecRepo(slug, body, auditEmitter)
		if err != nil {
			return fmt.Errorf("ensure repo: %w", err)
		}
	}

	// Resolve API key.
	apiKey := os.Getenv("TRANSPARA_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("TRANSPARA_API_KEY required")
	}

	payload, err := json.Marshal(map[string]string{
		"op":          ingestOp,
		"kind":        ingestKind,
		"title":       title,
		"description": body,
		"priority":    priority,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	url := fmt.Sprintf("%s/app/%s/op", apiBase, space)
	ctx, cancel := context.WithTimeout(ctx, ingestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("post to %s: %w", url, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, respBody)
	}

	var result struct {
		Node struct {
			ID string `json:"id"`
		} `json:"node"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if result.Node.ID == "" {
		return fmt.Errorf("server returned empty node ID")
	}
	fmt.Printf("ingested: %s — %s\n", result.Node.ID, title)
	if repoPath != "" {
		fmt.Printf("repo:     %s/%s → %s\n", ingestOrg(), slug, repoPath)
	}
	return nil
}

// ingestOrg returns the GitHub org for ingest repos.
// Reads HIVE_GITHUB_ORG, then GIT_ORG from config.env, falls back to default.
func ingestOrg() string {
	if v := os.Getenv("HIVE_GITHUB_ORG"); v != "" {
		return v
	}
	if v := os.Getenv("GIT_ORG"); v != "" {
		return v
	}
	return ingestDefaultOrg
}

// detectLanguage guesses the project language from spec content.
// Returns "go" if no language-specific keywords are found.
func detectLanguage(body string) (lang, buildCmd, testCmd string) {
	lower := strings.ToLower(body)
	switch {
	case strings.Contains(lower, "typescript") || strings.Contains(lower, "package.json") || strings.Contains(lower, "node.js"):
		return "typescript", "npm run build", "npm test"
	case strings.Contains(lower, "python") || strings.Contains(lower, "requirements.txt") || strings.Contains(lower, "pyproject.toml"):
		return "python", "python -m build", "python -m pytest"
	case strings.Contains(lower, "rust") || strings.Contains(lower, "cargo.toml"):
		return "rust", "cargo build", "cargo test"
	default:
		return ingestDefaultLang, "go build -buildvcs=false ./...", "go test -buildvcs=false ./..."
	}
}

// slugify converts a title to a kebab-case repo slug.
// Strips the [SPEC] prefix, common boilerplate prefixes, and non-alphanumeric characters.
func slugify(title string) string {
	s := strings.TrimPrefix(title, ingestTitlePrefix)
	// Strip common prefixes that don't add meaning to a repo name.
	for _, prefix := range []string{"Work Description: ", "Work Description:", "Spec: ", "Spec:"} {
		s = strings.TrimPrefix(s, prefix)
	}
	s = strings.ToLower(s)
	// Replace non-alphanumeric runs with a single hyphen.
	var b strings.Builder
	prevHyphen := true // suppress leading hyphen
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevHyphen = false
		} else if !prevHyphen {
			b.WriteByte('-')
			prevHyphen = true
		}
	}
	result := strings.TrimRight(b.String(), "-")
	if result == "" {
		result = "spec"
	}
	return result
}

// ensureSpecRepo creates a GitHub repo and local directory for a spec slug
// if they don't already exist, and registers the repo in repos.json.
// Returns early if the slug is already registered and the directory exists on disk.
func ensureSpecRepo(slug, specBody string, auditEmitter authorityRequestEmitter) (string, error) {
	org := ingestOrg()
	hiveDir := findHiveDir()
	reposJSONPath := filepath.Join(hiveDir, "repos.json")

	// Determine local path (sibling to hive repo, like ../site).
	repoDir, err := filepath.Abs(filepath.Join(hiveDir, "..", slug))
	if err != nil {
		return "", fmt.Errorf("resolve path: %w", err)
	}

	// Check if already in repos.json and on disk.
	registered, regErr := repoInRegistry(reposJSONPath, slug)
	if regErr != nil {
		return "", fmt.Errorf("check registry: %w", regErr)
	}
	if registered {
		if _, err := os.Stat(repoDir); err == nil {
			log.Printf("[ingest] repo %s already exists at %s", slug, repoDir)
			return repoDir, nil
		}
	}

	if err := authorizeIngestRepoBootstrap(slug, org, repoDir, auditEmitter); err != nil {
		return "", err
	}

	repoURL := fmt.Sprintf("https://github.com/%s/%s.git", org, slug)

	// Create GitHub repo (ignore error if it already exists).
	ghCreate := exec.Command("gh", "repo", "create",
		org+"/"+slug,
		"--private",
		"--description", fmt.Sprintf("Built by hive pipeline from [SPEC] %s", slug),
	)
	if out, err := ghCreate.CombinedOutput(); err != nil {
		outStr := string(out)
		if !strings.Contains(outStr, "already exists") {
			return "", fmt.Errorf("gh repo create: %s", outStr)
		}
		log.Printf("[ingest] repo %s/%s already exists on GitHub", org, slug)
	} else {
		log.Printf("[ingest] created repo %s/%s", org, slug)
	}

	// Init local directory if it doesn't exist.
	if _, err := os.Stat(repoDir); os.IsNotExist(err) {
		if err := os.MkdirAll(repoDir, 0755); err != nil {
			return "", fmt.Errorf("mkdir: %w", err)
		}
		// Push directly to the URL to avoid "origin" remote naming convention issues.
		for _, args := range [][]string{
			{"init"},
			{"remote", "add", org, repoURL},
			{"checkout", "-b", "main"},
			{"commit", "--allow-empty", "-m", "init: repo for " + slug},
			{"push", "-u", org, "main"},
		} {
			cmd := exec.Command("git", args...)
			cmd.Dir = repoDir
			if out, err := cmd.CombinedOutput(); err != nil {
				return "", fmt.Errorf("git %s: %s", args[0], out)
			}
		}
		log.Printf("[ingest] initialized local repo at %s", repoDir)
	}

	// Add to repos.json if not already present.
	registered, regErr = repoInRegistry(reposJSONPath, slug)
	if regErr != nil {
		return "", fmt.Errorf("check registry: %w", regErr)
	}
	if !registered {
		lang, buildCmd, testCmd := detectLanguage(specBody)
		if err := addToRegistry(reposJSONPath, slug, repoDir, org, lang, buildCmd, testCmd); err != nil {
			return "", fmt.Errorf("update repos.json: %w", err)
		}
		log.Printf("[ingest] added %s to repos.json (lang=%s)", slug, lang)
	}

	return repoDir, nil
}

func authorizeIngestRepoBootstrap(slug, org, repoDir string, auditEmitter authorityRequestEmitter) error {
	var errs []error
	if err := safety.RequireAuthorized(safety.ActionRepoCreate); err != nil {
		outcome := safety.DefaultOutcome(safety.ActionRepoCreate)
		log.Printf("repo.create.blocked action=%s outcome=%s org=%s repo=%s path=%s: %v",
			safety.ActionRepoCreate,
			outcome,
			org,
			slug,
			repoDir,
			err,
		)
		emitAuthorityRequest(auditEmitter, safety.ActionRepoCreate, outcome,
			fmt.Sprintf("ingest repo bootstrap blocked: create %s/%s at %s", org, slug, repoDir))
		errs = append(errs, err)
	}
	if err := safety.RequireAuthorized(safety.ActionRepoPushDefaultBranch); err != nil {
		outcome := safety.DefaultOutcome(safety.ActionRepoPushDefaultBranch)
		log.Printf("repo.push.blocked action=%s outcome=%s org=%s repo=%s branch=main path=%s git_args=%q: %v",
			safety.ActionRepoPushDefaultBranch,
			outcome,
			org,
			slug,
			repoDir,
			[]string{"push", "-u", org, "main"},
			err,
		)
		emitAuthorityRequest(auditEmitter, safety.ActionRepoPushDefaultBranch, outcome,
			fmt.Sprintf("ingest repo bootstrap blocked: initial push %s/%s main from %s", org, slug, repoDir))
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func emitAuthorityRequest(auditEmitter authorityRequestEmitter, action safety.ProtectedAction, outcome safety.AuthorityOutcome, justification string) {
	if auditEmitter == nil {
		return
	}
	if err := auditEmitter.EmitAuthorityRequest(action, outcome, justification); err != nil {
		log.Printf("authority.requested emit failed action=%s: %v", action, err)
	}
}

// repoInRegistry checks if a slug already exists in repos.json.
// Returns false with nil error if repos.json does not exist (not yet bootstrapped).
func repoInRegistry(path, slug string) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("read %s: %w", path, err)
	}
	var reg registry.Registry
	if err := json.Unmarshal(data, &reg); err != nil {
		return false, fmt.Errorf("parse %s: %w", path, err)
	}
	for _, r := range reg.Repos {
		if r.Name == slug {
			return true, nil
		}
	}
	return false, nil
}

// addToRegistry appends a new repo entry to repos.json.
// Creates an empty registry if the file does not exist.
func addToRegistry(path, slug, absPath, org, lang, buildCmd, testCmd string) error {
	var reg registry.Registry

	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("read %s: %w", path, err)
		}
		// Bootstrap empty registry.
	} else if err := json.Unmarshal(data, &reg); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}

	// Compute relative path from repos.json dir.
	dir := filepath.Dir(path)
	relPath, err := filepath.Rel(dir, absPath)
	if err != nil {
		relPath = absPath
	}

	reg.Repos = append(reg.Repos, registry.Repo{
		Name:      slug,
		URL:       fmt.Sprintf("https://github.com/%s/%s", org, slug),
		LocalPath: relPath,
		Language:  lang,
		BuildCmd:  buildCmd,
		TestCmd:   testCmd,
	})

	out, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal registry: %w", err)
	}
	return os.WriteFile(path, append(out, '\n'), 0644)
}

// ─── Runner mode ─────────────────────────────────────────────────────

func runRunner(role, space, apiBase, repoPath string, budget float64, agentID string, oneShot, direct, prMode bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Resolve API key.
	apiKey := os.Getenv("TRANSPARA_API_KEY")
	if apiKey == "" {
		if strings.Contains(apiBase, "localhost") || strings.Contains(apiBase, "127.0.0.1") {
			apiKey = "dev"
			log.Printf("[pipeline] no TRANSPARA_API_KEY — using 'dev' for local API")
		} else {
			return fmt.Errorf("TRANSPARA_API_KEY required (set env or use --api http://localhost:8082 for local mode)")
		}
	}

	// Resolve repo path.
	if repoPath == "" {
		repoPath = "."
	}
	absRepo, err := filepath.Abs(repoPath)
	if err != nil {
		return fmt.Errorf("resolve repo: %w", err)
	}

	// Find hive directory (for loading role prompts).
	hiveDir := findHiveDir()

	// Create intelligence provider.
	// Role prompt is passed in the instruction (with task context), not as system prompt.
	providerCfg := runner.ProviderConfig(role, budget)
	providerCfg.APIKey = resolveAnthropicKey()
	provider, err := intelligence.New(providerCfg)
	if err != nil {
		return fmt.Errorf("provider: %w", err)
	}
	log.Printf("[%s] provider=%s model=%s", role, providerCfg.Provider, providerCfg.Model)

	// Create API client.
	client := api.New(apiBase, apiKey)

	// Load role prompt for injection into task instructions.
	rolePrompt := runner.LoadRolePrompt(hiveDir, role)

	// Create and run the runner.
	r := runner.New(runner.Config{
		Role:       role,
		AgentID:    agentID,
		SpaceSlug:  space,
		RepoPath:   absRepo,
		HiveDir:    hiveDir,
		APIClient:  client,
		APIBase:    apiBase,
		Provider:   provider,
		RolePrompt: rolePrompt,
		BudgetUSD:  budget,
		OneShot:    oneShot,
		Direct:     direct || role != "builder",
		PRMode:     direct && prMode,
	})

	log.Printf("hive agent starting: role=%s model=%s space=%s repo=%s agent-id=%s one-shot=%v",
		role, providerCfg.Model, space, absRepo, agentID, oneShot)
	return r.Run(ctx)
}

// ─── Council mode ────────────────────────────────────────────────────

func runCouncilCmd(space, apiBase, repoPath string, budget float64, topic, catalogPath string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	apiKey := os.Getenv("TRANSPARA_API_KEY")
	client := api.New(apiBase, apiKey) // nil-safe if no key

	if repoPath == "" {
		repoPath = "."
	}
	absRepo, _ := filepath.Abs(repoPath)
	hiveDir := findHiveDir()

	resolver, err := buildCouncilResolver(catalogPath)
	if err != nil {
		return fmt.Errorf("resolver: %w", err)
	}
	// Thread --budget into every pool-built provider. Without this wrapper,
	// modelconfig.ToIntelligenceConfig produces configs with MaxBudgetUSD=0
	// and claude-cli falls back to its $1/call default, far below the $10
	// daily budget operators expect.
	pool := modelconfig.NewProviderPoolWithBuilder(resolver, councilProviderBuilder(intelligence.New, budget))

	// Precompute role → CanOperate map once, then pass as a closure on Config.
	// Bridges the runner package (which can't import hive) to the live
	// AgentDef registry without per-call slice allocation + linear scan.
	canOperateMap := make(map[string]bool)
	for _, def := range hive.StarterAgents("") {
		canOperateMap[def.Role] = def.CanOperate
	}
	canOperateLookup := func(role string) bool { return canOperateMap[role] }

	return runner.RunCouncil(ctx, runner.Config{
		SpaceSlug:        space,
		RepoPath:         absRepo,
		HiveDir:          hiveDir,
		APIClient:        client,
		APIBase:          apiBase,
		Pool:             pool,
		BudgetUSD:        budget,
		CouncilTopic:     topic,
		CanOperateLookup: canOperateLookup,
	})
}

// councilProviderBuilder returns a provider builder closure that injects the
// operator's --budget into every intelligence.Config before delegating to the
// real constructor. Threading budget through here is essential because the
// modelconfig.Resolver (used by ProviderPool) does not carry a per-call cost
// cap; without this wrapper, claude-cli providers default to a $1 per-call
// cap (far below the daily --budget operators set).
//
// The `inner` parameter is the underlying constructor (intelligence.New in
// production, a capturing fake in tests).
func councilProviderBuilder(
	inner func(intelligence.Config) (intelligence.Provider, error),
	budget float64,
) func(intelligence.Config) (intelligence.Provider, error) {
	return func(cfg intelligence.Config) (intelligence.Provider, error) {
		cfg.MaxBudgetUSD = budget
		return inner(cfg)
	}
}

// buildCouncilResolver returns a Resolver for the council. Precedence:
//  1. --catalog flag wins if non-empty.
//  2. COUNCIL_MODEL env var fallback ONLY when --catalog is unset.
//  3. DefaultResolver().
//
// Decision 5a of the design spec.
func buildCouncilResolver(catalogPath string) (*modelconfig.Resolver, error) {
	if catalogPath != "" {
		if env := os.Getenv("COUNCIL_MODEL"); env != "" {
			log.Printf("[council] note: --catalog provided; COUNCIL_MODEL=%s env var ignored", env)
		}
		return modelconfig.ResolverFromCatalogFile(catalogPath)
	}
	if env := os.Getenv("COUNCIL_MODEL"); env != "" {
		r, err := modelconfig.DefaultResolverWithModel(env)
		if err != nil {
			return nil, fmt.Errorf("COUNCIL_MODEL=%q: %w", env, err)
		}
		log.Printf("[council] legacy mode: COUNCIL_MODEL=%s → synthetic single-model resolver", env)
		return r, nil
	}
	return modelconfig.DefaultResolver(), nil
}

// ─── Pipeline mode ───────────────────────────────────────────────────

// runPipeline runs Scout → Builder → Critic in sequence. One full cycle.
func runPipeline(space, apiBase, repoPath string, budget float64, agentID string, repoMap map[string]string, direct, prMode, useWorktrees, autoClone bool, storeDSN string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	apiKey := os.Getenv("TRANSPARA_API_KEY")
	if apiKey == "" {
		if strings.Contains(apiBase, "localhost") || strings.Contains(apiBase, "127.0.0.1") {
			apiKey = "dev"
			log.Printf("[pipeline] no TRANSPARA_API_KEY — using 'dev' for local API")
		} else {
			return fmt.Errorf("TRANSPARA_API_KEY required (set env or use --api http://localhost:8082 for local mode)")
		}
	}

	if repoPath == "" {
		repoPath = "."
	}
	absRepo, err := filepath.Abs(repoPath)
	if err != nil {
		return fmt.Errorf("resolve repo: %w", err)
	}

	hiveDir := findHiveDir()

	// Load repo registry: repos.json first, fall back to --repos flag.
	var reg *registry.Registry
	regPath := filepath.Join(hiveDir, "repos.json")
	if r, err := registry.Load(regPath); err == nil {
		if err := r.Resolve(); err != nil {
			log.Printf("[pipeline] registry resolve warning: %v", err)
		}
		reg = r
		// Merge into repoMap for backward compat.
		for k, v := range r.RepoMap() {
			if _, exists := repoMap[k]; !exists {
				repoMap[k] = v
			}
		}
		log.Printf("[pipeline] loaded %d repos from registry", len(r.Available()))

		// Auto-clone missing repos and pull latest.
		if autoClone {
			if n := r.EnsureCloned(); n > 0 {
				log.Printf("[pipeline] cloned %d missing repos", n)
			}
		}
		r.PullAll()
	} else if len(repoMap) == 0 {
		log.Printf("[pipeline] no repos.json and no --repos flag")
	}

	// Log available repos.
	if len(repoMap) > 0 {
		var names []string
		for name := range repoMap {
			names = append(names, name)
		}
		log.Printf("[pipeline] repos available: %v (default: %s)", names, absRepo)
	}
	client := api.New(apiBase, apiKey)

	// Generate MCP config for knowledge server so agents can search.
	mcpConfigPath := writeMCPConfig(hiveDir, repoMap)
	if mcpConfigPath != "" {
		log.Printf("[pipeline] MCP knowledge server configured: %s", mcpConfigPath)
	}

	// Resolve target repo from graph or state.md.
	activeRepo := absRepo
	if len(repoMap) > 0 {
		if resolved := resolveTargetRepo(hiveDir, repoMap, client, space); resolved != "" {
			activeRepo = resolved
			log.Printf("[pipeline] target repo from directive: %s", filepath.Base(activeRepo))
		}
	}

	// Look up agent session IDs from the DB — each agent owns a persistent UUID.
	agentSessions := map[string]string{} // role → session UUID
	if client != nil {
		if agents, err := client.ListAgents(space); err == nil {
			for _, a := range agents {
				if a.SessionID != "" {
					agentSessions[a.Name] = a.SessionID
				}
			}
		}
	}

	// Create a runner that all state machine transitions use.
	makeRunner := func(role string) (*runner.Runner, error) {
		providerCfg := runner.ProviderConfig(role, budget)
		providerCfg.APIKey = resolveAnthropicKey()
		providerCfg.SessionID = agentSessions[role] // warm session from DB (empty = cold start)
		if mcpConfigPath != "" {
			providerCfg.MCPConfigPath = mcpConfigPath
		}
		provider, err := intelligence.New(providerCfg)
		if err != nil {
			return nil, fmt.Errorf("provider for %s: %w", role, err)
		}
		log.Printf("[%s] provider=%s model=%s", role, providerCfg.Provider, providerCfg.Model)

		return runner.New(runner.Config{
			Role:         role,
			AgentID:      agentID,
			SpaceSlug:    space,
			RepoPath:     activeRepo,
			HiveDir:      hiveDir,
			APIClient:    client,
			APIBase:      apiBase,
			Provider:     provider,
			RolePrompt:   runner.LoadRolePrompt(hiveDir, role),
			BudgetUSD:    budget,
			OneShot:      true,
			Direct:       direct,
			ProposalMode: !direct,
			NoPush:       role == "builder",
			PRMode:       direct && role == "builder" && prMode,
			UseWorktrees: role == "builder" && useWorktrees,
			RepoMap:      repoMap,
			Registry:     reg,
		}), nil
	}

	// Run the pipeline as a state machine.
	// Events drive transitions. Transitions invoke agents. No for-loop.
	smRunner, err := makeRunner("builder") // base runner — state machine overrides Role per state
	if err != nil {
		return err
	}
	sm := runner.NewPipelineStateMachine(smRunner, makeRunner)
	cycleID := "pipeline-" + time.Now().UTC().Format("20060102T150405Z")
	sm.SetCycleID(cycleID)

	// Create telemetry writer for pipeline snapshots.
	var tw *telemetry.Writer
	dsn := storeDSN
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn != "" {
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			log.Printf("[pipeline] telemetry: bad DSN: %v", err)
		} else {
			cfg.MaxConns = 3
			pool, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				log.Printf("[pipeline] telemetry: pool create: %v", err)
			} else {
				tw = telemetry.NewWriter(pool, nil, nil)
				log.Printf("[pipeline] telemetry writer connected")
			}
		}
	}

	// After each phase, persist the session ID and emit telemetry.
	phaseSeq := 0
	type sessionGetter interface{ SessionID() string }
	sm.SetPostPhase(func(role string, provider interface{}) {
		phaseSeq++

		// Emit telemetry snapshot for this phase.
		if tw != nil {
			cost := sm.CurrentRunner().Cost()
			tw.WritePipelineSnapshot(telemetry.PipelineAgentSnapshot{
				Role:      role,
				Model:     runner.ModelForRole(role),
				State:     "done",
				Iteration: phaseSeq,
				CostUSD:   cost.TotalCostUSD,
				TokensIn:  cost.InputTokens,
				TokensOut: cost.OutputTokens,
			})
		}

		sg, ok := provider.(sessionGetter)
		if !ok {
			log.Printf("[pipeline] provider for %s does not expose SessionID (type: %T)", role, provider)
			return
		}
		sid := sg.SessionID()
		if sid == "" {
			log.Printf("[pipeline] %s session ID empty after phase", role)
			return
		}
		if sid == agentSessions[role] {
			return // unchanged
		}
		agentSessions[role] = sid
		if err := client.UpdateAgentSession(space, role, sid); err != nil {
			log.Printf("[pipeline] failed to persist session for %s: %v", role, err)
		} else {
			log.Printf("[pipeline] persisted session for %s: %s", role, sid[:8])
		}
	})
	sm.SetPhaseObserver(func(e runner.PhaseEvent) {
		if tw == nil {
			return
		}
		tw.WritePipelinePhase(telemetry.PipelinePhaseSnapshot{
			CycleID:       e.CycleID,
			Phase:         e.Phase,
			WorkflowStage: e.WorkflowStage,
			Outcome:       e.Outcome,
			Repo:          e.Repo,
			TaskID:        e.TaskID,
			TaskTitle:     e.TaskTitle,
			DurationSecs:  e.DurationSecs,
			CostUSD:       e.CostUSD,
			InputTokens:   e.InputTokens,
			OutputTokens:  e.OutputTokens,
			BoardOpen:     e.BoardOpen,
			ReviseCount:   e.ReviseCount,
			Summary:       e.Summary,
			Error:         e.Error,
			InputRef:      e.InputRef,
			OutputRef:     e.OutputRef,
		})
	})

	if err := sm.Run(ctx); err != nil {
		log.Printf("[pipeline] state machine error: %v", err)
	}

	// Re-resolve repo in case PM changed it during the cycle.
	if len(repoMap) > 0 {
		if resolved := resolveTargetRepo(hiveDir, repoMap, client, space); resolved != "" {
			activeRepo = resolved
		}
	}

	if !direct {
		log.Printf("[pipeline] proposal mode; skipping final direct commit/push/deploy sweep")
		log.Printf("[pipeline] ── cycle complete ──")
		return nil
	}

	// Push ALL repos that have uncommitted changes — not just activeRepo.
	// The Builder may have modified any repo via Operate().
	auditEmitter, closeAudit, auditErr := newAuthorityAuditEmitter(ctx, dsn)
	if auditErr != nil {
		log.Printf("[pipeline] authority audit unavailable: %v", auditErr)
	}
	if closeAudit != nil {
		defer closeAudit()
	}

	if err := authorizeFinalPipelineSweep(repoMap, activeRepo, auditEmitter); err != nil {
		return err
	}
	log.Printf("[pipeline] ── pushing ──")
	siteChanged := false
	for name, repoPath := range repoMap {
		pusher := runner.New(runner.Config{RepoPath: repoPath})
		if pusher.HasChanges() {
			log.Printf("[pipeline] %s has changes — committing and pushing", name)
			commitMsg := fmt.Sprintf("[hive:pipeline] autonomous changes in %s", name)
			if err := pusher.CommitAll(commitMsg); err != nil {
				log.Printf("[pipeline] %s commit error: %v", name, err)
				continue
			}
			if err := pusher.Push(); err != nil {
				log.Printf("[pipeline] %s push error: %v", name, err)
			} else {
				log.Printf("[pipeline] %s pushed", name)
				if name == "site" {
					siteChanged = true
				}
			}
		}
	}
	// Also push activeRepo if not in the map.
	if _, inMap := repoMap[filepath.Base(activeRepo)]; !inMap {
		pusher := runner.New(runner.Config{RepoPath: activeRepo})
		if err := pusher.Push(); err != nil {
			log.Printf("[pipeline] push error: %v", err)
		}
	}

	if siteChanged {
		log.Printf("[pipeline] site changed; external deploy skipped (runtime target: Nucbuntu)")
	}

	log.Printf("[pipeline] ── cycle complete ──")
	return nil
}

func authorizeFinalPipelineSweep(repoMap map[string]string, activeRepo string, auditEmitter authorityRequestEmitter) error {
	if len(repoMap) == 0 {
		return nil
	}
	if err := safety.RequireAuthorized(safety.ActionRepoMutateCrossRepo); err != nil {
		outcome := safety.DefaultOutcome(safety.ActionRepoMutateCrossRepo)
		log.Printf("repo.mutate.cross_repo.blocked action=%s outcome=%s repos=%d active_repo=%s: %v",
			safety.ActionRepoMutateCrossRepo,
			outcome,
			len(repoMap),
			activeRepo,
			err,
		)
		emitAuthorityRequest(auditEmitter, safety.ActionRepoMutateCrossRepo, outcome,
			fmt.Sprintf("final pipeline multi-repo sweep blocked: repos=%d active_repo=%s", len(repoMap), activeRepo))
		return err
	}
	return nil
}

// ─── Daemon mode ─────────────────────────────────────────────────────

const (
	daemonMaxConsecFailures = 3
	daemonBackoffInterval   = 5 * time.Minute
)

// runDaemon loops runPipeline at the given interval until SIGINT/SIGTERM.
// On pipeline failure it retries after a short backoff. After 3 consecutive
// failures it halts. Writes loop/daemon.status after each cycle.
func runDaemon(space, apiBase, repoPath string, budget float64, agentID string, repoMap map[string]string, interval time.Duration, direct, prMode, useWorktrees, autoClone bool, storeDSN string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	hiveDir := findHiveDir()
	statusPath := filepath.Join(hiveDir, "loop", "daemon.status")

	dailyBudget := runner.NewDailyBudget(hiveDir)

	log.Printf("[daemon] starting: interval=%v space=%s budget=$%.0f/day", interval, space, budget)
	cycle := 0
	consecFailures := 0

	for {
		// Budget gate: halt if daily spend exceeds ceiling.
		spent := dailyBudget.Spent()
		if spent >= budget {
			log.Printf("[daemon] ⚠ daily budget exhausted ($%.2f/$%.0f) — halting", spent, budget)
			writeDaemonStatus(statusPath, daemonStatus(cycle, "budget_exhausted", spent, 0))
			return nil
		}
		remaining := budget - spent
		log.Printf("[daemon] budget: $%.2f spent, $%.2f remaining", spent, remaining)

		cycle++
		start := time.Now()
		log.Printf("[daemon] ── cycle %d start ──", cycle)

		if direct && prMode {
			daemonResetToMain(repoPath)
		}

		pipelineErr := runPipeline(space, apiBase, repoPath, budget, agentID, repoMap, direct, prMode, useWorktrees, autoClone, storeDSN)

		elapsed := time.Since(start)
		spent = dailyBudget.Spent()

		if pipelineErr != nil {
			consecFailures++
			log.Printf("[daemon] ████ cycle %d FAILED (%d/%d consecutive): %v ████",
				cycle, consecFailures, daemonMaxConsecFailures, pipelineErr)
			writeDaemonStatus(statusPath, daemonStatus(cycle, "error", spent, elapsed.Seconds()))

			if consecFailures >= daemonMaxConsecFailures {
				return fmt.Errorf("[daemon] halting after %d consecutive failures — last error: %w",
					daemonMaxConsecFailures, pipelineErr)
			}

			log.Printf("[daemon] backing off %v before retry (cycle %d)", daemonBackoffInterval, cycle+1)
			select {
			case <-ctx.Done():
				log.Printf("[daemon] shutdown during backoff after cycle %d", cycle)
				return nil
			case <-time.After(daemonBackoffInterval):
			}
			continue
		}

		consecFailures = 0
		log.Printf("[daemon] cycle %d complete in %v (spent today: $%.2f)", cycle, elapsed.Round(time.Second), spent)
		writeDaemonStatus(statusPath, daemonStatus(cycle, "ok", spent, elapsed.Seconds()))

		// Short cycle = PM idle. Long cycle = real work. Adjust cooldown.
		cooldown := 10 * time.Second
		if elapsed < 30*time.Second {
			cooldown = interval
			log.Printf("[daemon] short cycle (PM idle?) — waiting %v", cooldown)
		} else {
			log.Printf("[daemon] starting next cycle in %v", cooldown)
		}
		select {
		case <-ctx.Done():
			log.Printf("[daemon] shutdown after cycle %d", cycle)
			return nil
		case <-time.After(cooldown):
		}
	}
}

// daemonStatus builds a JSON status line for daemon.status.
// Readable by external monitors, scripts, or a future health endpoint.
func daemonStatus(cycle int, state string, spentUSD, lastCycleSecs float64) string {
	return fmt.Sprintf(`{"cycle":%d,"state":%q,"spent_usd":%.2f,"last_cycle_secs":%.0f,"time":%q}`,
		cycle, state, spentUSD, lastCycleSecs, time.Now().UTC().Format(time.RFC3339))
}

// daemonResetToMain fetches and resets the repo to origin/main before each
// PRMode cycle so stale feature branches don't accumulate across iterations.
func daemonResetToMain(repoPath string) {
	gitCmd := func(args ...string) error {
		cmd := exec.Command("git", args...)
		cmd.Dir = repoPath
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("[daemon] git %s failed: %v\n%s", args[0], err, string(out))
		}
		return err
	}

	// Log current branch for visibility.
	branchCmd := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD")
	branchCmd.Dir = repoPath
	branchOut, _ := branchCmd.Output()
	before := strings.TrimSpace(string(branchOut))
	log.Printf("[daemon] branch before reset: %s", before)

	if err := gitCmd("fetch", "origin"); err != nil {
		return
	}
	if err := gitCmd("checkout", "-f", "main"); err != nil {
		return
	}
	if err := gitCmd("pull", "origin", "main"); err != nil {
		return
	}

	log.Printf("[daemon] branch after reset: main (was: %s)", before)
}

// writeDaemonStatus writes a one-line status to the daemon status file.
func writeDaemonStatus(path, line string) {
	if err := os.WriteFile(path, []byte(line+"\n"), 0o644); err != nil {
		log.Printf("[daemon] warning: could not write status file %s: %v", path, err)
	}
}

// writeMCPConfig generates a temporary MCP config JSON file pointing at the
// knowledge server. Returns the absolute path, or "" if it can't be created.
func writeMCPConfig(hiveDir string, repoMap map[string]string) string {
	if hiveDir == "" {
		return ""
	}

	siteDir := ""
	workspace := filepath.Dir(hiveDir)
	if p, ok := repoMap["site"]; ok {
		siteDir = p
	} else {
		siteDir = filepath.Join(workspace, "site")
	}

	// Use go run to launch the knowledge server — no pre-build needed.
	goPath := "go"
	if p, err := exec.LookPath("go"); err == nil {
		goPath = p
	}

	config := map[string]any{
		"mcpServers": map[string]any{
			"knowledge": map[string]any{
				"command": goPath,
				"args":    []string{"run", "-buildvcs=false", filepath.Join(hiveDir, "cmd", "mcp-knowledge")},
				"env": map[string]string{
					"HIVE_DIR":  hiveDir,
					"SITE_DIR":  siteDir,
					"WORKSPACE": workspace,
				},
			},
		},
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return ""
	}

	configPath := filepath.Join(hiveDir, "loop", "mcp-knowledge.json")
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return ""
	}

	abs, _ := filepath.Abs(configPath)
	return abs
}

// parseRepos parses a "name=path,name=path" string into a map of absolute paths.
// Falls back to a single-entry map using the --repo flag if --repos is empty.
func parseRepos(reposFlag, defaultRepo string) map[string]string {
	m := make(map[string]string)
	if reposFlag != "" {
		for _, entry := range strings.Split(reposFlag, ",") {
			parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
			if len(parts) == 2 {
				abs, err := filepath.Abs(strings.TrimSpace(parts[1]))
				if err == nil {
					m[strings.TrimSpace(parts[0])] = abs
				}
			}
		}
	}
	// Always include the default repo if not already mapped.
	if len(m) == 0 && defaultRepo != "" {
		abs, _ := filepath.Abs(defaultRepo)
		m[filepath.Base(abs)] = abs
	}
	return m
}

// resolveTargetRepo reads the current Scout directive from state.md and
// looks for "Target repo: <name>" (case-insensitive). Returns the absolute
// path if found in repoMap, or "" if not found.
// resolveTargetRepo determines which repo the pipeline should target.
// Reads from the graph first (latest milestone body), falls back to state.md.
func resolveTargetRepo(hiveDir string, repoMap map[string]string, apiClient *api.Client, spaceSlug string) string {
	if len(repoMap) == 0 {
		return ""
	}

	// Graph-first: check latest milestone on the board for "target repo:" in body or title.
	if apiClient != nil {
		tasks, err := apiClient.GetTasks(spaceSlug, "")
		if err == nil {
			for _, t := range tasks {
				if t.Kind != "task" || t.State == "done" || t.State == "closed" {
					continue
				}
				// Check title and body for repo name.
				if repo := extractRepoName(t.Title, repoMap); repo != "" {
					return repo
				}
				if repo := extractRepoName(t.Body, repoMap); repo != "" {
					return repo
				}
			}
		}
	}

	// Fallback: read state.md.
	if hiveDir != "" {
		data, err := os.ReadFile(filepath.Join(hiveDir, "loop", "state.md"))
		if err == nil {
			if repo := extractRepoName(string(data), repoMap); repo != "" {
				return repo
			}
		}
	}
	return ""
}

// extractRepoName finds "target repo: <name>" in text and resolves to an absolute path.
func extractRepoName(text string, repoMap map[string]string) string {
	lower := strings.ToLower(text)
	for _, prefix := range []string{"target repo:", "**target repo:**", "target repo: `", "**target repo:** `"} {
		idx := strings.Index(lower, prefix)
		if idx < 0 {
			continue
		}
		rest := strings.TrimSpace(text[idx+len(prefix):])
		name := strings.TrimLeft(rest, " `")
		if end := strings.IndexAny(name, " \t\n\r`"); end > 0 {
			name = name[:end]
		}
		name = strings.TrimRight(name, "`.,;:")
		name = strings.ToLower(strings.TrimSpace(name))
		if abs, ok := repoMap[name]; ok {
			return abs
		}
	}
	return ""
}

// resolveAnthropicKey returns the Anthropic API key for the hive.
// HIVE_ANTHROPIC_API_KEY takes precedence so the hive can use API billing
// without interfering with Claude Code's Max subscription (which reads
// ANTHROPIC_API_KEY). Falls back to ANTHROPIC_API_KEY for backward compat.
func resolveAnthropicKey() string {
	if key := os.Getenv("HIVE_ANTHROPIC_API_KEY"); key != "" {
		return key
	}
	return os.Getenv("ANTHROPIC_API_KEY")
}

// findHiveDir returns the hive repo directory by walking up from cwd.
func findHiveDir() string {
	// Try cwd first.
	cwd, _ := os.Getwd()
	if _, err := os.Stat(filepath.Join(cwd, "agents")); err == nil {
		return cwd
	}
	// Try parent (if running from cmd/hive).
	parent := filepath.Dir(filepath.Dir(cwd))
	if _, err := os.Stat(filepath.Join(parent, "agents")); err == nil {
		return parent
	}
	return cwd
}

// ─── Legacy runtime mode ────────────────────────────────────────────

func runLegacy(humanName, idea, dsn string, approveRequests, approveRoles bool, bootstrapProfile hive.BootstrapProfile, growthPolicyVersion string, maximumDynamicActors int, automaticallyApprovedActions []safety.ProtectedAction, minimumIterationsBeforeQuiescence int, enforceOrganicGovernanceCausality bool, repoPath, repoWorkspaceRoot, catalogPath string, catalogReloadInterval time.Duration, loop bool, issueScanStageRoleRunner hive.IssueScanStageRoleOutputRunner, issueScanImplementationRunner hive.IssueScanImplementationRunner, issueScanReviewRunner hive.IssueScanAdversarialReviewRunner, issueScanBlockerRepairRunner hive.IssueScanBlockerRepairRunner, issueScanDraftPRAuthorityRequester hive.IssueScanDraftPRAuthorityRequester, issueScanDraftPRCreator work.Epic11PullRequestCreator, issueScanReadyPRRunner hive.IssueScanReadyPRRunner, issueScanScanner *issueScanScannerConfig, space, apiBase, webhookAddr, webhookBearerToken string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Validate before opening or bootstrapping either EventGraph or Work state.
	if err := hive.ValidateBootstrapConfig(hive.Config{
		ApproveRequests:                   approveRequests,
		ApproveRoles:                      approveRoles,
		BootstrapProfile:                  bootstrapProfile,
		GrowthPolicyVersion:               growthPolicyVersion,
		MaximumDynamicActors:              maximumDynamicActors,
		AutomaticallyApprovedActions:      automaticallyApprovedActions,
		MinimumIterationsBeforeQuiescence: minimumIterationsBeforeQuiescence,
		EnforceOrganicGovernanceCausality: enforceOrganicGovernanceCausality,
	}); err != nil {
		return err
	}

	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}

	var pool *pgxpool.Pool
	if dsn != "" {
		fmt.Fprintf(os.Stderr, "Postgres: %s\n", dsn)
		var err error
		pool, err = pgxpool.New(ctx, dsn)
		if err != nil {
			return fmt.Errorf("postgres: %w", err)
		}
		defer pool.Close()
	}

	// Register hive event types before opening the store. Postgres stores
	// deserialize event content on Head() — without these unmarshalers,
	// existing events with types like hive.run.completed fail to load.
	hive.RegisterEventTypes()
	work.RegisterEventTypes()

	s, err := openStore(ctx, pool)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer s.Close()

	actors, err := openActorStore(ctx, pool)
	if err != nil {
		return fmt.Errorf("actor store: %w", err)
	}

	if pool != nil {
		if err := telemetry.EnsureTables(ctx, pool); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: telemetry tables: %v (continuing without telemetry)\n", err)
		} else {
			fmt.Fprintln(os.Stderr, "Telemetry: tables ready")
		}
	}

	if pool != nil {
		fmt.Fprintln(os.Stderr, "WARNING: CLI key derivation is insecure for persistent Postgres stores.")
	}
	humanID, err := registerHuman(actors, humanName)
	if err != nil {
		return fmt.Errorf("register human: %w", err)
	}

	if err := bootstrapGraph(s, humanID); err != nil {
		return fmt.Errorf("bootstrap graph: %w", err)
	}

	if repoPath == "" {
		repoPath = "."
	}

	// Create telemetry writer if postgres is available.
	// The writer gets its own small pool (max 3 conns) so it never competes
	// with agent goroutines for connections on the shared store pool.
	var tw *telemetry.Writer
	if pool != nil {
		cfg, cfgErr := pgxpool.ParseConfig(dsn)
		if cfgErr != nil {
			return fmt.Errorf("telemetry pool config: %w", cfgErr)
		}
		cfg.MaxConns = 3
		telPool, telErr := pgxpool.NewWithConfig(ctx, cfg)
		if telErr != nil {
			return fmt.Errorf("telemetry pool: %w", telErr)
		}
		defer telPool.Close()
		tw = telemetry.NewWriter(telPool, s, nil) // budgetRegistry wired in Runtime.Run()
		pruner := telemetry.NewPruner(telPool)
		go pruner.Start(ctx)
	}

	apiKey := os.Getenv("TRANSPARA_API_KEY")
	siteOpsAPIKey := resolveSiteOpsAPIKey(os.Getenv("HIVE_SITE_OPS_API_KEY"), apiKey)
	var siteClient *api.Client
	if apiKey != "" {
		siteClient = api.New(apiBase, apiKey)
	}

	rt, err := hive.New(ctx, hive.Config{
		Store:                              s,
		Actors:                             actors,
		HumanID:                            humanID,
		ApproveRequests:                    approveRequests,
		ApproveRoles:                       approveRoles,
		BootstrapProfile:                   bootstrapProfile,
		GrowthPolicyVersion:                growthPolicyVersion,
		MaximumDynamicActors:               maximumDynamicActors,
		AutomaticallyApprovedActions:       automaticallyApprovedActions,
		RepoPath:                           repoPath,
		RepoWorkspaceRoot:                  repoWorkspaceRoot,
		IsolateRunTasks:                    !loop,
		CatalogPath:                        catalogPath,
		CatalogReloadInterval:              catalogReloadInterval,
		MinimumIterationsBeforeQuiescence:  minimumIterationsBeforeQuiescence,
		EnforceOrganicGovernanceCausality:  enforceOrganicGovernanceCausality,
		Loop:                               loop,
		IssueScanStageRoleOutputRunner:     issueScanStageRoleRunner,
		IssueScanImplementationRunner:      issueScanImplementationRunner,
		IssueScanAdversarialReviewRunner:   issueScanReviewRunner,
		IssueScanBlockerRepairRunner:       issueScanBlockerRepairRunner,
		IssueScanDraftPRAuthorityRequester: issueScanDraftPRAuthorityRequester,
		IssueScanDraftPRCreator:            issueScanDraftPRCreator,
		IssueScanReadyPRRunner:             issueScanReadyPRRunner,
		TelemetryWriter:                    tw,
		APIClient:                          siteClient,
	})
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}

	starterAgents, err := hive.StarterAgentsForProfile(humanName, bootstrapProfile)
	if err != nil {
		return err
	}
	for _, def := range starterAgents {
		if err := rt.Register(def); err != nil {
			return fmt.Errorf("register %s: %w", def.Name, err)
		}
	}

	// Start the webhook event listener so the site can push ops to us.
	// Runtime implements runner.Dispatcher — site ops are translated into
	// eventgraph bus events that agents already subscribe to.
	listenerPort := os.Getenv("HIVE_LISTENER_PORT")
	if listenerPort == "" {
		listenerPort = "8081"
	}
	go func() {
		if err := runner.StartEventListener(ctx, rt, runner.EventListenerConfig{
			Addr:        webhookAddr,
			Port:        listenerPort,
			BearerToken: webhookBearerToken,
		}); err != nil {
			log.Printf("event listener exited: %v", err)
		}
	}()

	// Reconciliation ticker: polls the site for ops missed by the webhook
	// path (site restart, network partition, hive downtime) and replays
	// them through the same dispatcher. Requires postgres + an API key —
	// otherwise the ticker can't store a watermark or talk to the site.
	if pool != nil && siteOpsAPIKey != "" && space != "" {
		if err := reconciliation.EnsureTables(ctx, pool); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: reconciliation tables: %v (continuing without reconciliation)\n", err)
		} else {
			source := reconciliation.NewHTTPSource(apiBase, siteOpsAPIKey)
			ticker := reconciliation.NewTicker(pool, rt, source, space)
			go ticker.Start(ctx)
			fmt.Fprintf(os.Stderr, "Reconciliation: polling %s/api/hive/site-ops for space=%s\n", apiBase, space)
		}
	} else {
		fmt.Fprintln(os.Stderr, "Reconciliation: skipped (need --store, HIVE_SITE_OPS_API_KEY or TRANSPARA_API_KEY, and --space)")
	}

	var scannerWG sync.WaitGroup
	var stopScanner context.CancelFunc
	if issueScanScanner != nil && issueScanScanner.Interval > 0 {
		scannerCtx, cancelScanner := context.WithCancel(ctx)
		stopScanner = cancelScanner
		scannerContext := newIssueScanScannerContext(s, actors, humanID)
		scannerWG.Add(1)
		go func() {
			defer scannerWG.Done()
			runIssueScanScannerLoop(scannerCtx, scannerContext, *issueScanScanner, ghIssueLister{})
		}()
		fmt.Fprintf(os.Stderr, "Issue-scan scanner: enabled interval=%s repos=%s limit=%d max_new_runs=%d max_duration=%s one_active=%t kill_switch=%q\n", issueScanScanner.Interval, strings.Join(issueScanScanner.Repos, ","), issueScanScanner.Limit, issueScanScanner.MaxNewRuns, issueScanScanner.MaxDuration, issueScanScanner.OneActive, issueScanScanner.KillSwitchPath)
	}

	runErr := rt.Run(ctx, idea)
	if stopScanner != nil {
		stopScanner()
		scannerWG.Wait()
	}
	if runErr != nil {
		return fmt.Errorf("run: %w", runErr)
	}

	count, _ := s.Count()
	fmt.Fprintf(os.Stderr, "Events recorded: %d\n", count)
	return nil
}

// resolveSiteOpsAPIKey keeps the reconciliation credential independent from
// the inbound webhook credential. The fallback preserves existing deployments
// that use TRANSPARA_API_KEY for both directions.
func resolveSiteOpsAPIKey(siteOpsAPIKey, transparaAPIKey string) string {
	if siteOpsAPIKey != "" {
		return siteOpsAPIKey
	}
	return transparaAPIKey
}

// ─── Store helpers ───────────────────────────────────────────────────

func openStore(ctx context.Context, pool *pgxpool.Pool) (store.Store, error) {
	if pool == nil {
		fmt.Fprintln(os.Stderr, "Store: in-memory")
		return store.NewInMemoryStore(), nil
	}
	fmt.Fprintln(os.Stderr, "Store: postgres")
	return pgstore.NewPostgresStoreFromPool(ctx, pool)
}

func openActorStore(ctx context.Context, pool *pgxpool.Pool) (actor.IActorStore, error) {
	if pool == nil {
		fmt.Fprintln(os.Stderr, "Actor store: in-memory")
		return actor.NewInMemoryActorStore(), nil
	}
	fmt.Fprintln(os.Stderr, "Actor store: postgres")
	return pgactor.NewPostgresActorStoreFromPool(ctx, pool)
}

func registerHuman(actors actor.IActorStore, displayName string) (types.ActorID, error) {
	h := sha256.Sum256([]byte("human:" + displayName))
	priv := ed25519.NewKeyFromSeed(h[:])
	pub := priv.Public().(ed25519.PublicKey)

	pk, err := types.NewPublicKey([]byte(pub))
	if err != nil {
		return types.ActorID{}, fmt.Errorf("public key: %w", err)
	}

	a, err := actors.Register(pk, displayName, event.ActorTypeHuman)
	if err != nil {
		return types.ActorID{}, err
	}
	return a.ID(), nil
}

func bootstrapGraph(s store.Store, humanID types.ActorID) error {
	head, err := s.Head()
	if err != nil {
		return fmt.Errorf("check head: %w", err)
	}
	if head.IsSome() {
		return nil
	}

	fmt.Fprintln(os.Stderr, "Bootstrapping event graph...")
	registry := event.DefaultRegistry()
	bsFactory := event.NewBootstrapFactory(registry)

	signer := &bootstrapSigner{humanID: humanID}
	bootstrap, err := bsFactory.Init(humanID, signer)
	if err != nil {
		return fmt.Errorf("create genesis event: %w", err)
	}
	if _, err := s.Append(bootstrap); err != nil {
		return fmt.Errorf("append genesis event: %w", err)
	}
	fmt.Fprintln(os.Stderr, "Event graph bootstrapped.")
	return nil
}

type bootstrapSigner struct {
	humanID types.ActorID
}

func (b *bootstrapSigner) Sign(data []byte) (types.Signature, error) {
	h := sha256.Sum256([]byte("signer:" + b.humanID.Value()))
	priv := ed25519.NewKeyFromSeed(h[:])
	sig := ed25519.Sign(priv, data)
	return types.NewSignature(sig)
}
