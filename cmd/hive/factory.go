package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/transpara-ai/eventgraph/go/pkg/actor"
	"github.com/transpara-ai/eventgraph/go/pkg/event"
	"github.com/transpara-ai/eventgraph/go/pkg/store"
	"github.com/transpara-ai/eventgraph/go/pkg/types"

	"github.com/transpara-ai/hive/pkg/hive"
	"github.com/transpara-ai/hive/pkg/safety"
	"github.com/transpara-ai/work"
)

// ─── factory ────────────────────────────────────────────────────────────────
//
// The Factory is ALWAYS-ON: `factory daemon` runs a governing loop forever
// (Keepalive=true → roles never exit on quiescence), and each Order is a
// bounded sub-flow submitted into that loop. The four subcommands are thin glue
// over seams built earlier in the Dark Factory reunification:
//
//   daemon       → the civilization daemon path with Keepalive forced on.
//   order        → work.SeedFactoryOrder (W1): one Order → one Work task.
//   scan-issues  → scan Transpara-AI GitHub issues, then queue one governed
//                  run request for the existing FactoryOrder dispatcher.
//   canary-scan  → bounded Level 1 canary scan that records only parked
//                  issue-scan evidence for non-PR-ready GitHub issues.
//   progress-issue-scan
//               → progress one named issue-scan run; optionally invoke
//                  explicitly configured external runners for that run only.
//   advance-issue-scan
//               → release the next issue-scan stage dependency barrier after
//                  its lifecycle contracts are present.
//   record-issue-scan-role-output
//               → record one civic role's durable output evidence for an
//                  issue-scan lifecycle stage without completing the stage.
//   run-issue-scan-stage-role-output
//               → invoke a configured planning-stage role-output runner, then
//                  record its returned role-output artifacts.
//   run-issue-scan-implementation
//               → invoke a configured implementation runner, then record its
//                  Operate result on the concrete implementation Work task.
//   record-issue-scan-review
//               → record an exact-head adversarial review receipt and emit the
//                  durable code.review.submitted event consumed by the
//                  issue-scan review stage.
//   run-issue-scan-review
//               → invoke a configured adversarial review runner with exact-head
//                  context, then record the returned receipt.
//   run-issue-scan-blocker-repair
//               → invoke a configured blocker repair runner after request_changes
//                  reopens implementation work, then record its Operate result.
//   record-issue-scan-draft-pr
//               → link the governed draft-PR creation receipt to the terminal
//                  issue-scan ready stage.
//   record-issue-scan-ready-pr
//               → record terminal ready-for-Human PR evidence and progress the
//                  final issue-scan stage.
//   run-issue-scan-ready-pr
//               → invoke a configured terminal ready-PR evidence runner, then
//                  record its draft receipt and ready-for-Human evidence.
//   issue-scan-runner-contracts
//               → print the external runner chain's JSON contract and operator
//                  wiring boundaries.
//   issue-scan-runner-contexts
//               → build ready/not-ready context probes for one stored
//                  issue-scan run; may dispatch/scaffold, but never runs
//                  external runners.
//   request-issue-scan-pr
//               → derive the issue-scan draft-PR title/body/target and raise
//                  the governed draft-PR authority request; the gate holds.
//   create-issue-scan-draft-pr
//               → consume the approved issue-scan draft-PR request, create the
//                  draft PR, and record its receipt on the ready stage.
//   complete-issue-scan-stage
//               → record governed runtime evidence for one issue-scan stage,
//                  complete that stage task, and optionally release the next
//                  stage barrier.
//   validate-issue-scan-runner-suite
//               → locally validate a runner-suite package manifest and its
//                  inert fixtures against the in-process runner contracts;
//                  reads files only, never executes runners.
//   request-pr   → (*hive.Runtime).RaiseDraftPRAuthorityRequest (H2): raise the
//                  guardian authority request; the gate HOLDS by design.
//   create-pr    → hive.LoadApprovedDraftPRTarget (the governance gate: load the
//                  recorded approval, refuse if denied/undecided) +
//                  hive.CreateDraftPRFromApprovedDecision (H4) + the live GitHub
//                  creator work.NewEpic11GitHubPullRequestCreator (W2).

func cmdFactory(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%w: hive factory <daemon|order|tlc-change-workflow|preflight-hive-unit|scan-issues|canary-scan|progress-issue-scan|advance-issue-scan|record-issue-scan-role-output|run-issue-scan-stage-role-output|run-issue-scan-implementation|record-issue-scan-review|run-issue-scan-review|run-issue-scan-blocker-repair|record-issue-scan-draft-pr|record-issue-scan-ready-pr|run-issue-scan-ready-pr|issue-scan-runner-contracts|issue-scan-runner-contexts|request-issue-scan-pr|create-issue-scan-draft-pr|complete-issue-scan-stage|validate-issue-scan-runner-suite|request-pr|create-pr> [flags]", errUsage)
	}
	subverb := args[0]
	rest := args[1:]
	switch subverb {
	case "daemon":
		return cmdFactoryDaemon(rest)
	case "order":
		return cmdFactoryOrder(rest)
	case "tlc-change-workflow":
		return cmdFactoryTLCChangeWorkflow(rest)
	case "preflight-hive-unit":
		return cmdFactoryPreflightHiveUnit(rest)
	case "scan-issues":
		return cmdFactoryScanIssues(rest)
	case "canary-scan":
		return cmdFactoryCanaryScan(rest)
	case "progress-issue-scan":
		return cmdFactoryProgressIssueScan(rest)
	case "advance-issue-scan":
		return cmdFactoryAdvanceIssueScan(rest)
	case "record-issue-scan-role-output":
		return cmdFactoryRecordIssueScanRoleOutput(rest)
	case "run-issue-scan-stage-role-output":
		return cmdFactoryRunIssueScanStageRoleOutput(rest)
	case "run-issue-scan-implementation":
		return cmdFactoryRunIssueScanImplementation(rest)
	case "record-issue-scan-review":
		return cmdFactoryRecordIssueScanReview(rest)
	case "run-issue-scan-review":
		return cmdFactoryRunIssueScanReview(rest)
	case "run-issue-scan-blocker-repair":
		return cmdFactoryRunIssueScanBlockerRepair(rest)
	case "record-issue-scan-draft-pr":
		return cmdFactoryRecordIssueScanDraftPR(rest)
	case "record-issue-scan-ready-pr":
		return cmdFactoryRecordIssueScanReadyPR(rest)
	case "run-issue-scan-ready-pr":
		return cmdFactoryRunIssueScanReadyPR(rest)
	case "issue-scan-runner-contracts":
		return cmdFactoryIssueScanRunnerContracts(rest)
	case "issue-scan-runner-contexts":
		return cmdFactoryIssueScanRunnerContexts(rest)
	case "request-issue-scan-pr":
		return cmdFactoryRequestIssueScanPR(rest)
	case "create-issue-scan-draft-pr":
		return cmdFactoryCreateIssueScanDraftPR(rest)
	case "validate-issue-scan-runner-suite":
		return cmdFactoryValidateIssueScanRunnerSuite(rest)
	case "complete-issue-scan-stage":
		return cmdFactoryCompleteIssueScanStage(rest)
	case "request-pr":
		return cmdFactoryRequestPR(rest)
	case "create-pr":
		return cmdFactoryCreatePR(rest)
	case "-h", "--help":
		fmt.Println("usage: hive factory <daemon|order|tlc-change-workflow|preflight-hive-unit|scan-issues|canary-scan|progress-issue-scan|advance-issue-scan|record-issue-scan-role-output|run-issue-scan-stage-role-output|run-issue-scan-implementation|record-issue-scan-review|run-issue-scan-review|run-issue-scan-blocker-repair|record-issue-scan-draft-pr|record-issue-scan-ready-pr|run-issue-scan-ready-pr|issue-scan-runner-contracts|issue-scan-runner-contexts|request-issue-scan-pr|create-issue-scan-draft-pr|complete-issue-scan-stage|validate-issue-scan-runner-suite|request-pr|create-pr> [flags]")
		fmt.Println("\nRun 'hive factory <sub> --help' for subcommand flags.")
		return nil
	default:
		return fmt.Errorf("unknown factory subverb %q (want daemon|order|tlc-change-workflow|preflight-hive-unit|scan-issues|canary-scan|progress-issue-scan|advance-issue-scan|record-issue-scan-role-output|run-issue-scan-stage-role-output|run-issue-scan-implementation|record-issue-scan-review|run-issue-scan-review|run-issue-scan-blocker-repair|record-issue-scan-draft-pr|record-issue-scan-ready-pr|run-issue-scan-ready-pr|issue-scan-runner-contracts|issue-scan-runner-contexts|request-issue-scan-pr|create-issue-scan-draft-pr|complete-issue-scan-stage|validate-issue-scan-runner-suite|request-pr|create-pr)", subverb)
	}
}

// cmdFactoryDaemon is the Factory spelling of the shared always-on governing
// loop.
func cmdFactoryDaemon(args []string) error {
	return cmdGovernedDaemon("factory daemon", args)
}

// cmdGovernedDaemon runs the always-on Civilization governing loop. Both
// `hive civilization daemon` and `hive factory daemon` use this parser so the
// operator-facing Civilization command exposes the same governed issue-scan
// controls as the Factory command. The final loop=true runLegacy call flows to
// hive.Config.Loop, which runtime.go maps to Keepalive, so governance roles block
// on the bus forever instead of exiting at quiescence.
func cmdGovernedDaemon(name string, args []string) error {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	human := fs.String("human", "", "Operator name (required)")
	seedSpec := fs.String("seed-spec", "", "Optional initial spec to ingest before daemon starts")
	storeDSN := fs.String("store", "", "Store DSN (postgres://... or empty for in-memory)")
	repo := fs.String("repo", "", "Path to repo for Operate (default: current dir)")
	repoWorkspaceRoot := fs.String("repo-workspace-root", "", "Path to directory containing Transpara-AI repo checkouts for issue-scan implementation targets")
	catalog := fs.String("catalog", "", "Custom YAML model catalog (merged with built-in defaults)")
	catalogReloadInterval := fs.Duration("catalog-reload-interval", 0, "Reload --catalog on this interval for future model resolution; 0 disables")
	issueScanInterval := fs.Duration("issue-scan-interval", 0, "Autonomously scan Transpara-AI GitHub issues on this interval; 0 disables")
	issueScanLimit := fs.Int("issue-scan-limit", 10, "Maximum open issues to read per repo for daemon issue scanning")
	issueScanMaxIterations := fs.Int("issue-scan-max-iterations", 30, "Queued issue-scan run iteration budget")
	issueScanMaxCostUSD := fs.Float64("issue-scan-max-cost-usd", 25, "Queued issue-scan run cost budget in USD")
	issueScanMaxNewRuns := fs.Int("issue-scan-max-new-runs", 1, "Maximum new issue-scan runs this daemon instance may queue; bounds unattended scan cost")
	issueScanMaxDuration := fs.Duration("issue-scan-max-duration", 0, "Hard wall-clock cap for this daemon issue-scan scanner process; 0 disables")
	issueScanKillSwitch := fs.String("issue-scan-kill-switch", "", "Path whose existence permanently halts daemon issue scanning for this process before queueing work")
	issueScanOneActive := fs.Bool("issue-scan-one-active", false, "Refuse to queue a new issue-scan run while any existing unparked issue-scan run lacks terminal ready evidence")
	issueScanReviewQueueThreshold := fs.Int("issue-scan-review-queue-threshold", issueScanDefaultReviewQueueThreshold, "Maximum open PRs counted as unproven exact-head review load before daemon issue scanning refuses new work-start")
	issueScanRegistry := fs.Bool("issue-scan-registry", false, "Scan every Transpara-AI GitHub repo in repos.json when no --issue-scan-repo is supplied")
	issueScanRequireFullChain := fs.Bool("issue-scan-require-full-chain", false, "Fail startup unless the issue-scan daemon is configured through ready-for-Human PR evidence")
	issueScanRepos := repeatedStringFlag{}
	issueScanLabels := repeatedStringFlag{}
	fs.Var(&issueScanRepos, "issue-scan-repo", "Transpara-AI repo slug to scan from daemon, e.g. transpara-ai/hive (repeatable; required with --issue-scan-interval unless --issue-scan-registry is set)")
	fs.Var(&issueScanLabels, "issue-scan-label", "GitHub issue label filter for daemon issue scanning (repeatable)")
	stageRoleRunner := fs.String("issue-scan-stage-role-runner", "", "Executable planning-stage issue-scan role-output runner; receives JSON context on stdin and returns role outputs JSON")
	stageRoleTimeout := fs.Duration("issue-scan-stage-role-timeout", 15*time.Minute, "Maximum runtime for --issue-scan-stage-role-runner")
	stageRoleRunnerArgs := repeatedStringFlag{}
	fs.Var(&stageRoleRunnerArgs, "issue-scan-stage-role-runner-arg", "Argument passed to --issue-scan-stage-role-runner (repeatable)")
	implementationRunner := fs.String("issue-scan-implementation-runner", "", "Executable issue-scan implementation runner; receives JSON context on stdin and returns Operate result JSON")
	implementationTimeout := fs.Duration("issue-scan-implementation-timeout", 15*time.Minute, "Maximum runtime for --issue-scan-implementation-runner")
	implementationRunnerArgs := repeatedStringFlag{}
	fs.Var(&implementationRunnerArgs, "issue-scan-implementation-runner-arg", "Argument passed to --issue-scan-implementation-runner (repeatable)")
	reviewRunner := fs.String("issue-scan-review-runner", "", "Executable exact-head issue-scan adversarial review runner; receives JSON context on stdin and returns receipt JSON")
	reviewTimeout := fs.Duration("issue-scan-review-timeout", 15*time.Minute, "Maximum runtime for --issue-scan-review-runner")
	reviewRunnerArgs := repeatedStringFlag{}
	fs.Var(&reviewRunnerArgs, "issue-scan-review-runner-arg", "Argument passed to --issue-scan-review-runner (repeatable)")
	blockerRepairRunner := fs.String("issue-scan-blocker-repair-runner", "", "Executable issue-scan blocker repair runner; receives JSON context on stdin and returns Operate result JSON")
	blockerRepairTimeout := fs.Duration("issue-scan-blocker-repair-timeout", 15*time.Minute, "Maximum runtime for --issue-scan-blocker-repair-runner")
	blockerRepairRunnerArgs := repeatedStringFlag{}
	fs.Var(&blockerRepairRunnerArgs, "issue-scan-blocker-repair-runner-arg", "Argument passed to --issue-scan-blocker-repair-runner (repeatable)")
	issueScanDraftPRRequest := fs.Bool("issue-scan-draft-pr-request", false, "Raise issue-scan draft PR authority requests after zero blockers; does not create PRs or approve requests")
	issueScanDraftPRRequestBase := fs.String("issue-scan-draft-pr-request-base", "main", "Base branch ref for --issue-scan-draft-pr-request")
	issueScanDraftPRCreate := fs.Bool("issue-scan-draft-pr-create", false, "Create approved issue-scan draft PRs after recorded Human approval; requires GITHUB_TOKEN")
	readyPRRunner := fs.String("issue-scan-ready-pr-runner", "", "Executable terminal ready-PR evidence runner; receives JSON context on stdin and returns draft receipt plus ready evidence JSON")
	readyPRMarkReady := fs.Bool("issue-scan-ready-pr-mark-ready", false, "Mark created issue-scan draft PRs ready, run exact-head ready-state review, then record ready-for-Human evidence; requires GITHUB_TOKEN")
	readyPRReviewRunner := fs.String("issue-scan-ready-pr-review-runner", "", "Executable ready-state exact-head review runner used with --issue-scan-ready-pr-mark-ready; receives JSON context on stdin and returns review receipt JSON")
	readyPRTimeout := fs.Duration("issue-scan-ready-pr-timeout", 15*time.Minute, "Maximum runtime for --issue-scan-ready-pr-runner")
	readyPRRunnerArgs := repeatedStringFlag{}
	readyPRReviewRunnerArgs := repeatedStringFlag{}
	fs.Var(&readyPRRunnerArgs, "issue-scan-ready-pr-runner-arg", "Argument passed to --issue-scan-ready-pr-runner (repeatable)")
	fs.Var(&readyPRReviewRunnerArgs, "issue-scan-ready-pr-review-runner-arg", "Argument passed to --issue-scan-ready-pr-review-runner (repeatable)")
	approveRequests := fs.Bool("approve-requests", false, "Auto-approve authority requests")
	approveRoles := fs.Bool("approve-roles", false, "Auto-approve role proposals")
	bootstrapProfile := fs.String("bootstrap-profile", string(hive.BootstrapProfileFull), "Bootstrap profile: full|organic-v1")
	approveActions := repeatedStringFlag{}
	fs.Var(&approveActions, "approve-action", "Auto-approve one exact known protected action (repeatable)")
	space := fs.String("space", "hive", "transpara.ai space slug")
	apiBase := fs.String("api", "https://transpara.ai", "transpara.ai API base URL")
	webhookAddr := fs.String("webhook-addr", "", "Webhook listen address (default: loopback 127.0.0.1 at HIVE_LISTENER_PORT or 8081; non-loopback binds require --webhook-require-auth)")
	webhookRequireAuth := fs.Bool("webhook-require-auth", false, "Require Authorization: Bearer TRANSPARA_API_KEY on POST /event (required for non-loopback --webhook-addr)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *human == "" {
		return fmt.Errorf("--human is required")
	}
	profile, growthPolicyVersion, maximumDynamicActors, actions, err := parseBootstrapRuntimeFlags(*bootstrapProfile, approveActions, *approveRequests)
	if err != nil {
		return err
	}
	webhookBearerToken, err := resolveWebhookBearerToken(*webhookAddr, *webhookRequireAuth, os.Getenv("TRANSPARA_API_KEY"))
	if err != nil {
		return err
	}
	if *issueScanInterval > 0 && *approveRequests {
		return fmt.Errorf("--issue-scan-interval cannot be combined with --approve-requests")
	}
	if *issueScanInterval > 0 && *approveRoles {
		return fmt.Errorf("--issue-scan-interval cannot be combined with --approve-roles")
	}
	if *issueScanDraftPRCreate && *approveRequests {
		return fmt.Errorf("--issue-scan-draft-pr-create cannot be combined with --approve-requests")
	}
	if *issueScanDraftPRCreate && *approveRoles {
		return fmt.Errorf("--issue-scan-draft-pr-create cannot be combined with --approve-roles")
	}
	if *issueScanDraftPRRequest && *approveRequests {
		return fmt.Errorf("--issue-scan-draft-pr-request cannot be combined with --approve-requests")
	}
	if *issueScanDraftPRRequest && *approveRoles {
		return fmt.Errorf("--issue-scan-draft-pr-request cannot be combined with --approve-roles")
	}
	issueScanDraftPRRequestBaseSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "issue-scan-draft-pr-request-base" {
			issueScanDraftPRRequestBaseSet = true
		}
	})
	if issueScanDraftPRRequestBaseSet && !*issueScanDraftPRRequest {
		return fmt.Errorf("--issue-scan-draft-pr-request is required when --issue-scan-draft-pr-request-base is set")
	}
	if *readyPRMarkReady && *approveRequests {
		return fmt.Errorf("--issue-scan-ready-pr-mark-ready cannot be combined with --approve-requests")
	}
	if *readyPRMarkReady && *approveRoles {
		return fmt.Errorf("--issue-scan-ready-pr-mark-ready cannot be combined with --approve-roles")
	}
	if *issueScanRequireFullChain {
		if err := requireIssueScanFullChainConfig(issueScanFullChainConfig{
			IssueScanInterval:     *issueScanInterval,
			HasIssueScanRepos:     len(issueScanRepos) > 0,
			IssueScanRegistry:     *issueScanRegistry,
			RepoWorkspaceRoot:     *repoWorkspaceRoot,
			StageRoleRunner:       *stageRoleRunner,
			ImplementationRunner:  *implementationRunner,
			ReviewRunner:          *reviewRunner,
			BlockerRepairRunner:   *blockerRepairRunner,
			DraftPRRequest:        *issueScanDraftPRRequest,
			DraftPRCreate:         *issueScanDraftPRCreate,
			ReadyPRMarkReady:      *readyPRMarkReady,
			ReadyPRReviewRunner:   *readyPRReviewRunner,
			GenericReadyPRRunner:  *readyPRRunner,
			GenericReadyPRRunArgs: len(readyPRRunnerArgs) > 0,
		}); err != nil {
			return err
		}
	}
	var issueScanScanner *issueScanScannerConfig
	if *issueScanInterval > 0 {
		if *issueScanLimit <= 0 {
			return fmt.Errorf("--issue-scan-limit must be greater than zero")
		}
		if *issueScanMaxIterations <= 0 {
			return fmt.Errorf("--issue-scan-max-iterations must be greater than zero")
		}
		if *issueScanMaxCostUSD < 0 {
			return fmt.Errorf("--issue-scan-max-cost-usd must be zero or greater")
		}
		if *issueScanMaxNewRuns <= 0 {
			return fmt.Errorf("--issue-scan-max-new-runs must be greater than zero")
		}
		if *issueScanMaxDuration < 0 {
			return fmt.Errorf("--issue-scan-max-duration must be zero or greater")
		}
		if *issueScanReviewQueueThreshold <= 0 {
			return fmt.Errorf("--issue-scan-review-queue-threshold must be greater than zero")
		}
		registryPath := ""
		if len(issueScanRepos) == 0 && *issueScanRegistry {
			var err error
			registryPath, err = issueScanRegistryPath()
			if err != nil {
				return err
			}
		}
		normalizedRepos, err := resolveIssueScanRepos(issueScanRepos, *issueScanRegistry, registryPath)
		if err != nil {
			return err
		}
		if *issueScanReviewQueueThreshold > issueScanReviewQueueReadLimit {
			return fmt.Errorf("--issue-scan-review-queue-threshold exceeds readable PR cap %d", issueScanReviewQueueReadLimit)
		}
		issueScanScanner = &issueScanScannerConfig{
			OperatorID:           hive.IssueScanOperatorID(*human),
			Repos:                normalizedRepos,
			Labels:               append([]string(nil), issueScanLabels...),
			Limit:                *issueScanLimit,
			MaxIterations:        *issueScanMaxIterations,
			MaxCostUSD:           *issueScanMaxCostUSD,
			MaxNewRuns:           *issueScanMaxNewRuns,
			MaxDuration:          *issueScanMaxDuration,
			KillSwitchPath:       *issueScanKillSwitch,
			OneActive:            *issueScanOneActive,
			ReviewQueueThreshold: *issueScanReviewQueueThreshold,
			Interval:             *issueScanInterval,
			AuthorityScope:       "transpara-ai issue scan to ready-for-Human PR; no merge or deploy",
		}
	}
	var issueScanStageRoleRunner hive.IssueScanStageRoleOutputRunner
	if strings.TrimSpace(*stageRoleRunner) == "" {
		if len(stageRoleRunnerArgs) > 0 {
			return fmt.Errorf("--issue-scan-stage-role-runner is required when --issue-scan-stage-role-runner-arg is set")
		}
	} else {
		if *stageRoleTimeout <= 0 {
			return fmt.Errorf("--issue-scan-stage-role-timeout must be greater than zero")
		}
		issueScanStageRoleRunner = issueScanStageRoleOutputCommandRunner(*stageRoleRunner, stageRoleRunnerArgs, *stageRoleTimeout)
	}
	var issueScanImplementationRunner hive.IssueScanImplementationRunner
	if strings.TrimSpace(*implementationRunner) == "" {
		if len(implementationRunnerArgs) > 0 {
			return fmt.Errorf("--issue-scan-implementation-runner is required when --issue-scan-implementation-runner-arg is set")
		}
	} else {
		if *implementationTimeout <= 0 {
			return fmt.Errorf("--issue-scan-implementation-timeout must be greater than zero")
		}
		issueScanImplementationRunner = issueScanImplementationCommandRunner(*implementationRunner, implementationRunnerArgs, *implementationTimeout)
	}
	var issueScanReviewRunner hive.IssueScanAdversarialReviewRunner
	if strings.TrimSpace(*reviewRunner) == "" {
		if len(reviewRunnerArgs) > 0 {
			return fmt.Errorf("--issue-scan-review-runner is required when --issue-scan-review-runner-arg is set")
		}
	} else {
		if *reviewTimeout <= 0 {
			return fmt.Errorf("--issue-scan-review-timeout must be greater than zero")
		}
		issueScanReviewRunner = issueScanReviewCommandRunner(*reviewRunner, reviewRunnerArgs, *reviewTimeout)
	}
	var issueScanBlockerRepairRunner hive.IssueScanBlockerRepairRunner
	if strings.TrimSpace(*blockerRepairRunner) == "" {
		if len(blockerRepairRunnerArgs) > 0 {
			return fmt.Errorf("--issue-scan-blocker-repair-runner is required when --issue-scan-blocker-repair-runner-arg is set")
		}
	} else {
		if *blockerRepairTimeout <= 0 {
			return fmt.Errorf("--issue-scan-blocker-repair-timeout must be greater than zero")
		}
		issueScanBlockerRepairRunner = issueScanBlockerRepairCommandRunner(*blockerRepairRunner, blockerRepairRunnerArgs, *blockerRepairTimeout)
	}
	var issueScanDraftPRAuthorityRequester hive.IssueScanDraftPRAuthorityRequester
	if *issueScanDraftPRRequest {
		if strings.TrimSpace(*issueScanDraftPRRequestBase) == "" {
			return fmt.Errorf("--issue-scan-draft-pr-request-base is required when --issue-scan-draft-pr-request is enabled")
		}
		issueScanDraftPRAuthorityRequester = issueScanDraftPRAuthorityRequestLocalGitRunner(*issueScanDraftPRRequestBase)
	}
	var issueScanDraftPRCreator work.Epic11PullRequestCreator
	if *issueScanDraftPRCreate {
		token := strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))
		if token == "" {
			return fmt.Errorf("GITHUB_TOKEN is required when --issue-scan-draft-pr-create is enabled")
		}
		issueScanDraftPRCreator = work.NewEpic11GitHubPullRequestCreator(token)
	}
	var issueScanReadyPRRunner hive.IssueScanReadyPRRunner
	if *readyPRMarkReady {
		if strings.TrimSpace(*readyPRRunner) != "" || len(readyPRRunnerArgs) > 0 {
			return fmt.Errorf("--issue-scan-ready-pr-mark-ready cannot be combined with --issue-scan-ready-pr-runner")
		}
		if strings.TrimSpace(*readyPRReviewRunner) == "" {
			return fmt.Errorf("--issue-scan-ready-pr-review-runner is required when --issue-scan-ready-pr-mark-ready is enabled")
		}
		if *readyPRTimeout <= 0 {
			return fmt.Errorf("--issue-scan-ready-pr-timeout must be greater than zero")
		}
		token := strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))
		if token == "" {
			return fmt.Errorf("GITHUB_TOKEN is required when --issue-scan-ready-pr-mark-ready is enabled")
		}
		issueScanReadyPRRunner = hive.NewIssueScanReadyPRFinalizerRunner(
			newIssueScanReadyPRGitHubClient(token),
			issueScanReadyStateReviewCommandRunner(*readyPRReviewRunner, readyPRReviewRunnerArgs, *readyPRTimeout),
		)
	} else if strings.TrimSpace(*readyPRReviewRunner) != "" || len(readyPRReviewRunnerArgs) > 0 {
		return fmt.Errorf("--issue-scan-ready-pr-mark-ready is required when --issue-scan-ready-pr-review-runner is set")
	} else if strings.TrimSpace(*readyPRRunner) == "" {
		if len(readyPRRunnerArgs) > 0 {
			return fmt.Errorf("--issue-scan-ready-pr-runner is required when --issue-scan-ready-pr-runner-arg is set")
		}
	} else {
		if *readyPRTimeout <= 0 {
			return fmt.Errorf("--issue-scan-ready-pr-timeout must be greater than zero")
		}
		issueScanReadyPRRunner = issueScanReadyPRCommandRunner(*readyPRRunner, readyPRRunnerArgs, *readyPRTimeout)
	}
	if *seedSpec != "" {
		if err := runIngest(*seedSpec, *space, *apiBase, "high"); err != nil {
			return fmt.Errorf("ingest seed-spec: %w", err)
		}
	}
	// loop=true → Keepalive=true: the governing loop never exits on quiescence.
	return runLegacy(*human, "", *storeDSN, *approveRequests, *approveRoles, profile, growthPolicyVersion, maximumDynamicActors, actions, 0, false, *repo, *repoWorkspaceRoot, *catalog, *catalogReloadInterval, true, issueScanStageRoleRunner, issueScanImplementationRunner, issueScanReviewRunner, issueScanBlockerRepairRunner, issueScanDraftPRAuthorityRequester, issueScanDraftPRCreator, issueScanReadyPRRunner, issueScanScanner, *space, *apiBase, *webhookAddr, webhookBearerToken)
}

func resolveWebhookBearerToken(addr string, requireAuth bool, apiKey string) (string, error) {
	if requireAuth {
		if strings.TrimSpace(apiKey) == "" {
			return "", fmt.Errorf("TRANSPARA_API_KEY is required with --webhook-require-auth")
		}
		return apiKey, nil
	}
	if webhookAddressIsLoopback(addr) {
		return "", nil
	}
	return "", fmt.Errorf("--webhook-require-auth is required for non-loopback --webhook-addr %q", addr)
}

func webhookAddressIsLoopback(addr string) bool {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return true
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type issueScanFullChainConfig struct {
	IssueScanInterval     time.Duration
	HasIssueScanRepos     bool
	IssueScanRegistry     bool
	RepoWorkspaceRoot     string
	StageRoleRunner       string
	ImplementationRunner  string
	ReviewRunner          string
	BlockerRepairRunner   string
	DraftPRRequest        bool
	DraftPRCreate         bool
	ReadyPRMarkReady      bool
	ReadyPRReviewRunner   string
	GenericReadyPRRunner  string
	GenericReadyPRRunArgs bool
}

func requireIssueScanFullChainConfig(cfg issueScanFullChainConfig) error {
	var missing []string
	var conflicts []string
	var unavailable []string
	if cfg.IssueScanInterval <= 0 {
		missing = append(missing, "--issue-scan-interval")
	}
	if !cfg.HasIssueScanRepos && !cfg.IssueScanRegistry {
		missing = append(missing, "--issue-scan-repo or --issue-scan-registry")
	}
	if strings.TrimSpace(cfg.RepoWorkspaceRoot) == "" {
		missing = append(missing, "--repo-workspace-root")
	}
	if strings.TrimSpace(cfg.StageRoleRunner) == "" {
		missing = append(missing, "--issue-scan-stage-role-runner")
	}
	if strings.TrimSpace(cfg.ImplementationRunner) == "" {
		missing = append(missing, "--issue-scan-implementation-runner")
	}
	if strings.TrimSpace(cfg.ReviewRunner) == "" {
		missing = append(missing, "--issue-scan-review-runner")
	}
	if strings.TrimSpace(cfg.BlockerRepairRunner) == "" {
		missing = append(missing, "--issue-scan-blocker-repair-runner")
	}
	if !cfg.DraftPRRequest {
		missing = append(missing, "--issue-scan-draft-pr-request")
	}
	if !cfg.DraftPRCreate {
		missing = append(missing, "--issue-scan-draft-pr-create")
	}
	if !cfg.ReadyPRMarkReady {
		missing = append(missing, "--issue-scan-ready-pr-mark-ready")
	}
	if strings.TrimSpace(cfg.ReadyPRReviewRunner) == "" {
		missing = append(missing, "--issue-scan-ready-pr-review-runner")
	}
	if strings.TrimSpace(cfg.GenericReadyPRRunner) != "" || cfg.GenericReadyPRRunArgs {
		conflicts = append(conflicts, "--issue-scan-ready-pr-runner")
	}
	if len(missing) == 0 && len(conflicts) == 0 {
		for _, runner := range []struct {
			flag    string
			command string
		}{
			{flag: "--issue-scan-stage-role-runner", command: cfg.StageRoleRunner},
			{flag: "--issue-scan-implementation-runner", command: cfg.ImplementationRunner},
			{flag: "--issue-scan-review-runner", command: cfg.ReviewRunner},
			{flag: "--issue-scan-blocker-repair-runner", command: cfg.BlockerRepairRunner},
			{flag: "--issue-scan-ready-pr-review-runner", command: cfg.ReadyPRReviewRunner},
		} {
			if err := requireIssueScanRunnerExecutable(runner.flag, runner.command); err != nil {
				unavailable = append(unavailable, err.Error())
			}
		}
	}
	if len(missing) > 0 || len(conflicts) > 0 || len(unavailable) > 0 {
		var parts []string
		if len(missing) > 0 {
			parts = append(parts, "requires "+strings.Join(missing, ", "))
		}
		if len(conflicts) > 0 {
			parts = append(parts, "cannot be combined with "+strings.Join(conflicts, ", "))
		}
		if len(unavailable) > 0 {
			parts = append(parts, "cannot find executable "+strings.Join(unavailable, ", "))
		}
		return fmt.Errorf("--issue-scan-require-full-chain %s", strings.Join(parts, "; "))
	}
	return nil
}

func requireIssueScanRunnerExecutable(flagName, command string) error {
	command = strings.TrimSpace(command)
	if command == "" {
		return nil
	}
	if _, err := exec.LookPath(command); err != nil {
		return fmt.Errorf("%s %q: %w", flagName, command, err)
	}
	return nil
}

// cmdFactoryOrder submits one Order into the (separately running) daemon by
// seeding a work.task.created. The --human guard fires BEFORE any side effect
// (no spec read, no store open) so the offline router test stays inert.
func cmdFactoryOrder(args []string) error {
	fs := flag.NewFlagSet("factory order", flag.ContinueOnError)
	human := fs.String("human", "", "Operator name (required)")
	spec := fs.String("spec", "", "Path to the order spec markdown file (required)")
	repo := fs.String("repo", "", "Path to repo for the order's workspace (default: current dir)")
	storeDSN := fs.String("store", "", "Store DSN (postgres://... or empty for in-memory)")
	id := fs.String("id", "", "Factory order ID (fo_ prefix; derived from spec name if empty)")
	title := fs.String("title", "", "Order title (defaults to the order ID)")
	catalog := fs.String("catalog", "", "Custom YAML model catalog used once to validate model override flags; does not reload a running daemon")
	modelOverrideFlags := factoryOrderModelOverrideFlags{}
	fs.Var(&modelOverrideFlags.models, "model", "Role-scoped model override role=model (repeatable)")
	fs.Var(&modelOverrideFlags.providers, "provider", "Role-scoped provider override role=provider (repeatable)")
	fs.Var(&modelOverrideFlags.profiles, "profile", "Role-scoped profile override role=profile (repeatable)")
	fs.Var(&modelOverrideFlags.authModes, "auth-mode", "Role-scoped auth-mode opt-in role=subscription|api-key|local (repeatable)")
	fs.Var(&modelOverrideFlags.preferredTiers, "preferred-tier", "Role-scoped preferred tier role=tier (repeatable)")
	fs.Var(&modelOverrideFlags.requiredCapabilities, "required-capability", "Role-scoped required capability role=capability[,capability...] (repeatable)")
	fs.Var(&modelOverrideFlags.maxCosts, "max-cost-per-call-usd", "Role-scoped per-call cost cap role=amount (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *human == "" {
		return fmt.Errorf("--human is required")
	}
	if *spec == "" {
		return fmt.Errorf("--spec is required")
	}
	modelOverrides, err := parseFactoryOrderModelOverrideFlags(modelOverrideFlags)
	if err != nil {
		return err
	}
	validatedModelOverrides, err := validateFactoryOrderModelOverrides(*catalog, modelOverrides)
	if err != nil {
		return err
	}
	intent, err := os.ReadFile(*spec)
	if err != nil {
		return fmt.Errorf("read spec: %w", err)
	}

	orderID := *id
	if orderID == "" {
		orderID = "fo_" + specStem(*spec)
	}
	orderTitle := *title
	if orderTitle == "" {
		orderTitle = orderID
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fc, err := openFactoryContext(ctx, *storeDSN, *human)
	if err != nil {
		return err
	}
	defer fc.close()

	if *repo == "" {
		*repo = "."
	}

	ts := work.NewTaskStore(fc.store, fc.factory, fc.signer)
	// Each order gets its own conversation ID and (via the task) its own work
	// Workspace. Derive a stable conversation ID from the order ID.
	conv := factoryOrderConversation(orderID)
	causes := fc.headCauses()

	// Optional readiness gates carried by the spec. Absent sections stay empty;
	// the planner attaches them later and work's Readiness gate enforces a
	// non-empty body before the task is assignable.
	dod, ac, testPlan := parseOrderGateSections(string(intent))
	task, err := work.SeedFactoryOrder(ts, fc.humanID, work.FactoryOrder{
		Kind:               work.OrderSoftwarePR,
		ID:                 orderID,
		Title:              orderTitle,
		Intent:             string(intent),
		DefinitionOfDone:   dod,
		AcceptanceCriteria: ac,
		TestPlan:           testPlan,
		ModelOverrides:     workFactoryOrderModelOverrides(validatedModelOverrides),
	}, causes, conv)
	if err != nil {
		return fmt.Errorf("seed factory order: %w", err)
	}
	fmt.Printf("seeded factory order %s as work task %s (conversation %s)\n", orderID, task.ID, conv)
	return nil
}

type repeatedStringFlag []string

func (f *repeatedStringFlag) String() string {
	if f == nil {
		return ""
	}
	return strings.Join(*f, ",")
}

func (f *repeatedStringFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

type factoryOrderModelOverrideFlags struct {
	models               repeatedStringFlag
	providers            repeatedStringFlag
	profiles             repeatedStringFlag
	authModes            repeatedStringFlag
	preferredTiers       repeatedStringFlag
	requiredCapabilities repeatedStringFlag
	maxCosts             repeatedStringFlag
}

func parseFactoryOrderModelOverrideFlags(flags factoryOrderModelOverrideFlags) ([]hive.ModelOverrideRequest, error) {
	byRole := map[string]*hive.ModelOverrideRequest{}
	var roles []string
	seenFields := map[string]map[string]struct{}{}
	ensure := func(role string) *hive.ModelOverrideRequest {
		if override, ok := byRole[role]; ok {
			return override
		}
		roles = append(roles, role)
		byRole[role] = &hive.ModelOverrideRequest{Role: role}
		return byRole[role]
	}
	markScalar := func(role, field string) error {
		if seenFields[role] == nil {
			seenFields[role] = map[string]struct{}{}
		}
		if _, exists := seenFields[role][field]; exists {
			return fmt.Errorf("--%s set more than once for role %q", field, role)
		}
		seenFields[role][field] = struct{}{}
		return nil
	}
	setScalar := func(flagName string, values repeatedStringFlag, set func(*hive.ModelOverrideRequest, string)) error {
		for _, raw := range values {
			role, value, err := parseFactoryOrderRoleValue(flagName, raw)
			if err != nil {
				return err
			}
			if err := markScalar(role, flagName); err != nil {
				return err
			}
			set(ensure(role), value)
		}
		return nil
	}
	if err := setScalar("model", flags.models, func(o *hive.ModelOverrideRequest, value string) { o.Model = value }); err != nil {
		return nil, err
	}
	if err := setScalar("provider", flags.providers, func(o *hive.ModelOverrideRequest, value string) { o.Provider = value }); err != nil {
		return nil, err
	}
	if err := setScalar("profile", flags.profiles, func(o *hive.ModelOverrideRequest, value string) { o.Profile = value }); err != nil {
		return nil, err
	}
	if err := setScalar("auth-mode", flags.authModes, func(o *hive.ModelOverrideRequest, value string) { o.AuthMode = value }); err != nil {
		return nil, err
	}
	if err := setScalar("preferred-tier", flags.preferredTiers, func(o *hive.ModelOverrideRequest, value string) { o.PreferredTier = value }); err != nil {
		return nil, err
	}
	for _, raw := range flags.requiredCapabilities {
		role, value, err := parseFactoryOrderRoleValue("required-capability", raw)
		if err != nil {
			return nil, err
		}
		caps := splitFactoryOrderCapabilities(value)
		if len(caps) == 0 {
			return nil, fmt.Errorf("--required-capability for role %q must include at least one capability", role)
		}
		override := ensure(role)
		override.RequiredCapabilities = append(override.RequiredCapabilities, caps...)
	}
	for _, raw := range flags.maxCosts {
		role, value, err := parseFactoryOrderRoleValue("max-cost-per-call-usd", raw)
		if err != nil {
			return nil, err
		}
		if err := markScalar(role, "max-cost-per-call-usd"); err != nil {
			return nil, err
		}
		amount, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("--max-cost-per-call-usd for role %q must be a number: %w", role, err)
		}
		override := ensure(role)
		override.MaxCostPerCallUSD = &amount
	}
	out := make([]hive.ModelOverrideRequest, 0, len(roles))
	for _, role := range roles {
		out = append(out, *byRole[role])
	}
	return out, nil
}

func parseFactoryOrderRoleValue(flagName, raw string) (string, string, error) {
	role, value, ok := strings.Cut(raw, "=")
	role = strings.TrimSpace(role)
	value = strings.TrimSpace(value)
	if !ok || role == "" || value == "" {
		return "", "", fmt.Errorf("--%s must use role=value", flagName)
	}
	return role, value, nil
}

func splitFactoryOrderCapabilities(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func validateFactoryOrderModelOverrides(catalogPath string, overrides []hive.ModelOverrideRequest) ([]hive.RunLaunchModelOverride, error) {
	if len(overrides) == 0 {
		return nil, nil
	}
	source, err := factoryModelSelectionSource(catalogPath)
	if err != nil {
		return nil, err
	}
	return validateFactoryOrderModelOverridesWithSource(source, overrides)
}

func validateFactoryOrderModelOverridesWithSource(source hive.OperatorModelSelectionSource, overrides []hive.ModelOverrideRequest) ([]hive.RunLaunchModelOverride, error) {
	if len(overrides) == 0 {
		return nil, nil
	}
	validated, err := hive.ValidateModelOverrides(overrides, source)
	if err != nil {
		return nil, fmt.Errorf("validate model overrides: %w", err)
	}
	return validated, nil
}

func factoryModelSelectionSource(catalogPath string) (hive.OperatorModelSelectionSource, error) {
	if catalogPath == "" {
		return nil, nil
	}
	config, err := hive.OperatorModelSelectionFromCatalogPath(catalogPath, time.Time{})
	if err != nil {
		return nil, fmt.Errorf("load catalog for model overrides: %w", err)
	}
	return func() hive.OperatorModelSelectionConfig { return config }, nil
}

func workFactoryOrderModelOverrides(overrides []hive.RunLaunchModelOverride) []work.FactoryOrderModelOverride {
	if len(overrides) == 0 {
		return nil
	}
	out := make([]work.FactoryOrderModelOverride, 0, len(overrides))
	for _, override := range overrides {
		out = append(out, work.FactoryOrderModelOverride{
			Role:                 override.Role,
			Model:                override.Model,
			Provider:             override.Provider,
			Profile:              override.Profile,
			RequestedAuthMode:    override.RequestedAuthMode,
			PreferredTier:        override.PreferredTier,
			RequiredCapabilities: append([]string(nil), override.RequiredCapabilities...),
			MaxCostPerCallUSD:    cloneFloat64Ptr(override.MaxCostPerCallUSD),
			ResolvedModel:        override.ResolvedModel,
			ResolvedProvider:     override.ResolvedProvider,
			AuthMode:             override.AuthMode,
		})
	}
	return out
}

func cloneFloat64Ptr(value *float64) *float64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

// cmdFactoryRequestPR raises the draft-PR authority request carrying the target
// scope. All required flags are validated BEFORE any store/runtime/GitHub
// access. The gate HOLDS by design (AuthorityError) with --approve-requests
// off, so the request id is printed and the command exits 0.
func cmdFactoryRequestPR(args []string) error {
	fs := flag.NewFlagSet("factory request-pr", flag.ContinueOnError)
	human := fs.String("human", "", "Guardian/operator name (required)")
	repo := fs.String("repo", "", "GitHub target Transpara-AI repo slug, e.g. transpara-ai/site (required)")
	baseRef := fs.String("base", "main", "Base branch ref")
	baseSHA := fs.String("base-sha", "", "Base commit SHA (required)")
	headRef := fs.String("head", "", "Head branch ref, e.g. codex/... (required)")
	headSHA := fs.String("head-sha", "", "Head commit SHA (required)")
	title := fs.String("title", "", "PR title (required)")
	bodyFile := fs.String("body-file", "", "Path to a file containing the PR body (required)")
	nonce := fs.String("nonce", "", "Single-use nonce (required)")
	storeDSN := fs.String("store", "", "Store DSN (postgres://... or empty for in-memory)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlags([]requiredFlag{
		{"repo", *repo}, {"human", *human}, {"base-sha", *baseSHA}, {"head", *headRef},
		{"head-sha", *headSHA}, {"title", *title}, {"body-file", *bodyFile}, {"nonce", *nonce},
	}); err != nil {
		return err
	}
	body, err := os.ReadFile(*bodyFile)
	if err != nil {
		return fmt.Errorf("read body-file: %w", err)
	}

	// These scope hashes are informational for the Gate-E operator display. The
	// AUTHORITATIVE Epic 11 evidence hashes are recomputed from Title/Body by
	// BuildEpic11DocsDraftPROptions during `create-pr`; an exact-match here is
	// not required (work's epic7Hash helper is unexported, so we reproduce its
	// "sha256:"+hex(sha256(value)) format locally).
	repoSlug := strings.ToLower(strings.TrimSpace(*repo))
	policyBundleID := work.Epic11PolicyBundleID
	policyBundleHash := work.Epic11DocsDraftPRPolicyBundleHash()
	if repoSlug != "transpara-ai/docs" {
		policyBundleID = hive.TransparaAIDraftPRPolicyBundleID
		policyBundleHash = hive.TransparaAIDraftPRPolicyBundleHash()
	}

	target := hive.DraftPRTarget{
		Repository:       repoSlug,
		BaseRef:          *baseRef,
		BaseSHA:          *baseSHA,
		HeadRef:          *headRef,
		HeadSHA:          *headSHA,
		TitleHash:        sha256Hash(*title),
		BodyHash:         sha256Hash(string(body)),
		PolicyBundleID:   policyBundleID,
		PolicyBundleHash: policyBundleHash,
		SingleUseNonce:   *nonce,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rt, fc, err := openFactoryRuntime(ctx, *storeDSN, *human, ".", "")
	if err != nil {
		return err
	}
	defer fc.close()

	requestID, err := rt.RaiseDraftPRAuthorityRequest(target, fc.humanID, "Guardian-initiated draft PR for "+*headRef)
	if err != nil {
		// The hold is the expected, successful outcome: an AuthorityError means
		// the request was recorded and awaits operator approval. Surface the id
		// and exit 0. Any non-authority error is a real failure.
		if isExpectedAuthorityHold(err) {
			fmt.Printf("draft-PR authority request raised (HELD pending approval): %s\n", requestID)
			return nil
		}
		return fmt.Errorf("raise draft-PR authority request: %w", err)
	}
	// --approve-requests was on: no hold. Still report the request id.
	fmt.Printf("draft-PR authority request raised (auto-approved): %s\n", requestID)
	return nil
}

// cmdFactoryCreatePR runs the gated, real-GitHub draft-PR creation from an
// approved decision. The target (repo, base/head refs+SHAs, policy bundle,
// nonce, and the approved title/body hashes) is the AUTHORITATIVE one loaded
// from the recorded authority decision for --request — NOT fresh CLI input. The
// only CLI inputs that flow into the PR are --title/--body-file (verified to
// hash-match the approved decision) and --changed-files (outside the authority
// chain). A request that is denied, never decided, or never raised has no
// approved decision and is refused before any GitHub access.
func cmdFactoryCreatePR(args []string) error {
	fs := flag.NewFlagSet("factory create-pr", flag.ContinueOnError)
	human := fs.String("human", "", "Operator name (required)")
	request := fs.String("request", "", "Approved authority request id (required)")
	title := fs.String("title", "", "PR title; must hash-match the approved decision (required)")
	bodyFile := fs.String("body-file", "", "Path to a file containing the PR body; must hash-match the approved decision (required)")
	changed := fs.String("changed-files", "", "Comma-separated changed file paths")
	storeDSN := fs.String("store", "", "Store DSN (postgres://... or empty for in-memory)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlags([]requiredFlag{
		{"request", *request}, {"human", *human}, {"title", *title}, {"body-file", *bodyFile},
	}); err != nil {
		return err
	}
	body, err := os.ReadFile(*bodyFile)
	if err != nil {
		return fmt.Errorf("read body-file: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fc, err := openFactoryContext(ctx, *storeDSN, *human)
	if err != nil {
		return err
	}
	defer fc.close()

	// GOVERNANCE gate: load the AUTHORITATIVE target from the recorded authority
	// decision. Refuses (no PR) if the request was denied, never decided, or
	// never raised. Then confirm the supplied title/body hash to exactly what the
	// human approved — a human can only authorize the content they reviewed.
	target, err := hive.LoadApprovedDraftPRTarget(fc.store, *request)
	if err != nil {
		return fmt.Errorf("create-pr refused for request %s: %w", *request, err)
	}
	if err := hive.VerifyDraftPRContent(target, *title, string(body)); err != nil {
		return fmt.Errorf("create-pr refused for request %s: %w", *request, err)
	}

	ts := work.NewTaskStore(fc.store, fc.factory, fc.signer)
	conv := factoryRequestConversation(*request)
	causes := fc.headCauses()

	art := hive.DraftPRArtifact{
		Target:         target,
		Title:          *title,
		Body:           string(body),
		ChangedFiles:   splitCSV(*changed),
		ActorRole:      "implementer",
		DeciderActorID: fc.humanID.Value(),
		DeciderRole:    "human",
	}

	// The real GitHub creator (W2). Token comes from the environment with no
	// fallback; an empty token will fail at the GitHub call, not here.
	client := work.NewEpic11GitHubPullRequestCreator(os.Getenv("GITHUB_TOKEN"))

	if strings.EqualFold(strings.TrimSpace(target.Repository), "transpara-ai/docs") && strings.TrimSpace(target.PolicyBundleID) == work.Epic11PolicyBundleID {
		run, err := hive.CreateDraftPRFromApprovedDecision(ctx, ts, fc.humanID, conv, client, art, causes...)
		if err != nil {
			return fmt.Errorf("create draft PR from approved decision %s: %w", *request, err)
		}
		fmt.Printf("created draft PR #%d for %s: %s\n", run.MutationResult.Number, run.MutationResult.Repository, run.MutationResult.URL)
		return nil
	}
	run, err := hive.CreateTransparaAIDraftPRFromApprovedDecision(ctx, ts, fc.humanID, conv, client, art, causes...)
	if err != nil {
		return fmt.Errorf("create Transpara-AI draft PR from approved decision %s: %w", *request, err)
	}
	fmt.Printf("created draft PR #%d for %s: %s\n", run.MutationResult.Number, run.MutationResult.Repository, run.MutationResult.URL)
	return nil
}

// ─── factory store/runtime context ──────────────────────────────────────────

// factoryContext bundles the store-layer handles the standalone factory
// subcommands need. It mirrors runLegacy's store-opening prologue so request-pr
// and create-pr open the SAME store the daemon uses (DATABASE_URL/Postgres or
// in-memory), with hive + work event types registered.
type factoryContext struct {
	pool    *pgxpool.Pool
	store   store.Store
	actors  actor.IActorStore
	factory *event.EventFactory
	signer  event.Signer
	humanID types.ActorID
}

// openFactoryContext opens the store, registers event types, registers the
// human actor, bootstraps the graph if empty, and derives the deterministic
// human signer. dsn falls back to DATABASE_URL; empty → in-memory.
func openFactoryContext(ctx context.Context, dsn, humanName string) (*factoryContext, error) {
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}

	var pool *pgxpool.Pool
	if dsn != "" {
		p, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return nil, fmt.Errorf("postgres: %w", err)
		}
		pool = p
	}

	// Register hive + work event types before opening the store so persisted
	// events (hive.*, work.*) deserialize on Head().
	hive.RegisterEventTypes()
	work.RegisterEventTypes()

	s, err := openStore(ctx, pool)
	if err != nil {
		if pool != nil {
			pool.Close()
		}
		return nil, fmt.Errorf("store: %w", err)
	}

	actors, err := openActorStore(ctx, pool)
	if err != nil {
		s.Close()
		if pool != nil {
			pool.Close()
		}
		return nil, fmt.Errorf("actor store: %w", err)
	}

	humanID, err := registerHuman(actors, humanName)
	if err != nil {
		s.Close()
		if pool != nil {
			pool.Close()
		}
		return nil, fmt.Errorf("register human: %w", err)
	}

	if err := bootstrapGraph(s, humanID); err != nil {
		s.Close()
		if pool != nil {
			pool.Close()
		}
		return nil, fmt.Errorf("bootstrap graph: %w", err)
	}

	return &factoryContext{
		pool:    pool,
		store:   s,
		actors:  actors,
		factory: newFactoryEventFactory(),
		signer:  &bootstrapSigner{humanID: humanID},
		humanID: humanID,
	}, nil
}

func newFactoryEventFactory() *event.EventFactory {
	registry := event.DefaultRegistry()
	hive.RegisterWithRegistry(registry)
	work.RegisterWithRegistry(registry)
	return event.NewEventFactory(registry)
}

func newIssueScanScannerContext(s store.Store, actors actor.IActorStore, humanID types.ActorID) *factoryContext {
	return &factoryContext{
		store:   s,
		actors:  actors,
		factory: newFactoryEventFactory(),
		signer:  &bootstrapSigner{humanID: humanID},
		humanID: humanID,
	}
}

func (fc *factoryContext) close() {
	if fc == nil {
		return
	}
	if fc.store != nil {
		fc.store.Close()
	}
	if fc.pool != nil {
		fc.pool.Close()
	}
}

// headCauses returns the current chain head as the single causal parent, or nil
// when the store is empty. The eventgraph requires at least one cause for
// non-genesis events; bootstrapGraph guarantees a head exists.
func (fc *factoryContext) headCauses() []types.EventID {
	head, err := fc.store.Head()
	if err != nil || head.IsNone() {
		return nil
	}
	return []types.EventID{head.Unwrap().ID()}
}

type factoryRuntimeOption func(*hive.Config)

// openFactoryRuntime builds a full hive.Runtime over the factory store context,
// for the request-pr authority seam. repoPath defaults to current dir.
func openFactoryRuntime(ctx context.Context, dsn, humanName, repoPath, repoWorkspaceRoot string, opts ...factoryRuntimeOption) (*hive.Runtime, *factoryContext, error) {
	fc, err := openFactoryContext(ctx, dsn, humanName)
	if err != nil {
		return nil, nil, err
	}
	if repoPath == "" {
		repoPath = "."
	}
	cfg := hive.Config{
		Store:                        fc.store,
		Actors:                       fc.actors, // hive.New verifies the human and registers the system actor.
		HumanID:                      fc.humanID,
		RepoPath:                     repoPath,
		RepoWorkspaceRoot:            repoWorkspaceRoot,
		IssueScanTargetStateResolver: ghIssueTargetStateResolver,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	rt, err := hive.New(ctx, cfg)
	if err != nil {
		fc.close()
		return nil, nil, fmt.Errorf("runtime: %w", err)
	}
	return rt, fc, nil
}

// ─── helpers ────────────────────────────────────────────────────────────────

// requiredFlag pairs a flag name with its parsed value for ordered validation.
type requiredFlag struct {
	name string
	val  string
}

// requireFlags returns an error naming the FIRST empty required flag, in the
// given order. Ordered (not map-based) so the error is deterministic.
func requireFlags(flags []requiredFlag) error {
	for _, f := range flags {
		if f.val == "" {
			return fmt.Errorf("--%s is required", f.name)
		}
	}
	return nil
}

// sha256Hash reproduces work.epic7Hash's "sha256:"+hex format (that helper is
// unexported). Used only for the informational scope hashes on the authority
// request; authoritative hashes are recomputed in create-pr.
func sha256Hash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func issueScanDraftPRAuthorityRequestLocalGitRunner(baseRef string) hive.IssueScanDraftPRAuthorityRequester {
	return func(ctx context.Context, requestContext hive.IssueScanDraftPRAuthorityRequestRunnerContext) (hive.IssueScanDraftPRAuthorityRequestRunnerResult, error) {
		repoPath := strings.TrimSpace(requestContext.RepoPath)
		if repoPath == "" {
			return hive.IssueScanDraftPRAuthorityRequestRunnerResult{}, fmt.Errorf("issue-scan draft PR authority request requires a resolved repository checkout")
		}
		base := valueOrString(strings.TrimSpace(baseRef), "main")
		baseSHA, err := gitBaseCommitSHA(ctx, repoPath, base)
		if err != nil {
			return hive.IssueScanDraftPRAuthorityRequestRunnerResult{}, err
		}
		return hive.IssueScanDraftPRAuthorityRequestRunnerResult{
			BaseRef: base,
			BaseSHA: baseSHA,
			Nonce:   issueScanDraftPRAuthorityNonce(requestContext, base, baseSHA),
		}, nil
	}
}

func gitBaseCommitSHA(ctx context.Context, repoPath, baseRef string) (string, error) {
	remoteRef := gitRemoteBaseRef(baseRef)
	if err := gitFetchBaseRef(ctx, repoPath, baseRef); err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, "git", "rev-parse", "--verify", remoteRef+"^{commit}")
	cmd.Dir = repoPath
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("resolve issue-scan draft PR base %s in %s: %w%s", remoteRef, repoPath, err, runnerStderrSuffix(string(out)))
	}
	sha := strings.TrimSpace(string(out))
	if sha == "" {
		return "", fmt.Errorf("resolve issue-scan draft PR base %s in %s: empty commit SHA", remoteRef, repoPath)
	}
	return sha, nil
}

func gitFetchBaseRef(ctx context.Context, repoPath, baseRef string) error {
	base := gitBaseBranchRef(baseRef)
	if err := validateGitBaseBranchRef(base); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "git", "fetch", "--quiet", "origin", "refs/heads/"+base+":refs/remotes/origin/"+base)
	cmd.Dir = repoPath
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("fetch issue-scan draft PR base origin/%s in %s: %w%s", base, repoPath, err, runnerStderrSuffix(string(out)))
	}
	return nil
}

func gitRemoteBaseRef(baseRef string) string {
	return "origin/" + gitBaseBranchRef(baseRef)
}

func gitBaseBranchRef(baseRef string) string {
	base := strings.TrimSpace(baseRef)
	base = strings.TrimPrefix(base, "refs/heads/")
	base = strings.TrimPrefix(base, "origin/")
	if base == "" {
		base = "main"
	}
	return base
}

func validateGitBaseBranchRef(base string) error {
	base = strings.TrimSpace(base)
	if base == "" {
		return fmt.Errorf("issue-scan draft PR base ref is empty")
	}
	if strings.HasPrefix(base, "-") {
		return fmt.Errorf("issue-scan draft PR base ref %q must not start with '-'", base)
	}
	if strings.HasPrefix(base, "/") || strings.HasSuffix(base, "/") || strings.Contains(base, "//") {
		return fmt.Errorf("issue-scan draft PR base ref %q is not a valid branch path", base)
	}
	if strings.Contains(base, "..") || strings.Contains(base, "@{") || strings.HasSuffix(base, ".") || strings.HasSuffix(base, ".lock") {
		return fmt.Errorf("issue-scan draft PR base ref %q is not a valid branch name", base)
	}
	for _, r := range base {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '/' || r == '_' || r == '-' || r == '.' {
			continue
		}
		return fmt.Errorf("issue-scan draft PR base ref %q contains invalid character %q", base, r)
	}
	return nil
}

func issueScanDraftPRAuthorityNonce(requestContext hive.IssueScanDraftPRAuthorityRequestRunnerContext, baseRef, baseSHA string) string {
	seed := strings.Join([]string{
		requestContext.RunID,
		requestContext.FactoryOrderID,
		requestContext.Repository,
		requestContext.OperateBranch,
		requestContext.OperateCommit,
		baseRef,
		baseSHA,
	}, "\n")
	sum := sha256.Sum256([]byte(seed))
	return "issue-scan-" + hex.EncodeToString(sum[:])[:24]
}

func valueOrString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

// isExpectedAuthorityHold reports whether err is the benign "approval required"
// hold that RaiseDraftPRAuthorityRequest returns when --approve-requests is off.
func isExpectedAuthorityHold(err error) bool {
	if err == nil {
		return false
	}
	var authErr safety.AuthorityError
	if errors.As(err, &authErr) {
		return authErr.Outcome == safety.ApprovalRequired
	}
	return false
}

// specStem returns a filename-derived order suffix: base name without extension.
func specStem(path string) string {
	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	if base == "" || base == "." {
		return "order"
	}
	return base
}

// parseOrderGateSections extracts the optional Definition of Done / Acceptance
// Criteria / Test Plan sections from an order spec's markdown. A section's body
// is the text under its "## <name>" heading up to the next heading, trimmed.
// Missing sections return "" so the planner attaches them later — work's
// Readiness gate enforces non-empty before the task becomes assignable.
func parseOrderGateSections(spec string) (definitionOfDone, acceptanceCriteria, testPlan string) {
	sections := map[string][]string{}
	current := ""
	for _, line := range strings.Split(spec, "\n") {
		if heading, ok := markdownHeading(line); ok {
			current = strings.ToLower(heading)
			continue
		}
		if current != "" {
			sections[current] = append(sections[current], line)
		}
	}
	body := func(name string) string {
		return strings.TrimSpace(strings.Join(sections[name], "\n"))
	}
	return body("definition of done"), body("acceptance criteria"), body("test plan")
}

// markdownHeading reports whether line is an ATX heading (one or more leading
// '#') and returns the heading text with the '#'s and surrounding space removed.
func markdownHeading(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "#") {
		return "", false
	}
	return strings.TrimSpace(strings.TrimLeft(trimmed, "#")), true
}

// factoryOrderConversation derives a stable, unique conversation ID for an
// order so each Order gets its own conversation (and thus its own work
// Workspace). A 16-byte hex digest of the order id keeps it deterministic.
func factoryOrderConversation(orderID string) types.ConversationID {
	sum := sha256.Sum256([]byte("order:" + orderID))
	return types.MustConversationID("conv_" + hex.EncodeToString(sum[:16]))
}

func factoryRequestConversation(requestID string) types.ConversationID {
	sum := sha256.Sum256([]byte("request:" + requestID))
	return types.MustConversationID("conv_" + hex.EncodeToString(sum[:16]))
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			if seg := s[start:i]; seg != "" {
				out = append(out, seg)
			}
			start = i + 1
		}
	}
	if seg := s[start:]; seg != "" {
		out = append(out, seg)
	}
	return out
}
