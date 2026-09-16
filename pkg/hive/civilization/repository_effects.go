package civilization

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/transpara-ai/hive/pkg/hive/tlcbridge"
)

type VerificationCommand struct {
	Name string
	Args []string
}

type RepositorySpec struct {
	Root                 string
	Remote               string
	BaseBranch           string
	VerificationCommands []VerificationCommand
}

type GitHubEffectsConfig struct {
	Runner                        *RunnerProvider
	Repositories                  map[string]RepositorySpec
	WorktreeRoot                  string
	GitExecutable                 string
	GitSHA256                     string
	GitHubExecutable              string
	GitHubSHA256                  string
	EnvironmentKeys               []string
	VerificationSandboxExecutable string
	VerificationSandboxSHA256     string
	VerificationSandboxProfile    string
	VerificationCodexHome         string
	VerificationConfigSHA256      string
	VerificationScratchRoot       string
	VerificationModuleCache       string
	OutputLimitBytes              int
	CommitUserName                string
	CommitUserEmail               string
	PublishEnabled                bool
	PublishAuthority              string
	AutoMergeEnabled              bool
	AutoMergeAuthority            string
}

// GitHubEffects is the only production boundary allowed to commit, push,
// create a pull request, or request auto-merge. Both mutation classes are
// disabled unless independently enabled with a Human authority reference.
type GitHubEffects struct {
	config      GitHubEffectsConfig
	git         string
	gitDigest   string
	gh          string
	ghDigest    string
	sandbox     string
	sandboxSHA  string
	verifyHome  string
	verifyCfg   string
	verifyTmp   string
	moduleCache string
	worktrees   string
	locksGuard  sync.Mutex
	locks       map[string]*sync.Mutex
}

var (
	repositoryNamePattern = regexp.MustCompile(`^transpara-ai/[a-z0-9][a-z0-9._-]*$`)
	workIDPattern         = regexp.MustCompile(`^work-[a-f0-9]{24}$`)
	gitSHA1Pattern        = regexp.MustCompile(`^[a-f0-9]{40}$`)
	sha256Pattern         = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

const verificationSandboxProfile = "civilization-verification"

func NewGitHubEffects(config GitHubEffectsConfig) (*GitHubEffects, error) {
	if config.Runner != nil && config.PublishEnabled {
		return nil, errors.New("runner mode does not authorize publication")
	}
	if len(config.Repositories) == 0 {
		return nil, errors.New("at least one repository is required")
	}
	if !filepath.IsAbs(config.WorktreeRoot) {
		return nil, errors.New("worktree root must be absolute")
	}
	if config.OutputLimitBytes <= 0 {
		return nil, errors.New("repository effect output limit must be positive")
	}
	if strings.TrimSpace(config.CommitUserName) == "" || strings.TrimSpace(config.CommitUserEmail) == "" {
		return nil, errors.New("commit identity is required")
	}
	if config.PublishEnabled && strings.TrimSpace(config.PublishAuthority) == "" {
		return nil, errors.New("publication cannot be enabled without a Human authority reference")
	}
	if config.AutoMergeEnabled && strings.TrimSpace(config.AutoMergeAuthority) == "" {
		return nil, errors.New("auto-merge cannot be enabled without a Human authority reference")
	}
	gitPath, gitDigest, err := resolvePinnedExecutable(config.GitExecutable)
	if err != nil {
		return nil, fmt.Errorf("resolve git: %w", err)
	}
	if gitDigest != config.GitSHA256 {
		return nil, errors.New("git executable SHA-256 does not match configured digest")
	}
	ghPath, ghDigest, err := resolvePinnedExecutable(config.GitHubExecutable)
	if err != nil {
		return nil, fmt.Errorf("resolve GitHub CLI: %w", err)
	}
	if ghDigest != config.GitHubSHA256 {
		return nil, errors.New("GitHub CLI executable SHA-256 does not match configured digest")
	}
	if !regexp.MustCompile(`^/[A-Za-z0-9._/-]+$`).MatchString(ghPath) {
		return nil, errors.New("GitHub CLI path contains characters unsafe for the git credential helper")
	}
	sandboxPath, sandboxDigest, err := resolvePinnedExecutable(config.VerificationSandboxExecutable)
	if err != nil {
		return nil, fmt.Errorf("resolve verification sandbox: %w", err)
	}
	if sandboxDigest != config.VerificationSandboxSHA256 {
		return nil, errors.New("verification sandbox executable SHA-256 does not match configured digest")
	}
	if config.VerificationSandboxProfile != verificationSandboxProfile {
		return nil, fmt.Errorf("verification sandbox profile must be %q", verificationSandboxProfile)
	}
	verificationHome, err := resolveDirectory(config.VerificationCodexHome, "verification Codex home")
	if err != nil {
		return nil, err
	}
	verificationConfig := filepath.Join(verificationHome, "config.toml")
	if !sha256Pattern.MatchString(config.VerificationConfigSHA256) {
		return nil, errors.New("verification config SHA-256 must be 64 lowercase hexadecimal characters")
	}
	if digest, digestErr := digestFile(verificationConfig); digestErr != nil || digest != config.VerificationConfigSHA256 {
		return nil, errors.New("verification config SHA-256 does not match configured digest")
	}
	if info, statErr := os.Stat(verificationConfig); statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 {
		return nil, errors.New("verification config must be a regular file not writable by group or other")
	}
	if !filepath.IsAbs(config.VerificationScratchRoot) {
		return nil, errors.New("verification scratch root must be absolute")
	}
	if err := os.MkdirAll(config.VerificationScratchRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create verification scratch root: %w", err)
	}
	verificationScratch, err := resolveDirectory(config.VerificationScratchRoot, "verification scratch root")
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(verificationScratch, 0o700); err != nil {
		return nil, fmt.Errorf("protect verification scratch root: %w", err)
	}
	moduleCache, err := resolveDirectory(config.VerificationModuleCache, "verification module cache")
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(config.WorktreeRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create worktree root: %w", err)
	}
	worktrees, err := filepath.EvalSymlinks(config.WorktreeRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve worktree root: %w", err)
	}
	if err := os.Chmod(worktrees, 0o700); err != nil {
		return nil, fmt.Errorf("protect worktree root: %w", err)
	}
	for name, spec := range config.Repositories {
		if !repositoryNamePattern.MatchString(name) {
			return nil, fmt.Errorf("invalid or non-Transpara repository %q", name)
		}
		root, err := confinedRepositoryRoot(spec.Root)
		if err != nil {
			return nil, fmt.Errorf("repository %s: %w", name, err)
		}
		_, expectedDirectory, _ := strings.Cut(name, "/")
		if filepath.Base(root) != expectedDirectory {
			return nil, fmt.Errorf("repository %s root must resolve to a directory named %q", name, expectedDirectory)
		}
		spec.Root = root
		if spec.Remote == "" {
			spec.Remote = "origin"
		}
		if spec.BaseBranch == "" {
			spec.BaseBranch = "main"
		}
		if !safeGitRefPart(spec.Remote) || !safeGitRefPart(spec.BaseBranch) {
			return nil, fmt.Errorf("repository %s has unsafe remote or base branch", name)
		}
		for _, command := range spec.VerificationCommands {
			if strings.TrimSpace(command.Name) == "" || len(command.Args) == 0 || strings.TrimSpace(command.Args[0]) == "" {
				return nil, fmt.Errorf("repository %s has an invalid verification command", name)
			}
		}
		config.Repositories[name] = spec
		for label, isolated := range map[string]string{
			"verification Codex home":   verificationHome,
			"verification scratch root": verificationScratch,
			"verification module cache": moduleCache,
		} {
			if pathsOverlap(root, isolated) {
				return nil, fmt.Errorf("repository %s overlaps %s", name, label)
			}
		}
	}
	if pathsOverlap(worktrees, verificationHome) || pathsOverlap(worktrees, verificationScratch) || pathsOverlap(worktrees, moduleCache) {
		return nil, errors.New("worktree root overlaps a verification support path")
	}
	if pathsOverlap(verificationHome, verificationScratch) || pathsOverlap(verificationHome, moduleCache) || pathsOverlap(verificationScratch, moduleCache) {
		return nil, errors.New("verification support paths must not overlap")
	}
	config.GitExecutable, config.GitHubExecutable = gitPath, ghPath
	config.VerificationSandboxExecutable = sandboxPath
	return &GitHubEffects{
		config: config, git: gitPath, gitDigest: gitDigest, gh: ghPath, ghDigest: ghDigest,
		sandbox: sandboxPath, sandboxSHA: sandboxDigest, verifyHome: verificationHome,
		verifyCfg: verificationConfig, verifyTmp: verificationScratch, moduleCache: moduleCache,
		worktrees: worktrees, locks: map[string]*sync.Mutex{},
	}, nil
}

func (e *GitHubEffects) RepositoryRoot(_ context.Context, repository string) (string, error) {
	spec, ok := e.config.Repositories[repository]
	if !ok {
		return "", fmt.Errorf("repository %q is not allowlisted", repository)
	}
	return confinedRepositoryRoot(spec.Root)
}

// CaptureImplementation binds the provider's changed-file claim to the exact
// file contents and modes currently present over the prepared base.
func (e *GitHubEffects) CaptureImplementation(ctx context.Context, workID string, bound tlcbridge.BoundRequest, workspace Workspace, implementation ProviderResult) (string, error) {
	unlock, err := e.lock(workID)
	if err != nil {
		return "", err
	}
	defer unlock()
	_, root, err := e.validateWorkspace(workID, bound, workspace)
	if err != nil {
		return "", err
	}
	if err := validateImplementation(implementation); err != nil {
		return "", err
	}
	snapshot, err := e.implementationSnapshot(ctx, root, workspace.BaseSHA)
	if err != nil {
		return "", err
	}
	if snapshot.HeadSHA != workspace.BaseSHA || len(snapshot.ChangedFiles) == 0 {
		return "", errors.New("new implementation must be an uncommitted diff over the prepared base")
	}
	claimed, err := normalizedProviderFiles(implementation.ChangedFiles)
	if err != nil {
		return "", err
	}
	if !equalStrings(snapshot.ChangedFiles, claimed) {
		return "", fmt.Errorf("provider changed-file report does not match git: reported=%v actual=%v", claimed, snapshot.ChangedFiles)
	}
	return snapshot.Digest, nil
}

// ImplementationMatches confirms that a recorded digest still names the exact
// diff in its deterministic worktree. This makes database recovery safe when
// ephemeral or host-level worktree state was lost.
func (e *GitHubEffects) ImplementationMatches(ctx context.Context, workID string, bound tlcbridge.BoundRequest, workspace Workspace, implementation ProviderResult, expectedDigest string) (bool, error) {
	unlock, err := e.lock(workID)
	if err != nil {
		return false, err
	}
	defer unlock()
	_, root, err := e.validateWorkspace(workID, bound, workspace)
	if err != nil {
		return false, err
	}
	return e.implementationMatches(ctx, root, workID, workspace.BaseSHA, implementation, expectedDigest)
}

func (e *GitHubEffects) implementationMatches(ctx context.Context, root, workID, baseSHA string, implementation ProviderResult, expectedDigest string) (bool, error) {
	if err := validateImplementation(implementation); err != nil || !sha256Pattern.MatchString(expectedDigest) {
		return false, nil
	}
	snapshot, err := e.implementationSnapshot(ctx, root, baseSHA)
	if err != nil {
		return false, err
	}
	claimed, claimedErr := normalizedProviderFiles(implementation.ChangedFiles)
	if claimedErr != nil {
		return false, nil
	}
	if len(snapshot.ChangedFiles) == 0 || !equalStrings(snapshot.ChangedFiles, claimed) || snapshot.Digest != expectedDigest {
		return false, nil
	}
	if snapshot.HeadSHA != baseSHA {
		message, messageErr := e.gitOutput(ctx, root, "log", "-1", "--format=%B")
		if messageErr != nil || !strings.Contains(message, "Civilization-Work-ID: "+workID) {
			return false, nil
		}
	}
	return true, nil
}

func (e *GitHubEffects) Prepare(ctx context.Context, workID string, bound tlcbridge.BoundRequest) (Workspace, error) {
	unlock, err := e.lock(workID)
	if err != nil {
		return Workspace{}, err
	}
	defer unlock()
	spec, ok := e.config.Repositories[bound.Source.Repository]
	if !ok {
		return Workspace{}, fmt.Errorf("repository %q is not allowlisted", bound.Source.Repository)
	}
	if bound.Effects.WorktreeRepository != bound.Source.Repository {
		return Workspace{}, errors.New("TLC transport repository effects do not match the source repository")
	}
	if dirty, err := e.gitOutput(ctx, spec.Root, "status", "--porcelain", "--untracked-files=all"); err != nil {
		return Workspace{}, err
	} else if dirty != "" {
		return Workspace{}, errors.New("accepted repository checkout is not clean")
	}
	remoteRef := "refs/remotes/" + spec.Remote + "/" + spec.BaseBranch
	refspec := "+refs/heads/" + spec.BaseBranch + ":" + remoteRef
	if _, err := e.gitRunAuthenticated(ctx, spec.Root, "fetch", "--no-tags", spec.Remote, refspec); err != nil {
		return Workspace{}, fmt.Errorf("fetch exact base: %w", err)
	}
	baseSHA, err := e.gitOutput(ctx, spec.Root, "rev-parse", "--verify", remoteRef+"^{commit}")
	if err != nil || !gitSHA1Pattern.MatchString(baseSHA) {
		return Workspace{}, fmt.Errorf("resolve exact base %s: %w", remoteRef, err)
	}
	branch := "civilization/" + workID
	path := filepath.Join(e.worktrees, workID)
	if _, statErr := os.Stat(path); statErr == nil {
		root, rootErr := confinedRepositoryRoot(path)
		if rootErr != nil || root != path {
			return Workspace{}, errors.New("existing deterministic worktree is invalid")
		}
		currentBranch, branchErr := e.gitOutput(ctx, path, "branch", "--show-current")
		if branchErr != nil || currentBranch != branch {
			return Workspace{}, errors.New("existing deterministic worktree is on a different branch")
		}
		mergeBase, mergeErr := e.gitOutput(ctx, path, "merge-base", "HEAD", remoteRef)
		if mergeErr != nil || !gitSHA1Pattern.MatchString(mergeBase) {
			return Workspace{}, errors.New("existing deterministic worktree has no valid base")
		}
		return Workspace{Root: path, Repository: bound.Source.Repository, Branch: branch, BaseSHA: mergeBase}, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return Workspace{}, fmt.Errorf("inspect deterministic worktree: %w", statErr)
	}

	if _, err := e.gitRun(ctx, spec.Root, "show-ref", "--verify", "--quiet", "refs/heads/"+branch); err == nil {
		if _, err := e.gitRun(ctx, spec.Root, "worktree", "add", path, branch); err != nil {
			return Workspace{}, fmt.Errorf("restore deterministic worktree: %w", err)
		}
	} else if _, err := e.gitRun(ctx, spec.Root, "worktree", "add", "-b", branch, path, remoteRef); err != nil {
		return Workspace{}, fmt.Errorf("create deterministic worktree: %w", err)
	}
	return Workspace{Root: path, Repository: bound.Source.Repository, Branch: branch, BaseSHA: baseSHA}, nil
}

func (e *GitHubEffects) Publish(ctx context.Context, workID string, bound tlcbridge.BoundRequest, workspace Workspace, implementation ProviderResult, implementationDigest string, review ProviderResult) (PullRequest, error) {
	if !e.config.PublishEnabled || strings.TrimSpace(e.config.PublishAuthority) == "" {
		return PullRequest{}, errors.New("publication is disabled; separate Human authority is required")
	}
	unlock, err := e.lock(workID)
	if err != nil {
		return PullRequest{}, err
	}
	defer unlock()
	spec, root, err := e.validateWorkspace(workID, bound, workspace)
	if err != nil {
		return PullRequest{}, err
	}
	if err := validateImplementation(implementation); err != nil {
		return PullRequest{}, err
	}
	if err := validateReview(review); err != nil {
		return PullRequest{}, err
	}
	matches, err := e.implementationMatches(ctx, root, workID, workspace.BaseSHA, implementation, implementationDigest)
	if err != nil {
		return PullRequest{}, err
	}
	if !matches {
		return PullRequest{}, errors.New("implementation content no longer matches the exact reviewed digest")
	}
	actual, err := e.uncommittedFiles(ctx, root)
	if err != nil {
		return PullRequest{}, err
	}
	head, err := e.gitOutput(ctx, root, "rev-parse", "HEAD")
	if err != nil {
		return PullRequest{}, err
	}
	claimed, err := normalizedProviderFiles(implementation.ChangedFiles)
	if err != nil {
		return PullRequest{}, err
	}
	if len(actual) > 0 {
		if head != workspace.BaseSHA {
			return PullRequest{}, errors.New("worktree mixes committed and uncommitted changes")
		}
		if !equalStrings(actual, claimed) {
			return PullRequest{}, fmt.Errorf("provider changed-file report does not match git: reported=%v actual=%v", claimed, actual)
		}
		if err := e.verify(ctx, root, spec.VerificationCommands); err != nil {
			return PullRequest{}, err
		}
		if matches, matchErr := e.implementationMatches(ctx, root, workID, workspace.BaseSHA, implementation, implementationDigest); matchErr != nil || !matches {
			return PullRequest{}, errors.New("implementation changed during independent verification")
		}
		args := append([]string{"add", "--"}, actual...)
		if _, err := e.gitRun(ctx, root, args...); err != nil {
			return PullRequest{}, fmt.Errorf("stage exact changes: %w", err)
		}
		if matches, matchErr := e.implementationMatches(ctx, root, workID, workspace.BaseSHA, implementation, implementationDigest); matchErr != nil || !matches {
			return PullRequest{}, errors.New("staged implementation differs from the exact reviewed digest")
		}
		message := boundedTitle(bound.Envelope.Brief.Outcome) + "\n\nCivilization-Work-ID: " + workID
		if _, err := e.gitRun(ctx, root, "-c", "user.name="+e.config.CommitUserName, "-c", "user.email="+e.config.CommitUserEmail, "commit", "-m", message); err != nil {
			return PullRequest{}, fmt.Errorf("commit exact changes: %w", err)
		}
		head, err = e.gitOutput(ctx, root, "rev-parse", "HEAD")
		if err != nil {
			return PullRequest{}, err
		}
		if matches, matchErr := e.implementationMatches(ctx, root, workID, workspace.BaseSHA, implementation, implementationDigest); matchErr != nil || !matches {
			return PullRequest{}, errors.New("committed implementation differs from the exact reviewed digest")
		}
	} else {
		if head == workspace.BaseSHA {
			return PullRequest{}, errors.New("implementation produced no changes")
		}
		message, messageErr := e.gitOutput(ctx, root, "log", "-1", "--format=%B")
		if messageErr != nil || !strings.Contains(message, "Civilization-Work-ID: "+workID) {
			return PullRequest{}, errors.New("existing commit is not a Civilization receipt for this work item")
		}
		committed, diffErr := e.changedFiles(ctx, root, workspace.BaseSHA+"..HEAD")
		if diffErr != nil || !equalStrings(committed, claimed) {
			return PullRequest{}, errors.New("existing Civilization commit differs from the recorded implementation")
		}
		if err := e.verify(ctx, root, spec.VerificationCommands); err != nil {
			return PullRequest{}, err
		}
		if matches, matchErr := e.implementationMatches(ctx, root, workID, workspace.BaseSHA, implementation, implementationDigest); matchErr != nil || !matches {
			return PullRequest{}, errors.New("committed implementation changed during independent verification")
		}
	}
	if dirty, err := e.gitOutput(ctx, root, "status", "--porcelain", "--untracked-files=all"); err != nil || dirty != "" {
		return PullRequest{}, errors.New("worktree is not clean after exact commit")
	}
	if _, err := e.gitRunAuthenticated(ctx, root, "push", "--set-upstream", spec.Remote, "HEAD:refs/heads/"+workspace.Branch); err != nil {
		return PullRequest{}, fmt.Errorf("push exact branch: %w", err)
	}
	pr, found, err := e.findPullRequest(ctx, bound.Source.Repository, workspace.Branch)
	if err != nil {
		return PullRequest{}, err
	}
	if !found {
		body := pullRequestBody(workID, bound)
		if _, err := e.ghRun(ctx, "", "pr", "create", "--repo", bound.Source.Repository, "--base", spec.BaseBranch, "--head", workspace.Branch, "--title", boundedTitle(bound.Envelope.Brief.Outcome), "--body", body); err != nil {
			return PullRequest{}, fmt.Errorf("create pull request: %w", err)
		}
		pr, found, err = e.findPullRequest(ctx, bound.Source.Repository, workspace.Branch)
		if err != nil || !found {
			return PullRequest{}, errors.New("created pull request could not be observed")
		}
	}
	if pr.HeadSHA != head {
		return PullRequest{}, errors.New("pull request head does not match the exact committed head")
	}
	pr.ReviewedHeadSHA, pr.ValidatedHeadSHA = head, head
	pr.CreatedByCivilization = pr.CreatedByCivilization && strings.Contains(pr.marker, "work_id="+workID)
	pr.marker = ""
	return pr.PullRequest, nil
}

func (e *GitHubEffects) ObservePullRequest(ctx context.Context, input PullRequest) (PullRequest, error) {
	if !repositoryNamePattern.MatchString(input.Repository) || input.Number <= 0 {
		return PullRequest{}, errors.New("valid Transpara pull request identity is required")
	}
	observed, err := e.viewPullRequest(ctx, input.Repository, input.Number)
	if err != nil {
		return PullRequest{}, err
	}
	if observed.HeadSHA == input.HeadSHA {
		observed.ReviewedHeadSHA = input.ReviewedHeadSHA
		observed.ValidatedHeadSHA = input.ValidatedHeadSHA
		observed.CreatedByCivilization = input.CreatedByCivilization && strings.Contains(observed.marker, "civilization:v1")
	}
	observed.marker = ""
	return observed.PullRequest, nil
}

func (e *GitHubEffects) EnableAutoMerge(ctx context.Context, input PullRequest, expectedHeadSHA string) error {
	if !e.config.AutoMergeEnabled || strings.TrimSpace(e.config.AutoMergeAuthority) == "" {
		return errors.New("auto-merge is disabled; separate Human authority is required")
	}
	if !gitSHA1Pattern.MatchString(expectedHeadSHA) || input.HeadSHA != expectedHeadSHA {
		return errors.New("auto-merge expected head is invalid or stale")
	}
	fresh, err := e.ObservePullRequest(ctx, input)
	if err != nil {
		return err
	}
	if fresh.HeadSHA == expectedHeadSHA && fresh.Merged {
		return nil
	}
	if fresh.HeadSHA != expectedHeadSHA || !pullRequestReady(fresh) {
		return errors.New("auto-merge refused after exact-head refresh")
	}
	_, err = e.ghRun(ctx, "", "pr", "merge", fmt.Sprintf("%d", fresh.Number), "--repo", fresh.Repository, "--auto", "--squash", "--match-head-commit", expectedHeadSHA)
	if err != nil {
		return fmt.Errorf("request protected auto-merge: %w", err)
	}
	return nil
}

type observedPullRequest struct {
	PullRequest
	marker string
}

type ghPRJSON struct {
	Number            int    `json:"number"`
	URL               string `json:"url"`
	State             string `json:"state"`
	IsDraft           bool   `json:"isDraft"`
	HeadRefOID        string `json:"headRefOid"`
	Body              string `json:"body"`
	ChangedFilesCount int    `json:"changedFiles"`
	Files             []struct {
		Path string `json:"path"`
	} `json:"files"`
}

func (e *GitHubEffects) findPullRequest(ctx context.Context, repository, branch string) (observedPullRequest, bool, error) {
	raw, err := e.ghRun(ctx, "", "pr", "list", "--repo", repository, "--head", branch, "--state", "all", "--limit", "2", "--json", "number,url,state,isDraft,headRefOid,body,changedFiles,files")
	if err != nil {
		return observedPullRequest{}, false, fmt.Errorf("list pull requests: %w", err)
	}
	var candidates []ghPRJSON
	if err := json.Unmarshal(raw, &candidates); err != nil {
		return observedPullRequest{}, false, fmt.Errorf("decode pull-request list: %w", err)
	}
	if len(candidates) == 0 {
		return observedPullRequest{}, false, nil
	}
	if len(candidates) != 1 {
		return observedPullRequest{}, false, errors.New("deterministic branch has multiple pull requests")
	}
	result, err := e.observedFromJSON(ctx, repository, candidates[0])
	return result, true, err
}

func (e *GitHubEffects) viewPullRequest(ctx context.Context, repository string, number int) (observedPullRequest, error) {
	raw, err := e.ghRun(ctx, "", "pr", "view", fmt.Sprintf("%d", number), "--repo", repository, "--json", "number,url,state,isDraft,headRefOid,body,changedFiles,files")
	if err != nil {
		return observedPullRequest{}, fmt.Errorf("view pull request: %w", err)
	}
	var candidate ghPRJSON
	if err := json.Unmarshal(raw, &candidate); err != nil {
		return observedPullRequest{}, fmt.Errorf("decode pull request: %w", err)
	}
	return e.observedFromJSON(ctx, repository, candidate)
}

func (e *GitHubEffects) observedFromJSON(ctx context.Context, repository string, candidate ghPRJSON) (observedPullRequest, error) {
	files := make([]string, 0, len(candidate.Files))
	for _, file := range candidate.Files {
		files = append(files, file.Path)
	}
	files = normalizedFiles(files)
	filesComplete := candidate.ChangedFilesCount > 0 && candidate.ChangedFilesCount == len(candidate.Files) && len(files) == len(candidate.Files)
	checksPassing := false
	checksState := "pending"
	if strings.EqualFold(candidate.State, "OPEN") {
		raw, err := e.ghChecks(ctx, "", "pr", "checks", fmt.Sprintf("%d", candidate.Number), "--repo", repository, "--required", "--json", "bucket,name,state")
		if err != nil {
			return observedPullRequest{}, fmt.Errorf("observe required checks: %w", err)
		}
		var checks []struct {
			Bucket string `json:"bucket"`
			Name   string `json:"name"`
			State  string `json:"state"`
		}
		if err := json.Unmarshal(raw, &checks); err != nil {
			return observedPullRequest{}, errors.New("decode required pull-request checks")
		}
		if len(checks) > 0 {
			checksPassing = true
			checksState = "passed"
			for _, check := range checks {
				switch strings.ToLower(check.Bucket) {
				case "pass":
				case "fail", "cancel":
					checksPassing = false
					checksState = "failed"
				default:
					checksPassing = false
					if checksState != "failed" {
						checksState = "pending"
					}
				}
				if check.Name == "" {
					checksPassing = false
					checksState = "failed"
				}
			}
		}
	} else {
		checksState = "not_applicable"
	}
	return observedPullRequest{PullRequest: PullRequest{
		Repository: repository, Number: candidate.Number, URL: candidate.URL, HeadSHA: candidate.HeadRefOID,
		Open: strings.EqualFold(candidate.State, "OPEN"), Merged: strings.EqualFold(candidate.State, "MERGED"), Draft: candidate.IsDraft,
		ChecksPassing: checksPassing, ChecksState: checksState, ChangedFiles: files, ChangedFilesComplete: filesComplete,
		CreatedByCivilization: strings.Contains(candidate.Body, "civilization:v1"),
	}, marker: candidate.Body}, nil
}

func (e *GitHubEffects) validateWorkspace(workID string, bound tlcbridge.BoundRequest, workspace Workspace) (RepositorySpec, string, error) {
	spec, ok := e.config.Repositories[bound.Source.Repository]
	if !ok || workspace.Repository != bound.Source.Repository {
		return RepositorySpec{}, "", errors.New("workspace repository is not allowlisted or does not match")
	}
	expected := filepath.Join(e.worktrees, workID)
	root, err := confinedRepositoryRoot(workspace.Root)
	if err != nil || root != expected || workspace.Branch != "civilization/"+workID || !gitSHA1Pattern.MatchString(workspace.BaseSHA) {
		return RepositorySpec{}, "", errors.New("workspace does not match the deterministic prepared boundary")
	}
	return spec, root, nil
}

func (e *GitHubEffects) verify(ctx context.Context, root string, commands []VerificationCommand) error {
	if len(commands) == 0 {
		return errors.New("repository has no independent verification commands")
	}
	for _, command := range commands {
		if _, err := e.verificationRun(ctx, root, command.Args[0], command.Args[1:]...); err != nil {
			return fmt.Errorf("independent verification %q failed: %w", command.Name, err)
		}
	}
	return nil
}

func (e *GitHubEffects) verificationRun(ctx context.Context, root, executable string, args ...string) ([]byte, error) {
	_, current, err := resolvePinnedExecutable(e.sandbox)
	if err != nil || current != e.sandboxSHA {
		return nil, errors.New("pinned verification sandbox executable changed")
	}
	if currentConfig, configErr := digestFile(e.verifyCfg); configErr != nil || currentConfig != e.config.VerificationConfigSHA256 {
		return nil, errors.New("pinned verification sandbox config changed")
	}
	for _, directory := range []string{"home", "cache", "tmp", "go-build"} {
		path := filepath.Join(e.verifyTmp, directory)
		if err := os.MkdirAll(path, 0o700); err != nil {
			return nil, fmt.Errorf("prepare verification scratch: %w", err)
		}
	}
	sandboxArgs := []string{"sandbox", "-P", verificationSandboxProfile, "-C", root, executable}
	sandboxArgs = append(sandboxArgs, args...)
	command := exec.CommandContext(ctx, e.sandbox, sandboxArgs...)
	command.Dir = root
	command.Env = []string{
		"PATH=/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin",
		"HOME=" + filepath.Join(e.verifyTmp, "home"),
		"CODEX_HOME=" + e.verifyHome,
		"TMPDIR=" + filepath.Join(e.verifyTmp, "tmp"),
		"XDG_CONFIG_HOME=" + filepath.Join(e.verifyTmp, "home"),
		"XDG_CACHE_HOME=" + filepath.Join(e.verifyTmp, "cache"),
		"GOCACHE=" + filepath.Join(e.verifyTmp, "go-build"),
		"GOTMPDIR=" + filepath.Join(e.verifyTmp, "tmp"),
		"GOMODCACHE=" + e.moduleCache,
		"GOROOT=/usr/local/go",
		"GOPROXY=off",
		"GOTOOLCHAIN=local",
		"GOENV=off",
		"GOTELEMETRY=off",
		"CGO_ENABLED=0",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
	}
	stdout, stderr := newBoundedBuffer(e.config.OutputLimitBytes), newBoundedBuffer(e.config.OutputLimitBytes)
	command.Stdout, command.Stderr = stdout, stderr
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("offline credential-blind sandbox: %w%s", err, boundedStderr(stderr.String()))
	}
	if stdout.Overflowed() || stderr.Overflowed() {
		return nil, errors.New("verification command output exceeded configured limit")
	}
	return []byte(stdout.String()), nil
}

type implementationSnapshot struct {
	HeadSHA      string
	ChangedFiles []string
	Digest       string
}

func (e *GitHubEffects) implementationSnapshot(ctx context.Context, root, baseSHA string) (implementationSnapshot, error) {
	head, err := e.gitOutput(ctx, root, "rev-parse", "HEAD")
	if err != nil {
		return implementationSnapshot{}, err
	}
	files, err := e.uncommittedFiles(ctx, root)
	if err != nil {
		return implementationSnapshot{}, err
	}
	if len(files) > 0 && head != baseSHA {
		return implementationSnapshot{}, errors.New("worktree mixes committed and uncommitted changes")
	}
	if len(files) == 0 && head != baseSHA {
		files, err = e.changedFiles(ctx, root, baseSHA+"..HEAD")
		if err != nil {
			return implementationSnapshot{}, err
		}
	}
	digest := sha256.New()
	writeHashField(digest, []byte(baseSHA))
	for _, file := range files {
		if file == "" || filepath.IsAbs(file) || filepath.Clean(file) != file || strings.HasPrefix(file, ".."+string(filepath.Separator)) {
			return implementationSnapshot{}, fmt.Errorf("unsafe changed file path %q", file)
		}
		writeHashField(digest, []byte(filepath.ToSlash(file)))
		fullPath := filepath.Join(root, filepath.FromSlash(file))
		relative, relErr := filepath.Rel(root, fullPath)
		if relErr != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return implementationSnapshot{}, fmt.Errorf("changed file escapes worktree: %q", file)
		}
		info, statErr := os.Lstat(fullPath)
		if errors.Is(statErr, os.ErrNotExist) {
			writeHashField(digest, []byte("deleted"))
			continue
		}
		if statErr != nil {
			return implementationSnapshot{}, fmt.Errorf("inspect changed file %q: %w", file, statErr)
		}
		parent, parentErr := filepath.EvalSymlinks(filepath.Dir(fullPath))
		parentRelative, relativeErr := filepath.Rel(root, parent)
		if parentErr != nil || relativeErr != nil || parentRelative == ".." || strings.HasPrefix(parentRelative, ".."+string(filepath.Separator)) {
			return implementationSnapshot{}, fmt.Errorf("changed file has an escaping parent directory: %q", file)
		}
		switch {
		case info.Mode().IsRegular():
			if info.Mode().Perm()&0o111 != 0 {
				writeHashField(digest, []byte("regular-executable"))
			} else {
				writeHashField(digest, []byte("regular"))
			}
			handle, openErr := os.Open(fullPath)
			if openErr != nil {
				return implementationSnapshot{}, fmt.Errorf("open changed file %q: %w", file, openErr)
			}
			writeHashLength(digest, uint64(info.Size()))
			copied, copyErr := io.Copy(digest, handle)
			if copyErr != nil {
				handle.Close()
				return implementationSnapshot{}, fmt.Errorf("hash changed file %q: %w", file, copyErr)
			}
			if copied != info.Size() {
				handle.Close()
				return implementationSnapshot{}, fmt.Errorf("changed file %q changed size while hashing", file)
			}
			if closeErr := handle.Close(); closeErr != nil {
				return implementationSnapshot{}, fmt.Errorf("close changed file %q: %w", file, closeErr)
			}
		case info.Mode()&os.ModeSymlink != 0:
			target, readErr := os.Readlink(fullPath)
			if readErr != nil {
				return implementationSnapshot{}, fmt.Errorf("read changed symlink %q: %w", file, readErr)
			}
			writeHashField(digest, []byte("symlink"))
			writeHashField(digest, []byte(target))
		default:
			return implementationSnapshot{}, fmt.Errorf("changed path %q has unsupported file type", file)
		}
	}
	return implementationSnapshot{
		HeadSHA: head, ChangedFiles: files, Digest: hex.EncodeToString(digest.Sum(nil)),
	}, nil
}

func writeHashField(destination hash.Hash, value []byte) {
	writeHashLength(destination, uint64(len(value)))
	_, _ = destination.Write(value)
}

func writeHashLength(destination hash.Hash, value uint64) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], value)
	_, _ = destination.Write(length[:])
}

func (e *GitHubEffects) changedFiles(ctx context.Context, root, revision string) ([]string, error) {
	raw, err := e.gitRun(ctx, root, "diff", "--name-only", "-z", revision)
	if err != nil {
		return nil, err
	}
	return normalizedNULFiles(raw), nil
}

func (e *GitHubEffects) uncommittedFiles(ctx context.Context, root string) ([]string, error) {
	changed, err := e.changedFiles(ctx, root, "HEAD")
	if err != nil {
		return nil, err
	}
	untracked, err := e.gitRun(ctx, root, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, err
	}
	return normalizedFiles(append(changed, normalizedNULFiles(untracked)...)), nil
}

func normalizedNULFiles(raw []byte) []string {
	parts := bytes.Split(raw, []byte{0})
	files := make([]string, 0, len(parts))
	for _, part := range parts {
		if len(part) > 0 {
			files = append(files, string(part))
		}
	}
	return normalizedFiles(files)
}

func normalizedFiles(files []string) []string {
	set := map[string]struct{}{}
	for _, file := range files {
		file = filepath.ToSlash(file)
		if file != "" && file != "." && !strings.HasPrefix(file, "../") && !filepath.IsAbs(file) {
			set[file] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for file := range set {
		result = append(result, file)
	}
	sort.Strings(result)
	return result
}

func normalizedProviderFiles(files []string) ([]string, error) {
	normalized := normalizedFiles(files)
	if len(normalized) != len(files) {
		return nil, errors.New("provider changed-file report contains an empty, duplicate, absolute, or escaping path")
	}
	for _, file := range files {
		cleaned := filepath.ToSlash(filepath.Clean(file))
		if file == "" || filepath.IsAbs(file) || cleaned != filepath.ToSlash(file) ||
			cleaned == "." || strings.HasPrefix(cleaned, "../") {
			return nil, fmt.Errorf("provider changed-file report contains unsafe path %q", file)
		}
	}
	return normalized, nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (e *GitHubEffects) lock(workID string) (func(), error) {
	if !workIDPattern.MatchString(workID) {
		return nil, errors.New("invalid deterministic work id")
	}
	e.locksGuard.Lock()
	lock := e.locks[workID]
	if lock == nil {
		lock = &sync.Mutex{}
		e.locks[workID] = lock
	}
	e.locksGuard.Unlock()
	lock.Lock()
	return lock.Unlock, nil
}

func (e *GitHubEffects) gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	raw, err := e.gitRun(ctx, dir, args...)
	return strings.TrimSpace(string(raw)), err
}

func (e *GitHubEffects) gitRun(ctx context.Context, dir string, args ...string) ([]byte, error) {
	return e.pinnedRun(ctx, dir, e.git, e.gitDigest, args...)
}

func (e *GitHubEffects) gitRunAuthenticated(ctx context.Context, dir string, args ...string) ([]byte, error) {
	authenticated := []string{"-c", "credential.helper=!" + e.gh + " auth git-credential"}
	authenticated = append(authenticated, args...)
	return e.gitRun(ctx, dir, authenticated...)
}

func (e *GitHubEffects) ghRun(ctx context.Context, dir string, args ...string) ([]byte, error) {
	return e.pinnedRun(ctx, dir, e.gh, e.ghDigest, args...)
}

// ghChecks preserves structured check output for GitHub CLI's pending and
// failed-check exit codes. All other command failures remain errors.
func (e *GitHubEffects) ghChecks(ctx context.Context, dir string, args ...string) ([]byte, error) {
	_, current, err := resolvePinnedExecutable(e.gh)
	if err != nil || current != e.ghDigest {
		return nil, errors.New("pinned effect executable changed")
	}
	command := exec.CommandContext(ctx, e.gh, args...)
	command.Dir = dir
	command.Env = allowedEnvironment(e.config.EnvironmentKeys)
	stdout, stderr := newBoundedBuffer(e.config.OutputLimitBytes), newBoundedBuffer(e.config.OutputLimitBytes)
	command.Stdout, command.Stderr = stdout, stderr
	err = command.Run()
	if stdout.Overflowed() || stderr.Overflowed() {
		return nil, errors.New("effect command output exceeded configured limit")
	}
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || (exitErr.ExitCode() != 1 && exitErr.ExitCode() != 8) {
			return nil, fmt.Errorf("%s: %w%s", filepath.Base(e.gh), err, boundedStderr(stderr.String()))
		}
	}
	return []byte(stdout.String()), nil
}

func (e *GitHubEffects) pinnedRun(ctx context.Context, dir, executable, digest string, args ...string) ([]byte, error) {
	_, current, err := resolvePinnedExecutable(executable)
	if err != nil || current != digest {
		return nil, errors.New("pinned effect executable changed")
	}
	return e.run(ctx, dir, executable, args...)
}

func (e *GitHubEffects) run(ctx context.Context, dir, executable string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir = dir
	command.Env = allowedEnvironment(e.config.EnvironmentKeys)
	stdout, stderr := newBoundedBuffer(e.config.OutputLimitBytes), newBoundedBuffer(e.config.OutputLimitBytes)
	command.Stdout, command.Stderr = stdout, stderr
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("%s: %w%s", filepath.Base(executable), err, boundedStderr(stderr.String()))
	}
	if stdout.Overflowed() || stderr.Overflowed() {
		return nil, errors.New("effect command output exceeded configured limit")
	}
	return []byte(stdout.String()), nil
}

func safeGitRefPart(value string) bool {
	return value != "" && !strings.ContainsAny(value, "~^:?*[\\ ") && !strings.Contains(value, "..") && !strings.HasPrefix(value, "-")
}

func boundedTitle(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		value = "Civilization work item"
	}
	if len(value) > 120 {
		value = value[:120]
	}
	return value
}

func pullRequestBody(workID string, bound tlcbridge.BoundRequest) string {
	brief := bound.Envelope.Brief
	return fmt.Sprintf("<!-- civilization:v1 work_id=%s bound=%s -->\n\n%s\n\nRoute: `%s`\n\nTests requested by the accepted brief:\n- %s\n\nThis pull request was prepared by Civilization. Its reviewed and validated authority remains bound to the exact head.",
		workID, bound.IdempotencyKey, brief.Outcome, bound.Envelope.Route, strings.Join(brief.Tests, "\n- "))
}

func digestFile(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func resolveDirectory(path, label string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("%s must be absolute", label)
	}
	realpath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", label, err)
	}
	info, err := os.Stat(realpath)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("%s must be a directory", label)
	}
	return realpath, nil
}

func pathsOverlap(left, right string) bool {
	within := func(parent, child string) bool {
		relative, err := filepath.Rel(parent, child)
		return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
	}
	return within(left, right) || within(right, left)
}
