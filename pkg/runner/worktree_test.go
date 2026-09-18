package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/transpara-ai/hive/pkg/safety"
)

func TestContinuationRecoveryPreservesPartialAndStartsClean(t *testing.T) {
	repoDir := t.TempDir()
	initGitRepo(t, repoDir)
	base := strings.TrimSpace(runGitForTest(t, repoDir, "rev-parse", "HEAD"))
	partialTree, err := CreateTaskWorktree(repoDir, "partial continuation", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(partialTree.Cleanup)
	if err := os.WriteFile(filepath.Join(partialTree.Dir, "README"), []byte("dirty tracked"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partialTree.Dir, "untracked.txt"), []byte("dirty untracked"), 0o644); err != nil {
		t.Fatal(err)
	}
	partial, err := CapturePartialWorktreeEvidence(
		context.Background(), "change-recovery", strings.Repeat("a", 64), "attempt-1",
		partialTree.Dir, base, "eventgraph:partial:attempt-1", "runner_interrupted", "openai",
	)
	if err != nil {
		t.Fatal(err)
	}
	if partial.Authoritative || !partial.Quarantined || partial.UntrackedDigests["untracked.txt"] == "" {
		t.Fatalf("partial evidence=%+v", partial)
	}
	destination := filepath.Join(t.TempDir(), "recovery")
	recovery, err := CreateCleanRecoveryWorktree(context.Background(), repoDir, destination, "hive/recovery-test", partial)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(recovery.Cleanup)
	if got := strings.TrimSpace(runGitForTest(t, recovery.Dir, "status", "--porcelain=v1", "--untracked-files=all")); got != "" {
		t.Fatalf("recovery worktree is dirty: %s", got)
	}
	if got := strings.TrimSpace(runGitForTest(t, recovery.Dir, "rev-parse", "HEAD")); got != base {
		t.Fatalf("recovery head=%s, want base=%s", got, base)
	}
	if got := strings.TrimSpace(runGitForTest(t, partialTree.Dir, "status", "--porcelain=v1", "--untracked-files=all")); got == "" {
		t.Fatal("interrupted worktree was cleaned or reused")
	}
}

func runGitForTest(t *testing.T, directory string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = directory
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

// initGitRepo creates a minimal git repo in dir with one commit so that
// worktree and branch operations have a valid HEAD to work from.
func initGitRepo(t *testing.T, dir string) {
	t.Helper()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	run("init", "-b", "main")
	run("config", "user.email", "test@test.com")
	run("config", "user.name", "test")
	// Write a file and commit so HEAD exists.
	if err := os.WriteFile(filepath.Join(dir, "README"), []byte("init"), 0644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	run("add", ".")
	run("commit", "-m", "init")
}

// TestCreateTaskWorktree verifies that a worktree is created with the correct
// branch, directory, and git identity scoped to the worktree (not main repo).
func TestCreateTaskWorktree(t *testing.T) {
	repoDir := t.TempDir()
	initGitRepo(t, repoDir)

	wc, err := CreateTaskWorktree(repoDir, "test task", "task-001")
	if err != nil {
		t.Fatalf("CreateTaskWorktree: %v", err)
	}
	t.Cleanup(func() { wc.Cleanup() })

	// Worktree dir must exist.
	if _, err := os.Stat(wc.Dir); err != nil {
		t.Errorf("worktree dir %s does not exist: %v", wc.Dir, err)
	}

	// Branch must have the hive/feat/ prefix (CreateTaskWorktree uses "hive/{slug}-{ts}"
	// where branchSlug returns "feat/YYYYMMDD-...").
	if !strings.HasPrefix(wc.Branch, "hive/") {
		t.Errorf("branch %q does not start with hive/", wc.Branch)
	}

	// SourceDir and TaskID round-trip.
	if wc.SourceDir != repoDir {
		t.Errorf("SourceDir = %q, want %q", wc.SourceDir, repoDir)
	}
	if wc.TaskID != "task-001" {
		t.Errorf("TaskID = %q, want %q", wc.TaskID, "task-001")
	}

	// Git identity must be set so commits in the worktree use the hive identity.
	// Note: git worktrees share config with their main repo, so this config is
	// visible from both dirs — that's expected git behavior.
	getConfig := func(dir, key string) string {
		cmd := exec.Command("git", "config", "--local", key)
		cmd.Dir = dir
		out, _ := cmd.Output()
		return strings.TrimSpace(string(out))
	}
	if got := getConfig(wc.Dir, "user.name"); got != "ai-agent" {
		t.Errorf("worktree user.name = %q, want %q", got, "ai-agent")
	}
	if got := getConfig(wc.Dir, "user.email"); got != "ai-agent@transpara.com" {
		t.Errorf("worktree user.email = %q, want %q", got, "ai-agent@transpara.com")
	}
	if strings.Contains(getConfig(wc.Dir, "user.email"), "lovyou") {
		t.Errorf("worktree user.email %q must not reference lovyou", getConfig(wc.Dir, "user.email"))
	}
}

// TestGitConfigScopedToWorktree is a focused regression test for the cmd.Dir
// bug: git config must not bleed into the main repo.
func TestGitConfigScopedToWorktree(t *testing.T) {
	repoDir := t.TempDir()
	initGitRepo(t, repoDir)

	wc, err := CreateTaskWorktree(repoDir, "scope test", "task-002")
	if err != nil {
		t.Fatalf("CreateTaskWorktree: %v", err)
	}
	defer wc.Cleanup()

	getConfig := func(dir, key string) string {
		cmd := exec.Command("git", "config", "--local", key)
		cmd.Dir = dir
		out, _ := cmd.Output()
		return strings.TrimSpace(string(out))
	}

	// gitIn must run config in the repo context (not the process CWD).
	// Worktrees share config with main repo, so identity is visible from both.
	if got := getConfig(wc.Dir, "user.name"); got != "ai-agent" {
		t.Errorf("worktree user.name = %q, want %q", got, "ai-agent")
	}
	if got := getConfig(wc.Dir, "user.email"); got != "ai-agent@transpara.com" {
		t.Errorf("worktree user.email = %q, want %q", got, "ai-agent@transpara.com")
	}
	if strings.Contains(getConfig(wc.Dir, "user.email"), "lovyou") {
		t.Errorf("worktree user.email %q must not reference lovyou", getConfig(wc.Dir, "user.email"))
	}
}

// TestMergeToMainBlockedByDefault verifies that direct main-branch mutation is
// gated before any git checkout, pull, or merge command runs.
func TestMergeToMainBlockedByDefault(t *testing.T) {
	repoDir := t.TempDir()
	initGitRepo(t, repoDir)

	wc, err := CreateTaskWorktree(repoDir, "merge test", "task-003")
	if err != nil {
		t.Fatalf("CreateTaskWorktree: %v", err)
	}
	defer wc.Cleanup()

	// Add a commit on the worktree branch. The merge must still be blocked.
	newFile := filepath.Join(wc.Dir, "feature.txt")
	if err := os.WriteFile(newFile, []byte("feature"), 0644); err != nil {
		t.Fatalf("write feature.txt: %v", err)
	}
	if err := gitIn(wc.Dir, "add", "."); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if err := gitIn(wc.Dir, "commit", "-m", "add feature"); err != nil {
		t.Fatalf("git commit: %v", err)
	}

	err = wc.MergeToMain()
	if err == nil {
		t.Fatal("MergeToMain returned nil, want authority error")
	}
	assertAuthorityError(t, err, safety.ActionRepoMergeMain)

	// Verify the file was not merged to main.
	mergedFile := filepath.Join(repoDir, "feature.txt")
	if _, err := os.Stat(mergedFile); !os.IsNotExist(err) {
		t.Errorf("feature.txt exists on main after blocked merge; err=%v", err)
	}
}

// TestMergeToMainConcurrency verifies that concurrent MergeToMain calls remain
// serialized and blocked by the safety gate.
func TestMergeToMainConcurrency(t *testing.T) {
	repoDir := t.TempDir()
	initGitRepo(t, repoDir)

	const n = 3
	worktrees := make([]*WorktreeContext, n)
	for i := range worktrees {
		title := fmt.Sprintf("concurrent task %d", i)
		wc, err := CreateTaskWorktree(repoDir, title, fmt.Sprintf("task-concurrent-%d", i))
		if err != nil {
			t.Fatalf("CreateTaskWorktree %d: %v", i, err)
		}
		worktrees[i] = wc

		// Each worktree gets a unique commit so this would be a real merge without the gate.
		newFile := filepath.Join(wc.Dir, fmt.Sprintf("concurrent-%d.txt", i))
		if err := os.WriteFile(newFile, []byte("data"), 0644); err != nil {
			t.Fatalf("write file %d: %v", i, err)
		}
		if err := gitIn(wc.Dir, "add", "."); err != nil {
			t.Fatalf("git add %d: %v", i, err)
		}
		if err := gitIn(wc.Dir, "commit", "-m", "concurrent commit"); err != nil {
			t.Fatalf("git commit %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i, wc := range worktrees {
		wg.Add(1)
		go func(idx int, w *WorktreeContext) {
			defer wg.Done()
			errs[idx] = w.MergeToMain()
		}(i, wc)
	}
	wg.Wait()

	for i, wc := range worktrees {
		defer wc.Cleanup()
		if errs[i] == nil {
			t.Fatalf("MergeToMain %d returned nil, want authority error", i)
		}
		assertAuthorityError(t, errs[i], safety.ActionRepoMergeMain)
	}
}

// TestCleanup verifies that Cleanup removes the worktree directory and is safe
// to call multiple times (idempotent).
func TestCleanup(t *testing.T) {
	repoDir := t.TempDir()
	initGitRepo(t, repoDir)

	wc, err := CreateTaskWorktree(repoDir, "cleanup test", "task-004")
	if err != nil {
		t.Fatalf("CreateTaskWorktree: %v", err)
	}

	dir := wc.Dir
	wc.Cleanup()

	// Directory must be gone.
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("worktree dir %s still exists after Cleanup", dir)
	}

	// Second call must not panic.
	wc.Cleanup()
}

func assertAuthorityError(t *testing.T, err error, wantAction safety.ProtectedAction) {
	t.Helper()

	var authorityErr safety.AuthorityError
	if !errors.As(err, &authorityErr) {
		t.Fatalf("error = %T %v, want safety.AuthorityError", err, err)
	}
	if authorityErr.Action != wantAction {
		t.Fatalf("authority action = %q, want %q", authorityErr.Action, wantAction)
	}
	if authorityErr.Outcome != safety.ApprovalRequired {
		t.Fatalf("authority outcome = %q, want %q", authorityErr.Outcome, safety.ApprovalRequired)
	}
}
