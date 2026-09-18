package runner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/transpara-ai/hive/pkg/hive/factoryv1"
)

// RecoverContinuationWorktree is the guarded recovery entrypoint. It commits
// partial evidence before calling CreateCleanRecoveryWorktree and carries the
// TLC RepoX/frontier plus observe-before-retry decision immediately to the Git
// mutation boundary.
func RecoverContinuationWorktree(
	ctx context.Context,
	store factoryv1.Store,
	work factoryv1.ContinuationWorkStore,
	report factoryv1.ContinuationReport,
	expectedRepository factoryv1.RepositoryIdentity,
	candidateID string,
	observation factoryv1.EffectObservationState,
	authorityApplicable, budgetAvailable bool,
	partial factoryv1.PartialWorktreeEvidence,
	causalRecordIDs []string,
	principal factoryv1.Principal,
	capturedBy string,
	observedAt time.Time,
	sourceRepository, destination, branch string,
) (*WorktreeContext, factoryv1.EffectDecision, error) {
	var recovered *WorktreeContext
	decision, err := factoryv1.PersistPartialAndExecuteRecovery(
		ctx, store, work, partial, causalRecordIDs, principal, capturedBy, observedAt,
		report, expectedRepository, candidateID, "create_worktree", observation,
		authorityApplicable, budgetAvailable,
		func(ctx context.Context) (factoryv1.EffectObservationState, error) {
			var recoveryErr error
			recovered, recoveryErr = CreateCleanRecoveryWorktree(ctx, sourceRepository, destination, branch, partial)
			if recoveryErr != nil {
				return factoryv1.EffectUnknown, recoveryErr
			}
			return factoryv1.EffectExact, nil
		},
	)
	return recovered, decision, err
}

// CapturePartialWorktreeEvidence captures exact dirty-state identities without
// granting them artifact, review, or continuation credit. patchReference must
// already name the executor's durable preservation of the binary patch.
func CapturePartialWorktreeEvidence(
	ctx context.Context,
	chainID, sourceHead, attemptID, worktreeDir, baseCommit, patchReference, providerOutcome, authorLineage string,
) (factoryv1.PartialWorktreeEvidence, error) {
	common, err := gitOutputContext(ctx, worktreeDir, "rev-parse", "--git-common-dir")
	if err != nil {
		return factoryv1.PartialWorktreeEvidence{}, err
	}
	common, err = absoluteGitPath(worktreeDir, common)
	if err != nil {
		return factoryv1.PartialWorktreeEvidence{}, err
	}
	common, err = filepath.EvalSymlinks(common)
	if err != nil {
		return factoryv1.PartialWorktreeEvidence{}, fmt.Errorf("resolve partial Git common directory: %w", err)
	}
	head, err := gitOutputContext(ctx, worktreeDir, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return factoryv1.PartialWorktreeEvidence{}, err
	}
	branch, err := gitOutputContext(ctx, worktreeDir, "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil {
		return factoryv1.PartialWorktreeEvidence{}, errors.New("partial worktree must retain an exact branch identity")
	}
	patch, err := gitBytesContext(ctx, worktreeDir, "diff", "--binary", "--no-ext-diff", baseCommit, "--")
	if err != nil {
		return factoryv1.PartialWorktreeEvidence{}, err
	}
	trackedDigest := sha256.Sum256(patch)
	untrackedRaw, err := gitBytesContext(ctx, worktreeDir, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return factoryv1.PartialWorktreeEvidence{}, err
	}
	untracked := make(map[string]string)
	for _, raw := range bytes.Split(untrackedRaw, []byte{0}) {
		if len(raw) == 0 {
			continue
		}
		relative := string(raw)
		path := filepath.Join(worktreeDir, filepath.FromSlash(relative))
		info, err := os.Lstat(path)
		if err != nil {
			return factoryv1.PartialWorktreeEvidence{}, fmt.Errorf("stat partial untracked file %s: %w", relative, err)
		}
		if !info.Mode().IsRegular() {
			return factoryv1.PartialWorktreeEvidence{}, fmt.Errorf("partial untracked path %s is not a regular file", relative)
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return factoryv1.PartialWorktreeEvidence{}, err
		}
		digest := sha256.Sum256(content)
		untracked[filepath.ToSlash(relative)] = hex.EncodeToString(digest[:])
	}
	partial := factoryv1.PartialWorktreeEvidence{
		PartialID:          "partial-" + factoryv1.HashText(chainID + "\x00" + attemptID + "\x00" + head + "\x00" + hex.EncodeToString(trackedDigest[:]))[:32],
		ChainID:            chainID,
		SourceChainHead:    sourceHead,
		AttemptID:          attemptID,
		GitCommonDirectory: common,
		BaseCommit:         baseCommit,
		CurrentHead:        head,
		Branch:             branch,
		TrackedDiffSHA256:  hex.EncodeToString(trackedDigest[:]),
		PatchReference:     patchReference,
		UntrackedDigests:   untracked,
		ProviderOutcome:    providerOutcome,
		AuthorLineage:      authorLineage,
	}
	return factoryv1.SealPartialWorktreeEvidence(partial)
}

// CreateCleanRecoveryWorktree creates a new worktree at the admitted base. It
// never reuses, cleans, resets, or deletes the interrupted worktree.
func CreateCleanRecoveryWorktree(
	ctx context.Context,
	sourceRepository, destination, branch string,
	partial factoryv1.PartialWorktreeEvidence,
) (*WorktreeContext, error) {
	if err := factoryv1.ValidatePartialWorktreeEvidence(partial); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(destination) || strings.TrimSpace(branch) == "" {
		return nil, errors.New("clean recovery requires an absolute destination and explicit branch")
	}
	if _, err := os.Lstat(destination); err == nil || !os.IsNotExist(err) {
		return nil, errors.New("clean recovery destination must not already exist")
	}
	common, err := gitOutputContext(ctx, sourceRepository, "rev-parse", "--git-common-dir")
	if err != nil {
		return nil, err
	}
	common, err = absoluteGitPath(sourceRepository, common)
	if err != nil {
		return nil, err
	}
	common, err = filepath.EvalSymlinks(common)
	if err != nil {
		return nil, err
	}
	if common != partial.GitCommonDirectory {
		return nil, errors.New("recovery repository does not match the partial Git common directory")
	}
	if _, err := gitOutputContext(ctx, sourceRepository, "rev-parse", "--verify", partial.BaseCommit+"^{commit}"); err != nil {
		return nil, fmt.Errorf("recovery base commit is unavailable: %w", err)
	}
	if _, err := gitOutputContext(ctx, sourceRepository, "worktree", "add", "--detach", destination, partial.BaseCommit); err != nil {
		return nil, fmt.Errorf("create clean recovery worktree: %w", err)
	}
	if _, err := gitOutputContext(ctx, destination, "checkout", "-b", branch); err != nil {
		return nil, fmt.Errorf("create clean recovery branch: %w", err)
	}
	head, err := gitOutputContext(ctx, destination, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return nil, err
	}
	status, err := gitBytesContext(ctx, destination, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return nil, err
	}
	if head != partial.BaseCommit || len(status) != 0 {
		return nil, errors.New("recovery worktree is not clean at the admitted base commit")
	}
	return &WorktreeContext{Dir: destination, Branch: branch, SourceDir: sourceRepository, TaskID: partial.AttemptID}, nil
}

func gitOutputContext(ctx context.Context, directory string, args ...string) (string, error) {
	output, err := gitBytesContext(ctx, directory, args...)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func absoluteGitPath(directory, value string) (string, error) {
	if filepath.IsAbs(value) {
		return filepath.Clean(value), nil
	}
	return filepath.Abs(filepath.Join(directory, value))
}

func gitBytesContext(ctx context.Context, directory string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = directory
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}
