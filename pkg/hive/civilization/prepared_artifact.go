package civilization

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/transpara-ai/hive/pkg/hive/tlcbridge"
)

func (e *GitHubEffects) PublicationEnabled() bool { return e.config.PublishEnabled }

// PreparedArtifact independently verifies without committing or publishing.
// The returned snapshot is persisted for subsequent read-only inspection.
func (e *GitHubEffects) PreparedArtifact(ctx context.Context, workID string, bound tlcbridge.BoundRequest, workspace Workspace, implementation ProviderResult, digest string) (Artifact, error) {
	unlock, err := e.lock(workID)
	if err != nil {
		return Artifact{}, err
	}
	defer unlock()
	spec, root, err := e.validateWorkspace(workID, bound, workspace)
	if err != nil {
		return Artifact{}, err
	}
	if err := validateImplementation(implementation); err != nil {
		return Artifact{}, err
	}
	check := func() error {
		matches, err := e.implementationMatches(ctx, root, workID, workspace.BaseSHA, implementation, digest)
		if err != nil {
			return err
		}
		if !matches {
			return errors.New("worktree differs from the reviewed implementation; recover and review again")
		}
		return nil
	}
	if err := check(); err != nil {
		return Artifact{}, err
	}
	var verification *RunnerEvidence
	if e.config.Runner != nil {
		verification, err = e.config.Runner.Verify(ctx, workID, bound.Source.Repository, root, workspace.BaseSHA)
		if err != nil {
			return Artifact{}, err
		}
	} else if err := e.verify(ctx, root, spec.VerificationCommands); err != nil {
		return Artifact{}, err
	}
	patch, err := e.gitRun(ctx, root, "diff", "--no-ext-diff", "--no-textconv", "--binary", workspace.BaseSHA, "--")
	if err != nil {
		return Artifact{}, err
	}
	untracked, err := e.gitRun(ctx, root, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return Artifact{}, err
	}
	// Untracked content is presented explicitly alongside the tracked Git diff.
	// Never follow symlinks or return an unbounded file.
	for _, file := range normalizedNULFiles(untracked) {
		path := filepath.Join(root, file)
		info, err := os.Lstat(path)
		if err != nil {
			return Artifact{}, err
		}
		patch = append(patch, []byte("\nNew file: "+file+"\n")...)
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return Artifact{}, err
			}
			patch = append(patch, []byte("Symlink: "+target+"\n")...)
		} else if info.Mode().IsRegular() && info.Size() <= int64(e.config.OutputLimitBytes-len(patch)) {
			raw, err := os.ReadFile(path)
			if err != nil {
				return Artifact{}, err
			}
			if strings.IndexByte(string(raw), 0) >= 0 {
				patch = append(patch, []byte("Binary file; inspect in the worktree.\n")...)
			} else {
				patch = append(patch, raw...)
			}
		} else {
			return Artifact{}, fmt.Errorf("artifact %q exceeds inspection limit or has an unsupported type", file)
		}
		if len(patch) > e.config.OutputLimitBytes {
			return Artifact{}, errors.New("artifact exceeds inspection limit; inspect the worktree")
		}
	}
	if err := check(); err != nil {
		return Artifact{}, err
	}
	return Artifact{Verification: verification, Repository: bound.Source.Repository, Branch: workspace.Branch, BaseSHA: workspace.BaseSHA, WorkspaceDigest: digest, ChangedFiles: implementation.ChangedFiles, Patch: string(patch)}, nil
}
