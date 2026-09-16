package civilization

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// RunnerEvidence is supplied by Platform, not by the provider result.
type RunnerEvidence struct {
	AttemptID    string `json:"attempt_id"`
	Image        string `json:"image"`
	PolicySHA256 string `json:"policy_sha256"`
	InputSHA256  string `json:"input_sha256"`
	OutputSHA256 string `json:"output_sha256"`
	Operation    string `json:"operation"`
}

type runnerRequest struct {
	Version    int                `json:"version"`
	AttemptID  string             `json:"attempt_id"`
	WorkID     string             `json:"work_id"`
	Repository string             `json:"repository"`
	Operation  string             `json:"operation"`
	BaseSHA    string             `json:"base_sha"`
	Patch      string             `json:"patch"`
	Prompt     string             `json:"prompt"`
	Selection  ExecutionSelection `json:"selection"`
}

type runnerJob struct {
	Request  runnerRequest   `json:"request"`
	State    string          `json:"state"`
	Result   ProviderResult  `json:"result"`
	Patch    string          `json:"patch"`
	Evidence *RunnerEvidence `json:"evidence"`
	Error    string          `json:"error"`
}

// RunnerProvider is deliberately unable to express Docker options or commands.
// An unavailable runner never falls back to local execution.
var ErrRunnerPending = errors.New("runner attempt is pending; reconcile the same assignment")

type RunnerProvider struct {
	endpoint, token string
	client          *http.Client
}

func NewRunnerProvider(endpoint, token string) (*RunnerProvider, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost"))) || len(token) < 32 {
		return nil, errors.New("runner requires HTTPS or loopback HTTP and a dedicated token")
	}
	return &RunnerProvider{endpoint: strings.TrimRight(endpoint, "/"), token: token, client: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (p *RunnerProvider) exchange(ctx context.Context, method, path string, input any) (runnerJob, int, error) {
	var body io.Reader
	if input != nil {
		raw, err := json.Marshal(input)
		if err != nil {
			return runnerJob{}, 0, err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.endpoint+path, body)
	if err != nil {
		return runnerJob{}, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Content-Type", "application/json")
	res, err := p.client.Do(req)
	if err != nil {
		return runnerJob{}, 0, fmt.Errorf("runner connection unavailable: %w", err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 8*1024*1024+1))
	if err != nil || len(raw) > 8*1024*1024 {
		return runnerJob{}, res.StatusCode, errors.New("runner response exceeds limit")
	}
	var job runnerJob
	if err = json.Unmarshal(raw, &job); err != nil {
		return job, res.StatusCode, errors.New("invalid runner response")
	}
	if res.StatusCode != 200 && res.StatusCode != 404 {
		return job, res.StatusCode, fmt.Errorf("runner rejected request (HTTP %d)", res.StatusCode)
	}
	return job, res.StatusCode, nil
}

func runnerGit(ctx context.Context, root string, input []byte, index string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "/usr/bin/git", append([]string{"-C", root}, args...)...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=/nonexistent", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=core.hooksPath", "GIT_CONFIG_VALUE_0=/dev/null"}
	if index != "" {
		cmd.Env = append(cmd.Env, "GIT_INDEX_FILE="+index)
	}
	cmd.Stdin = bytes.NewReader(input)
	output := newBoundedBuffer(2 * 1024 * 1024)
	cmd.Stdout = output
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("runner artifact Git operation failed: %w", err)
	}
	if output.Overflowed() {
		return nil, errors.New("runner artifact exceeds limit")
	}
	return []byte(output.String()), nil
}

func runnerSnapshot(ctx context.Context, root string) (string, string, error) {
	head, err := runnerGit(ctx, root, nil, "", "rev-parse", "HEAD")
	if err != nil {
		return "", "", err
	}
	file, err := os.CreateTemp("", "civilization-index-")
	if err != nil {
		return "", "", err
	}
	index := file.Name()
	file.Close()
	os.Remove(index)
	defer os.Remove(index)
	if _, err = runnerGit(ctx, root, nil, index, "read-tree", "HEAD"); err != nil {
		return "", "", err
	}
	if _, err = runnerGit(ctx, root, nil, index, "add", "-A", "--", "."); err != nil {
		return "", "", err
	}
	patch, err := runnerGit(ctx, root, nil, index, "diff", "--cached", "--no-renames", "--binary", "--no-ext-diff", "--no-textconv", "HEAD", "--")
	return strings.TrimSpace(string(head)), string(patch), err
}

func patchDigest(patch string) string {
	sum := sha256.Sum256([]byte(patch))
	return hex.EncodeToString(sum[:])
}

func (p *RunnerProvider) job(ctx context.Context, request runnerRequest, root string) (runnerJob, error) {
	// Fetch first: after a Hive crash the original input might already have been
	// replaced by the completed artifact. It must not become a new request.
	path := "/v1/jobs/" + request.AttemptID
	job, status, err := p.exchange(ctx, "GET", path, nil)
	if err != nil {
		return job, classifyRunnerError(status, err)
	}
	if status == 404 {
		head, patch, err := runnerSnapshot(ctx, root)
		if err != nil {
			return job, err
		}
		if request.BaseSHA != "" && head != request.BaseSHA {
			return job, errors.New("runner workspace base changed")
		}
		request.BaseSHA, request.Patch = head, patch
		job, status, err = p.exchange(ctx, "POST", "/v1/jobs", request)
		if err != nil {
			return job, classifyRunnerError(status, err)
		}
	}
	for {
		got := job.Request
		if got.AttemptID != request.AttemptID || got.WorkID != request.WorkID || got.Repository != request.Repository || got.Operation != request.Operation || got.Prompt != request.Prompt || got.Selection != request.Selection || (request.BaseSHA != "" && got.BaseSHA != request.BaseSHA) {
			return job, errors.New("runner attempt identity does not match Hive assignment")
		}
		if job.State == "failed" {
			return job, fmt.Errorf("runner attempt failed: %s", job.Error)
		}
		if job.State == "succeeded" {
			break
		}
		switch job.State {
		case "queued", "preparing", "launching", "running":
		default:
			return job, errors.New("invalid runner state")
		}
		select {
		case <-ctx.Done():
			return job, ctx.Err()
		case <-time.After(time.Second):
		}
		job, status, err = p.exchange(ctx, "GET", path, nil)
		if err != nil {
			return job, classifyRunnerError(status, err)
		}
		if status != 200 {
			return job, errors.New("runner lost durable attempt")
		}
	}
	ev := job.Evidence
	if ev == nil || ev.AttemptID != request.AttemptID || ev.Operation != request.Operation || ev.InputSHA256 != patchDigest(job.Request.Patch) || ev.OutputSHA256 != patchDigest(job.Patch) || !sha256Pattern.MatchString(ev.PolicySHA256) || !strings.HasPrefix(ev.Image, "sha256:") || !sha256Pattern.MatchString(strings.TrimPrefix(ev.Image, "sha256:")) {
		return job, errors.New("runner artifact evidence does not match returned bytes")
	}
	head, current, err := runnerSnapshot(ctx, root)
	if err != nil {
		return job, err
	}
	if head != job.Request.BaseSHA || (current != job.Request.Patch && current != job.Patch) {
		return job, errors.New("Hive workspace changed during runner execution")
	}
	if request.Operation != "implement" && job.Patch != job.Request.Patch {
		return job, errors.New("read-only attempt changed artifact")
	}
	if current != job.Patch {
		// Apply a patch between the two snapshots without resetting the controller's
		// worktree. Validate both applications before making the bounded change.
		if job.Request.Patch != "" {
			if _, err = runnerGit(ctx, root, []byte(job.Request.Patch), "", "apply", "--check", "--reverse", "--binary", "-"); err != nil {
				return job, err
			}
			if _, err = runnerGit(ctx, root, []byte(job.Request.Patch), "", "apply", "--reverse", "--binary", "-"); err != nil {
				return job, err
			}
		}
		if job.Patch != "" {
			if _, err = runnerGit(ctx, root, []byte(job.Patch), "", "apply", "--binary", "--whitespace=nowarn", "-"); err != nil {
				// Restore the exact old snapshot if applying the new artifact failed.
				if job.Request.Patch != "" {
					_, _ = runnerGit(ctx, root, []byte(job.Request.Patch), "", "apply", "--binary", "-")
				}
				return job, err
			}
		}
	}
	_, current, err = runnerSnapshot(ctx, root)
	if err != nil {
		return job, err
	}
	if current != job.Patch {
		return job, errors.New("applied artifact differs from runner evidence")
	}
	return job, nil
}

func (p *RunnerProvider) Run(ctx context.Context, request ProviderRequest) (ProviderResult, error) {
	if !workIDPattern.MatchString(request.WorkID) || !repositoryNamePattern.MatchString(request.Repository) || !providerAttemptPattern.MatchString(request.AttemptID) || !filepath.IsAbs(request.RepositoryRoot) {
		return ProviderResult{}, errors.New("runner requires a bounded Hive assignment")
	}
	job, err := p.job(ctx, runnerRequest{Version: 1, AttemptID: request.AttemptID, WorkID: request.WorkID, Repository: request.Repository, Operation: string(request.Operation), BaseSHA: request.BaseSHA, Prompt: request.Prompt, Selection: request.Selection}, request.RepositoryRoot)
	if err != nil {
		return ProviderResult{}, err
	}
	job.Result.Runner = job.Evidence
	return job.Result, nil
}

type runnerVerificationAttemptKey struct{}

func (p *RunnerProvider) Verify(ctx context.Context, workID, repository, root, baseSHA string) (*RunnerEvidence, error) {
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	attempt, _ := ctx.Value(runnerVerificationAttemptKey{}).(string)
	if attempt == "" {
		attempt = hex.EncodeToString(nonce)
	}
	job, err := p.job(ctx, runnerRequest{Version: 1, AttemptID: attempt, WorkID: workID, Repository: repository, Operation: "verify", BaseSHA: baseSHA}, root)
	if err != nil {
		return nil, err
	}
	if job.Result.Status != "passed" || len(job.Result.Checks) == 0 {
		return nil, errors.New("isolated verification did not pass")
	}
	for _, check := range job.Result.Checks {
		if check.Status != "passed" {
			return nil, errors.New("isolated verification returned a failing check")
		}
	}
	return job.Evidence, nil
}

func classifyRunnerError(status int, err error) error {
	if status == 0 || status == http.StatusConflict || status >= 500 {
		return fmt.Errorf("%w: %v", ErrRunnerPending, err)
	}
	return err
}

// Check keeps readiness truthful without blocking read-only work inspection.
func (p *RunnerProvider) Check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, status, err := p.exchange(ctx, "GET", "/healthz", nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return errors.New("runner is unavailable")
	}
	return nil
}
