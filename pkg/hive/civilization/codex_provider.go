package civilization

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type ProviderOperation string

const (
	OperationRoute     ProviderOperation = "route"
	OperationImplement ProviderOperation = "implement"
	OperationReview    ProviderOperation = "review"
)

type ProviderRequest struct {
	WorkID         string
	Repository     string
	BaseSHA        string
	Selection      ExecutionSelection
	Operation      ProviderOperation
	AttemptID      string
	RepositoryRoot string
	Prompt         string
}

type CheckResult struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Summary string `json:"summary"`
}

type ReviewResult struct {
	Status   string   `json:"status"`
	Summary  string   `json:"summary"`
	Findings []string `json:"findings"`
}

// ProviderResult is the small machine result returned by every provider. A
// route operation carries the complete TLC transport document in TLCEnvelope.
type ProviderResult struct {
	Runner       *RunnerEvidence    `json:"runner,omitempty"`
	Execution    *ExecutionEvidence `json:"execution,omitempty"`
	Status       string             `json:"status"`
	Summary      string             `json:"summary"`
	TLCEnvelope  json.RawMessage    `json:"tlc_envelope,omitempty"`
	ChangedFiles []string           `json:"changed_files"`
	Checks       []CheckResult      `json:"checks"`
	Review       *ReviewResult      `json:"review,omitempty"`
	Blocker      string             `json:"blocker,omitempty"`
	NextAction   string             `json:"next_action"`
}

type Provider interface {
	Run(ctx context.Context, request ProviderRequest) (ProviderResult, error)
}

type CodexCLIConfig struct {
	Executable                string
	ExecutableSHA256          string
	ManagedRequirementsFile   string
	ManagedRequirementsSHA256 string
	Model                     string
	Profile                   string
	Timeout                   time.Duration
	OutputLimitBytes          int
	EnvironmentKeys           []string
	ReceiptDirectory          string
}

// CodexCLI invokes stable Codex non-interactive mode. It never uses the
// dangerous sandbox bypass and passes only explicitly allowed environment
// keys to the child process.
type CodexCLI struct {
	config           CodexCLIConfig
	realpath         string
	executableSHA256 string
	requirementsPath string
	requirementsSHA  string
}

var providerAttemptPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type providerReceipt struct {
	Schema        string         `json:"schema"`
	AttemptID     string         `json:"attempt_id"`
	RequestSHA256 string         `json:"request_sha256"`
	Result        ProviderResult `json:"result"`
}

func NewCodexCLI(config CodexCLIConfig) (*CodexCLI, error) {
	if strings.TrimSpace(config.Executable) == "" || strings.TrimSpace(config.ExecutableSHA256) == "" {
		return nil, errors.New("Codex executable and SHA-256 are required")
	}
	if err := validateSelection(ExecutionSelection{Provider: "codex", Model: config.Model}); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(config.ManagedRequirementsFile) || !providerAttemptPattern.MatchString(config.ManagedRequirementsSHA256) {
		return nil, errors.New("absolute Codex managed requirements file and SHA-256 are required")
	}
	requirementsPath, err := filepath.EvalSymlinks(config.ManagedRequirementsFile)
	if err != nil {
		return nil, fmt.Errorf("resolve Codex managed requirements: %w", err)
	}
	info, err := os.Stat(requirementsPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 {
		return nil, errors.New("Codex managed requirements must be a regular file not writable by group or other")
	}
	if digest, digestErr := digestFile(requirementsPath); digestErr != nil || digest != config.ManagedRequirementsSHA256 {
		return nil, errors.New("Codex managed requirements SHA-256 does not match configured digest")
	}
	if config.Timeout <= 0 {
		return nil, errors.New("Codex timeout must be positive")
	}
	if config.OutputLimitBytes <= 0 {
		return nil, errors.New("Codex output limit must be positive")
	}
	if config.ReceiptDirectory != "" {
		if !filepath.IsAbs(config.ReceiptDirectory) {
			return nil, errors.New("Codex receipt directory must be absolute")
		}
		if err := os.MkdirAll(config.ReceiptDirectory, 0o700); err != nil {
			return nil, fmt.Errorf("create Codex receipt directory: %w", err)
		}
		receiptDir, err := filepath.EvalSymlinks(config.ReceiptDirectory)
		if err != nil {
			return nil, fmt.Errorf("resolve Codex receipt directory: %w", err)
		}
		if err := os.Chmod(receiptDir, 0o700); err != nil {
			return nil, fmt.Errorf("protect Codex receipt directory: %w", err)
		}
		config.ReceiptDirectory = receiptDir
	}
	realpath, digest, err := resolvePinnedExecutable(config.Executable)
	if err != nil {
		return nil, err
	}
	if digest != config.ExecutableSHA256 {
		return nil, errors.New("Codex executable SHA-256 does not match configured digest")
	}
	config.Executable = realpath
	config.ManagedRequirementsFile = requirementsPath
	return &CodexCLI{
		config: config, realpath: realpath, executableSHA256: digest,
		requirementsPath: requirementsPath, requirementsSHA: config.ManagedRequirementsSHA256,
	}, nil
}

func (c *CodexCLI) Run(ctx context.Context, request ProviderRequest) (ProviderResult, error) {
	requested := request.Selection
	if requested.Provider != "" && requested.Provider != "codex" {
		return ProviderResult{}, errors.New("Codex cannot execute another provider selection")
	}
	if err := validateSelection(requested); err != nil {
		return ProviderResult{}, err
	}
	request.Selection.Provider = "codex"
	modelSource := "invocation"
	if request.Selection.Model == "" {
		request.Selection.Model = c.config.Model
		modelSource = "configured_provider_default"
	}
	if request.Selection.Model == "" {
		modelSource = "provider_default"
	}
	if request.Selection.ReasoningEffort == "max" && !codexSupportsMaxEffort(request.Selection.Model) {
		return ProviderResult{}, errors.New("Codex max reasoning requires GPT-6 Astra or a GPT-5.6 model; choose a current model or a lower effort")
	}
	if request.Operation != OperationRoute && request.Operation != OperationImplement && request.Operation != OperationReview {
		return ProviderResult{}, fmt.Errorf("unsupported provider operation %q", request.Operation)
	}
	if strings.TrimSpace(request.Prompt) == "" {
		return ProviderResult{}, errors.New("provider prompt is required")
	}
	if c.config.ReceiptDirectory != "" && !providerAttemptPattern.MatchString(request.AttemptID) {
		return ProviderResult{}, errors.New("provider attempt id must be a 64-character lowercase hex digest when receipts are enabled")
	}
	root, err := confinedRepositoryRoot(request.RepositoryRoot)
	if err != nil {
		return ProviderResult{}, err
	}
	_, digest, err := resolvePinnedExecutable(c.realpath)
	if err != nil {
		return ProviderResult{}, err
	}
	if digest != c.executableSHA256 {
		return ProviderResult{}, errors.New("Codex executable changed after provider initialization")
	}
	if digest, digestErr := digestFile(c.requirementsPath); digestErr != nil || digest != c.requirementsSHA {
		return ProviderResult{}, errors.New("Codex managed requirements changed after provider initialization")
	}
	requestDigest := providerRequestDigest(request, root+"\x00"+c.executableSHA256+"\x00"+c.requirementsSHA+"\x00"+c.config.Profile)
	if c.config.ReceiptDirectory != "" {
		if result, found, err := c.loadReceipt(request.AttemptID, requestDigest); err != nil {
			return ProviderResult{}, err
		} else if found {
			return result, nil
		}
	}

	tempDir, err := os.MkdirTemp("", "civilization-codex-")
	if err != nil {
		return ProviderResult{}, fmt.Errorf("create Codex result directory: %w", err)
	}
	defer os.RemoveAll(tempDir)
	if err := os.Chmod(tempDir, 0o700); err != nil {
		return ProviderResult{}, fmt.Errorf("protect Codex result directory: %w", err)
	}
	schemaPath := filepath.Join(tempDir, "result.schema.json")
	resultPath := filepath.Join(tempDir, "result.json")
	if err := os.WriteFile(schemaPath, providerResultSchema, 0o600); err != nil {
		return ProviderResult{}, fmt.Errorf("write Codex result schema: %w", err)
	}

	args := []string{
		"exec", "--ephemeral", "--strict-config", "--color", "never",
		"--cd", root,
		"--output-schema", schemaPath,
		"--output-last-message", resultPath,
	}
	if request.Selection.Model != "" {
		args = append(args, "--model", request.Selection.Model)
	}
	if request.Selection.ReasoningEffort != "" {
		args = append(args, "-c", "model_reasoning_effort="+strconv.Quote(request.Selection.ReasoningEffort))
	}
	if c.config.Profile != "" {
		args = append(args, "--profile", c.config.Profile)
	}
	if request.Operation == OperationImplement {
		args = append(args, "--sandbox", "workspace-write")
	} else {
		args = append(args, "--sandbox", "read-only")
	}
	args = append(args, "-")

	runCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, c.realpath, args...)
	cmd.Dir = root
	cmd.Stdin = strings.NewReader(request.Prompt)
	cmd.Env = allowedEnvironment(c.config.EnvironmentKeys)
	stdout := newBoundedBuffer(c.config.OutputLimitBytes)
	stderr := newBoundedBuffer(c.config.OutputLimitBytes)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			return ProviderResult{}, fmt.Errorf("Codex %s timed out after %s", request.Operation, c.config.Timeout)
		}
		return ProviderResult{}, fmt.Errorf("Codex %s failed: %w%s", request.Operation, err, boundedStderr(stderr.String()))
	}
	if stdout.Overflowed() || stderr.Overflowed() {
		return ProviderResult{}, errors.New("Codex output exceeded configured limit")
	}
	raw, err := os.ReadFile(resultPath)
	if err != nil {
		return ProviderResult{}, fmt.Errorf("read Codex structured result: %w", err)
	}
	if len(raw) > c.config.OutputLimitBytes {
		return ProviderResult{}, errors.New("Codex structured result exceeded configured limit")
	}
	result, err := decodeProviderResult(raw)
	if err != nil {
		return ProviderResult{}, err
	}
	if request.Operation == OperationRoute && len(bytes.TrimSpace(result.TLCEnvelope)) == 0 {
		return ProviderResult{}, errors.New("Codex route result omitted TLC envelope")
	}
	if request.Operation != OperationRoute && len(bytes.TrimSpace(result.TLCEnvelope)) != 0 {
		return ProviderResult{}, errors.New("non-route Codex result unexpectedly returned a TLC envelope")
	}
	result.Execution = &ExecutionEvidence{Requested: requested, Effective: request.Selection, ModelSource: modelSource}
	if c.config.ReceiptDirectory != "" {
		if err := c.storeReceipt(providerReceipt{Schema: "civilization-provider-receipt/v1", AttemptID: request.AttemptID, RequestSHA256: requestDigest, Result: result}); err != nil {
			return ProviderResult{}, err
		}
	}
	return result, nil
}

func providerRequestDigest(request ProviderRequest, root string) string {
	hash := sha256.New()
	for _, value := range []string{string(request.Operation), request.AttemptID, root, request.Prompt, request.Selection.Provider, request.Selection.Model, request.Selection.ReasoningEffort} {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func (c *CodexCLI) loadReceipt(attemptID, requestDigest string) (ProviderResult, bool, error) {
	path := filepath.Join(c.config.ReceiptDirectory, attemptID+".json")
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return ProviderResult{}, false, nil
	}
	if err != nil {
		return ProviderResult{}, false, fmt.Errorf("read Codex receipt: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var receipt providerReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return ProviderResult{}, false, fmt.Errorf("decode Codex receipt: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ProviderResult{}, false, errors.New("Codex receipt contains multiple JSON values")
	}
	if receipt.Schema != "civilization-provider-receipt/v1" || receipt.AttemptID != attemptID || receipt.RequestSHA256 != requestDigest {
		return ProviderResult{}, false, errors.New("Codex receipt does not match the exact provider request")
	}
	return receipt.Result, true, nil
}

func (c *CodexCLI) storeReceipt(receipt providerReceipt) error {
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("encode Codex receipt: %w", err)
	}
	temporary, err := os.CreateTemp(c.config.ReceiptDirectory, ".receipt-")
	if err != nil {
		return fmt.Errorf("create Codex receipt: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("protect Codex receipt: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		temporary.Close()
		return fmt.Errorf("write Codex receipt: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync Codex receipt: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close Codex receipt: %w", err)
	}
	if err := os.Rename(temporaryPath, filepath.Join(c.config.ReceiptDirectory, receipt.AttemptID+".json")); err != nil {
		return fmt.Errorf("publish Codex receipt: %w", err)
	}
	return nil
}

func resolvePinnedExecutable(name string) (string, string, error) {
	resolved, err := exec.LookPath(name)
	if err != nil {
		return "", "", fmt.Errorf("resolve executable: %w", err)
	}
	realpath, err := filepath.EvalSymlinks(resolved)
	if err != nil {
		return "", "", fmt.Errorf("resolve executable realpath: %w", err)
	}
	info, err := os.Stat(realpath)
	if err != nil {
		return "", "", fmt.Errorf("stat executable: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return "", "", errors.New("provider executable must be an executable regular file")
	}
	file, err := os.Open(realpath)
	if err != nil {
		return "", "", fmt.Errorf("open executable: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", "", fmt.Errorf("hash executable: %w", err)
	}
	return realpath, hex.EncodeToString(hash.Sum(nil)), nil
}

func confinedRepositoryRoot(root string) (string, error) {
	if !filepath.IsAbs(root) {
		return "", errors.New("repository root must be absolute")
	}
	realpath, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolve repository root: %w", err)
	}
	info, err := os.Stat(filepath.Join(realpath, ".git"))
	if err != nil {
		return "", errors.New("repository root must contain a .git entry")
	}
	if !info.IsDir() && !info.Mode().IsRegular() {
		return "", errors.New("repository .git entry has unsupported type")
	}
	return realpath, nil
}

func allowedEnvironment(keys []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" || strings.Contains(key, "=") {
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		if value, ok := os.LookupEnv(key); ok {
			result = append(result, key+"="+value)
		}
	}
	return result
}

func decodeProviderResult(raw []byte) (ProviderResult, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var result ProviderResult
	if err := decoder.Decode(&result); err != nil {
		return ProviderResult{}, fmt.Errorf("decode Codex structured result: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ProviderResult{}, errors.New("Codex structured result contains multiple JSON values")
	}
	if result.Status != "passed" && result.Status != "blocked" {
		return ProviderResult{}, errors.New("Codex result status must be passed or blocked")
	}
	if strings.TrimSpace(result.Summary) == "" || strings.TrimSpace(result.NextAction) == "" {
		return ProviderResult{}, errors.New("Codex result summary and next action are required")
	}
	if result.ChangedFiles == nil || result.Checks == nil {
		return ProviderResult{}, errors.New("Codex result changed_files and checks must be arrays")
	}
	if result.Status == "blocked" && strings.TrimSpace(result.Blocker) == "" {
		return ProviderResult{}, errors.New("blocked Codex result must name a blocker")
	}
	// Strict structured output cannot carry an open-ended object. The provider
	// transports the TLC document as JSON text; durable results still contain
	// the original object, including fields owned by future workflow versions.
	envelope := bytes.TrimSpace(result.TLCEnvelope)
	if bytes.Equal(envelope, []byte("null")) {
		result.TLCEnvelope = nil
	} else if len(envelope) > 0 {
		if envelope[0] == '"' {
			var document string
			if err := json.Unmarshal(envelope, &document); err != nil {
				return ProviderResult{}, fmt.Errorf("decode TLC envelope text: %w", err)
			}
			envelope = bytes.TrimSpace([]byte(document))
		}
		if len(envelope) == 0 || envelope[0] != '{' || !json.Valid(envelope) {
			return ProviderResult{}, errors.New("TLC envelope must contain one JSON object")
		}
		result.TLCEnvelope = envelope
	}
	return result, nil
}

func boundedStderr(stderr string) string {
	stderr = strings.TrimSpace(stderr)
	if stderr == "" {
		return ""
	}
	return ": " + stderr
}

type boundedBuffer struct {
	buffer    bytes.Buffer
	remaining int
	overflow  bool
}

func newBoundedBuffer(limit int) *boundedBuffer { return &boundedBuffer{remaining: limit} }

func (b *boundedBuffer) Write(p []byte) (int, error) {
	written := len(p)
	if len(p) > b.remaining {
		p = p[:b.remaining]
		b.overflow = true
	}
	b.remaining -= len(p)
	_, _ = b.buffer.Write(p)
	return written, nil
}

func (b *boundedBuffer) String() string   { return b.buffer.String() }
func (b *boundedBuffer) Overflowed() bool { return b.overflow }

var providerResultSchema = []byte(`{
  "$schema":"https://json-schema.org/draft/2020-12/schema",
  "type":"object",
  "additionalProperties":false,
  "required":["status","summary","tlc_envelope","changed_files","checks","review","blocker","next_action"],
  "properties":{
    "status":{"enum":["passed","blocked"]},
    "summary":{"type":"string","minLength":1},
    "tlc_envelope":{"type":["string","null"],"description":"For route only: JSON text encoding the complete tlc-envelope/v1 object with schema_version, workflow (name and version), route, and brief (outcome, scope, non_goals, assumptions, constraints, tests, next_action). brief.scope, brief.non_goals, brief.assumptions, brief.constraints, and brief.tests are arrays of strings, including empty arrays. brief.outcome and brief.next_action are strings. Preserve additional workflow fields. For other operations return null."},
    "changed_files":{"type":"array","items":{"type":"string"}},
    "checks":{"type":"array","items":{"type":"object","additionalProperties":false,"required":["name","status","summary"],"properties":{"name":{"type":"string"},"status":{"type":"string"},"summary":{"type":"string"}}}},
    "review":{"type":["object","null"],"additionalProperties":false,"required":["status","summary","findings"],"properties":{"status":{"type":"string"},"summary":{"type":"string"},"findings":{"type":"array","items":{"type":"string"}}}},
    "blocker":{"type":"string"},
    "next_action":{"type":"string","minLength":1}
  }
}`)

func codexSupportsMaxEffort(model string) bool {
	switch model {
	case "gpt-6-astra", "gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna":
		return true
	default:
		return false
	}
}
