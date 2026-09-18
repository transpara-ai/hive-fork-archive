package factoryv1

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
	"strings"
	"time"
)

const (
	tlcSchemaRelativePath = "schemas/tlc-change-continuation-v1.schema.json"
	tlcSkillRelativePath  = "skills/tlc-change-workflow/SKILL.md"
	tlcCoreRelativePath   = "skills/tlc-change-workflow/scripts/contract_core.py"
	maxTLCWorkflowOutput  = 16 << 20
)

// CommandTLCChangeWorkflowRunner is Hive's concrete installed-plugin runner.
// It authenticates source bytes against adapter-binding.json, then invokes one
// exact executable directly (never through a shell and never through legacy
// tlc-v1 fallback) with invocation JSON on stdin.
type CommandTLCChangeWorkflowRunner struct {
	pluginRoot string
	executable string
	args       []string
	timeout    time.Duration
}

func NewCommandTLCChangeWorkflowRunner(pluginRoot, executable string, args []string, timeout time.Duration) (*CommandTLCChangeWorkflowRunner, error) {
	if !filepath.IsAbs(pluginRoot) || !filepath.IsAbs(executable) {
		return nil, errors.New("TLC plugin root and runner executable must be absolute paths")
	}
	if timeout <= 0 {
		return nil, errors.New("TLC workflow runner timeout must be positive")
	}
	info, err := os.Lstat(executable)
	if err != nil {
		return nil, fmt.Errorf("stat TLC workflow runner executable: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return nil, errors.New("TLC workflow runner executable must be a regular executable file, not a symlink")
	}
	for _, arg := range args {
		if strings.IndexByte(arg, 0) >= 0 {
			return nil, errors.New("TLC workflow runner argument contains NUL")
		}
	}
	return &CommandTLCChangeWorkflowRunner{
		pluginRoot: filepath.Clean(pluginRoot), executable: executable,
		args: append([]string(nil), args...), timeout: timeout,
	}, nil
}

func (r *CommandTLCChangeWorkflowRunner) ObservedIdentity(ctx context.Context) (TLCWorkflowIdentity, error) {
	if err := ctx.Err(); err != nil {
		return TLCWorkflowIdentity{}, err
	}
	rootInfo, err := os.Lstat(r.pluginRoot)
	if err != nil {
		return TLCWorkflowIdentity{}, fmt.Errorf("stat installed TLC plugin root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return TLCWorkflowIdentity{}, errors.New("installed TLC plugin root must be a directory, not a symlink")
	}
	manifest, err := readJSONObject(filepath.Join(r.pluginRoot, ".codex-plugin", "plugin.json"))
	if err != nil {
		return TLCWorkflowIdentity{}, fmt.Errorf("read installed TLC plugin manifest: %w", err)
	}
	binding, err := readJSONObject(filepath.Join(r.pluginRoot, "adapter-binding.json"))
	if err != nil {
		return TLCWorkflowIdentity{}, fmt.Errorf("read installed TLC adapter binding: %w", err)
	}
	adapter, ok := binding["adapter"].(map[string]any)
	if !ok {
		return TLCWorkflowIdentity{}, errors.New("installed TLC adapter binding has no adapter identity")
	}
	name, _ := manifest["name"].(string)
	version, _ := manifest["version"].(string)
	if adapter["name"] != name || adapter["version"] != version {
		return TLCWorkflowIdentity{}, errors.New("installed TLC manifest and adapter binding identities differ")
	}
	payload, ok := binding["payload"].(map[string]any)
	if !ok {
		return TLCWorkflowIdentity{}, errors.New("installed TLC adapter binding has no payload")
	}
	records, ok := payload["files"].([]any)
	if !ok {
		return TLCWorkflowIdentity{}, errors.New("installed TLC adapter binding payload has no files")
	}
	bound := make(map[string]string, len(records))
	for _, raw := range records {
		record, ok := raw.(map[string]any)
		if !ok {
			return TLCWorkflowIdentity{}, errors.New("installed TLC adapter binding has malformed file record")
		}
		path, pathOK := record["path"].(string)
		digest, digestOK := record["sha256"].(string)
		if !pathOK || !digestOK {
			return TLCWorkflowIdentity{}, errors.New("installed TLC adapter binding has malformed file identity")
		}
		bound[path] = digest
	}
	schemaSHA, err := verifyBoundPluginFile(r.pluginRoot, tlcSchemaRelativePath, bound[tlcSchemaRelativePath])
	if err != nil {
		return TLCWorkflowIdentity{}, err
	}
	skillSHA, err := verifyBoundPluginFile(r.pluginRoot, tlcSkillRelativePath, bound[tlcSkillRelativePath])
	if err != nil {
		return TLCWorkflowIdentity{}, err
	}
	coreSHA, err := verifyBoundPluginFile(r.pluginRoot, tlcCoreRelativePath, bound[tlcCoreRelativePath])
	if err != nil {
		return TLCWorkflowIdentity{}, err
	}
	runnerSHA, err := sha256RegularFile(r.executable, "TLC workflow runner executable")
	if err != nil {
		return TLCWorkflowIdentity{}, err
	}
	argvJSON, err := json.Marshal(append([]string{r.executable}, r.args...))
	if err != nil {
		return TLCWorkflowIdentity{}, fmt.Errorf("encode TLC workflow runner argv identity: %w", err)
	}
	argvDigest := sha256.Sum256(argvJSON)
	return TLCWorkflowIdentity{
		PluginName: name, PluginVersion: version, SkillName: TLCChangeWorkflowSkill,
		ContractVersion: TLCContinuationVersion, SchemaSHA256: schemaSHA, SkillSHA256: skillSHA,
		ContractCoreSHA256: coreSHA, RunnerSHA256: runnerSHA,
		RunnerArgvSHA256: hex.EncodeToString(argvDigest[:]),
	}, nil
}

func (r *CommandTLCChangeWorkflowRunner) Evaluate(ctx context.Context, invocationJSON []byte) ([]byte, error) {
	if len(invocationJSON) == 0 || len(invocationJSON) > maxTLCWorkflowOutput {
		return nil, errors.New("TLC workflow invocation size is empty or exceeds the bounded limit")
	}
	runCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, r.executable, r.args...)
	cmd.Stdin = bytes.NewReader(invocationJSON)
	var stdout boundedBuffer
	var stderr boundedBuffer
	stdout.limit = maxTLCWorkflowOutput
	stderr.limit = 64 << 10
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("TLC workflow runner timed out after %s", r.timeout)
		}
		return nil, fmt.Errorf("TLC workflow runner failed: %w%s", err, stderr.suffix())
	}
	if stdout.exceeded {
		return nil, errors.New("TLC workflow runner stdout exceeded the bounded limit")
	}
	return append([]byte(nil), stdout.Bytes()...), nil
}

type boundedBuffer struct {
	bytes.Buffer
	limit    int
	exceeded bool
}

func (b *boundedBuffer) Write(value []byte) (int, error) {
	if b.limit <= 0 {
		b.exceeded = true
		return len(value), nil
	}
	remaining := b.limit - b.Len()
	if remaining <= 0 {
		b.exceeded = true
		return len(value), nil
	}
	written := len(value)
	if len(value) > remaining {
		value = value[:remaining]
		b.exceeded = true
	}
	_, _ = b.Buffer.Write(value)
	return written, nil
}

func (b *boundedBuffer) suffix() string {
	text := strings.TrimSpace(b.String())
	if text == "" {
		return ""
	}
	return ": " + text
}

func readJSONObject(path string) (map[string]any, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, errors.New("identity file must be regular and not a symlink")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	decoder := json.NewDecoder(bytes.NewReader(content))
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("JSON object required")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("identity file has trailing JSON")
	}
	return result, nil
}

func verifyBoundPluginFile(root, relative, expected string) (string, error) {
	if expected == "" {
		return "", fmt.Errorf("installed TLC adapter binding omits %s", relative)
	}
	path := filepath.Join(root, filepath.FromSlash(relative))
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("stat installed TLC payload %s: %w", relative, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", fmt.Errorf("installed TLC payload %s must be regular and not a symlink", relative)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(content)
	observed := hex.EncodeToString(digest[:])
	if observed != expected {
		return "", fmt.Errorf("installed TLC payload %s does not match adapter binding", relative)
	}
	return observed, nil
}

func sha256RegularFile(path, label string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("stat %s: %w", label, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s must be regular and not a symlink", label)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", label, err)
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", fmt.Errorf("hash %s: %w", label, err)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
