// civilization-worker executes one Platform-approved attempt. It has no Hive
// database, signing key, GitHub credential, or container-launching capability.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/transpara-ai/hive/pkg/hive/civilization"
)

type input struct {
	Selection civilization.ExecutionSelection    `json:"selection"`
	Operation civilization.ProviderOperation     `json:"operation"`
	AttemptID string                             `json:"attempt_id"`
	Prompt    string                             `json:"prompt"`
	Commands  []civilization.VerificationCommand `json:"commands"`
}

type output struct {
	Result *civilization.ProviderResult `json:"result,omitempty"`
	Error  string                       `json:"error,omitempty"`
}

func main() {
	result, err := run()
	out := output{Result: result}
	if err != nil {
		out.Error = err.Error()
	}
	failed := out.Error != ""
	raw, err := json.Marshal(out)
	if err != nil {
		os.Exit(1)
	}
	if err = os.WriteFile("/job/output/result.json", raw, 0600); err != nil {
		os.Exit(1)
	}
	if failed {
		os.Exit(1)
	}
}

func fileDigest(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func run() (*civilization.ProviderResult, error) {
	raw, err := os.ReadFile("/job/input.json")
	if err != nil {
		return nil, err
	}
	var request input
	if err = json.Unmarshal(raw, &request); err != nil {
		return nil, err
	}
	root, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll("/tmp/home", 0700); err != nil {
		return nil, err
	}
	if err = os.WriteFile("/tmp/home/.gitconfig", []byte("[safe]\n\tdirectory = "+root+"\n[core]\n\thooksPath = /dev/null\n"), 0600); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	if request.Operation == "verify" {
		if len(request.Commands) == 0 {
			return nil, fmt.Errorf("no approved verification commands")
		}
		checks := []civilization.CheckResult{}
		for _, check := range request.Commands {
			if len(check.Args) == 0 {
				return nil, fmt.Errorf("empty verification command")
			}
			cmd := exec.CommandContext(ctx, check.Args[0], check.Args[1:]...)
			cmd.Dir = root
			// Verification has no credentials. Keep a bounded diagnostic tail so a
			// failed native check is actionable without retaining unbounded output.
			log := &diagnosticTail{}
			cmd.Stdout, cmd.Stderr = log, log
			if err = cmd.Run(); err != nil {
				return nil, fmt.Errorf("verification %q failed: %w: %s", check.Name, err, log.String())
			}
			checks = append(checks, civilization.CheckResult{Name: check.Name, Status: "passed", Summary: "Isolated command exited successfully."})
		}
		return &civilization.ProviderResult{Status: "passed", Summary: "Isolated verification passed.", Checks: checks, ChangedFiles: []string{}}, nil
	}
	// Platform mounts a dedicated CODEX_HOME authenticated natively on the
	// runner host. Codex owns its login and refresh lifecycle; this worker never
	// copies or promotes tokens between environments.
	auth, err := os.Lstat(filepath.Join(os.Getenv("CODEX_HOME"), "auth.json"))
	if err != nil || !auth.Mode().IsRegular() {
		return nil, fmt.Errorf("native provider login unavailable")
	}
	requirements := "/etc/codex/requirements.toml"
	executable := "/usr/local/bin/codex"
	executableSHA, err := fileDigest(executable)
	if err != nil {
		return nil, err
	}
	requirementsSHA, err := fileDigest(requirements)
	if err != nil {
		return nil, err
	}
	provider, err := civilization.NewCodexCLI(civilization.CodexCLIConfig{
		Executable: executable, ExecutableSHA256: executableSHA,
		ManagedRequirementsFile: requirements, ManagedRequirementsSHA256: requirementsSHA,
		Timeout: 29 * time.Minute, OutputLimitBytes: 2 * 1024 * 1024,
		EnvironmentKeys:  []string{"PATH", "HOME", "CODEX_HOME", "SSL_CERT_FILE", "SSL_CERT_DIR"},
		ReceiptDirectory: "/var/lib/civilization/receipts",
	})
	if err != nil {
		return nil, err
	}
	result, err := provider.Run(ctx, civilization.ProviderRequest{Selection: request.Selection, Operation: request.Operation, AttemptID: request.AttemptID, RepositoryRoot: root, Prompt: request.Prompt})
	return &result, err
}

type diagnosticTail struct {
	mu   sync.Mutex
	data []byte
}

func (d *diagnosticTail) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(p)
	d.data = append(d.data, p...)
	if len(d.data) > 16384 {
		d.data = append([]byte(nil), d.data[len(d.data)-16384:]...)
	}
	return n, nil
}
func (d *diagnosticTail) String() string { d.mu.Lock(); defer d.mu.Unlock(); return string(d.data) }
