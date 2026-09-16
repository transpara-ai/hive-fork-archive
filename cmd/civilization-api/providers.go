package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/transpara-ai/hive/pkg/hive/civilization"
)

func configuredProviders(codexPath, codexDigest string) (*civilization.ProviderRouter, error) {
	router := &civilization.ProviderRouter{DefaultProvider: envOr("CIVILIZATION_PROVIDER", "codex"), Hosts: map[string]civilization.ProviderHost{}}
	if endpoint := requiredEnvValue("CIVILIZATION_RUNNER_URL"); endpoint != "" {
		provider, err := configuredRunner()
		if err != nil {
			return nil, err
		}
		router.Hosts["codex"] = civilization.ProviderHost{Provider: provider, DefaultModel: requiredEnvValue("CIVILIZATION_CODEX_MODEL")}
		if _, err := router.Resolve(civilization.ExecutionSelection{}); err != nil {
			return nil, err
		}
		return router, nil
	}
	receiptDir := requiredEnvValue("CIVILIZATION_RECEIPT_DIR")
	if receiptDir == "" {
		return nil, fmt.Errorf("CIVILIZATION_RECEIPT_DIR is required")
	}
	if codexPath != "" {
		provider, err := civilization.NewCodexCLI(civilization.CodexCLIConfig{
			Executable: codexPath, ExecutableSHA256: codexDigest,
			ManagedRequirementsFile:   requiredEnvValue("CIVILIZATION_CODEX_REQUIREMENTS_FILE"),
			ManagedRequirementsSHA256: requiredEnvValue("CIVILIZATION_CODEX_REQUIREMENTS_SHA256"),
			Profile:                   os.Getenv("CIVILIZATION_CODEX_PROFILE"), Timeout: durationEnv("CIVILIZATION_CODEX_TIMEOUT", 30*time.Minute),
			OutputLimitBytes: intEnv("CIVILIZATION_COMMAND_OUTPUT_LIMIT", 2*1024*1024),
			EnvironmentKeys:  []string{"PATH", "HOME", "CODEX_HOME", "OPENAI_API_KEY", "SSL_CERT_FILE", "SSL_CERT_DIR"},
			ReceiptDirectory: receiptDir,
		})
		if err != nil {
			return nil, fmt.Errorf("configure Codex provider: %w", err)
		}
		router.Hosts["codex"] = civilization.ProviderHost{Provider: provider, DefaultModel: requiredEnvValue("CIVILIZATION_CODEX_MODEL")}
	}
	if path := requiredEnvValue("CIVILIZATION_CLAUDE_PATH"); path != "" {
		provider, err := civilization.NewClaudeCLI(civilization.ClaudeCLIConfig{
			Executable: path, ExecutableSHA256: requiredEnvValue("CIVILIZATION_CLAUDE_SHA256"),
			SettingsFile: requiredEnvValue("CIVILIZATION_CLAUDE_SETTINGS_FILE"), SettingsSHA256: requiredEnvValue("CIVILIZATION_CLAUDE_SETTINGS_SHA256"),
			Timeout: durationEnv("CIVILIZATION_CLAUDE_TIMEOUT", 30*time.Minute), OutputLimitBytes: intEnv("CIVILIZATION_COMMAND_OUTPUT_LIMIT", 2*1024*1024),
			EnvironmentKeys:  []string{"PATH", "HOME", "CLAUDE_CONFIG_DIR", "ANTHROPIC_API_KEY", "SSL_CERT_FILE", "SSL_CERT_DIR"},
			ReceiptDirectory: filepath.Join(receiptDir, "claude"),
		})
		if err != nil {
			return nil, fmt.Errorf("configure Claude provider: %w", err)
		}
		router.Hosts["claude"] = civilization.ProviderHost{Provider: provider, DefaultModel: requiredEnvValue("CIVILIZATION_CLAUDE_MODEL")}
	}
	if _, err := router.Resolve(civilization.ExecutionSelection{}); err != nil {
		return nil, err
	}
	return router, nil
}

func configuredRunner() (*civilization.RunnerProvider, error) {
	endpoint := requiredEnvValue("CIVILIZATION_RUNNER_URL")
	if endpoint == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(requiredEnvValue("CIVILIZATION_RUNNER_TOKEN_FILE"))
	if err != nil {
		return nil, fmt.Errorf("read runner credential: %w", err)
	}
	return civilization.NewRunnerProvider(endpoint, strings.TrimSpace(string(raw)))
}
