package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/transpara-ai/eventgraph/go/pkg/decision"
	"github.com/transpara-ai/eventgraph/go/pkg/event"
	"github.com/transpara-ai/eventgraph/go/pkg/intelligence"
	"github.com/transpara-ai/eventgraph/go/pkg/modelconfig"
)

// fakeBudgetProvider satisfies intelligence.Provider for tests so no
// subprocess or network call is made. It records the MaxBudgetUSD it was
// constructed with.
type fakeBudgetProvider struct {
	provider     string
	model        string
	maxBudgetUSD float64
}

func (f *fakeBudgetProvider) Name() string  { return f.provider }
func (f *fakeBudgetProvider) Model() string { return f.model }
func (f *fakeBudgetProvider) Reason(_ context.Context, _ string, _ []event.Event) (decision.Response, error) {
	return decision.Response{}, nil
}

type logCaptureMain struct {
	original io.Writer
	buf      *bytes.Buffer
}

func captureLogMain(t *testing.T) *logCaptureMain {
	t.Helper()
	buf := &bytes.Buffer{}
	orig := log.Writer()
	log.SetOutput(buf)
	return &logCaptureMain{original: orig, buf: buf}
}

func (l *logCaptureMain) String() string { return l.buf.String() }
func (l *logCaptureMain) restore()       { log.SetOutput(l.original) }

func writeMinimalCatalog(t *testing.T) string {
	t.Helper()
	path := t.TempDir() + "/catalog.yaml"
	body := `models:
  - id: claude-sonnet-4-6
    aliases: [sonnet]
    provider: claude-cli
    auth_mode: subscription
    tier: execution
    capabilities: [tools, reasoning]
    context_window: 200000
    max_output_tokens: 8192
    pricing:
      input_per_million: 3.00
      output_per_million: 15.00

tier_defaults:
  judgment: sonnet
  execution: sonnet
  volume: sonnet
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestResolveSiteOpsAPIKey(t *testing.T) {
	tests := []struct {
		name         string
		siteOpsKey   string
		transparaKey string
		want         string
	}{
		{name: "dedicated credential wins", siteOpsKey: "site-ops", transparaKey: "webhook", want: "site-ops"},
		{name: "legacy credential remains compatible", transparaKey: "legacy", want: "legacy"},
		{name: "no credential", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveSiteOpsAPIKey(tt.siteOpsKey, tt.transparaKey); got != tt.want {
				t.Fatalf("resolveSiteOpsAPIKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildCouncilResolver_CatalogPrecedence(t *testing.T) {
	tests := []struct {
		name           string
		catalogPath    string
		envValue       string
		wantErr        bool
		logMustContain string
	}{
		{name: "no catalog, no env: default resolver", catalogPath: "", envValue: "", wantErr: false, logMustContain: ""},
		{name: "env set, no catalog: legacy mode logged", catalogPath: "", envValue: "sonnet", wantErr: false, logMustContain: "legacy mode: COUNCIL_MODEL=sonnet"},
		{name: "unknown COUNCIL_MODEL: error", catalogPath: "", envValue: "not-a-real-model", wantErr: true, logMustContain: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				t.Setenv("COUNCIL_MODEL", tt.envValue)
			} else {
				os.Unsetenv("COUNCIL_MODEL")
			}
			logBuf := captureLogMain(t)
			defer logBuf.restore()

			_, err := buildCouncilResolver(tt.catalogPath)
			if (err != nil) != tt.wantErr {
				t.Fatalf("buildCouncilResolver: err=%v wantErr=%v", err, tt.wantErr)
			}
			if tt.logMustContain != "" && !strings.Contains(logBuf.String(), tt.logMustContain) {
				t.Errorf("log missing %q; got: %s", tt.logMustContain, logBuf.String())
			}
		})
	}

	t.Run("council provider builder threads budget", func(t *testing.T) {
		// Verify the council pool, when built with councilProviderBuilder, threads
		// --budget into every constructed provider. This guards against the
		// regression where modelconfig.ToIntelligenceConfig produces configs with
		// MaxBudgetUSD=0 and claude-cli falls back to its $1/call default.
		os.Unsetenv("COUNCIL_MODEL")
		resolver, err := buildCouncilResolver("")
		if err != nil {
			t.Fatalf("buildCouncilResolver: %v", err)
		}

		const wantBudget = 10.0
		var (
			mu          sync.Mutex
			seenConfigs []intelligence.Config
		)
		recordingBuilder := func(cfg intelligence.Config) (intelligence.Provider, error) {
			mu.Lock()
			seenConfigs = append(seenConfigs, cfg)
			mu.Unlock()
			return &fakeBudgetProvider{
				provider:     cfg.Provider,
				model:        cfg.Model,
				maxBudgetUSD: cfg.MaxBudgetUSD,
			}, nil
		}

		pool := modelconfig.NewProviderPoolWithBuilder(
			resolver,
			councilProviderBuilder(recordingBuilder, wantBudget),
		)

		provider, err := pool.For("planner")
		if err != nil {
			t.Fatalf("pool.For(planner): %v", err)
		}
		fp, ok := provider.(*fakeBudgetProvider)
		if !ok {
			t.Fatalf("provider type: got %T, want *fakeBudgetProvider", provider)
		}
		if fp.maxBudgetUSD != wantBudget {
			t.Errorf("provider MaxBudgetUSD: got %v, want %v", fp.maxBudgetUSD, wantBudget)
		}

		mu.Lock()
		defer mu.Unlock()
		if len(seenConfigs) == 0 {
			t.Fatal("recordingBuilder was never called")
		}
		for i, cfg := range seenConfigs {
			if cfg.MaxBudgetUSD != wantBudget {
				t.Errorf("seenConfigs[%d].MaxBudgetUSD: got %v, want %v", i, cfg.MaxBudgetUSD, wantBudget)
			}
		}
	})

	// Separate subtest for catalog + env coexistence; uses a real temp catalog file.
	t.Run("catalog and env: env ignored, note logged", func(t *testing.T) {
		catalogPath := writeMinimalCatalog(t)
		t.Setenv("COUNCIL_MODEL", "sonnet")
		logBuf := captureLogMain(t)
		defer logBuf.restore()

		if _, err := buildCouncilResolver(catalogPath); err != nil {
			t.Fatalf("buildCouncilResolver: %v", err)
		}
		if !strings.Contains(logBuf.String(), "COUNCIL_MODEL=sonnet env var ignored") {
			t.Errorf("log missing the env-ignored note; got: %s", logBuf.String())
		}
	})
}
