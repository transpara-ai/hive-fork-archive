package civilization

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunnerArtifactRecoveryAndTampering(t *testing.T) {
	for _, scenario := range []string{"apply", "already_applied", "tampered_evidence", "changed_workspace", "wrong_assignment"} {
		t.Run(scenario, func(t *testing.T) {
			root := testRepositoryWithOrigin(t)
			base, initial, err := runnerSnapshot(context.Background(), root)
			if err != nil {
				t.Fatal(err)
			}
			if initial != "" {
				t.Fatal("test repository must be clean")
			}
			if err = os.WriteFile(filepath.Join(root, "runner-artifact.txt"), []byte("bounded artifact\n"), 0600); err != nil {
				t.Fatal(err)
			}
			_, patch, err := runnerSnapshot(context.Background(), root)
			if err != nil {
				t.Fatal(err)
			}
			os.Remove(filepath.Join(root, "runner-artifact.txt"))
			request := ProviderRequest{WorkID: "work-" + strings.Repeat("b", 24), Repository: "transpara-ai/hive", BaseSHA: base, Operation: OperationImplement, AttemptID: strings.Repeat("a", 64), RepositoryRoot: root, Prompt: "implement", Selection: ExecutionSelection{Provider: "codex"}}
			job := runnerJob{State: "succeeded", Request: runnerRequest{Version: 1, AttemptID: request.AttemptID, WorkID: request.WorkID, Repository: request.Repository, Operation: "implement", BaseSHA: base, Prompt: request.Prompt, Selection: request.Selection}, Patch: patch, Result: ProviderResult{Status: "passed"}, Evidence: &RunnerEvidence{AttemptID: request.AttemptID, Operation: "implement", Image: "sha256:" + strings.Repeat("c", 64), PolicySHA256: strings.Repeat("d", 64), InputSHA256: patchDigest(""), OutputSHA256: patchDigest(patch)}}
			switch scenario {
			case "already_applied":
				os.WriteFile(filepath.Join(root, "runner-artifact.txt"), []byte("bounded artifact\n"), 0600)
			case "tampered_evidence":
				job.Evidence.OutputSHA256 = patchDigest("forged")
			case "changed_workspace":
				os.WriteFile(filepath.Join(root, "unrelated.txt"), []byte("human work\n"), 0600)
			case "wrong_assignment":
				job.Request.WorkID = "work-" + strings.Repeat("e", 24)
			}
			posts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					posts++
				}
				json.NewEncoder(w).Encode(job)
			}))
			defer server.Close()
			provider, err := NewRunnerProvider(server.URL, strings.Repeat("t", 32))
			if err != nil {
				t.Fatal(err)
			}
			result, err := provider.Run(context.Background(), request)
			if scenario == "apply" || scenario == "already_applied" {
				if err != nil {
					t.Fatal(err)
				}
				if result.Runner == nil {
					t.Fatal("runner evidence lost")
				}
				_, actual, err := runnerSnapshot(context.Background(), root)
				if err != nil || actual != patch {
					t.Fatal("artifact not applied exactly", err)
				}
			} else if err == nil {
				t.Fatal("tampered or stale result accepted")
			}
			if posts != 0 {
				t.Fatal("recovery must fetch existing attempt before submitting")
			}
		})
	}
}

func TestRunnerOutageIsPendingWithoutLocalFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		w.Write([]byte(`{"error":"unavailable"}`))
	}))
	defer server.Close()
	provider, _ := NewRunnerProvider(server.URL, strings.Repeat("t", 32))
	_, err := provider.Run(context.Background(), ProviderRequest{WorkID: "work-" + strings.Repeat("b", 24), Repository: "transpara-ai/hive", Operation: OperationRoute, AttemptID: strings.Repeat("a", 64), RepositoryRoot: t.TempDir()})
	if !errors.Is(err, ErrRunnerPending) {
		t.Fatalf("outage must retain same attempt: %v", err)
	}
}

func TestRunnerRejectsUnsafeEndpoint(t *testing.T) {
	for _, endpoint := range []string{"http://public.example", "http://user:password@localhost", "http://localhost?token=x", "file:///run/docker.sock"} {
		if _, err := NewRunnerProvider(endpoint, strings.Repeat("t", 32)); err == nil {
			t.Fatalf("accepted %s", endpoint)
		}
	}
}

func TestRunnerPermanentRejectionRequiresRepair(t *testing.T) {
	for _, status := range []int{400, 401, 403} {
		err := classifyRunnerError(status, errors.New("rejected"))
		if errors.Is(err, ErrRunnerPending) {
			t.Fatalf("HTTP %d must surface a repairable error", status)
		}
	}
	for _, status := range []int{0, 409, 503} {
		if !errors.Is(classifyRunnerError(status, errors.New("retry")), ErrRunnerPending) {
			t.Fatalf("HTTP %d must retain the attempt", status)
		}
	}
}
