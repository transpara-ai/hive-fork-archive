package civilization

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPHandlerNaturalLanguageJourneyAndAuth(t *testing.T) {
	engine, _, _ := newTestEngine(t, "Routine", false)
	handler, err := NewHTTPHandler(HTTPConfig{Engine: engine, APIKey: "test-secret"})
	if err != nil {
		t.Fatal(err)
	}

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/civilization/v1/work", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}
	rawToken := httptest.NewRequest(http.MethodGet, "/api/civilization/v1/work", nil)
	rawToken.Header.Set("Authorization", "test-secret")
	rawTokenResponse := httptest.NewRecorder()
	handler.ServeHTTP(rawTokenResponse, rawToken)
	if rawTokenResponse.Code != http.StatusUnauthorized {
		t.Fatalf("raw token status = %d, want %d", rawTokenResponse.Code, http.StatusUnauthorized)
	}

	body := []byte(`{"source_kind":"human","source_identity":"human:http-test","repository":"transpara-ai/hive","text":"Improve the operator experience."}`)
	intake := apiRequest(t, handler, http.MethodPost, "/api/civilization/v1/intake", body)
	if intake.Code != http.StatusAccepted {
		t.Fatalf("intake status=%d body=%s", intake.Code, intake.Body.String())
	}
	var accepted WorkProjection
	if err := json.Unmarshal(intake.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.State != StateRouting || accepted.WorkID == "" {
		t.Fatalf("accepted = %+v", accepted)
	}

	run := apiRequest(t, handler, http.MethodPost, "/api/civilization/v1/work/"+accepted.WorkID+"/run", nil)
	if run.Code != http.StatusOK {
		t.Fatalf("run status=%d body=%s", run.Code, run.Body.String())
	}
	var ready WorkProjection
	if err := json.Unmarshal(run.Body.Bytes(), &ready); err != nil {
		t.Fatal(err)
	}
	if ready.State != StateAwaitingConfirmation {
		t.Fatalf("ready = %+v", ready)
	}
	confirmation, _ := json.Marshal(map[string]string{"brief_id": ready.Bound.IdempotencyKey})
	confirmed := apiRequest(t, handler, http.MethodPost, "/api/civilization/v1/work/"+accepted.WorkID+"/confirm", confirmation)
	if confirmed.Code != http.StatusAccepted {
		t.Fatalf("confirm: %s", confirmed.Body.String())
	}
	run = apiRequest(t, handler, http.MethodPost, "/api/civilization/v1/work/"+accepted.WorkID+"/run", nil)
	if err := json.Unmarshal(run.Body.Bytes(), &ready); err != nil || ready.State != StateReady {
		t.Fatalf("run after confirmation: %s", run.Body.String())
	}

	list := apiRequest(t, handler, http.MethodGet, "/api/civilization/v1/work", nil)
	if list.Code != http.StatusOK || !bytes.Contains(list.Body.Bytes(), []byte(accepted.WorkID)) {
		t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
	}

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status = %d", health.Code)
	}
}

func apiRequest(t *testing.T, handler http.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer test-secret")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestRunnerReadinessFailureKeepsWorkInspectable(t *testing.T) {
	engine, _, _ := newTestEngine(t, "Routine", false)
	available := false
	handler, err := NewHTTPHandler(HTTPConfig{Engine: engine, APIKey: "test-secret", Readiness: func(context.Context) error {
		if !available {
			return context.DeadlineExceeded
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/readyz", "/healthz", "/api/civilization/v1/work"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer test-secret")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		expected := http.StatusOK
		if path == "/readyz" {
			expected = http.StatusServiceUnavailable
		}
		if response.Code != expected {
			t.Fatalf("%s: got %d want %d", path, response.Code, expected)
		}
	}
	available = true
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusOK {
		t.Fatal("readiness did not recover")
	}
}
