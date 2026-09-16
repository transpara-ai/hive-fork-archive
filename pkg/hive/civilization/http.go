package civilization

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/transpara-ai/hive/pkg/hive/tlcbridge"
)

type HTTPConfig struct {
	Readiness            func(context.Context) error
	RequireHumanIdentity bool
	Engine               *Engine
	APIKey               string
	MaxBodyBytes         int64
}

type HTTPHandler struct {
	readiness            func(context.Context) error
	requireHumanIdentity bool
	engine               *Engine
	apiKey               string
	maxBodyBytes         int64
	mux                  *http.ServeMux
}

func NewHTTPHandler(config HTTPConfig) (*HTTPHandler, error) {
	if config.Engine == nil {
		return nil, errors.New("Civilization HTTP API requires an engine")
	}
	if strings.TrimSpace(config.APIKey) == "" {
		return nil, errors.New("Civilization HTTP API requires a bearer token")
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = 256 * 1024
	}
	handler := &HTTPHandler{engine: config.Engine, apiKey: config.APIKey, maxBodyBytes: config.MaxBodyBytes, mux: http.NewServeMux()}
	handler.requireHumanIdentity = config.RequireHumanIdentity
	handler.readiness = config.Readiness
	handler.mux.HandleFunc("GET /healthz", handler.health)
	handler.mux.HandleFunc("GET /readyz", handler.ready)
	handler.mux.HandleFunc("GET /api/civilization/v1/work", handler.list)
	handler.mux.HandleFunc("GET /api/civilization/v1/work/{workID}", handler.get)
	handler.mux.HandleFunc("POST /api/civilization/v1/intake", handler.intake)
	handler.mux.HandleFunc("POST /api/civilization/v1/work/{workID}/run", handler.run)
	handler.mux.HandleFunc("POST /api/civilization/v1/work/{workID}/confirm", handler.confirm)
	handler.mux.HandleFunc("POST /api/civilization/v1/work/{workID}/result-review", handler.reviewResult)
	handler.mux.HandleFunc("POST /api/civilization/v1/work/{workID}/human-owner", handler.assignHumanOwner)
	handler.mux.HandleFunc("GET /api/civilization/v1/work/{workID}/artifact", handler.artifact)
	handler.mux.HandleFunc("POST /api/civilization/v1/work/{workID}/interventions/{interventionID}/resolve", handler.resolve)
	return handler, nil
}

func (h *HTTPHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	if request.URL.Path != "/healthz" && request.URL.Path != "/readyz" && !h.authorized(request) {
		response.Header().Set("WWW-Authenticate", `Bearer realm="civilization"`)
		writeAPIError(response, http.StatusUnauthorized, "unauthorized")
		return
	}
	if h.requireHumanIdentity && request.Method != http.MethodGet && request.Method != http.MethodHead {
		actor, role := request.Header.Get("X-Civilization-Actor"), request.Header.Get("X-Civilization-Role")
		if !humanActionAllowed(actor, role, request.Method, request.URL.Path) {
			writeAPIError(response, http.StatusForbidden, "a named operator with permission for this action is required")
			return
		}
		request = request.WithContext(context.WithValue(request.Context(), humanIdentityKey{}, actor))
	}
	h.mux.ServeHTTP(response, request)
}

func (h *HTTPHandler) authorized(request *http.Request) bool {
	value, found := strings.CutPrefix(request.Header.Get("Authorization"), "Bearer ")
	if !found || value == "" || value != strings.TrimSpace(value) || len(value) != len(h.apiKey) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(value), []byte(h.apiKey)) == 1
}

func (h *HTTPHandler) health(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok", "service": "civilization"})
}

func (h *HTTPHandler) ready(response http.ResponseWriter, request *http.Request) {
	if h.readiness != nil {
		if err := h.readiness(request.Context()); err != nil {
			writeAPIError(response, http.StatusServiceUnavailable, "worker runner unavailable")
			return
		}
	}
	if _, err := h.engine.List(request.Context()); err != nil {
		writeAPIError(response, http.StatusServiceUnavailable, "event store unavailable")
		return
	}
	writeJSON(response, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *HTTPHandler) list(response http.ResponseWriter, request *http.Request) {
	items, err := h.engine.List(request.Context())
	if err != nil {
		writeAPIError(response, http.StatusInternalServerError, "list work failed")
		return
	}
	for i := range items {
		items[i].Artifact = nil
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func (h *HTTPHandler) get(response http.ResponseWriter, request *http.Request) {
	item, err := h.engine.Get(request.Context(), request.PathValue("workID"))
	if err != nil {
		writeAPIError(response, http.StatusNotFound, "work item not found")
		return
	}
	writeJSON(response, http.StatusOK, item)
}

type intakeRequest struct {
	Selection      ExecutionSelection   `json:"selection,omitempty"`
	SourceKind     tlcbridge.SourceKind `json:"source_kind"`
	SourceIdentity string               `json:"source_identity"`
	Repository     string               `json:"repository"`
	Text           string               `json:"text"`
}

func (h *HTTPHandler) intake(response http.ResponseWriter, request *http.Request) {
	var input intakeRequest
	if err := h.decode(response, request, &input); err != nil {
		return
	}
	if actor := humanActor(request.Context()); actor != "" && (input.SourceKind != tlcbridge.SourceHuman || !strings.HasPrefix(input.SourceIdentity, "human:"+actor+":")) {
		writeAPIError(response, http.StatusForbidden, "intake identity must belong to the authenticated operator")
		return
	}
	item, err := h.engine.AcceptSelectedText(request.Context(), tlcbridge.Source{
		Kind: input.SourceKind, Identity: strings.TrimSpace(input.SourceIdentity), Repository: strings.TrimSpace(input.Repository),
	}, input.Text, input.Selection, true)
	if err != nil {
		writeAPIError(response, statusForEngineError(err), err.Error())
		return
	}
	writeJSON(response, http.StatusAccepted, item)
}

func (h *HTTPHandler) run(response http.ResponseWriter, request *http.Request) {
	item, err := h.engine.Advance(request.Context(), request.PathValue("workID"))
	if err != nil {
		writeAPIError(response, statusForEngineError(err), err.Error())
		return
	}
	writeJSON(response, http.StatusOK, item)
}

func (h *HTTPHandler) confirm(response http.ResponseWriter, request *http.Request) {
	var input struct {
		BriefID string `json:"brief_id"`
	}
	if err := h.decode(response, request, &input); err != nil {
		return
	}
	item, err := h.engine.Confirm(request.Context(), request.PathValue("workID"), input.BriefID)
	if err != nil {
		writeAPIError(response, statusForEngineError(err), err.Error())
		return
	}
	writeJSON(response, http.StatusAccepted, item)
}

func (h *HTTPHandler) reviewResult(response http.ResponseWriter, request *http.Request) {
	var input ResultReviewRequest
	if err := h.decode(response, request, &input); err != nil {
		return
	}
	if actor := humanActor(request.Context()); actor != "" {
		if input.ReviewedBy != "" && input.ReviewedBy != actor {
			writeAPIError(response, http.StatusForbidden, "review actor does not match authenticated operator")
			return
		}
		input.ReviewedBy = actor
	}
	item, err := h.engine.ReviewResult(request.Context(), request.PathValue("workID"), input)
	if err != nil {
		writeAPIError(response, statusForEngineError(err), err.Error())
		return
	}
	item.Artifact = nil
	writeJSON(response, http.StatusOK, item)
}

func (h *HTTPHandler) artifact(response http.ResponseWriter, request *http.Request) {
	artifact, err := h.engine.Artifact(request.Context(), request.PathValue("workID"))
	if err != nil {
		writeAPIError(response, http.StatusConflict, err.Error())
		return
	}
	writeJSON(response, http.StatusOK, artifact)
}

func (h *HTTPHandler) assignHumanOwner(response http.ResponseWriter, request *http.Request) {
	var input HumanOwnerAssignment
	if err := h.decode(response, request, &input); err != nil {
		return
	}
	if actor := humanActor(request.Context()); actor != "" {
		if input.AssignedBy != "" && input.AssignedBy != actor {
			writeAPIError(response, http.StatusForbidden, "assignment actor does not match authenticated operator")
			return
		}
		input.AssignedBy = actor
	}
	item, err := h.engine.AssignHumanOwner(request.Context(), request.PathValue("workID"), input)
	if err != nil {
		writeAPIError(response, statusForEngineError(err), err.Error())
		return
	}
	writeJSON(response, http.StatusOK, item)
}

func (h *HTTPHandler) resolve(response http.ResponseWriter, request *http.Request) {
	var input struct {
		Resolution string `json:"resolution"`
	}
	if err := h.decode(response, request, &input); err != nil {
		return
	}
	item, err := h.engine.ResolveIntervention(request.Context(), request.PathValue("workID"), request.PathValue("interventionID"), input.Resolution)
	if err != nil {
		writeAPIError(response, statusForEngineError(err), err.Error())
		return
	}
	writeJSON(response, http.StatusOK, item)
}

func (h *HTTPHandler) decode(response http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(response, request.Body, h.maxBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeAPIError(response, http.StatusBadRequest, "invalid JSON request")
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeAPIError(response, http.StatusBadRequest, "request must contain one JSON value")
		return errors.New("multiple JSON values")
	}
	return nil
}

func statusForEngineError(err error) int {
	message := err.Error()
	if errors.Is(err, ErrResultReviewConflict) || errors.Is(err, ErrHumanOwnerConflict) || errors.Is(err, ErrIdempotencyConflict) || strings.Contains(message, "requires Human resolution") || strings.Contains(message, "not runnable") {
		return http.StatusConflict
	}
	if strings.Contains(message, "required") || strings.Contains(message, "invalid") || strings.Contains(message, "not found") {
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func writeAPIError(response http.ResponseWriter, status int, message string) {
	writeJSON(response, status, map[string]string{"error": fmt.Sprintf("%s", message)})
}
