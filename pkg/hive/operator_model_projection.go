package hive

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/transpara-ai/eventgraph/go/pkg/modelconfig"
)

const (
	operatorModelCatalogSourceEmbedded = "embedded-defaults"
	operatorModelCatalogReloadMode     = "startup-static"
	operatorModelCatalogReloadModeHot  = "hot-reload"
)

var defaultOperatorModelSelectionLoadedAt = time.Now().UTC()

// OperatorModelSelectionSource returns the current model-selection config.
// Long-running processes use this seam to expose a refreshed catalog without
// making Site a source of truth.
type OperatorModelSelectionSource func() OperatorModelSelectionConfig

// OperatorModelSelectionConfig describes the model resolver Hive uses to build
// the read-only operator projection.
type OperatorModelSelectionConfig struct {
	Resolver        *modelconfig.Resolver
	CatalogSource   string
	LoadedAt        time.Time
	ReloadMode      string
	HotReload       bool
	LastReloadAt    time.Time
	LastReloadError string
	RolePolicyError string
	RolePolicies    map[string]OperatorModelRolePolicy
}

// OperatorModelRolePolicy is Hive-owned durable role policy layered over the
// static starter role policy for projection and execution planning.
type OperatorModelRolePolicy struct {
	Policy            *modelconfig.RoleModelPolicy
	RequestedAuthMode string
	ResolvedModel     string
	ResolvedProvider  string
	AuthMode          string
	EventID           string
	UpdatedAt         time.Time
}

// DefaultOperatorModelSelectionConfig returns the built-in claude-cli resolver
// projection config. Callers that loaded a custom catalog should use
// OperatorModelSelectionFromCatalogPath instead.
func DefaultOperatorModelSelectionConfig(loadedAt time.Time) OperatorModelSelectionConfig {
	if loadedAt.IsZero() {
		loadedAt = defaultOperatorModelSelectionLoadedAt
	}
	return OperatorModelSelectionConfig{
		Resolver:      modelconfig.DefaultResolver(),
		CatalogSource: operatorModelCatalogSourceEmbedded,
		LoadedAt:      loadedAt,
		ReloadMode:    operatorModelCatalogReloadMode,
		HotReload:     false,
	}
}

// OperatorModelSelectionFromCatalogPath loads the resolver for the ops API
// projection. Empty catalogPath means embedded defaults.
func OperatorModelSelectionFromCatalogPath(catalogPath string, loadedAt time.Time) (OperatorModelSelectionConfig, error) {
	if catalogPath == "" {
		return DefaultOperatorModelSelectionConfig(loadedAt), nil
	}
	resolver, err := modelconfig.ResolverFromCatalogFile(catalogPath)
	if err != nil {
		return OperatorModelSelectionConfig{}, err
	}
	return OperatorModelSelectionConfig{
		Resolver:      resolver,
		CatalogSource: catalogPath,
		LoadedAt:      loadedAt,
		ReloadMode:    operatorModelCatalogReloadMode,
		HotReload:     false,
	}, nil
}

// OperatorModelSelectionManager owns the Hive model catalog state for a
// long-running process. Reload failures are recorded for projection, but the
// previous valid resolver remains active so a bad YAML edit cannot silently
// corrupt model assignment state.
type OperatorModelSelectionManager struct {
	mu          sync.RWMutex
	catalogPath string
	config      OperatorModelSelectionConfig
	lastModTime time.Time
}

// NewOperatorModelSelectionManager loads the initial catalog. When hotReload is
// true and catalogPath is non-empty, ReloadIfChanged may refresh the resolver at
// runtime without restarting the process.
func NewOperatorModelSelectionManager(catalogPath string, loadedAt time.Time, hotReload bool) (*OperatorModelSelectionManager, error) {
	config, err := OperatorModelSelectionFromCatalogPath(catalogPath, loadedAt)
	if err != nil {
		return nil, err
	}
	if hotReload && catalogPath != "" {
		config.ReloadMode = operatorModelCatalogReloadModeHot
		config.HotReload = true
	}
	manager := &OperatorModelSelectionManager{
		catalogPath: catalogPath,
		config:      config,
	}
	if catalogPath != "" {
		if info, err := os.Stat(catalogPath); err == nil {
			manager.lastModTime = info.ModTime()
		}
	}
	return manager, nil
}

// Snapshot returns the current catalog config. The resolver is immutable after
// construction, so sharing its pointer is safe.
func (m *OperatorModelSelectionManager) Snapshot() OperatorModelSelectionConfig {
	if m == nil {
		return DefaultOperatorModelSelectionConfig(time.Time{})
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

// ReloadIfChanged reloads catalogPath when its mtime advances. A parse/load
// failure is surfaced in Snapshot().LastReloadError and leaves the previous
// resolver untouched.
func (m *OperatorModelSelectionManager) ReloadIfChanged(now time.Time) (bool, error) {
	if m == nil || m.catalogPath == "" {
		return false, nil
	}
	info, err := os.Stat(m.catalogPath)
	if err != nil {
		m.recordReloadError(now, err)
		return false, err
	}
	m.mu.RLock()
	lastModTime := m.lastModTime
	m.mu.RUnlock()
	if !lastModTime.IsZero() && !info.ModTime().After(lastModTime) {
		return false, nil
	}
	return true, m.reload(now, info.ModTime())
}

// Reload forces a catalog reload even when the file mtime did not change.
func (m *OperatorModelSelectionManager) Reload(now time.Time) error {
	if m == nil || m.catalogPath == "" {
		return nil
	}
	info, err := os.Stat(m.catalogPath)
	if err != nil {
		m.recordReloadError(now, err)
		return err
	}
	return m.reload(now, info.ModTime())
}

func (m *OperatorModelSelectionManager) reload(now time.Time, modTime time.Time) error {
	resolver, err := modelconfig.ResolverFromCatalogFile(m.catalogPath)
	if err != nil {
		m.recordReloadError(now, err)
		return err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config.Resolver = resolver
	m.config.CatalogSource = m.catalogPath
	m.config.LoadedAt = now.UTC()
	m.config.ReloadMode = operatorModelCatalogReloadModeHot
	m.config.HotReload = true
	m.config.LastReloadAt = now.UTC()
	m.config.LastReloadError = ""
	m.lastModTime = modTime
	return nil
}

func (m *OperatorModelSelectionManager) recordReloadError(now time.Time, err error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config.LastReloadAt = now.UTC()
	m.config.LastReloadError = err.Error()
}

// OperatorModelSelection is the Site-facing, read-only model catalog and role
// assignment projection. Site may render it, but Hive remains the source of
// truth.
type OperatorModelSelection struct {
	Source        string                        `json:"source"`
	CatalogSource string                        `json:"catalog_source"`
	TierDefaults  map[string]string             `json:"tier_defaults"`
	LoadedAt      time.Time                     `json:"loaded_at"`
	ReloadMode    string                        `json:"reload_mode"`
	HotReload     bool                          `json:"hot_reload"`
	LastReloadAt  *time.Time                    `json:"last_reload_at,omitempty"`
	Models        []OperatorModelCatalogEntry   `json:"models"`
	Assignments   []OperatorModelRoleAssignment `json:"assignments"`
	Errors        []string                      `json:"errors,omitempty"`
}

type OperatorModelCatalogEntry struct {
	ID              string               `json:"id"`
	Aliases         []string             `json:"aliases,omitempty"`
	Provider        string               `json:"provider"`
	BaseURL         string               `json:"base_url,omitempty"`
	AuthMode        string               `json:"auth_mode"`
	Tier            string               `json:"tier"`
	Capabilities    []string             `json:"capabilities,omitempty"`
	Pricing         OperatorModelPricing `json:"pricing"`
	ContextWindow   int                  `json:"context_window"`
	MaxOutputTokens int                  `json:"max_output_tokens"`
	Deprecated      bool                 `json:"deprecated,omitempty"`
	Metadata        map[string]string    `json:"metadata,omitempty"`
}

type OperatorModelPricing struct {
	InputPerMillion      float64 `json:"input_per_million"`
	OutputPerMillion     float64 `json:"output_per_million"`
	CacheReadPerMillion  float64 `json:"cache_read_per_million,omitempty"`
	CacheWritePerMillion float64 `json:"cache_write_per_million,omitempty"`
}

type OperatorModelRoleAssignment struct {
	Role       string `json:"role"`
	Tier       string `json:"tier,omitempty"`
	CanOperate bool   `json:"can_operate"`
	Model      string `json:"model,omitempty"`
	Provider   string `json:"provider,omitempty"`
	AuthMode   string `json:"auth_mode,omitempty"`
	// SelectionMode mirrors the resolver's ResolvedConfig.Mode for a
	// successfully resolved assignment (manual-explicit, auto-tier,
	// system-default). Empty when the assignment was built without a
	// successful Resolve — never synthesized; consumers treat absence as
	// not-projected and fall back to their own inference.
	SelectionMode        string   `json:"selection_mode,omitempty"`
	Profile              string   `json:"profile,omitempty"`
	PolicyModel          string   `json:"policy_model,omitempty"`
	PolicyProvider       string   `json:"policy_provider,omitempty"`
	PreferredTier        string   `json:"preferred_tier,omitempty"`
	RequiredCapabilities []string `json:"required_capabilities,omitempty"`
	MaxCostPerCallUSD    *float64 `json:"max_cost_per_call_usd,omitempty"`
	SelectionStrategy    string   `json:"selection_strategy,omitempty"`
	Source               string   `json:"source"`
	PolicyEventID        string   `json:"policy_event_id,omitempty"`
	Trace                []string `json:"trace,omitempty"`
	Error                string   `json:"error,omitempty"`
}

// BuildOperatorModelSelection resolves the starter role assignments against the
// configured resolver and mirrors the catalog metadata needed by an operator UI.
func BuildOperatorModelSelection(config OperatorModelSelectionConfig) OperatorModelSelection {
	config = normalizeOperatorModelSelectionConfig(config)
	projection := OperatorModelSelection{
		Source:        "hive",
		CatalogSource: config.CatalogSource,
		TierDefaults:  make(map[string]string),
		LoadedAt:      config.LoadedAt,
		ReloadMode:    config.ReloadMode,
		HotReload:     config.HotReload,
	}
	for tier, model := range config.Resolver.Defaults().TierModels {
		projection.TierDefaults[string(tier)] = model
	}
	if !config.LastReloadAt.IsZero() {
		lastReloadAt := config.LastReloadAt
		projection.LastReloadAt = &lastReloadAt
	}
	if config.LastReloadError != "" {
		projection.Errors = append(projection.Errors, "catalog reload: "+config.LastReloadError)
	}
	if config.RolePolicyError != "" {
		projection.Errors = append(projection.Errors, "role policy: "+config.RolePolicyError)
	}

	catalog := config.Resolver.Catalog()
	for _, entry := range catalog.All() {
		projection.Models = append(projection.Models, operatorModelCatalogEntry(entry))
	}
	sort.Slice(projection.Models, func(i, j int) bool {
		return projection.Models[i].ID < projection.Models[j].ID
	})

	roles := StarterRoleDefinitions()
	roleNames := make([]string, 0, len(roles))
	for name := range roles {
		roleNames = append(roleNames, name)
	}
	sort.Strings(roleNames)
	for _, name := range roleNames {
		roleDef := roles[name]
		assignment := operatorModelRoleAssignment(config.Resolver, roleDef, config.RolePolicies[name])
		if assignment.Error != "" {
			projection.Errors = append(projection.Errors, fmt.Sprintf("%s: %s", assignment.Role, assignment.Error))
		}
		projection.Assignments = append(projection.Assignments, assignment)
	}
	return projection
}

func normalizeOperatorModelSelectionConfig(config OperatorModelSelectionConfig) OperatorModelSelectionConfig {
	if config.Resolver == nil {
		config.Resolver = modelconfig.DefaultResolver()
	}
	if config.CatalogSource == "" {
		config.CatalogSource = operatorModelCatalogSourceEmbedded
	}
	if config.LoadedAt.IsZero() {
		config.LoadedAt = time.Now().UTC()
	}
	if config.ReloadMode == "" {
		config.ReloadMode = operatorModelCatalogReloadMode
	}
	return config
}

func operatorModelCatalogEntry(entry modelconfig.ModelCatalogEntry) OperatorModelCatalogEntry {
	return OperatorModelCatalogEntry{
		ID:           entry.ID,
		Aliases:      append([]string(nil), entry.Aliases...),
		Provider:     entry.Provider,
		BaseURL:      entry.BaseURL,
		AuthMode:     string(entry.AuthMode),
		Tier:         string(entry.Tier),
		Capabilities: capabilityStrings(entry.Capabilities),
		Pricing: OperatorModelPricing{
			InputPerMillion:      entry.Pricing.InputPerMillion,
			OutputPerMillion:     entry.Pricing.OutputPerMillion,
			CacheReadPerMillion:  entry.Pricing.CacheReadPerMillion,
			CacheWritePerMillion: entry.Pricing.CacheWritePerMillion,
		},
		ContextWindow:   entry.ContextWindow,
		MaxOutputTokens: entry.MaxOutputTokens,
		Deprecated:      entry.Deprecated,
		Metadata:        cloneStringMap(entry.Metadata),
	}
}

func operatorModelRoleAssignment(resolver *modelconfig.Resolver, role *modelconfig.RoleDefinition, storedPolicy OperatorModelRolePolicy) OperatorModelRoleAssignment {
	policy := role.ModelPolicy
	source := "starter-role-definition"
	if storedPolicy.Policy != nil {
		policy = mergeRoleModelPolicy(role.ModelPolicy, storedPolicy.Policy)
		source = "hive-model-policy-event"
	}
	assignment := OperatorModelRoleAssignment{
		Role:          role.Name,
		Tier:          role.Tier,
		CanOperate:    role.CanOperate,
		Source:        source,
		PolicyEventID: storedPolicy.EventID,
	}
	if policy != nil {
		assignment.Profile = policy.Profile
		assignment.PolicyModel = policy.Model
		assignment.PolicyProvider = policy.Provider
		assignment.PreferredTier = string(policy.PreferredTier)
		assignment.RequiredCapabilities = capabilityStrings(policy.RequiredCapabilities)
		assignment.MaxCostPerCallUSD = cloneModelPolicyFloat64Ptr(policy.MaxCostPerCallUSD)
		assignment.SelectionStrategy = policy.SelectionStrategy
	}
	resolved, err := resolver.Resolve(modelconfig.ResolutionInput{
		Role:       role.Name,
		Policy:     policy,
		CanOperate: role.CanOperate,
	})
	if err != nil {
		assignment.Error = err.Error()
		return assignment
	}
	if storedPolicy.Policy != nil {
		if err := validateRunLaunchOverrideResolvedConfig(0, role.Name, storedPolicy.RequestedAuthMode, resolved); err != nil {
			assignment.Error = err.Error()
			return assignment
		}
		if err := validateStoredModelRolePolicyResolution(role.Name, storedPolicy, resolved); err != nil {
			assignment.Error = err.Error()
			return assignment
		}
	}
	assignment.Model = resolved.Model
	assignment.Provider = resolved.Provider
	assignment.AuthMode = string(resolved.AuthMode)
	// Populated only from a successful resolution — every error path above
	// returns before this point, so an unresolved assignment never carries a
	// synthesized mode (packet D3 / CFADA2-adv4).
	assignment.SelectionMode = string(resolved.Mode)
	assignment.Trace = append([]string(nil), resolved.Trace...)
	return assignment
}

func mergeRoleModelPolicy(base, overlay *modelconfig.RoleModelPolicy) *modelconfig.RoleModelPolicy {
	if base == nil && overlay == nil {
		return nil
	}
	out := &modelconfig.RoleModelPolicy{}
	if base != nil {
		*out = *base
		out.RequiredCapabilities = append([]modelconfig.Capability(nil), base.RequiredCapabilities...)
		out.MaxCostPerCallUSD = cloneModelPolicyFloat64Ptr(base.MaxCostPerCallUSD)
	}
	if overlay == nil {
		return out
	}
	if overlay.Model != "" {
		out.Model = overlay.Model
	}
	if overlay.Provider != "" {
		out.Provider = overlay.Provider
	}
	if overlay.Profile != "" {
		out.Profile = overlay.Profile
	}
	if overlay.PreferredTier != "" {
		out.PreferredTier = overlay.PreferredTier
	}
	if len(overlay.RequiredCapabilities) > 0 {
		out.RequiredCapabilities = mergeCapabilities(out.RequiredCapabilities, overlay.RequiredCapabilities)
	}
	if overlay.MaxCostPerCallUSD != nil {
		out.MaxCostPerCallUSD = cloneModelPolicyFloat64Ptr(overlay.MaxCostPerCallUSD)
	}
	if overlay.AllowDowngrade {
		out.AllowDowngrade = true
	}
	if overlay.SelectionStrategy != "" {
		out.SelectionStrategy = overlay.SelectionStrategy
	}
	return out
}

func mergeCapabilities(a, b []modelconfig.Capability) []modelconfig.Capability {
	if len(a) == 0 {
		return append([]modelconfig.Capability(nil), b...)
	}
	out := append([]modelconfig.Capability(nil), a...)
	seen := make(map[modelconfig.Capability]struct{}, len(out)+len(b))
	for _, cap := range out {
		seen[cap] = struct{}{}
	}
	for _, cap := range b {
		if _, ok := seen[cap]; ok {
			continue
		}
		seen[cap] = struct{}{}
		out = append(out, cap)
	}
	return out
}

func validateStoredModelRolePolicyResolution(role string, stored OperatorModelRolePolicy, resolved modelconfig.ResolvedConfig) error {
	if stored.ResolvedModel != "" && stored.ResolvedModel != resolved.Model {
		return fmt.Errorf("model policy for role %q stored resolved_model %q but current resolver produced %q", role, stored.ResolvedModel, resolved.Model)
	}
	if stored.ResolvedProvider != "" && stored.ResolvedProvider != resolved.Provider {
		return fmt.Errorf("model policy for role %q stored resolved_provider %q but current resolver produced %q", role, stored.ResolvedProvider, resolved.Provider)
	}
	if stored.AuthMode != "" && stored.AuthMode != string(resolved.AuthMode) {
		return fmt.Errorf("model policy for role %q stored auth_mode %q but current resolver produced %q", role, stored.AuthMode, resolved.AuthMode)
	}
	return nil
}

func cloneModelPolicyFloat64Ptr(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func capabilityStrings(capabilities []modelconfig.Capability) []string {
	if len(capabilities) == 0 {
		return nil
	}
	out := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		out = append(out, string(capability))
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
