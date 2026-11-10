// Package native hosts the opt-in Drasi runtime outside workflow replay and History locks.
package native

import (
	"encoding/json"
	"errors"
	"fmt"
)

const ABIVersion = 1

var (
	ErrDisabled = errors.New("drasi requires temporal_with_drasi and cgo")
	ErrClosed   = errors.New("drasi runtime is closed")
)

type Code int32

const (
	CodeInvalidArgument    Code = 1
	CodeNotFound           Code = 2
	CodeConflict           Code = 3
	CodeCancelled          Code = 4
	CodeDeadline           Code = 5
	CodeGap                Code = 6
	CodeInternal           Code = 7
	CodeFailedPrecondition Code = 8
)

type Error struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return fmt.Sprintf("Drasi (%d): %s", e.Code, e.Message) }

type Config struct {
	Version        uint32              `json:"version"`
	StateDir       string              `json:"state_dir"`
	OwnerEpoch     uint64              `json:"owner_epoch"`
	InstanceID     string              `json:"instance_id"`
	StartMode      StartMode           `json:"start_mode,omitempty"`
	RestoreFrom    string              `json:"restore_from,omitempty"`
	Sources        []Source            `json:"sources,omitempty"`
	Queries        []Query             `json:"queries,omitempty"`
	Plugins        []Plugin            `json:"plugins,omitempty"`
	Catalogs       []CatalogConfig     `json:"catalogs,omitempty"`
	ManagedPlugins []ManagedPluginPair `json:"managed_plugins,omitempty"`
	Limits         *Limits             `json:"limits,omitempty"`
}

type StartMode string

const (
	StartModeAutomatic          StartMode = "automatic"
	StartModeDormant            StartMode = "dormant"
	StartModeCheckpointRecovery StartMode = "checkpoint_recovery"
)

type Limits struct {
	FeedEvents    uint64 `json:"feed_events"`
	OutboxEntries int    `json:"outbox_entries"`
	Subscriptions int    `json:"subscriptions"`
	SnapshotRows  int    `json:"snapshot_rows"`
	PendingBytes  int    `json:"pending_bytes"`
}

type SourceKind string

const (
	SourceTemporal SourceKind = "temporal"
	SourcePlugin   SourceKind = "plugin"
)

type Source struct {
	ID         string          `json:"id"`
	Kind       SourceKind      `json:"kind"`
	Plugin     string          `json:"plugin,omitempty"`
	PluginKind string          `json:"plugin_kind,omitempty"`
	Config     json.RawMessage `json:"config,omitempty"`
	Bootstrap  *Bootstrap      `json:"bootstrap,omitempty"`
}

type Bootstrap struct {
	Plugin string          `json:"plugin"`
	Kind   string          `json:"kind"`
	Config json.RawMessage `json:"config"`
}

type Language string

const (
	Cypher Language = "cypher"
	GQL    Language = "gql"
)

type Query struct {
	ID       string   `json:"id"`
	Revision uint64   `json:"revision"`
	Language Language `json:"language"`
	Text     string   `json:"text"`
	Sources  []string `json:"sources"`
}

type ResourceLifetime struct {
	NamespaceID string `json:"namespace_id"`
	ResourceID  string `json:"resource_id"`
	Incarnation string `json:"incarnation"`
}

type NodeRecipe interface{ recipe() }

type TemporalSourceRecipe struct{}

type HistorySourceRecipe struct {
	Scope HistoryScope
}

type QueryRecipe struct {
	Language Language
	Text     string
	Sources  []string
}

type SecretReference struct {
	Provider string `json:"provider"`
	Key      string `json:"key"`
	Version  string `json:"version"`
}

type PluginComponentRef struct {
	Digest    string `json:"digest"`
	Component string `json:"component"`
}

type ManagedPluginPair struct {
	Source    PluginComponentRef `json:"source"`
	Bootstrap PluginComponentRef `json:"bootstrap"`
}

type ComponentRecipe struct {
	Plugin        PluginComponentRef         `json:"plugin"`
	Configuration json.RawMessage            `json:"configuration"`
	Secrets       map[string]SecretReference `json:"secrets,omitempty"`
}

type PluginSourceRecipe struct {
	Source    ComponentRecipe `json:"source"`
	Bootstrap ComponentRecipe `json:"bootstrap"`
}

func (TemporalSourceRecipe) recipe() {}
func (HistorySourceRecipe) recipe()  {}
func (QueryRecipe) recipe()          {}
func (PluginSourceRecipe) recipe()   {}

type NodeDefinition struct {
	Lifetime   ResourceLifetime
	Activation uint64
	Recipe     NodeRecipe
}

type nodeRecipeJSON struct {
	Kind      string           `json:"kind"`
	Language  Language         `json:"language,omitempty"`
	Text      string           `json:"text,omitempty"`
	Sources   []string         `json:"sources,omitempty"`
	Scope     *HistoryScope    `json:"scope,omitempty"`
	Source    *ComponentRecipe `json:"source,omitempty"`
	Bootstrap *ComponentRecipe `json:"bootstrap,omitempty"`
}

type nodeDefinitionJSON struct {
	Lifetime   ResourceLifetime `json:"lifetime"`
	Activation uint64           `json:"activation"`
	Recipe     nodeRecipeJSON   `json:"recipe"`
}

func (definition NodeDefinition) MarshalJSON() ([]byte, error) {
	var recipe nodeRecipeJSON
	switch value := definition.Recipe.(type) {
	case TemporalSourceRecipe:
		recipe.Kind = "temporal_source"
	case HistorySourceRecipe:
		recipe.Kind, recipe.Scope = "history_source", &value.Scope
	case QueryRecipe:
		recipe.Kind, recipe.Language, recipe.Text, recipe.Sources = "query", value.Language, value.Text, value.Sources
	case PluginSourceRecipe:
		recipe.Kind, recipe.Source, recipe.Bootstrap = "plugin_source", &value.Source, &value.Bootstrap
	default:
		return nil, &Error{Code: CodeInvalidArgument, Message: "unknown or nil node recipe"}
	}
	return json.Marshal(nodeDefinitionJSON{definition.Lifetime, definition.Activation, recipe})
}

func (definition *NodeDefinition) UnmarshalJSON(data []byte) error {
	var wire nodeDefinitionJSON
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	var recipe NodeRecipe
	if wire.Recipe.Kind != "plugin_source" && (wire.Recipe.Source != nil || wire.Recipe.Bootstrap != nil) {
		return &Error{Code: CodeInvalidArgument, Message: "node recipe contains plugin source fields"}
	}
	switch wire.Recipe.Kind {
	case "temporal_source":
		if wire.Recipe.Language != "" || wire.Recipe.Text != "" || len(wire.Recipe.Sources) != 0 || wire.Recipe.Scope != nil {
			return &Error{Code: CodeInvalidArgument, Message: "temporal source recipe contains query fields"}
		}
		recipe = TemporalSourceRecipe{}
	case "history_source":
		if wire.Recipe.Scope == nil || wire.Recipe.Language != "" || wire.Recipe.Text != "" || len(wire.Recipe.Sources) != 0 {
			return &Error{Code: CodeInvalidArgument, Message: "History source recipe requires a scope and no query fields"}
		}
		recipe = HistorySourceRecipe{Scope: *wire.Recipe.Scope}
	case "query":
		if wire.Recipe.Scope != nil {
			return &Error{Code: CodeInvalidArgument, Message: "query recipe contains History source fields"}
		}
		recipe = QueryRecipe{Language: wire.Recipe.Language, Text: wire.Recipe.Text, Sources: wire.Recipe.Sources}
	case "plugin_source":
		if wire.Recipe.Source == nil || wire.Recipe.Bootstrap == nil ||
			wire.Recipe.Scope != nil || wire.Recipe.Language != "" || wire.Recipe.Text != "" || len(wire.Recipe.Sources) != 0 {
			return &Error{Code: CodeInvalidArgument, Message: "plugin source recipe requires source and bootstrap components only"}
		}
		recipe = PluginSourceRecipe{Source: *wire.Recipe.Source, Bootstrap: *wire.Recipe.Bootstrap}
	default:
		return &Error{Code: CodeInvalidArgument, Message: "unknown or missing node recipe"}
	}
	*definition = NodeDefinition{Lifetime: wire.Lifetime, Activation: wire.Activation, Recipe: recipe}
	return nil
}

type NodeStatus string

const (
	NodeIntended         NodeStatus = "intended"
	NodeDeferred         NodeStatus = "deferred"
	NodeStarting         NodeStatus = "starting"
	NodeRunning          NodeStatus = "running"
	NodeStopping         NodeStatus = "stopping"
	NodeStopped          NodeStatus = "stopped"
	NodeRetiring         NodeStatus = "retiring"
	NodeRetired          NodeStatus = "retired"
	NodeReconfiguring    NodeStatus = "reconfiguring"
	NodeFailed           NodeStatus = "failed"
	NodeRecoveryRequired NodeStatus = "recovery_required"
)

type NodeObservation struct {
	HistoryBlocked bool           `json:"history_blocked,omitempty"`
	NativeID       string         `json:"native_id"`
	QueryRevision  uint64         `json:"query_revision"`
	Status         NodeStatus     `json:"status"`
	Definition     NodeDefinition `json:"definition"`
}

type Plugin struct {
	ID               string `json:"id"`
	Reference        string `json:"reference"`
	BinarySHA256     string `json:"binary_sha256"`
	Filename         string `json:"filename"`
	LocalCatalogPath string `json:"local_catalog_path,omitempty"`
}

// CatalogConfig contains only server-configured, immutable releases.
// Local mirrors retain the same artifact and binary content pins.
type CatalogConfig struct {
	ID          string           `json:"id"`
	DisplayName string           `json:"display_name"`
	Plugins     []CatalogRelease `json:"plugins"`
}

type CatalogRelease struct {
	Version  string `json:"version"`
	Artifact Plugin `json:"artifact"`
}

type PluginSelection struct {
	CatalogID string `json:"catalog_id"`
	PluginID  string `json:"plugin_id"`
	Version   string `json:"version"`
}

type Catalog struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
}

type PluginHostState string

const (
	PluginAvailable       PluginHostState = "available"
	PluginInstalled       PluginHostState = "installed"
	PluginLoaded          PluginHostState = "loaded"
	PluginIncompatible    PluginHostState = "incompatible"
	PluginFailed          PluginHostState = "failed"
	PluginRestartRequired PluginHostState = "restart_required"
)

type PluginCategory string

const (
	PluginSource      PluginCategory = "source"
	PluginBootstrap   PluginCategory = "bootstrap"
	PluginReaction    PluginCategory = "reaction"
	PluginIdentity    PluginCategory = "identity"
	PluginSecretStore PluginCategory = "secret_store"
)

type PluginDescriptor struct {
	Plugin              string          `json:"plugin"`
	Component           string          `json:"kind"`
	Category            PluginCategory  `json:"category"`
	ConfigVersion       string          `json:"config_version"`
	SchemaName          string          `json:"schema_name"`
	ConfigurationSchema json.RawMessage `json:"schema"`
	Supported           bool            `json:"instantiable"`
}

type PluginDescriptors struct {
	SDKABI      string             `json:"sdk_abi"`
	Target      string             `json:"target"`
	Descriptors []PluginDescriptor `json:"descriptors"`
}

type DiscoveredPlugin struct {
	CatalogID   string             `json:"catalog_id"`
	ID          string             `json:"id"`
	Version     string             `json:"version"`
	Digest      string             `json:"digest"`
	Platform    string             `json:"platform"`
	ABIVersion  string             `json:"abi_version"`
	SDKVersion  string             `json:"sdk_version"`
	CoreVersion string             `json:"core_version"`
	LibVersion  string             `json:"lib_version"`
	Descriptors []PluginDescriptor `json:"descriptors"`
	Compatible  bool               `json:"compatible"`
	Message     string             `json:"message"`
}

// PluginObservation is a host-local cache observation, not Temporal desired state.
type PluginObservation struct {
	Plugin                 DiscoveredPlugin `json:"plugin"`
	HostID                 string           `json:"host_id"`
	State                  PluginHostState  `json:"state"`
	Message                string           `json:"message"`
	UpdatedAtUnixMS        uint64           `json:"updated_at_unix_ms"`
	VerifiedSignerIdentity *Identity        `json:"verified_signer_identity"`
}

type Identity struct {
	Issuer  string `json:"issuer"`
	Subject string `json:"subject"`
}

type Subscription struct {
	ID            string `json:"id"`
	QueryID       string `json:"query_id"`
	QueryRevision uint64 `json:"query_revision"`
}

// WatchBinding retains public pins without storing workflow payloads or owner grants.
// The bound subscription ID is also the stable CHASM control execution ID.
type WatchBinding struct {
	Watch         ResourceLifetime `json:"watch"`
	WatchRevision uint64           `json:"watch_revision"`
	Query         ResourceLifetime `json:"query"`
	QueryRevision uint64           `json:"query_revision"`
}

type SubscriptionObservation struct {
	Definition Subscription  `json:"definition"`
	Binding    *WatchBinding `json:"binding"`
	Deleted    bool          `json:"deleted"`
}

type ChangeKind string

const (
	Insert ChangeKind = "insert"
	Update ChangeKind = "update"
	Delete ChangeKind = "delete"
)

type Change struct {
	Kind          ChangeKind     `json:"kind"`
	ElementID     string         `json:"element_id"`
	Labels        []string       `json:"labels"`
	EffectiveFrom uint64         `json:"effective_from"`
	Properties    map[string]any `json:"properties,omitempty"`
	Relation      *Relation      `json:"relation,omitempty"`
}

type Relation struct {
	InSource  string `json:"in_source"`
	InID      string `json:"in_id"`
	OutSource string `json:"out_source"`
	OutID     string `json:"out_id"`
}

// Command is a closed union; callers cannot submit arbitrary bridge operation JSON.
type Command interface{ operation() string }

type AdmitNodeCommand struct {
	Definition NodeDefinition `json:"definition"`
}

type ObserveNodesCommand struct{}

type checkPluginSourceCommand struct {
	Definition NodeDefinition `json:"definition"`
}

type ValidatePluginSourcesCommand struct {
	Recipes []PluginSourceRecipe `json:"recipes"`
}

type SourceSchemaVerdict string

const (
	SourceSchemaValid                SourceSchemaVerdict = "valid"
	SourceSchemaInvalidConfiguration SourceSchemaVerdict = "invalid_configuration"
)

type StopNodeCommand struct {
	NativeID string `json:"native_id"`
}

type RetireNodeCommand struct {
	NativeID string `json:"native_id"`
}

type FeedCommand struct {
	SourceID string `json:"source_id"`
	Position uint64 `json:"position"`
	Change   Change `json:"change"`
}

type SubscribeCommand struct {
	Subscription Subscription `json:"subscription"`
}

type BindWatchCommand struct {
	Binding WatchBinding `json:"binding"`
	QueryID string       `json:"query_id"`
}

type ListSubscriptionsCommand struct {
	After string `json:"after,omitempty"`
	Limit int    `json:"limit"`
}

type PollWatchCommand struct {
	SubscriptionID string `json:"subscription_id"`
}

type UnsubscribeCommand struct {
	SubscriptionID string `json:"subscription_id"`
}

type SnapshotCommand struct {
	QueryID string `json:"query_id"`
}

type DescriptorsCommand struct{}

type ListCatalogsCommand struct{}

type DiscoverPluginsCommand struct {
	CatalogID string `json:"catalog_id,omitempty"`
	Search    string `json:"search,omitempty"`
	Offset    uint32 `json:"offset,omitempty"`
	Limit     uint32 `json:"limit,omitempty"`
}

type InspectPluginCommand struct {
	Selection PluginSelection `json:"selection"`
}

type InstallPluginCommand struct {
	Selection PluginSelection `json:"selection"`
	Digest    string          `json:"digest"`
}

type LoadPluginCommand struct {
	Digest string `json:"digest"`
}

type ListPluginInstallationsCommand struct{}

type GetPluginInstallationCommand struct {
	Digest string `json:"digest"`
}

// CheckpointCommand quiesces and permanently closes the runtime before copying its stores.
type CheckpointCommand struct {
	Destination string `json:"destination"`
}

func (FeedCommand) operation() string                    { return "feed" }
func (SubscribeCommand) operation() string               { return "subscribe" }
func (BindWatchCommand) operation() string               { return "bind_watch" }
func (ListSubscriptionsCommand) operation() string       { return "list_subscriptions" }
func (PollWatchCommand) operation() string               { return "poll_watch" }
func (UnsubscribeCommand) operation() string             { return "unsubscribe" }
func (SnapshotCommand) operation() string                { return "snapshot" }
func (DescriptorsCommand) operation() string             { return "descriptors" }
func (CheckpointCommand) operation() string              { return "checkpoint" }
func (ListCatalogsCommand) operation() string            { return "list_catalogs" }
func (DiscoverPluginsCommand) operation() string         { return "discover_plugins" }
func (InspectPluginCommand) operation() string           { return "inspect_plugin" }
func (InstallPluginCommand) operation() string           { return "install_plugin" }
func (LoadPluginCommand) operation() string              { return "load_plugin" }
func (ListPluginInstallationsCommand) operation() string { return "list_plugin_installations" }
func (GetPluginInstallationCommand) operation() string   { return "get_plugin_installation" }
func (AdmitNodeCommand) operation() string               { return "admit_node" }
func (checkPluginSourceCommand) operation() string       { return "check_plugin_source" }
func (ValidatePluginSourcesCommand) operation() string   { return "validate_plugin_sources" }
func (ObserveNodesCommand) operation() string            { return "observe_nodes" }
func (StopNodeCommand) operation() string                { return "stop_node" }
func (RetireNodeCommand) operation() string              { return "retire_node" }

func marshalCommand(command Command) ([]byte, error) {
	if command == nil {
		return nil, &Error{Code: CodeInvalidArgument, Message: "unknown or nil command"}
	}
	fields, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(fields, &envelope); err != nil {
		return nil, err
	}
	if envelope == nil {
		return nil, &Error{Code: CodeInvalidArgument, Message: "unknown or nil command"}
	}
	envelope["op"], err = json.Marshal(command.operation())
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope)
}

type EventKind string

const (
	EventSnapshot EventKind = "snapshot"
	EventDelta    EventKind = "delta"
)

type Row struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type Diff struct {
	Type         string          `json:"type"`
	RowSignature uint64          `json:"row_signature"`
	Data         json.RawMessage `json:"data,omitempty"`
	Before       json.RawMessage `json:"before,omitempty"`
	After        json.RawMessage `json:"after,omitempty"`
}

type Event struct {
	SubscriptionID string    `json:"subscription_id"`
	QueryID        string    `json:"query_id"`
	QueryRevision  uint64    `json:"query_revision"`
	Incarnation    string    `json:"incarnation"`
	Sequence       uint64    `json:"sequence"`
	Kind           EventKind `json:"kind"`
	Rows           []Row     `json:"rows"`
	Changes        []Diff    `json:"changes"`
}

// Receipt must be sent only after the referenced acceptance has durably committed in Temporal.
type Receipt struct {
	SubscriptionID string    `json:"subscription_id"`
	QueryRevision  uint64    `json:"query_revision"`
	Incarnation    string    `json:"incarnation"`
	Sequence       uint64    `json:"sequence"`
	Kind           EventKind `json:"kind"`
	AcceptanceID   string    `json:"acceptance_id"`
}

func (e Event) Receipt(acceptanceID string) Receipt {
	return Receipt{
		SubscriptionID: e.SubscriptionID, QueryRevision: e.QueryRevision,
		Incarnation: e.Incarnation, Sequence: e.Sequence, Kind: e.Kind, AcceptanceID: acceptanceID,
	}
}

type Checkpoint struct {
	Version      uint32            `json:"version"`
	Upstream     string            `json:"upstream"`
	InstanceID   string            `json:"instance_id"`
	OwnerEpoch   uint64            `json:"owner_epoch"`
	RetirementID string            `json:"retirement_id,omitempty"`
	Files        map[string]string `json:"files"`
}

type Result struct {
	SourceSchemas           []SourceSchemaVerdict     `json:"source_schemas,omitempty"`
	MaterializationRequired *bool                     `json:"materialization_required,omitempty"`
	HistoryProgress         *HistoryProgress          `json:"progress,omitempty"`
	HistoryReceipt          *HistoryReceipt           `json:"receipt,omitempty"`
	Event                   *Event                    `json:"event,omitempty"`
	Subscription            *SubscriptionObservation  `json:"subscription,omitempty"`
	Subscriptions           []SubscriptionObservation `json:"subscriptions,omitempty"`
	NextPageToken           string                    `json:"next_page_token,omitempty"`
	Node                    *NodeObservation          `json:"node,omitempty"`
	Nodes                   []NodeObservation         `json:"nodes,omitempty"`
	TopologyBusy            *bool                     `json:"topology_busy,omitempty"`
	CleanupFailure          *string                   `json:"cleanup_failure,omitempty"`
	Position                uint64                    `json:"position,omitempty"`
	Snapshot                *Event                    `json:"snapshot,omitempty"`
	Checkpoint              *Checkpoint               `json:"checkpoint,omitempty"`
	Plugins                 json.RawMessage           `json:"plugins,omitempty"`
	Catalogs                []Catalog                 `json:"catalogs,omitempty"`
	DiscoveredPlugins       []DiscoveredPlugin        `json:"discovered_plugins,omitempty"`
	MorePlugins             bool                      `json:"more_plugins,omitempty"`
	Plugin                  *DiscoveredPlugin         `json:"plugin,omitempty"`
	Installation            *PluginObservation        `json:"installation,omitempty"`
	Installations           []PluginObservation       `json:"installations,omitempty"`
}
