use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const VERSION: u32 = 1;
pub const UPSTREAM: &str =
    "9469f3e01ff22c4f0092c9900dfbd5051ceae545+development.198a2fbe0e9eb5f819d4ac5cce5622252d6ffe72";
pub const MAX_INPUT: usize = 1 << 20;
pub const MAX_OUTPUT: usize = 16 << 20;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    pub state_dir: String,
    pub owner_epoch: u64,
    pub instance_id: String,
    #[serde(default)]
    pub start_mode: StartMode,
    #[serde(default)]
    pub restore_from: Option<String>,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    #[serde(default)]
    pub queries: Vec<QueryConfig>,
    #[serde(default)]
    pub plugins: Vec<PluginConfig>,
    #[serde(default)]
    pub catalogs: Vec<CatalogConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub managed_plugins: Vec<ManagedPluginPair>,
    #[serde(default)]
    pub trust: Trust,
    #[serde(default)]
    pub limits: Limits,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartMode {
    #[default]
    Automatic,
    Dormant,
    CheckpointRecovery,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub feed_events: u64,
    pub outbox_entries: usize,
    pub subscriptions: usize,
    pub snapshot_rows: usize,
    pub pending_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            feed_events: 100_000,
            outbox_entries: 10_000,
            subscriptions: 1024,
            snapshot_rows: 10_000,
            pending_bytes: 1 << 20,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub id: String,
    pub kind: SourceKind,
    #[serde(default)]
    pub plugin: Option<String>,
    #[serde(default)]
    pub plugin_kind: Option<String>,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub bootstrap: Option<BootstrapConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Temporal,
    Plugin,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapConfig {
    pub plugin: String,
    pub kind: String,
    pub config: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretRef {
    pub provider: String,
    pub key: String,
    pub version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginComponentRef {
    pub digest: String,
    pub component: String,
}

impl PluginComponentRef {
    pub(crate) fn validate(&self) -> Result<()> {
        identifier(&self.component)?;
        if !self.digest.strip_prefix("sha256:").is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }) {
            return Err(Error::new(
                1,
                "plugin component requires an immutable SHA256 digest",
            ));
        }
        Ok(())
    }
}

/// Administrator-reviewed pairs whose Source stop/drop joins all work and whose
/// bootstrap operation owns its work until completion. This is not a plugin claim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedPluginPair {
    pub source: PluginComponentRef,
    pub bootstrap: PluginComponentRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentRecipe {
    pub plugin: PluginComponentRef,
    pub configuration: Value,
    #[serde(default)]
    pub secrets: BTreeMap<String, SecretRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginSourceRecipe {
    pub source: ComponentRecipe,
    pub bootstrap: ComponentRecipe,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceSchemaVerdict {
    Valid,
    InvalidConfiguration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryConfig {
    pub id: String,
    pub revision: u64,
    pub language: Language,
    pub text: String,
    pub sources: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    Cypher,
    Gql,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLifetime {
    pub namespace_id: String,
    pub resource_id: String,
    pub incarnation: String,
}

impl ResourceLifetime {
    pub(crate) fn validate(&self) -> Result<()> {
        let parsed = uuid::Uuid::parse_str(&self.namespace_id)
            .map_err(|_| Error::new(1, "resource namespace requires a canonical nonzero UUID"))?;
        if parsed.is_nil() || parsed.to_string() != self.namespace_id {
            return Err(Error::new(
                1,
                "resource namespace requires a canonical nonzero UUID",
            ));
        }
        identifier(&self.incarnation)?;
        if self.resource_id.len() > 128
            || !self
                .resource_id
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            || !self
                .resource_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
        {
            return Err(Error::new(
                1,
                "resource IDs require 1..128 ASCII letters, digits, '.', '-' or '_', starting with a letter or digit",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDefinition {
    pub lifetime: ResourceLifetime,
    pub activation: u64,
    pub recipe: NodeRecipe,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NodeRecipe {
    TemporalSource,
    HistorySource {
        scope: HistoryScope,
    },
    PluginSource {
        source: Box<ComponentRecipe>,
        bootstrap: Box<ComponentRecipe>,
    },
    Query {
        language: Language,
        text: String,
        sources: Vec<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Intended,
    Deferred,
    Starting,
    Running,
    Stopping,
    Stopped,
    Retiring,
    Retired,
    Reconfiguring,
    Failed,
    RecoveryRequired,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeObservation {
    #[serde(default)]
    pub history_blocked: bool,
    pub native_id: String,
    pub query_revision: u64,
    pub status: NodeStatus,
    pub definition: NodeDefinition,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginConfig {
    pub id: String,
    pub reference: String,
    pub binary_sha256: String,
    pub filename: String,
    #[serde(default)]
    pub local_catalog_path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogConfig {
    pub id: String,
    pub display_name: String,
    pub plugins: Vec<CatalogRelease>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogRelease {
    pub version: String,
    pub artifact: PluginConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginSelection {
    pub catalog_id: String,
    pub plugin_id: String,
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Catalog {
    pub id: String,
    pub display_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginHostState {
    Available,
    Installed,
    Loaded,
    Incompatible,
    Failed,
    RestartRequired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginCategory {
    Source,
    Bootstrap,
    Reaction,
    Identity,
    SecretStore,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginDescriptor {
    pub plugin: String,
    #[serde(rename = "kind")]
    pub component: String,
    pub category: PluginCategory,
    pub config_version: String,
    pub schema_name: String,
    #[serde(rename = "schema")]
    pub configuration_schema: Value,
    #[serde(rename = "instantiable")]
    pub supported: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoveredPlugin {
    pub catalog_id: String,
    pub id: String,
    pub version: String,
    pub digest: String,
    pub platform: String,
    pub abi_version: String,
    pub sdk_version: String,
    pub core_version: String,
    pub lib_version: String,
    pub descriptors: Vec<PluginDescriptor>,
    pub compatible: bool,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginObservation {
    pub plugin: DiscoveredPlugin,
    pub host_id: String,
    pub state: PluginHostState,
    pub message: String,
    pub updated_at_unix_ms: u64,
    pub verified_signer_identity: Option<Identity>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trust {
    #[serde(default)]
    pub registry_prefixes: Vec<String>,
    #[serde(default)]
    pub identities: Vec<Identity>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub issuer: String,
    pub subject: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub id: String,
    pub query_id: String,
    pub query_revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchBinding {
    pub watch: ResourceLifetime,
    pub watch_revision: u64,
    pub query: ResourceLifetime,
    pub query_revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubscriptionObservation {
    pub definition: Subscription,
    pub binding: Option<WatchBinding>,
    pub deleted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Checkpoint {
        destination: String,
    },
    #[serde(untagged)]
    Command(Command),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    AdmitNode {
        definition: NodeDefinition,
    },
    CheckPluginSource {
        definition: NodeDefinition,
    },
    ValidatePluginSources {
        recipes: Vec<PluginSourceRecipe>,
    },
    ObserveNodes,
    StopNode {
        native_id: String,
    },
    RetireNode {
        native_id: String,
    },
    Feed {
        source_id: String,
        position: u64,
        change: Change,
    },
    HistoryProgress {
        source_id: String,
        shard_id: u32,
    },
    AcceptHistory {
        source_id: String,
        step: HistoryStep,
    },
    BlockHistory {
        source_id: String,
        problem: String,
    },
    SuspendHistory {
        source_id: String,
    },
    Subscribe {
        subscription: Subscription,
    },
    BindWatch {
        binding: WatchBinding,
        query_id: String,
    },
    ListSubscriptions {
        #[serde(default)]
        after: String,
        limit: usize,
    },
    PollWatch {
        subscription_id: String,
    },
    Unsubscribe {
        subscription_id: String,
    },
    Snapshot {
        query_id: String,
    },
    Descriptors,
    ListCatalogs,
    DiscoverPlugins {
        #[serde(default)]
        catalog_id: String,
        #[serde(default)]
        search: String,
        #[serde(default)]
        offset: usize,
        #[serde(default)]
        limit: usize,
    },
    InspectPlugin {
        selection: PluginSelection,
    },
    InstallPlugin {
        selection: PluginSelection,
        digest: String,
    },
    LoadPlugin {
        digest: String,
    },
    ListPluginInstallations,
    GetPluginInstallation {
        digest: String,
    },
}

impl Command {
    pub(crate) fn is_plugin_management(&self) -> bool {
        matches!(
            self,
            Self::Descriptors
                | Self::ValidatePluginSources { .. }
                | Self::ListCatalogs
                | Self::DiscoverPlugins { .. }
                | Self::InspectPlugin { .. }
                | Self::InstallPlugin { .. }
                | Self::LoadPlugin { .. }
                | Self::ListPluginInstallations
                | Self::GetPluginInstallation { .. }
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryScope {
    pub namespace_id: String,
    pub shard_count: u32,
}

impl HistoryScope {
    pub(crate) fn validate(&self) -> Result<()> {
        let namespace = uuid::Uuid::parse_str(&self.namespace_id)
            .map_err(|_| Error::new(1, "History input requires a canonical namespace UUID"))?;
        if namespace.is_nil()
            || namespace.to_string() != self.namespace_id
            || self.shard_count == 0
            || self.shard_count > 16_384
        {
            return Err(Error::new(
                1,
                "History input requires a canonical nonzero namespace UUID and 1..16384 shards",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryCursor {
    Unstarted,
    Capturing {
        capture_epoch: i64,
        reconstruction_start: i64,
        next_position: i64,
        scan: HistoryScan,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryScan {
    Scanning { token: String, cutoff: i64 },
    Complete { cutoff: i64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryProgress {
    pub revision: u64,
    pub cursor: HistoryCursor,
    pub receipt: Option<HistoryReceipt>,
}

impl Default for HistoryProgress {
    fn default() -> Self {
        Self {
            revision: 0,
            cursor: HistoryCursor::Unstarted,
            receipt: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryReceipt {
    pub request_id: String,
    pub revision: u64,
    pub first_position: Option<u64>,
    pub head: u64,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryStep {
    pub request_id: String,
    pub shard_id: u32,
    pub expected_revision: u64,
    pub page: HistoryPage,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryPage {
    Bootstrap {
        capture_epoch: i64,
        reconstruction_start: i64,
        next_token: String,
        cutoff: i64,
    },
    Records {
        capture_epoch: i64,
        start_position: i64,
        next_position: i64,
        published_cutoff: i64,
        records: Vec<HistoryRecord>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRecord {
    pub position: i64,
    pub capture_epoch: i64,
    pub namespace_id: String,
    pub execution_key: String,
    pub effective_from: u64,
    pub state: Vec<ElementImage>,
    pub facts: Vec<ElementImage>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElementImage {
    pub element_id: String,
    pub labels: Vec<String>,
    pub effective_from: u64,
    pub properties: BTreeMap<String, Value>,
    pub relation: Option<Relation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Change {
    pub kind: ChangeKind,
    pub element_id: String,
    pub labels: Vec<String>,
    pub effective_from: u64,
    #[serde(default)]
    pub properties: BTreeMap<String, Value>,
    #[serde(default)]
    pub relation: Option<Relation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Relation {
    pub in_source: String,
    pub in_id: String,
    pub out_source: String,
    pub out_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Row {
    pub key: String,
    pub value: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub subscription_id: String,
    pub query_id: String,
    pub query_revision: u64,
    pub incarnation: String,
    pub sequence: u64,
    pub kind: EventKind,
    pub rows: Vec<Row>,
    pub changes: Vec<OutputDiff>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum OutputDiff {
    #[serde(rename = "ADD")]
    Add { row_signature: u64, data: Value },
    #[serde(rename = "UPDATE")]
    Update {
        row_signature: u64,
        before: Value,
        after: Value,
    },
    #[serde(rename = "DELETE")]
    Delete { row_signature: u64, data: Value },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Snapshot,
    Delta,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub subscription_id: String,
    pub query_revision: u64,
    pub incarnation: String,
    pub sequence: u64,
    pub kind: EventKind,
    pub acceptance_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub version: u32,
    pub upstream: String,
    pub instance_id: String,
    pub owner_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retirement_id: Option<String>,
    pub files: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Error {
    pub code: i32,
    pub message: String,
}

impl Error {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Self::new(7, format!("{error:#}"))
    }
}

impl From<drasi_lib::DrasiError> for Error {
    fn from(error: drasi_lib::DrasiError) -> Self {
        use drasi_lib::DrasiError;
        let code = match error {
            DrasiError::InvalidConfig { .. } | DrasiError::Validation { .. } => 1,
            DrasiError::ComponentNotFound { .. } => 2,
            DrasiError::AlreadyExists { .. } | DrasiError::InvalidState { .. } => 3,
            DrasiError::OperationFailed { .. } | DrasiError::Internal(_) => 7,
        };
        Self::new(code, error.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::new(1, error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn identifier(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Error::new(
            1,
            "identifiers must contain 1..128 ASCII letters, digits, '-' or '_'",
        ));
    }
    Ok(())
}
