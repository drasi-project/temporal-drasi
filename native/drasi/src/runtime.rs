use crate::{
    checkpoint,
    credentials::{AdmissionValues, CredentialLease, CredentialScope},
    plugins::managed::ManagedInstance,
    plugins::Plugins,
    protocol::*,
    source::{BootstrapLimits, Journal, TemporalSource},
};
use drasi_index_rocksdb::RocksDbIndexProvider;
use drasi_lib::{queries::output_state::FetchError, DrasiLib, Query, StateStoreProvider};
use drasi_state_store_redb::RedbStateStoreProvider;
use drasi_wal_redb::RedbWalProvider;
use fs2::FileExt;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    path::Path,
    sync::Arc,
};
use tokio::sync::Mutex;

const CONTROL: &str = "temporal-control-v1";
const SUBSCRIPTIONS: &str = "temporal-handoffs-v1";

mod catalog;
use catalog::{AdmissionLedger, Phase, Retirement};
mod bindings;
mod recovery;

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
pub(crate) mod plugin_tests;
#[cfg(test)]
pub(crate) mod postgres_tests;
#[cfg(test)]
pub(crate) mod shutdown_tests;
#[cfg(test)]
pub(crate) mod tests;

#[derive(Clone, Serialize, Deserialize)]
struct Handoff {
    definition: Subscription,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    binding: Option<WatchBinding>,
    incarnation: String,
    cursor: u64,
    pending: Option<Event>,
    last_receipt: Option<Receipt>,
    deleted: bool,
    #[serde(default)]
    results: Option<BTreeMap<String, Value>>,
}

pub struct Host {
    config: Config,
    core: Option<DrasiLib>,
    store: Option<Arc<dyn StateStoreProvider>>,
    sources: Mutex<BTreeMap<String, TemporalSource>>,
    managed_sources: Mutex<BTreeMap<String, ManagedInstance>>,
    deferred: Mutex<BTreeSet<String>>,
    plugins: Option<Mutex<Plugins>>,
    handoffs: Mutex<()>,
    binding_gates: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
    catalog: Option<Mutex<AdmissionLedger>>,
    topology: Mutex<()>,
    cleanup_failure: Mutex<Option<Error>>,
    shutdown: Shutdown,
    identity: String,
    credential_graph: uuid::Uuid,
    _lock: Option<File>,
}

enum Shutdown {
    Open,
    Closing,
    Closed,
    Failed(Error),
}

fn normalize_changes(
    results: &mut BTreeMap<String, Value>,
    changes: &[drasi_lib::channels::ResultDiff],
) -> Result<Vec<OutputDiff>> {
    use drasi_lib::channels::ResultDiff;

    let mut output = Vec::new();
    for change in changes {
        let (signature, after) = match change {
            ResultDiff::Add {
                row_signature,
                data,
            } => (row_signature, Some(data)),
            ResultDiff::Update {
                row_signature,
                after,
                ..
            }
            | ResultDiff::Aggregation {
                row_signature,
                after,
                ..
            } => (row_signature, Some(after)),
            ResultDiff::Delete { row_signature, .. } => (row_signature, None),
            ResultDiff::Noop => continue,
        };
        if after.is_some_and(|row| !row.is_object()) {
            return Err(Error::new(7, "query result change row is invalid"));
        }
        let key = signature.to_string();
        let before = results.remove(&key);
        match (before, after) {
            (Some(before), Some(after)) => {
                if before != *after {
                    output.push(OutputDiff::Update {
                        row_signature: *signature,
                        before,
                        after: after.clone(),
                    });
                }
                results.insert(key, after.clone());
            }
            (None, Some(after)) => {
                output.push(OutputDiff::Add {
                    row_signature: *signature,
                    data: after.clone(),
                });
                results.insert(key, after.clone());
            }
            (Some(before), None) => {
                output.push(OutputDiff::Delete {
                    row_signature: *signature,
                    data: before,
                });
            }
            (None, None) => {}
        }
    }
    Ok(output)
}

impl Config {
    fn validate(&self) -> Result<()> {
        if self.version != VERSION
            || self.owner_epoch == 0
            || !Path::new(&self.state_dir).is_absolute()
        {
            return Err(Error::new(
                1,
                "require protocol version 1, nonzero owner epoch and absolute state directory",
            ));
        }
        identifier(&self.instance_id)?;
        if self.start_mode != StartMode::Automatic
            && (!self.sources.is_empty() || !self.queries.is_empty())
        {
            return Err(Error::new(
                1,
                "dormant startup requires an empty source/query envelope; use the admission catalog",
            ));
        }
        if self.sources.len() > 64
            || self.queries.len() > 256
            || self.plugins.len() > 64
            || self.limits.feed_events == 0
            || self.limits.feed_events > 1_000_000
            || self.limits.outbox_entries == 0
            || self.limits.outbox_entries > 100_000
            || self.limits.subscriptions == 0
            || self.limits.subscriptions > 10_000
            || self.limits.snapshot_rows == 0
            || self.limits.snapshot_rows > 100_000
            || self.limits.pending_bytes == 0
            || self.limits.pending_bytes > MAX_OUTPUT / 2
        {
            return Err(Error::new(1, "native runtime resource limits are invalid"));
        }
        let mut sources = BTreeSet::new();
        for source in &self.sources {
            identifier(&source.id)?;
            if !sources.insert(&source.id) {
                return Err(Error::new(1, "duplicate source id"));
            }
            match source.kind {
                SourceKind::Temporal
                    if source.plugin.is_some()
                        || source.bootstrap.is_some()
                        || source.plugin_kind.is_some() =>
                {
                    return Err(Error::new(
                        1,
                        "built-in Temporal sources cannot use native plugin configuration",
                    ))
                }
                SourceKind::Plugin
                    if source.plugin.is_none()
                        || source.plugin_kind.is_none()
                        || source.bootstrap.is_none() =>
                {
                    return Err(Error::new(
                        1,
                        "plugin sources require explicit source and bootstrap descriptors",
                    ))
                }
                _ => {}
            }
        }
        let mut queries = BTreeSet::new();
        for query in &self.queries {
            identifier(&query.id)?;
            if !queries.insert(&query.id)
                || query.revision == 0
                || query.text.is_empty()
                || query.sources.is_empty()
                || query.sources.iter().any(|source| !sources.contains(source))
            {
                return Err(Error::new(
                    1,
                    "invalid query definition or source reference",
                ));
            }
            if query.sources.iter().collect::<BTreeSet<_>>().len() != query.sources.len() {
                return Err(Error::new(1, "duplicate query source reference"));
            }
        }
        for plugin in &self.plugins {
            crate::plugins::validate_policy(plugin, &self.trust)?;
        }
        if self.managed_plugins.len() > 64 {
            return Err(Error::new(1, "managed plugin policy capacity exceeded"));
        }
        let mut pairs = BTreeSet::new();
        for pair in &self.managed_plugins {
            pair.source.validate()?;
            pair.bootstrap.validate()?;
            if !pairs.insert(serde_json::to_vec(pair)?) {
                return Err(Error::new(1, "duplicate managed plugin policy"));
            }
        }
        crate::plugins::validate_catalogs(&self.catalogs, &self.trust)?;
        Ok(())
    }

    fn durable_definition(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&json!({
            "version": self.version, "upstream": UPSTREAM, "instance_id": self.instance_id,
            "sources": self.sources, "queries": self.queries,
            "plugins": self.plugins, "limits": self.limits, "catalog_version": 1,
        }))?)
    }
}

impl QueryConfig {
    fn native(
        &self,
        recovery: drasi_lib::RecoveryPolicy,
        outbox_entries: usize,
        auto_start: bool,
    ) -> drasi_lib::config::QueryConfig {
        let mut builder = match self.language {
            Language::Cypher => Query::cypher(&self.id),
            Language::Gql => Query::gql(&self.id),
        }
        .query(&self.text)
        .with_recovery_policy(recovery)
        .with_outbox_capacity(outbox_entries)
        .auto_start(auto_start);
        for source in &self.sources {
            builder = builder.from_source(source);
        }
        builder.build()
    }
}

impl Host {
    pub async fn open(config: Config) -> Result<Self> {
        config.validate()?;
        if config.start_mode == StartMode::CheckpointRecovery {
            return Self::open_checkpoint_recovery(config).await;
        }
        if let Some(checkpoint) = config.restore_from.as_ref() {
            checkpoint::restore(Path::new(checkpoint), Path::new(&config.state_dir), &config)?;
        }
        std::fs::create_dir_all(&config.state_dir).map_err(io)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config.state_dir, std::fs::Permissions::from_mode(0o700))
                .map_err(io)?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(Path::new(&config.state_dir).join("owner.lock"))
            .map_err(io)?;
        lock.try_lock_exclusive()
            .map_err(|_| Error::new(3, "native directory is already owned by another runtime"))?;
        let mut existing_directory = false;
        for entry in std::fs::read_dir(&config.state_dir).map_err(io)? {
            if entry.map_err(io)?.file_name() != "owner.lock" {
                existing_directory = true;
                break;
            }
        }
        if existing_directory {
            let metadata =
                std::fs::symlink_metadata(Path::new(&config.state_dir).join("state.redb"))
                    .map_err(io)?;
            if !metadata.is_file() || metadata.len() == 0 {
                return Err(Error::new(
                    7,
                    "existing native directory has no retained state database",
                ));
            }
        }
        let store: Arc<dyn StateStoreProvider> = Arc::new(
            RedbStateStoreProvider::new(Path::new(&config.state_dir).join("state.redb"))
                .map_err(io)?,
        );
        let definition = config.durable_definition()?;
        let previous = store.get(CONTROL, "definition").await.map_err(io)?;
        let recovering = previous.is_some();
        if let Some(previous) = previous {
            if previous != definition
                && !recovery::is_isolated_retired_restore(store.as_ref(), &config, &previous)
                    .await?
            {
                return Err(Error::new(3, "runtime definition changed; use a new isolated generation instead of resetting persisted queries"));
            }
            let epoch = store
                .get(CONTROL, "owner_epoch")
                .await
                .map_err(io)?
                .ok_or_else(|| Error::new(7, "persisted owner epoch is missing"))?;
            if epoch != config.owner_epoch.to_be_bytes() && config.restore_from.is_none() {
                return Err(Error::new(
                    3,
                    "owner epoch changed without an isolated checkpoint restore",
                ));
            }
        } else if existing_directory {
            return Err(Error::new(
                7,
                "native admission metadata is missing in a nonempty directory; explicit recovery or reset is required",
            ));
        }
        let identity = match store.get(CONTROL, "identity").await.map_err(io)? {
            Some(bytes) => {
                let identity = String::from_utf8(bytes)
                    .map_err(|_| Error::new(7, "persisted runtime identity is invalid"))?;
                uuid::Uuid::parse_str(&identity)
                    .map_err(|_| Error::new(7, "persisted runtime identity is invalid"))?;
                identity
            }
            None if recovering => {
                return Err(Error::new(7, "persisted runtime identity is missing"))
            }
            None => uuid::Uuid::new_v4().to_string(),
        };
        let mut catalog = match store.get(CONTROL, "catalog").await.map_err(io)? {
            Some(bytes) => serde_json::from_slice::<AdmissionLedger>(&bytes)
                .map_err(|_| Error::new(7, "persisted admission catalog is invalid"))?,
            None if recovering => {
                return Err(Error::new(7, "persisted admission catalog is missing"))
            }
            None => AdmissionLedger::default(),
        };
        catalog.validate(&config)?;
        let credential_graph = uuid::Uuid::parse_str(&identity)
            .map_err(|_| Error::new(7, "persisted runtime identity is invalid"))?;
        if CredentialLease::retained_for_graph(credential_graph)? {
            return Err(Error::new(
                3,
                "previous plugin cleanup is unproved; process recovery is required",
            ));
        }
        bindings::validate_recovery(store.as_ref(), &config, &catalog).await?;
        let mut completed_retirements = false;
        for admission in catalog.nodes.values_mut() {
            if admission.retirement == Some(Retirement::Retiring) {
                admission.retirement = Some(Retirement::Retired);
                completed_retirements = true;
            }
        }
        catalog.validate(&config)?;
        let catalog_bytes = serde_json::to_vec(&catalog)?;
        // Creation is reserved before SDK I/O; retries must recover existing state.
        store
            .set_many(
                CONTROL,
                &[
                    ("definition", &definition),
                    ("identity", identity.as_bytes()),
                    ("owner_epoch", &config.owner_epoch.to_be_bytes()),
                    ("catalog", &catalog_bytes),
                ],
            )
            .await
            .map_err(io)?;
        if completed_retirements {
            store.sync().await.map_err(io)?;
        }
        let plugins = Plugins::open(
            &config.plugins,
            &config.catalogs,
            &config.trust,
            &Path::new(&config.state_dir).join("plugins"),
            &config.instance_id,
        )
        .await?;
        let journal = Arc::new(Journal::new(store.clone(), config.limits.feed_events));
        let indexes =
            RocksDbIndexProvider::new(Path::new(&config.state_dir).join("indexes"), true, false)
                .with_memory_budget_bytes(256 << 20)
                .map_err(io)?;
        let mut builder = DrasiLib::builder()
            .with_id(&config.instance_id)
            .with_bootstrap_certification(true)
            .with_default_index_provider("persistent", Arc::new(indexes))
            .with_state_store_provider(store.clone())
            .with_wal_provider(Arc::new(RedbWalProvider::new(
                Path::new(&config.state_dir).join("wal"),
            )))
            .with_default_recovery_policy(drasi_lib::RecoveryPolicy::ExistingOnly);
        let mut source_configs = config.sources.clone();
        let mut query_configs = config.queries.clone();
        let mut deferred = BTreeSet::new();
        for (id, admission) in &catalog.nodes {
            if admission.retirement.is_none()
                && admission.phase != Phase::Intended
                && matches!(admission.definition.recipe, NodeRecipe::PluginSource { .. })
            {
                deferred.insert(id.clone());
            }
        }
        for (id, admission) in &catalog.nodes {
            if admission.retirement.is_none()
                && admission.phase != Phase::Intended
                && matches!(&admission.definition.recipe, NodeRecipe::Query { sources, .. } if sources.iter().any(|source| deferred.contains(source)))
            {
                deferred.insert(id.clone());
            }
        }
        for (id, admission) in &catalog.nodes {
            if admission.retirement.is_none()
                && admission.phase != Phase::Intended
                && !deferred.contains(id)
                && admission.fault.is_none()
            {
                source_configs.extend(admission.definition.source_config(id));
                query_configs.extend(admission.definition.query_config(id));
            }
        }
        let mut sources = BTreeMap::new();
        let auto_start = config.start_mode == StartMode::Automatic;
        for source in &source_configs {
            match source.kind {
                SourceKind::Temporal => {
                    let mut instance = TemporalSource::new(
                        &source.id,
                        journal.clone(),
                        auto_start,
                        BootstrapLimits::new(
                            config.limits.snapshot_rows,
                            config.limits.pending_bytes,
                        )
                        .map_err(Error::from)?,
                    )
                    .map_err(Error::from)?;
                    if !source.config.is_null() {
                        instance = instance
                            .with_history(serde_json::from_value(source.config.clone())?)?;
                        instance.prepare_history(!recovering).await?;
                    }
                    builder = builder.with_source(instance.shared());
                    sources.insert(source.id.clone(), instance);
                }
                SourceKind::Plugin => {
                    builder = builder.with_source(plugins.source(source, auto_start).await?)
                }
            }
        }
        for query in &query_configs {
            builder = builder.with_query(query.native(
                if recovering {
                    drasi_lib::RecoveryPolicy::ExistingOnly
                } else {
                    drasi_lib::RecoveryPolicy::Strict
                },
                config.limits.outbox_entries,
                auto_start,
            ));
        }
        let core = builder.build().await.map_err(Error::from)?;
        let mut host = Self {
            config,
            core: Some(core),
            store: Some(store),
            sources: Mutex::new(sources),
            managed_sources: Mutex::new(BTreeMap::new()),
            deferred: Mutex::new(deferred),
            plugins: Some(Mutex::new(plugins)),
            handoffs: Mutex::new(()),
            binding_gates: Mutex::new(BTreeMap::new()),
            catalog: Some(Mutex::new(catalog)),
            topology: Mutex::new(()),
            cleanup_failure: Mutex::new(None),
            shutdown: Shutdown::Open,
            identity,
            credential_graph,
            _lock: Some(lock),
        };
        let startup = async {
            let core = host.core()?;
            core.start().await.map_err(Error::from)?;
            // The SDK starts queries best-effort and can return success with failed admissions.
            for query in &host.config.queries {
                let runtime = core
                    .query_manager()
                    .get_query_instance(&query.id)
                    .await
                    .map_err(|reason| Error::new(7, reason))?;
                let status = runtime.status().await;
                if !matches!(
                    status,
                    drasi_lib::channels::ComponentStatus::Starting
                        | drasi_lib::channels::ComponentStatus::Running
                ) {
                    return Err(Error::new(
                        3,
                        format!(
                            "query '{}' failed native admission ({status:?}); recovery or explicit reset is required",
                            query.id
                        ),
                    ));
                }
            }
            Ok(())
        }
        .await;
        if let Err(error) = startup {
            if let Err(cleanup) = host.close().await {
                return Err(Error::new(
                    error.code,
                    format!(
                        "{}; native startup cleanup failed: {}",
                        error.message, cleanup.message
                    ),
                ));
            }
            return Err(error);
        }
        Ok(host)
    }

    fn core(&self) -> Result<&DrasiLib> {
        self.core
            .as_ref()
            .ok_or_else(|| Error::new(3, "runtime is closed"))
    }

    fn store(&self) -> Result<&Arc<dyn StateStoreProvider>> {
        self.store
            .as_ref()
            .ok_or_else(|| Error::new(3, "runtime is closed"))
    }

    async fn query_config(&self, id: &str) -> Result<QueryConfig> {
        let query = if let Some(query) = self.config.queries.iter().find(|query| query.id == id) {
            query.clone()
        } else {
            self.catalog()?
                .lock()
                .await
                .nodes
                .get(id)
                .filter(|admission| admission.retirement.is_none())
                .and_then(|admission| admission.definition.query_config(id))
                .ok_or_else(|| Error::new(2, "query is not configured"))?
        };
        for id in &query.sources {
            let source = self
                .sources
                .lock()
                .await
                .get(id)
                .map(TemporalSource::shared);
            if let Some(source) = source {
                source.require_input_ready().await?;
            }
        }
        Ok(query)
    }

    async fn history_source(&self, id: &str) -> Result<TemporalSource> {
        self.sources
            .lock()
            .await
            .get(id)
            .map(TemporalSource::shared)
            .ok_or_else(|| Error::new(2, "built-in Temporal source is not retained"))
    }

    fn catalog(&self) -> Result<&Mutex<AdmissionLedger>> {
        self.catalog
            .as_ref()
            .ok_or_else(|| Error::new(3, "runtime is closed"))
    }

    fn incarnation(&self, query: &str, hash: u64, generation: u64) -> String {
        format!("{}:{query}:{hash:016x}:{generation}", self.identity)
    }

    pub async fn apply(&self, command: Command) -> Result<Value> {
        self.core()?;
        match command {
            Command::AdmitNode { definition } => {
                Ok(json!({ "node": self.admit_node(definition).await? }))
            }
            Command::CheckPluginSource { definition } => Ok(json!({
                "materialization_required": self.check_plugin_source(definition).await?
            })),
            Command::ValidatePluginSources { recipes } => Ok(json!({
                "source_schemas": self.validate_plugin_sources(recipes).await?
            })),
            Command::ObserveNodes => Ok(json!({
                "nodes": self.observe_nodes().await?,
                "topology_busy": self.topology.try_lock().is_err(),
                "cleanup_failure": self.cleanup_failure.lock().await.as_ref().map(|error| error.message.clone())
            })),
            Command::StopNode { native_id } => {
                Ok(json!({ "node": self.stop_node(&native_id).await? }))
            }
            Command::RetireNode { native_id } => {
                Ok(json!({ "node": self.retire_node(&native_id).await? }))
            }
            Command::Feed {
                source_id,
                position,
                change,
            } => {
                let source = self
                    .sources
                    .lock()
                    .await
                    .get(&source_id)
                    .map(TemporalSource::shared)
                    .ok_or_else(|| {
                        Error::new(2, "source is not an application-fed Temporal source")
                    })?;
                source.feed(position, change).await?;
                Ok(json!({ "position": position }))
            }
            Command::HistoryProgress {
                source_id,
                shard_id,
            } => {
                let source = self.history_source(&source_id).await?;
                Ok(json!({ "progress": source.history_progress(shard_id).await? }))
            }
            Command::AcceptHistory { source_id, step } => {
                let source = self.history_source(&source_id).await?;
                Ok(json!({ "receipt": source.accept_history(step).await? }))
            }
            Command::BlockHistory { source_id, problem } => {
                self.history_source(&source_id)
                    .await?
                    .block_history(problem)
                    .await?;
                Ok(json!({}))
            }
            Command::SuspendHistory { source_id } => {
                self.history_source(&source_id)
                    .await?
                    .suspend_history()
                    .await?;
                Ok(json!({}))
            }
            Command::Subscribe { subscription } => {
                self.subscribe(subscription, None).await?;
                Ok(json!({}))
            }
            Command::BindWatch { binding, query_id } => {
                let subscription = Subscription {
                    id: binding.subscription_id()?,
                    query_id,
                    query_revision: 1,
                };
                bindings::validate_binding(
                    &*self.catalog()?.lock().await,
                    &subscription,
                    &binding,
                )?;
                let id = subscription.id.clone();
                let gate = {
                    let mut gates = self.binding_gates.lock().await;
                    gates.retain(|_, gate| Arc::strong_count(gate) > 1);
                    if gates.len() >= self.config.limits.subscriptions && !gates.contains_key(&id) {
                        return Err(Error::new(3, "binding operation capacity exhausted"));
                    }
                    gates
                        .entry(id.clone())
                        .or_insert_with(|| Arc::new(Mutex::new(())))
                        .clone()
                };
                let outcome = match gate.clone().try_lock_owned() {
                    Ok(_operation) => self.subscribe(subscription, Some(binding)).await,
                    Err(_) => Err(Error::new(
                        3,
                        "watch binding operation is already in progress",
                    )),
                };
                let mut gates = self.binding_gates.lock().await;
                if Arc::strong_count(&gate) == 2 {
                    gates.remove(&id);
                }
                let observation = outcome?;
                Ok(json!({"subscription": observation}))
            }
            Command::ListSubscriptions { after, limit } => {
                self.list_subscriptions(&after, limit).await
            }
            Command::PollWatch { subscription_id } => {
                identifier(&subscription_id)?;
                let _handoffs = self.handoffs.lock().await;
                let handoff = self.handoff(&subscription_id).await?;
                if handoff.binding.is_none() || handoff.deleted {
                    return Err(Error::new(
                        3,
                        "watch polling requires an active public binding",
                    ));
                }
                Ok(json!({"event": self.poll_handoff(handoff).await?}))
            }
            Command::Unsubscribe { subscription_id } => {
                let _handoffs = self.handoffs.lock().await;
                let mut handoff = self.handoff(&subscription_id).await?;
                handoff.deleted = true;
                handoff.pending = None;
                handoff.results = None;
                self.persist(&handoff).await?;
                Ok(json!({}))
            }
            Command::Snapshot { query_id } => {
                Ok(json!({ "snapshot": self.snapshot(&query_id, "").await? }))
            }
            Command::Descriptors => {
                Ok(json!({ "plugins": self.plugins()?.lock().await.descriptors()? }))
            }
            Command::ListCatalogs => {
                Ok(json!({ "catalogs": self.plugins()?.lock().await.catalogs() }))
            }
            Command::DiscoverPlugins {
                catalog_id,
                search,
                offset,
                limit,
            } => {
                let (plugins, more) = self
                    .plugins()?
                    .lock()
                    .await
                    .discover_page(&catalog_id, &search, offset, limit)
                    .await?;
                Ok(json!({ "discovered_plugins": plugins, "more_plugins": more }))
            }
            Command::InspectPlugin { selection } => Ok(json!({
                "plugin": self.plugins()?.lock().await.inspect(&selection).await?,
            })),
            Command::InstallPlugin { selection, digest } => Ok(json!({
                "installation": self.plugins()?.lock().await.install(&selection, &digest).await?,
            })),
            Command::LoadPlugin { digest } => Ok(json!({
                "installation": self.plugins()?.lock().await.load_digest(&digest).await?,
            })),
            Command::ListPluginInstallations => Ok(json!({
                "installations": self.plugins()?.lock().await.installations()?,
            })),
            Command::GetPluginInstallation { digest } => Ok(json!({
                "installation": self.plugins()?.lock().await.installation(&digest)?,
            })),
        }
    }

    pub async fn checkpoint(&mut self, destination: &str) -> Result<Value> {
        self.close().await?;
        let retirement = recovery::retire(&self.config).await?;
        let checkpoint = if self.config.start_mode == StartMode::CheckpointRecovery {
            checkpoint::publish_recovered(
                Path::new(&self.config.state_dir),
                Path::new(destination),
                &self.config,
                &retirement,
            )?
        } else {
            checkpoint::publish(
                Path::new(&self.config.state_dir),
                Path::new(destination),
                &self.config,
                Some(retirement),
            )?
        };
        Ok(json!({ "checkpoint": checkpoint }))
    }

    fn plugins(&self) -> Result<&Mutex<Plugins>> {
        self.plugins
            .as_ref()
            .ok_or_else(|| Error::new(3, "runtime is closed"))
    }

    async fn has_subscription(
        &self,
        subscription: &Subscription,
        binding: Option<&WatchBinding>,
    ) -> Result<bool> {
        let Some(bytes) = self
            .store()?
            .get(SUBSCRIPTIONS, &subscription.id)
            .await
            .map_err(io)?
        else {
            return Ok(false);
        };
        let previous: Handoff = serde_json::from_slice(&bytes)?;
        if previous.deleted
            || previous.definition != *subscription
            || previous.binding.as_ref() != binding
        {
            return Err(Error::new(
                3,
                "subscription id is already bound to another definition or tombstone",
            ));
        }
        Ok(true)
    }

    async fn snapshot(&self, query_id: &str, subscription_id: &str) -> Result<Event> {
        let revision = self.query_config(query_id).await?.revision;
        self.require_query_resident(query_id).await?;
        let query = self
            .core()?
            .query_manager()
            .get_query_instance(query_id)
            .await
            .map_err(|error| Error::new(2, error))?;
        if query.is_volatile() {
            return Err(Error::new(
                3,
                "durable handoff cannot attach to a volatile query",
            ));
        }
        let snapshot = query.fetch_snapshot().await.map_err(fetch)?;
        if snapshot.len() > self.config.limits.snapshot_rows {
            return Err(Error::new(3, "snapshot exceeds configured row limit"));
        }
        let sequence = snapshot.as_of_sequence;
        let incarnation =
            self.incarnation(query_id, snapshot.config_hash, snapshot.output_generation);
        let mut rows: Vec<_> = snapshot
            .stream_keyed()
            .map(|(key, value)| Row {
                key: key.to_string(),
                value,
            })
            .collect()
            .await;
        rows.sort_by(|left, right| left.key.cmp(&right.key));
        let event = Event {
            subscription_id: subscription_id.into(),
            query_id: query_id.into(),
            query_revision: revision,
            incarnation,
            sequence,
            kind: EventKind::Snapshot,
            rows,
            changes: Vec::new(),
        };
        self.check_size(&event)?;
        Ok(event)
    }

    fn check_size(&self, event: &Event) -> Result<()> {
        if serde_json::to_vec(event)?.len() > self.config.limits.pending_bytes {
            return Err(Error::new(3, "handoff event exceeds configured byte limit"));
        }
        Ok(())
    }

    async fn persist(&self, handoff: &Handoff) -> Result<()> {
        self.store()?
            .set(
                SUBSCRIPTIONS,
                &handoff.definition.id,
                serde_json::to_vec(handoff)?,
            )
            .await
            .map_err(io)
    }

    async fn handoff(&self, id: &str) -> Result<Handoff> {
        let bytes = self
            .store()?
            .get(SUBSCRIPTIONS, id)
            .await
            .map_err(io)?
            .ok_or_else(|| Error::new(2, "subscription does not exist"))?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub async fn poll(&self, limit: usize) -> Result<Vec<Event>> {
        self.core()?;
        if limit == 0 || limit > 64 {
            return Err(Error::new(1, "poll limit must be in 1..64"));
        }
        let _handoffs = self.handoffs.lock().await;
        let mut events = Vec::new();
        let mut bytes = 0;
        let mut keys = self.store()?.list_keys(SUBSCRIPTIONS).await.map_err(io)?;
        keys.sort();
        for id in keys {
            let handoff = self.handoff(&id).await?;
            if handoff.deleted {
                continue;
            }
            if let Some(event) = self.poll_handoff(handoff).await? {
                bytes += serde_json::to_vec(&event)?.len();
                if bytes > MAX_OUTPUT - 1024 {
                    break;
                }
                events.push(event);
                if events.len() == limit {
                    break;
                }
            }
        }
        Ok(events)
    }

    async fn poll_handoff(&self, mut handoff: Handoff) -> Result<Option<Event>> {
        self.query_config(&handoff.definition.query_id).await?;
        let results = handoff.results.as_mut().ok_or_else(|| {
            Error::new(
                6,
                "subscription has no prior results; explicit reset is required",
            )
        })?;
        if handoff.pending.is_none() {
            self.require_query_resident(&handoff.definition.query_id)
                .await?;
            let query = self
                .core()?
                .query_manager()
                .get_query_instance(&handoff.definition.query_id)
                .await
                .map_err(|error| Error::new(2, error))?;
            let outbox = query.fetch_outbox(handoff.cursor).await.map_err(fetch)?;
            let incarnation = self.incarnation(
                &handoff.definition.query_id,
                outbox.config_hash,
                outbox.output_generation,
            );
            if incarnation != handoff.incarnation {
                return Err(Error::new(
                    6,
                    "query output incarnation changed; explicit reset is required",
                ));
            }
            if let Some(result) = outbox.results.first() {
                if handoff.cursor.checked_add(1) != Some(result.sequence) {
                    return Err(Error::new(6, "non-contiguous query outbox sequence"));
                }
                let changes = normalize_changes(results, &result.results)?;
                if results.len() > self.config.limits.snapshot_rows
                    || serde_json::to_vec(&results)?.len() > self.config.limits.pending_bytes
                {
                    return Err(Error::new(
                        3,
                        "subscription results exceed configured limits",
                    ));
                }
                let event = Event {
                    subscription_id: handoff.definition.id.clone(),
                    query_id: result.query_id.clone(),
                    query_revision: handoff.definition.query_revision,
                    incarnation,
                    sequence: result.sequence,
                    kind: EventKind::Delta,
                    rows: Vec::new(),
                    changes,
                };
                self.check_size(&event)?;
                handoff.pending = Some(event);
                self.persist(&handoff).await?;
            }
        }
        Ok(handoff.pending)
    }

    pub async fn ack(&self, receipt: Receipt) -> Result<()> {
        self.core()?;
        if receipt.acceptance_id.is_empty() || receipt.acceptance_id.len() > 1024 {
            return Err(Error::new(
                1,
                "receipt must identify the durable Temporal acceptance",
            ));
        }
        let _handoffs = self.handoffs.lock().await;
        let mut handoff = self.handoff(&receipt.subscription_id).await?;
        if handoff.deleted
            || receipt.query_revision != handoff.definition.query_revision
            || receipt.incarnation != handoff.incarnation
        {
            return Err(Error::new(
                3,
                "receipt does not match the active subscription revision and incarnation",
            ));
        }
        if let Some(previous) = &handoff.last_receipt {
            if serde_json::to_vec(previous)? == serde_json::to_vec(&receipt)? {
                return Ok(());
            }
        }
        let pending = handoff
            .pending
            .as_ref()
            .ok_or_else(|| Error::new(3, "receipt has no pending handoff"))?;
        if receipt.sequence != pending.sequence || receipt.kind != pending.kind {
            return Err(Error::new(
                6,
                "receipt skips the pending snapshot or output sequence",
            ));
        }
        handoff.cursor = receipt.sequence;
        handoff.pending = None;
        handoff.last_receipt = Some(receipt);
        self.persist(&handoff).await
    }

    pub async fn close(&mut self) -> Result<()> {
        if matches!(self.shutdown, Shutdown::Open) {
            if CredentialLease::quarantined_for_graph(self.credential_graph)? {
                self.shutdown = Shutdown::Failed(Error::new(
                    3,
                    "native plugin cleanup is quarantined; checkpointing is forbidden",
                ));
            }
            if let Some(error) = self.cleanup_failure.lock().await.clone() {
                self.shutdown = Shutdown::Failed(error);
            }
        }
        let failure = match &self.shutdown {
            Shutdown::Closed => return Ok(()),
            Shutdown::Closing => Some(Error::new(3, "native shutdown did not complete")),
            Shutdown::Failed(error) => Some(error.clone()),
            Shutdown::Open => None,
        };
        if let Some(error) = failure {
            self.shutdown = Shutdown::Failed(error.clone());
            if let Err(cleanup) = self.close_graph().await {
                return Err(Error::new(
                    error.code,
                    format!("{}; cleanup retry: {}", error.message, cleanup.message),
                ));
            }
            return Err(error);
        }
        self.shutdown = Shutdown::Closing;
        let result = self.close_graph().await;
        self.shutdown = match &result {
            Ok(()) => Shutdown::Closed,
            Err(error) => Shutdown::Failed(error.clone()),
        };
        result
    }

    async fn close_graph(&mut self) -> Result<()> {
        if let Some(core) = self.core.as_ref() {
            core.shutdown().await.map_err(Error::from)?;
            // Pinned Drasi graph runtimes reference their managers and graph. Break those
            // in-memory cycles after shutdown without deprovisioning persistent data.
            let mut sources = Vec::new();
            let mut queries = Vec::new();
            let mut reactions = Vec::new();
            {
                let graph = core.component_graph();
                let mut graph = graph.write().await;
                let nodes = graph.snapshot().nodes;
                for node in nodes {
                    use drasi_lib::component_graph::ComponentKind;
                    match node.kind {
                        ComponentKind::Source => {
                            sources
                                .extend(graph.take_runtime::<Arc<dyn drasi_lib::Source>>(&node.id));
                        }
                        ComponentKind::Query => {
                            queries.extend(
                                graph.take_runtime::<Arc<dyn drasi_lib::queries::Query>>(&node.id),
                            );
                        }
                        ComponentKind::Reaction => {
                            reactions.extend(
                                graph.take_runtime::<Arc<dyn drasi_lib::Reaction>>(&node.id),
                            );
                        }
                        _ => {}
                    }
                }
            }
            self.sources.get_mut().clear();
            self.plugins.take();
            self.catalog.take();
            drop(sources);
            drop(queries);
            drop(reactions);
            self.core.take();
            let managed = std::mem::take(self.managed_sources.get_mut());
            let mut failure = None;
            for instance in managed.into_values() {
                if let Err(error) = instance.close().await {
                    failure.get_or_insert(error);
                }
            }
            if let Some(error) = failure {
                return Err(error);
            }
            if CredentialLease::retained_for_graph(self.credential_graph)? {
                return Err(Error::new(
                    3,
                    "native plugin credentials remain retained; checkpointing is forbidden",
                ));
            }
            if let Some(store) = self.store.take() {
                store.sync().await.map_err(io)?;
            }
        }
        Ok(())
    }

    async fn require_query_resident(&self, id: &str) -> Result<()> {
        if self.deferred.lock().await.contains(id) {
            return Err(Error::new(
                3,
                "query output is deferred until its exact plugin dependencies recover",
            ));
        }
        Ok(())
    }

    async fn require_admission_safe(&self) -> Result<()> {
        if self.cleanup_failure.lock().await.is_some()
            || CredentialLease::quarantined_for_graph(self.credential_graph)?
        {
            return Err(Error::new(
                3,
                "native cleanup is unproved; new admission is forbidden",
            ));
        }
        Ok(())
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        if CredentialLease::retained_for_graph(self.credential_graph).unwrap_or(true) {
            if let Some(lock) = self._lock.take() {
                std::mem::forget(lock);
            }
            if let Some(core) = self.core.take() {
                std::mem::forget(core);
            }
            if let Some(store) = self.store.take() {
                std::mem::forget(store);
            }
        }
    }
}

fn fetch(error: FetchError) -> Error {
    Error::new(
        if matches!(error, FetchError::OutboxGap(_)) {
            6
        } else {
            3
        },
        error.to_string(),
    )
}

fn io(error: impl std::fmt::Display) -> Error {
    Error::new(7, error.to_string())
}

#[cfg(test)]
mod normalized_output_tests {
    use super::*;

    #[test]
    fn aggregation_uses_membership_instead_of_default_before_values() {
        use drasi_lib::channels::ResultDiff;

        let mut results = BTreeMap::new();
        let diff = |before, after| ResultDiff::Aggregation {
            row_signature: 7,
            before,
            after,
        };
        let zero = json!({"group": "group", "total": 0.0});
        let first = json!({"group": "group", "total": 12.0});
        let second = json!({"group": "group", "total": 13.0});
        let entered = normalize_changes(&mut results, &[diff(Some(zero), first.clone())]).unwrap();
        assert_eq!(
            serde_json::to_value(entered).unwrap(),
            json!([{"type": "ADD", "row_signature": 7, "data": first}])
        );
        let updated = normalize_changes(&mut results, &[diff(None, second.clone())]).unwrap();
        assert_eq!(
            serde_json::to_value(updated).unwrap(),
            json!([{"type": "UPDATE", "row_signature": 7, "before": first, "after": second}])
        );
        assert!(normalize_changes(
            &mut results,
            &[diff(None, second.clone()), ResultDiff::Noop]
        )
        .unwrap()
        .is_empty());
        let left = normalize_changes(
            &mut results,
            &[ResultDiff::Delete {
                row_signature: 7,
                data: second.clone(),
            }],
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(left).unwrap(),
            json!([{"type": "DELETE", "row_signature": 7, "data": second}])
        );
        assert!(results.is_empty());
    }
}
