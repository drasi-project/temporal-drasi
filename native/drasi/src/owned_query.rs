//! Owns query epochs and their joins without using DrasiQuery's private tasks.
//! Complete source snapshots become visible only after a sealed index transaction.
//! The durable JSON format is private to this actor; source positions remain opaque.

mod admission;
mod codec;
mod durable;
mod inputs;
#[cfg(test)]
mod tests;

use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use drasi_core::{
    evaluation::functions::FunctionRegistry,
    interface::{IndexBackendPlugin, SessionGuard},
    middleware::MiddlewareTypeRegistry,
    models::{QueryJoin, QueryJoinKey},
    query::{ContinuousQuery, QueryBuilder},
};
use drasi_functions_cypher::CypherFunctionSet;
use drasi_functions_gql::GQLFunctionSet;
use drasi_lib::{
    channels::{
        ChangeReceiver, ComponentStatus, QueryResult, QuerySubscriptionResponse, SourceEvent,
    },
    component_graph::ComponentGraph,
    config::{QueryConfig, QueryLanguage},
    context::QueryRuntimeContext,
    queries::{
        compute_config_hash, output_epoch_hash, FetchError, OutboxResponse, Query, QueryBase,
        QueryOutputState, SnapshotResponse,
    },
    sources::SourceManager,
    DrasiLib,
};
use drasi_query_cypher::CypherParser;
use drasi_query_gql::GQLParser;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    sync::{broadcast, watch, Mutex, Notify, RwLock},
    task::JoinHandle,
};

use durable::{index_error, State, Storage};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cut {
    upstream: Option<u64>,
    position: Vec<u8>,
    offset: u64,
}

struct RunOwner {
    stop: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

struct Publication {
    output: Option<QueryOutputState>,
    sender: broadcast::Sender<Arc<QueryResult>>,
}

#[derive(Clone)]
struct Failure {
    message: String,
    output_capacity: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct OutputCapacityExceeded;

impl std::fmt::Display for OutputCapacityExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("subscription results exceed configured limits")
    }
}

impl std::error::Error for OutputCapacityExceeded {}

impl From<OutputCapacityExceeded> for crate::protocol::Error {
    fn from(error: OutputCapacityExceeded) -> Self {
        Self::new(3, error.to_string())
    }
}

impl OutputCapacityExceeded {
    fn in_error(error: &anyhow::Error) -> bool {
        error.is::<Self>()
            || matches!(
                error.downcast_ref::<drasi_core::evaluation::EvaluationError>(),
                Some(drasi_core::evaluation::EvaluationError::IndexError(
                    drasi_core::interface::IndexError::Other(error)
                )) if error.is::<Self>()
            )
    }
}

#[derive(Clone)]
struct OwnedQuery {
    base: Arc<QueryBase>,
    indexes: Arc<dyn IndexBackendPlugin>,
    middleware: Arc<MiddlewareTypeRegistry>,
    graph: Weak<RwLock<ComponentGraph>>,
    logs: Arc<drasi_lib::managers::ComponentLogRegistry>,
    instance: String,
    limits: crate::protocol::Limits,
    config_hash: u64,
    capacity: usize,
    lifecycle: Arc<Mutex<Option<RunOwner>>>,
    publication: Arc<Mutex<Publication>>,
    changed: Arc<Notify>,
    active_inputs: Arc<std::sync::atomic::AtomicUsize>,
    inputs: Arc<Mutex<Option<inputs::Inputs>>>,
    failure: Arc<Mutex<Option<Failure>>>,
    admission: Arc<admission::Admission>,
}

/// `control_store` must survive loss of the query index directory.
/// `expected_existing` forbids treating an unwitnessed existing admission as new.
pub(crate) async fn install(
    core: &DrasiLib,
    indexes: Arc<dyn IndexBackendPlugin>,
    config: QueryConfig,
    limits: crate::protocol::Limits,
    control_store: Arc<dyn drasi_lib::StateStoreProvider>,
    expected_existing: bool,
) -> Result<()> {
    ensure!(
        !indexes.is_volatile(),
        "owned queries require a persistent index backend"
    );
    ensure!(
        limits.feed_events > 0
            && limits.outbox_entries > 0
            && limits.subscriptions > 0
            && limits.snapshot_rows > 0
            && limits.pending_bytes > 0,
        "owned query limits must be nonzero"
    );
    ensure!(
        !config.sources.is_empty(),
        "owned query requires at least one source"
    );
    ensure!(
        config
            .sources
            .iter()
            .all(|source| { !source.source_id.is_empty() && !source.source_id.contains('\0') })
            && config
                .sources
                .iter()
                .map(|source| &source.source_id)
                .collect::<HashSet<_>>()
                .len()
                == config.sources.len(),
        "owned query requires distinct nonempty source identifiers"
    );
    let capacity = config.outbox_capacity.max(1).min(limits.outbox_entries);
    let (sender, _) = broadcast::channel(
        config
            .dispatch_buffer_capacity
            .unwrap_or(1000)
            .max(1)
            .min(limits.outbox_entries),
    );
    let graph = core.component_graph();
    let base = Arc::new(QueryBase::new(config.clone())?);
    let config_hash = compute_config_hash(&config);
    let source_ids: Vec<_> = config
        .sources
        .iter()
        .map(|source| source.source_id.clone())
        .collect();
    let update_tx = {
        let mut graph = graph.write().await;
        graph.register_query(
            &config.id,
            HashMap::from([("auto_start".to_string(), config.auto_start.to_string())]),
            &source_ids,
        )?;
        graph.update_sender()
    };
    let admission = match admission::Admission::open(
        control_store,
        &config.id,
        config_hash,
        expected_existing,
        limits.pending_bytes,
    )
    .await
    {
        Ok(admission) => admission,
        Err(error) => {
            graph.write().await.remove_component(&config.id)?;
            return Err(error);
        }
    };
    let query: Arc<dyn Query> = Arc::new(OwnedQuery {
        config_hash,
        base: base.clone(),
        indexes,
        middleware: core.middleware_registry(),
        graph: Arc::downgrade(&graph),
        logs: core.log_registry(),
        instance: core.get_config().id.clone(),
        limits,
        capacity,
        lifecycle: Arc::new(Mutex::new(None)),
        publication: Arc::new(Mutex::new(Publication {
            output: None,
            sender,
        })),
        changed: Arc::new(Notify::new()),
        active_inputs: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        inputs: Arc::new(Mutex::new(None)),
        failure: Arc::new(Mutex::new(None)),
        admission,
    });
    base.initialize(QueryRuntimeContext::new(
        &core.get_config().id,
        &config.id,
        update_tx,
    ))
    .await;
    let mut graph = graph.write().await;
    if let Err(error) = graph.set_runtime(&config.id, Box::new(query)) {
        graph.remove_component(&config.id)?;
        return Err(error);
    }
    Ok(())
}

pub(crate) async fn check_output_failure(
    query: &dyn Query,
) -> std::result::Result<(), OutputCapacityExceeded> {
    let Some(query) = query.as_any().downcast_ref::<OwnedQuery>() else {
        return Ok(());
    };
    if query.base.get_status().await == ComponentStatus::Error
        && query
            .failure
            .lock()
            .await
            .as_ref()
            .is_some_and(|failure| failure.output_capacity)
    {
        return Err(OutputCapacityExceeded);
    }
    Ok(())
}

impl OwnedQuery {
    async fn build_core(&self, storage: &Storage) -> Result<ContinuousQuery> {
        let config = &self.base.config;
        let functions = Arc::new(FunctionRegistry::new());
        let functions = match config.query_language {
            QueryLanguage::Cypher => functions.with_cypher_function_set(),
            QueryLanguage::GQL => functions.with_gql_function_set(),
        };
        let mut builder = match config.query_language {
            QueryLanguage::Cypher => QueryBuilder::new(
                &config.query,
                Arc::new(CypherParser::new(functions.clone())),
            ),
            QueryLanguage::GQL => {
                QueryBuilder::new(&config.query, Arc::new(GQLParser::new(functions.clone())))
            }
        }
        .with_function_registry(functions)
        .with_middleware_registry(self.middleware.clone())
        .with_element_index(storage.created.set.element_index.clone())
        .with_archive_index(storage.created.set.archive_index.clone())
        .with_result_index(storage.created.set.result_index.clone())
        .with_future_queue(storage.created.set.future_queue.clone())
        .with_session_control(storage.created.set.session_control.clone());
        for middleware in &config.middleware {
            builder = builder.with_source_middleware(Arc::new(middleware.clone()));
        }
        for source in &config.sources {
            builder = builder.with_source_pipeline(&source.source_id, &source.pipeline);
        }
        if let Some(joins) = &config.joins {
            builder = builder.with_joins(
                joins
                    .iter()
                    .map(|join| QueryJoin {
                        id: join.id.clone(),
                        keys: join
                            .keys
                            .iter()
                            .map(|key| QueryJoinKey {
                                label: key.label.clone(),
                                property: key.property.clone(),
                            })
                            .collect(),
                    })
                    .collect(),
            );
        }
        Ok(builder.try_build().await?)
    }

    fn bound_output(&self, state: &QueryOutputState) -> Result<()> {
        ensure!(
            state.results_len() <= self.limits.snapshot_rows,
            OutputCapacityExceeded
        );
        let bytes = state
            .clone_results()
            .values()
            .try_fold(0usize, |total, row| -> Result<_> {
                total
                    .checked_add(serde_json::to_vec(row)?.len())
                    .ok_or_else(|| OutputCapacityExceeded.into())
            })?;
        ensure!(bytes <= self.limits.pending_bytes, OutputCapacityExceeded);
        Ok(())
    }

    async fn publish(&self, state: &State, result: Option<QueryResult>) {
        let mut publication = self.publication.lock().await;
        publication.output = Some(state.output.clone());
        if let Some(result) = result {
            // Broadcast never waits for a consumer; lagged subscriptions fail closed.
            let _ = publication.sender.send(Arc::new(result));
        }
    }

    async fn run(self, mut stop: watch::Receiver<bool>) -> Result<()> {
        let mut owned_inputs = self.inputs.lock().await;
        if owned_inputs.is_none() {
            let graph = self.graph.upgrade().context("query graph was released")?;
            let update_tx = graph.read().await.update_sender();
            let manager = SourceManager::new(&self.instance, self.logs.clone(), graph, update_tx);
            *owned_inputs = Some(inputs::Inputs::new(&self, &manager).await?);
        }
        let inputs = owned_inputs
            .as_mut()
            .context("owned inputs are unavailable")?;
        let outcome = self.run_epoch(inputs, &mut stop).await;
        inputs.detach();
        self.active_inputs
            .store(0, std::sync::atomic::Ordering::Release);
        outcome
    }

    async fn run_epoch(
        &self,
        inputs: &mut inputs::Inputs,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let storage = Storage::new(self.indexes.create_indexes(&self.base.config.id).await?)?;
        let restored = storage.hydrate(self).await?;
        let (core, mut state) = if let Some(state) = restored {
            self.admission.committed().await?;
            let core = self.build_core(&storage).await?;
            inputs.attach(self, &state.cuts, stop).await?;
            (core, state)
        } else {
            self.admission.uncommitted().await?;
            let seed = inputs.seed(self, stop).await?;
            if *stop.borrow() {
                return Ok(());
            }
            storage.admit(self).await?;
            let core = self.build_core(&storage).await?;
            let mut state = State {
                output: QueryOutputState::new(self.capacity),
                cuts: seed.cuts,
                outbox_bytes: BTreeMap::new(),
            };
            let guard = SessionGuard::begin(storage.created.set.session_control.clone()).await?;
            for record in seed.records {
                if *stop.borrow() {
                    bail!("owned query seed cancelled before commit");
                }
                let contexts = core.process_source_change(record.change).await?;
                let diffs = codec::diffs(&contexts);
                state.output.apply_diffs(&diffs);
                self.bound_output(&state.output)?;
                storage.stage_rows(&self.base.config.id, &diffs).await?;
            }
            for (source, cut) in &state.cuts {
                storage.stage_cut(source, cut).await?;
            }
            state.output.mark_initialized();
            storage.stage_seal(self, &state).await?;
            self.admission.before_commit().await?;
            guard.commit().await?;
            self.admission.committed().await?;
            inputs.attach(self, &state.cuts, stop).await?;
            (core, state)
        };
        if *stop.borrow() {
            return Ok(());
        }
        self.publish(&state, None).await;
        self.base.set_status(ComponentStatus::Running, None).await;
        self.changed.notify_waiters();
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            // Core's persistent pop is unconditional. Recheck before every individual pop.
            let due = storage.created.set.future_queue.peek_due_time().await?;
            if due.is_some_and(|due| due <= now_millis()) {
                let mut staged = None;
                core.process_due_futures_with_hook(|contexts, source| {
                    let diffs = codec::diffs(contexts);
                    let source = source.to_string();
                    let staged = &mut staged;
                    let storage = &storage;
                    let state = &state;
                    async move {
                        let next = storage
                            .stage(self, state, diffs, &source, None)
                            .await
                            .map_err(index_error)?;
                        *staged = Some(next);
                        Ok(())
                    }
                })
                .await?;
                if let Some(staged) = staged {
                    state = staged.state;
                    self.publish(&state, staged.result).await;
                }
            }
            if *stop.borrow() {
                return Ok(());
            }
            let event = inputs.next(stop, due).await?;
            let Some((rank, event)) = event else { continue };
            let source = &inputs.ports[rank].id;
            ensure!(
                &event.source_id == source,
                "source event identity does not match subscription"
            );
            let SourceEvent::Change(change) = &event.event else {
                // Control messages do not certify a durable source cursor.
                continue;
            };
            ensure!(
                !matches!(change, drasi_core::models::SourceChange::Future { .. }),
                "source stream cannot inject owned future evaluations"
            );
            let current = state
                .cuts
                .get(source)
                .context("owned input is missing a durable cut")?;
            if current
                .upstream
                .is_some_and(|sequence| event.sequence <= sequence)
            {
                continue;
            }
            let position = event
                .source_position
                .as_ref()
                .context("replayable source event has no opaque position")?;
            ensure!(
                !position.is_empty() && position.len() <= 65536,
                "source position is empty or oversized"
            );
            let cut = Cut {
                upstream: Some(event.sequence),
                position: position.to_vec(),
                offset: current
                    .offset
                    .checked_add(1)
                    .context("owned input offset exhausted")?,
            };
            let mut staged = None;
            core.process_source_change_with_hook(change.clone(), |contexts| {
                let diffs = codec::diffs(contexts);
                let staged = &mut staged;
                let storage = &storage;
                let state = &state;
                async move {
                    let next = storage
                        .stage(self, state, diffs, source, Some(cut))
                        .await
                        .map_err(index_error)?;
                    *staged = Some(next);
                    Ok(())
                }
            })
            .await?;
            let staged = staged.context("owned query committed without staged output")?;
            state = staged.state;
            inputs.ports[rank].ack(event.sequence);
            self.publish(&state, staged.result).await;
        }
    }

    async fn wait_running(&self) -> std::result::Result<(), FetchError> {
        let wait = async {
            loop {
                let changed = self.changed.notified();
                let status = self.base.get_status().await;
                match status {
                    ComponentStatus::Running => return Ok(()),
                    ComponentStatus::Starting | ComponentStatus::Added => changed.await,
                    status => return Err(FetchError::NotRunning { status }),
                }
            }
        };
        tokio::time::timeout(
            Duration::from_secs(self.base.config.bootstrap_timeout_secs),
            wait,
        )
        .await
        .map_err(|_| FetchError::TimedOut)?
    }
}

fn now_millis() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

#[async_trait]
impl Query for OwnedQuery {
    async fn start(&self) -> Result<()> {
        let mut owner = self.lifecycle.lock().await;
        if let Some(running) = owner.as_ref() {
            if !running.task.is_finished() {
                drop(owner);
                return self.wait_running().await.map_err(Into::into);
            }
        }
        if let Some(finished) = owner.take() {
            // Observe completion before reopening this query's indexes.
            let _ = finished.task.await.context("owned query task panicked")?;
        }
        self.publication.lock().await.output = None;
        *self.failure.lock().await = None;
        self.base.set_status(ComponentStatus::Starting, None).await;
        self.changed.notify_waiters();
        let (stop, receiver) = watch::channel(false);
        let query = self.clone();
        let task = tokio::spawn(async move {
            let outcome = query.clone().run(receiver).await;
            if let Err(error) = &outcome {
                let output_capacity = OutputCapacityExceeded::in_error(error);
                let message = if output_capacity {
                    OutputCapacityExceeded.to_string()
                } else {
                    format!("{error:#}")
                };
                *query.failure.lock().await = Some(Failure {
                    message: message.clone(),
                    output_capacity,
                });
                query
                    .base
                    .set_status(ComponentStatus::Error, Some(message))
                    .await;
                query.changed.notify_waiters();
            }
            outcome
        });
        *owner = Some(RunOwner { stop, task });
        drop(owner);
        match self.wait_running().await {
            Ok(()) => Ok(()),
            Err(error) => {
                if let Some(failure) = self.failure.lock().await.clone() {
                    bail!("{}", failure.message);
                }
                Err(error.into())
            }
        }
    }

    async fn stop(&self) -> Result<()> {
        let mut owner = self.lifecycle.lock().await;
        let joined = if let Some(running) = owner.as_mut() {
            running.stop.send_replace(true);
            // Keep the handle in its owner if this stop future is cancelled.
            let joined = (&mut running.task).await;
            owner.take();
            Some(joined)
        } else {
            None
        };
        if let Some(inputs) = self.inputs.lock().await.as_mut() {
            inputs.detach();
        }
        self.active_inputs
            .store(0, std::sync::atomic::Ordering::Release);
        self.base.set_status(ComponentStatus::Stopped, None).await;
        self.changed.notify_waiters();
        if let Some(joined) = joined {
            let _outcome = joined.context("owned query task panicked")?;
        }
        Ok(())
    }

    async fn status(&self) -> ComponentStatus {
        self.base.get_status().await
    }

    fn get_config(&self) -> &QueryConfig {
        &self.base.config
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn subscription_count(&self) -> usize {
        self.active_inputs
            .load(std::sync::atomic::Ordering::Acquire)
    }

    async fn subscribe(&self, _reaction_id: String) -> Result<QuerySubscriptionResponse> {
        self.wait_running().await?;
        let publication = self.publication.lock().await;
        ensure!(
            publication.sender.receiver_count() < self.limits.subscriptions,
            "owned query subscription limit exceeded"
        );
        let output = publication
            .output
            .as_ref()
            .context("owned query output is unavailable")?;
        Ok(QuerySubscriptionResponse {
            query_id: self.base.config.id.clone(),
            receiver: Box::new(ResultReceiver(Some(publication.sender.subscribe()))),
            as_of_sequence: output.as_of_sequence(),
        })
    }

    async fn fetch_snapshot(&self) -> std::result::Result<SnapshotResponse, FetchError> {
        self.wait_running().await?;
        let publication = self.publication.lock().await;
        let state = publication.output.as_ref().ok_or(FetchError::NotRunning {
            status: ComponentStatus::Starting,
        })?;
        Ok(SnapshotResponse::new(
            state.clone_results(),
            state.as_of_sequence(),
            output_epoch_hash(self.config_hash, state.generation()),
        )
        .with_output_generation(state.generation()))
    }

    async fn fetch_outbox(
        &self,
        after_sequence: u64,
    ) -> std::result::Result<OutboxResponse, FetchError> {
        self.wait_running().await?;
        let publication = self.publication.lock().await;
        let state = publication.output.as_ref().ok_or(FetchError::NotRunning {
            status: ComponentStatus::Starting,
        })?;
        let config_hash = output_epoch_hash(self.config_hash, state.generation());
        let results = state
            .fetch_outbox_after(after_sequence)
            .map_err(|mut error| {
                error.config_hash = config_hash;
                FetchError::OutboxGap(error)
            })?;
        Ok(OutboxResponse {
            results,
            latest_sequence: state.as_of_sequence(),
            config_hash,
            output_generation: state.generation(),
        })
    }

    async fn output_generation(&self) -> u64 {
        self.publication
            .lock()
            .await
            .output
            .as_ref()
            .map_or(0, QueryOutputState::generation)
    }

    fn is_volatile(&self) -> bool {
        false
    }

    async fn release_persistent_handles(&self) {
        // All index handles belong to the joined epoch, not this registered runtime.
        let _ = self.stop().await;
        if let Some(mut inputs) = self.inputs.lock().await.take() {
            inputs.close(&self.base.config.id).await;
        }
    }
}

struct ResultReceiver(Option<broadcast::Receiver<Arc<QueryResult>>>);

#[async_trait]
impl ChangeReceiver<QueryResult> for ResultReceiver {
    async fn recv(&mut self) -> Result<Arc<QueryResult>> {
        let result = self
            .0
            .as_mut()
            .context("owned result subscription is closed")?
            .recv()
            .await;
        match result {
            Ok(result) => Ok(result),
            Err(error) => {
                self.0.take();
                Err(error.into())
            }
        }
    }
}
