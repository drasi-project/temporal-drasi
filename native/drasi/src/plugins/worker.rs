//! Process ownership for foreign sources. Only the companion process loads plugins.

#[path = "worker/child.rs"]
mod child;
#[cfg(test)]
#[path = "worker/tests.rs"]
mod tests;
#[path = "worker/transport.rs"]
mod transport;

use crate::protocol::{PluginDescriptor, PluginSourceRecipe};
use async_trait::async_trait;
use drasi_lib::{
    bootstrap::BootstrapResult,
    channels::{
        BootstrapEvent, ChangeReceiver, ComponentStatus, DispatchMode, SourceEventWrapper,
        SubscriptionResponse,
    },
    config::SourceSubscriptionSettings,
    context::SourceRuntimeContext,
    Source,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use transport::{Binding, Command, ConstructionPhase, Event, Frame, Request, Response};

const MAX_SUBSCRIPTIONS: usize = 256;
const MAX_RETIRED_CALLBACKS: usize = 1024;
const QUEUE_RECORDS: usize = 32;
const QUEUE_BYTES: usize = 32 << 20;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_GRACE: Duration = Duration::from_secs(2);
const MAX_POSITION: usize = 65536;

pub type WorkerResolver = dyn Fn(&str, &[u8]) -> anyhow::Result<String> + Send + Sync + 'static;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustedArtifact {
    pub path: PathBuf,
    pub binary_sha256: String,
    pub sdk_version: String,
    pub core_version: String,
    pub lib_version: String,
    pub target_triple: String,
    pub plugin_version: String,
    pub abi_version: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerDescriptor {
    pub artifact: TrustedArtifact,
    pub descriptors: Vec<PluginDescriptor>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerSpec {
    /// An immutable graph/node/revision identity, not a display name.
    pub identity: String,
    pub source_id: String,
    pub recipe: PluginSourceRecipe,
    pub source_artifact: TrustedArtifact,
    pub bootstrap_artifact: TrustedArtifact,
    pub source_config: Value,
    pub bootstrap_config: Value,
    pub bootstrap_source_config: Value,
    /// A source-exclusive directory beneath the native state tree.
    pub state_directory: PathBuf,
}

fn failed(phase: &'static str) -> anyhow::Error {
    anyhow::anyhow!(phase)
}

fn position(value: Option<&[u8]>) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.is_none_or(|value| !value.is_empty() && value.len() <= MAX_POSITION),
        "foreign source position is not representable by the published plugin ABI"
    );
    Ok(())
}

struct Budget {
    bytes: Arc<AtomicUsize>,
    size: usize,
}

impl Budget {
    fn reserve(bytes: &Arc<AtomicUsize>, size: usize) -> anyhow::Result<Self> {
        bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(size).filter(|next| *next <= QUEUE_BYTES)
            })
            .map_err(|_| failed("foreign source IPC byte capacity exceeded"))?;
        Ok(Self {
            bytes: bytes.clone(),
            size,
        })
    }
}

impl Drop for Budget {
    fn drop(&mut self) {
        self.bytes.fetch_sub(self.size, Ordering::AcqRel);
    }
}

struct Queued<T> {
    value: T,
    _budget: Budget,
}

struct Lane {
    source: String,
    query: String,
    live: mpsc::Sender<Queued<SourceEventWrapper>>,
    bootstrap: Option<mpsc::Sender<BootstrapEvent>>,
    result: Option<oneshot::Sender<anyhow::Result<BootstrapResult>>>,
    delivered: u64,
    bootstrap_count: usize,
    bootstrap_ended: bool,
    bootstrap_result: Option<(usize, Option<Vec<u8>>)>,
    bootstrap_failed: bool,
    bootstrap_budget: Budget,
    closing: bool,
}

struct Pin {
    position: Arc<AtomicU64>,
    sent: u64,
    delivered: u64,
    removing: bool,
    attached: bool,
}

#[derive(Debug, Default)]
struct Diagnostics {
    phase: ConstructionPhase,
    failure: Option<&'static str>,
    exit_code: Option<i32>,
    exit_signal: Option<i32>,
}

struct Shared {
    binding: Binding,
    commands: std::sync::mpsc::SyncSender<Frame>,
    pending: Mutex<BTreeMap<u64, oneshot::Sender<anyhow::Result<Response>>>>,
    lanes: Mutex<BTreeMap<u64, Lane>>,
    pins: Arc<Mutex<BTreeMap<String, Pin>>>,
    next: AtomicU64,
    failed: Arc<AtomicBool>,
    closing: AtomicBool,
    reaped: AtomicBool,
    stopped: AtomicBool,
    joined: (Mutex<bool>, Condvar),
    bytes: Arc<AtomicUsize>,
    status: Mutex<ComponentStatus>,
    diagnostics: Mutex<Diagnostics>,
    // Keep the scoped grant alive until the process and every IPC pump are joined.
    _resolver: Arc<WorkerResolver>,
}

impl Shared {
    fn fail_at(&self, phase: &'static str) {
        self.diagnostics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failure
            .get_or_insert(phase);
        self.fail();
    }

    fn record_exit(&self, status: std::process::ExitStatus) {
        use std::os::unix::process::ExitStatusExt;
        let mut diagnostics = self
            .diagnostics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        diagnostics.exit_code = status.code();
        diagnostics.exit_signal = status.signal();
    }

    fn construction_error(&self) -> anyhow::Error {
        let diagnostics = self
            .diagnostics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let error = anyhow::anyhow!("foreign source construction failed: {diagnostics:?}");
        #[cfg(test)]
        eprintln!("{error}");
        error
    }

    fn fail(&self) {
        self.failed.store(true, Ordering::Release);
        if let Ok(mut status) = self.status.lock() {
            *status = ComponentStatus::Error;
        }
    }

    fn enqueue(&self, command: Command) -> anyhow::Result<()> {
        if self.failed.load(Ordering::Acquire) || self.closing.load(Ordering::Acquire) {
            return Err(failed("foreign source worker is unavailable"));
        }
        self.commands
            .try_send(Frame::new(&self.binding, command))
            .map_err(|_| {
                self.fail_at("command capacity");
                failed("foreign source command capacity exceeded")
            })
    }

    async fn request(&self, request: Request) -> anyhow::Result<Response> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| failed("foreign source RPC lock failed"))?;
            anyhow::ensure!(pending.len() < 128, "foreign source RPC capacity exceeded");
            pending.insert(id, sender);
        }
        if let Err(error) = self.enqueue(Command::Request { id, request }) {
            self.pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&id);
            return Err(error);
        }
        match tokio::time::timeout(REQUEST_TIMEOUT, receiver).await {
            Ok(Ok(response)) => response,
            _ => {
                self.fail_at("RPC completion");
                Err(failed("foreign source operation did not complete"))
            }
        }
    }

    fn unsubscribe(&self, id: u64) {
        if self.closing.load(Ordering::Acquire) {
            return;
        }
        {
            let mut lanes = self
                .lanes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(lane) = lanes.get_mut(&id) else {
                return;
            };
            if lane.closing {
                return;
            }
            lane.closing = true;
        }
        // The child acknowledges receiver cleanup before this lane disappears.
        if self
            .enqueue(Command::Unsubscribe { subscription: id })
            .is_err()
        {
            self.fail();
        }
    }

    fn joined(&self) -> bool {
        *self
            .joined
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct Owner {
    shared: Arc<Shared>,
    supervisor: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.closing.store(true, Ordering::Release);
        // A dropped graph still owns completion. The supervisor never awaits an SDK task.
        if let Some(thread) = self
            .supervisor
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = thread.join();
        }
    }
}

#[derive(Clone)]
pub struct WorkerSource {
    process: Arc<Process>,
    spec: Arc<WorkerSpec>,
}

struct Process {
    owner: Mutex<Arc<Owner>>,
    path: PathBuf,
    resolver: Arc<WorkerResolver>,
    pins: Arc<Mutex<BTreeMap<String, Pin>>>,
    failed: Arc<AtomicBool>,
    terminal: AtomicBool,
    auto_start: AtomicBool,
    lifecycle: tokio::sync::Mutex<()>,
}

impl Drop for Process {
    fn drop(&mut self) {
        let owner = self
            .owner
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        owner.shared.closing.store(true, Ordering::Release);
        let supervisor = owner
            .supervisor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(supervisor) = supervisor {
            let _ = supervisor.join();
        }
    }
}

impl WorkerSource {
    fn owner(&self) -> Arc<Owner> {
        self.process
            .owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub async fn spawn(
        worker_path: &Path,
        spec: WorkerSpec,
        resolver: Arc<WorkerResolver>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !spec.identity.is_empty()
                && spec.identity.len() <= 4096
                && !spec.source_id.is_empty()
                && spec.source_id.len() <= 256
                && spec.state_directory.is_absolute(),
            "foreign source identity or state directory is invalid"
        );
        let pins = Arc::new(Mutex::new(BTreeMap::new()));
        let failed_state = Arc::new(AtomicBool::new(false));
        let owner = launch(
            worker_path,
            &spec.identity,
            resolver.clone(),
            pins.clone(),
            failed_state.clone(),
        )
        .inspect_err(|_error| {
            #[cfg(test)]
            eprintln!("foreign source launch failed: {_error}");
        })?;
        let source = Self {
            process: Arc::new(Process {
                owner: Mutex::new(owner.clone()),
                path: worker_path.into(),
                resolver,
                pins,
                failed: failed_state,
                terminal: AtomicBool::new(false),
                auto_start: AtomicBool::new(false),
                lifecycle: tokio::sync::Mutex::new(()),
            }),
            spec: Arc::new(spec),
        };
        let result = owner
            .shared
            .request(Request::Construct(Box::new((*source.spec).clone())))
            .await;
        if !matches!(result, Ok(Response::Constructed)) {
            owner.shared.fail_at("construction response");
            let _ = source.close().await;
            return Err(owner.shared.construction_error());
        }
        Ok(source)
    }

    pub fn with_auto_start(self, auto_start: bool) -> Self {
        self.process.auto_start.store(auto_start, Ordering::Release);
        self
    }

    /// Permanently close this source identity after joining its final worker.
    pub async fn close(&self) -> anyhow::Result<()> {
        let _lifecycle = self.process.lifecycle.lock().await;
        self.process.terminal.store(true, Ordering::Release);
        let result = Self::stop_owner(&self.owner()).await;
        self.process
            .pins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        result
    }

    async fn stop_owner(owner: &Owner) -> anyhow::Result<()> {
        owner.shared.closing.store(true, Ordering::Release);
        while !owner.shared.joined() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let supervisor = owner
            .supervisor
            .lock()
            .map_err(|_| failed("foreign source supervisor lock failed"))?
            .take();
        if let Some(supervisor) = supervisor {
            if supervisor.join().is_err() {
                owner.shared.fail();
            }
        }
        anyhow::ensure!(
            !owner.shared.failed.load(Ordering::Acquire),
            "foreign source worker failed; checkpoint publication is prohibited"
        );
        Ok(())
    }

    pub fn is_joined(&self) -> bool {
        self.owner().shared.joined()
    }
}

struct Receiver {
    receiver: mpsc::Receiver<Queued<SourceEventWrapper>>,
    owner: Arc<Owner>,
    subscription: u64,
}

struct PendingSubscription {
    shared: Arc<Shared>,
    subscription: u64,
    completed: bool,
}

impl Drop for PendingSubscription {
    fn drop(&mut self) {
        if !self.completed {
            self.shared.unsubscribe(self.subscription);
        }
    }
}

#[async_trait]
impl ChangeReceiver<SourceEventWrapper> for Receiver {
    async fn recv(&mut self) -> anyhow::Result<Arc<SourceEventWrapper>> {
        anyhow::ensure!(
            !self.owner.shared.failed.load(Ordering::Acquire),
            "foreign source input failed"
        );
        let event = self
            .receiver
            .recv()
            .await
            .ok_or_else(|| failed("foreign source input ended"))?;
        Ok(Arc::new(event.value))
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.receiver.close();
        self.owner.shared.unsubscribe(self.subscription);
    }
}

#[async_trait]
impl Source for WorkerSource {
    fn id(&self) -> &str {
        &self.spec.source_id
    }
    fn type_name(&self) -> &str {
        &self.spec.recipe.source.plugin.component
    }
    fn properties(&self) -> HashMap<String, Value> {
        HashMap::from([(
            "managed_recipe".into(),
            serde_json::json!(&self.spec.recipe),
        )])
    }
    fn dispatch_mode(&self) -> DispatchMode {
        DispatchMode::Channel
    }
    fn auto_start(&self) -> bool {
        self.process.auto_start.load(Ordering::Acquire)
    }
    fn supports_replay(&self) -> bool {
        true
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    async fn initialize(&self, _context: SourceRuntimeContext) {
        // Foreign context/store ownership is established inside the worker.
    }
    async fn start(&self) -> anyhow::Result<()> {
        let _lifecycle = self.process.lifecycle.lock().await;
        anyhow::ensure!(
            !self.process.terminal.load(Ordering::Acquire)
                && !self.process.failed.load(Ordering::Acquire),
            "foreign source cannot restart after failure or retirement"
        );
        let mut owner = self.owner();
        if owner.shared.joined() {
            {
                let mut pins = self
                    .process
                    .pins
                    .lock()
                    .map_err(|_| failed("foreign source pin lock failed"))?;
                for pin in pins.values_mut() {
                    pin.attached = false;
                    pin.sent = u64::MAX;
                }
            }
            owner = launch(
                &self.process.path,
                &self.spec.identity,
                self.process.resolver.clone(),
                self.process.pins.clone(),
                self.process.failed.clone(),
            )?;
            *self
                .process
                .owner
                .lock()
                .map_err(|_| failed("foreign source epoch lock failed"))? = owner.clone();
            if !matches!(
                owner
                    .shared
                    .request(Request::Construct(Box::new((*self.spec).clone())))
                    .await,
                Ok(Response::Constructed)
            ) {
                owner.shared.fail();
                let _ = Self::stop_owner(&owner).await;
                return Err(failed("foreign source reconstruction failed"));
            }
        }
        match owner.shared.request(Request::Start).await? {
            Response::Ok => {
                *owner
                    .shared
                    .status
                    .lock()
                    .map_err(|_| failed("foreign source status lock failed"))? =
                    ComponentStatus::Running;
                Ok(())
            }
            _ => {
                owner.shared.fail();
                Err(failed("foreign source start failed"))
            }
        }
    }
    async fn stop(&self) -> anyhow::Result<()> {
        let _lifecycle = self.process.lifecycle.lock().await;
        Self::stop_owner(&self.owner()).await
    }
    async fn status(&self) -> ComponentStatus {
        let owner = self.owner();
        if owner.shared.failed.load(Ordering::Acquire) {
            ComponentStatus::Error
        } else if owner.shared.joined() {
            ComponentStatus::Stopped
        } else if owner.shared.closing.load(Ordering::Acquire) {
            ComponentStatus::Stopping
        } else {
            match owner.shared.request(Request::Status).await {
                Ok(Response::Status(status)) => status,
                _ if owner.shared.closing.load(Ordering::Acquire) => {
                    if owner.shared.joined() {
                        ComponentStatus::Stopped
                    } else {
                        ComponentStatus::Stopping
                    }
                }
                _ => {
                    owner.shared.fail();
                    ComponentStatus::Error
                }
            }
        }
    }
    async fn subscribe(
        &self,
        settings: SourceSubscriptionSettings,
    ) -> anyhow::Result<SubscriptionResponse> {
        let owner = self.owner();
        position(settings.resume_from.as_deref())?;
        anyhow::ensure!(
            settings.source_id == self.id()
                && !settings.query_id.is_empty()
                && settings.query_id.len() <= 256
                && settings.nodes.len() <= 1024
                && settings.relations.len() <= 1024,
            "foreign source subscription identity is invalid"
        );
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        loop {
            let previous = owner
                .shared
                .lanes
                .lock()
                .map_err(|_| failed("foreign source lane lock failed"))?
                .values()
                .any(|lane| lane.query == settings.query_id && lane.closing);
            if !previous {
                break;
            }
            anyhow::ensure!(
                !owner.shared.failed.load(Ordering::Acquire) && Instant::now() < deadline,
                "foreign source previous receiver did not detach"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let subscription = owner.shared.next.fetch_add(1, Ordering::Relaxed);
        let (live_tx, live_rx) = mpsc::channel(QUEUE_RECORDS);
        let (bootstrap_tx, bootstrap_rx) = mpsc::channel(QUEUE_RECORDS);
        let (result_tx, result_rx) = oneshot::channel();
        let handle = if settings.request_position_handle {
            let mut pins = owner
                .shared
                .pins
                .lock()
                .map_err(|_| failed("foreign source pin lock failed"))?;
            anyhow::ensure!(
                pins.len() < MAX_SUBSCRIPTIONS || pins.contains_key(&settings.query_id),
                "foreign source retained pin capacity exceeded"
            );
            let pin = pins
                .entry(settings.query_id.clone())
                .or_insert_with(|| Pin {
                    position: Arc::new(AtomicU64::new(u64::MAX)),
                    sent: u64::MAX,
                    delivered: settings.resume_sequence.unwrap_or(0),
                    removing: false,
                    attached: false,
                });
            anyhow::ensure!(!pin.removing, "foreign source pin removal is in progress");
            pin.position.clone()
        } else {
            Arc::new(AtomicU64::new(u64::MAX))
        };
        {
            let mut lanes = owner
                .shared
                .lanes
                .lock()
                .map_err(|_| failed("foreign source lane lock failed"))?;
            anyhow::ensure!(
                !owner.shared.closing.load(Ordering::Acquire)
                    && lanes.len() < MAX_SUBSCRIPTIONS
                    && !lanes.values().any(|lane| lane.query == settings.query_id),
                "foreign source subscription capacity or identity conflict"
            );
            lanes.insert(
                subscription,
                Lane {
                    source: settings.source_id.clone(),
                    query: settings.query_id.clone(),
                    live: live_tx,
                    bootstrap: settings.enable_bootstrap.then_some(bootstrap_tx),
                    result: settings.enable_bootstrap.then_some(result_tx),
                    delivered: settings.resume_sequence.unwrap_or(0),
                    bootstrap_count: 0,
                    bootstrap_ended: !settings.enable_bootstrap,
                    bootstrap_result: None,
                    bootstrap_failed: false,
                    bootstrap_budget: Budget::reserve(&owner.shared.bytes, 0)?,
                    closing: false,
                },
            );
        }
        let mut pending = PendingSubscription {
            shared: owner.shared.clone(),
            subscription,
            completed: false,
        };
        let reply = owner
            .shared
            .request(Request::Subscribe {
                subscription,
                settings: transport::Settings::from(&settings),
            })
            .await?;
        let Response::Subscribed {
            bootstrap,
            position_handle,
        } = reply
        else {
            owner.shared.fail();
            return Err(failed("foreign source subscribe failed"));
        };
        if bootstrap != settings.enable_bootstrap
            || position_handle != settings.request_position_handle
        {
            owner.shared.fail();
            return Err(failed(
                "foreign source subscription contract was not satisfied",
            ));
        }
        if let Some(pin) = owner
            .shared
            .pins
            .lock()
            .map_err(|_| failed("foreign source pin lock failed"))?
            .get_mut(&settings.query_id)
        {
            pin.attached = position_handle;
        }
        pending.completed = true;
        Ok(SubscriptionResponse {
            query_id: settings.query_id,
            source_id: settings.source_id,
            receiver: Box::new(Receiver {
                receiver: live_rx,
                owner,
                subscription,
            }),
            bootstrap_receiver: bootstrap.then_some(bootstrap_rx),
            position_handle: position_handle.then_some(handle),
            bootstrap_result_receiver: bootstrap.then_some(result_rx),
        })
    }
    async fn remove_position_handle(&self, query: &str) {
        let owner = self.owner();
        {
            let mut pins = owner
                .shared
                .pins
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(pin) = pins.get_mut(query) else {
                return;
            };
            if pin.removing {
                return;
            }
            pin.removing = true;
        }
        let subscriptions: Vec<_> = owner
            .shared
            .lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter_map(|(id, lane)| (lane.query == query).then_some(*id))
            .collect();
        for id in &subscriptions {
            owner.shared.unsubscribe(*id);
        }
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        loop {
            let retained = owner
                .shared
                .lanes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .keys()
                .any(|id| subscriptions.contains(id));
            if !retained
                || owner.shared.failed.load(Ordering::Acquire)
                || owner.shared.closing.load(Ordering::Acquire)
            {
                break;
            }
            if Instant::now() >= deadline {
                owner.shared.fail();
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if owner.shared.joined() {
            owner
                .shared
                .pins
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(query);
            return;
        }
        if matches!(
            owner
                .shared
                .request(Request::RemovePin {
                    query: query.into()
                })
                .await,
            Ok(Response::Ok)
        ) {
            owner
                .shared
                .pins
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(query);
        } else {
            owner.shared.fail();
        }
    }

    async fn deprovision(&self) -> anyhow::Result<()> {
        match self.owner().shared.request(Request::Deprovision).await? {
            Response::Ok => Ok(()),
            _ => Err(failed("foreign source deprovision failed")),
        }
    }
}

pub async fn probe(
    worker_path: &Path,
    artifact: TrustedArtifact,
) -> anyhow::Result<WorkerDescriptor> {
    let owner = launch(
        worker_path,
        "descriptor-probe",
        Arc::new(|_, _| Err(failed("descriptor probe has no credentials"))),
        Arc::new(Mutex::new(BTreeMap::new())),
        Arc::new(AtomicBool::new(false)),
    )?;
    let response = owner.shared.request(Request::Probe(artifact)).await;
    owner.shared.closing.store(true, Ordering::Release);
    while !owner.shared.joined() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let value = match response? {
        Response::Descriptor(value) => value,
        Response::ProbeFailed(message) if message.len() <= 4096 => anyhow::bail!("{message}"),
        _ => return Err(failed("foreign plugin descriptor probe failed")),
    };
    anyhow::ensure!(
        !owner.shared.failed.load(Ordering::Acquire),
        "foreign plugin descriptor worker failed"
    );
    Ok(*value)
}

pub fn plugin_worker_main() -> i32 {
    child::main()
}

pub fn companion_for_library(library: &Path) -> anyhow::Result<PathBuf> {
    anyhow::ensure!(
        library.is_absolute(),
        "native library package path must be absolute"
    );
    let parent = library
        .parent()
        .ok_or_else(|| failed("native library package directory is unavailable"))?;
    let worker = parent.join("temporal-drasi-plugin-worker");
    let metadata = std::fs::symlink_metadata(&worker)
        .map_err(|_| failed("packaged plugin worker is unavailable"))?;
    anyhow::ensure!(
        metadata.is_file(),
        "packaged plugin worker is not a regular file"
    );
    Ok(worker)
}

fn launch(
    path: &Path,
    identity: &str,
    resolver: Arc<WorkerResolver>,
    pins: Arc<Mutex<BTreeMap<String, Pin>>>,
    failed_state: Arc<AtomicBool>,
) -> anyhow::Result<Arc<Owner>> {
    anyhow::ensure!(
        path.is_absolute(),
        "plugin worker must be an absolute packaged companion"
    );
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| failed("packaged plugin worker is unavailable"))?;
    anyhow::ensure!(
        metadata.is_file(),
        "packaged plugin worker is not a regular file"
    );
    let mut child = std::process::Command::new(path)
        .env_clear()
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|error| {
            anyhow::anyhow!(
                "packaged plugin worker could not be started: {:?}",
                error.kind()
            )
        })?;
    let stdin = child.stdin.take().expect("piped worker stdin");
    let stdout = child.stdout.take().expect("piped worker stdout");
    let child = Arc::new(Mutex::new(child));
    let (commands, receiver) = std::sync::mpsc::sync_channel(128);
    let shared = Arc::new(Shared {
        binding: Binding {
            nonce: uuid::Uuid::new_v4().to_string(),
            identity: identity.into(),
        },
        commands,
        pending: Mutex::default(),
        lanes: Mutex::default(),
        pins,
        next: AtomicU64::new(1),
        failed: failed_state,
        closing: AtomicBool::new(false),
        reaped: AtomicBool::new(false),
        stopped: AtomicBool::new(false),
        joined: (Mutex::new(false), Condvar::new()),
        bytes: Arc::new(AtomicUsize::new(0)),
        status: Mutex::new(ComponentStatus::Stopped),
        diagnostics: Mutex::default(),
        _resolver: resolver,
    });
    let writer_state = shared.clone();
    let writer = match std::thread::Builder::new()
        .name("drasi-worker-input".into())
        .spawn(move || {
            let fault = writer_state.clone();
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                transport::parent_writer(stdin, receiver, writer_state)
            }))
            .is_err()
            {
                fault.fail();
            }
        }) {
        Ok(writer) => writer,
        Err(_) => {
            shared.fail_at("parent input pump spawn");
            drop(stdout);
            return Err(failed_launch(&child, &shared, Vec::new()));
        }
    };
    let reader_state = shared.clone();
    let reader = match std::thread::Builder::new()
        .name("drasi-worker-output".into())
        .spawn(move || {
            let fault = reader_state.clone();
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                transport::parent_reader(stdout, reader_state)
            }))
            .is_err()
            {
                fault.fail();
            }
        }) {
        Ok(reader) => reader,
        Err(_) => {
            shared.fail_at("parent output pump spawn");
            return Err(failed_launch(&child, &shared, vec![writer]));
        }
    };
    let pumps = Arc::new(Mutex::new(Some((writer, reader))));
    let supervisor_pumps = pumps.clone();
    let supervised_child = child.clone();
    let state = shared.clone();
    let supervisor = std::thread::Builder::new()
        .name("drasi-worker-owner".into())
        .spawn(move || {
            let mut child = supervised_child
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut stop_sent = None;
            let mut waited = false;
            loop {
                if state.failed.load(Ordering::Acquire) {
                    let _ = child.kill();
                    break;
                }
                if state.closing.load(Ordering::Acquire) && stop_sent.is_none() {
                    stop_sent = Some(Instant::now());
                    if state
                        .commands
                        .try_send(Frame::new(&state.binding, Command::Stop))
                        .is_err()
                    {
                        state.fail();
                    }
                }
                if stop_sent.is_some_and(|at| at.elapsed() >= STOP_GRACE) {
                    state.fail();
                    let _ = child.kill();
                    break;
                }
                match child.try_wait() {
                    Ok(Some(status)) => {
                        waited = true;
                        state.record_exit(status);
                        if !status.success() || !state.closing.load(Ordering::Acquire) {
                            state.fail();
                        }
                        break;
                    }
                    Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                    Err(_) => {
                        state.fail();
                        let _ = child.kill();
                        break;
                    }
                }
            }
            if !waited {
                wait_confirmed(&mut child, &state);
            }
            state.reaped.store(true, Ordering::Release);
            state.closing.store(true, Ordering::Release);
            let (writer, reader) = supervisor_pumps
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .expect("owned IPC pumps");
            if writer.join().is_err() {
                state.fail();
            }
            if reader.join().is_err() {
                state.fail();
            }
            if !state.stopped.load(Ordering::Acquire) {
                state.fail();
            }
            state
                .lanes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
            state
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
            *state
                .joined
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
            state.joined.1.notify_all();
        });
    let supervisor = match supervisor {
        Ok(supervisor) => supervisor,
        Err(_) => {
            shared.fail_at("parent supervisor spawn");
            let (writer, reader) = pumps
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .expect("owned IPC pumps");
            return Err(failed_launch(&child, &shared, vec![writer, reader]));
        }
    };
    Ok(Arc::new(Owner {
        shared,
        supervisor: Mutex::new(Some(supervisor)),
    }))
}

fn wait_confirmed(child: &mut std::process::Child, state: &Shared) {
    loop {
        match child.wait() {
            Ok(status) => {
                state.record_exit(status);
                return;
            }
            Err(_) => {
                state.fail();
                // A failed wait is not completion. Keep all ownership and retry
                // the same OS child instead of authorizing resource release.
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn failed_launch(
    child: &Arc<Mutex<std::process::Child>>,
    state: &Shared,
    pumps: Vec<std::thread::JoinHandle<()>>,
) -> anyhow::Error {
    state.fail();
    let mut child = child
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _ = child.kill();
    wait_confirmed(&mut child, state);
    state.reaped.store(true, Ordering::Release);
    state.closing.store(true, Ordering::Release);
    for pump in pumps {
        let _ = pump.join();
    }
    state.construction_error()
}
