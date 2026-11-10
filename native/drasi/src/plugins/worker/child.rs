use super::*;
use crate::plugins::native::{self, LoadedPlugin};
use crate::protocol::PluginCategory;
use drasi_host_sdk::state_store_bridge::StateStoreVtableBuilder;
use drasi_plugin_sdk::{
    descriptor::{
        BootstrapPluginDescriptor, IdentityProviderPluginDescriptor, ReactionPluginDescriptor,
        SecretStorePluginDescriptor, SourcePluginDescriptor,
    },
    ffi::{
        metadata::{PluginMetadata, FFI_SDK_VERSION, TARGET_TRIPLE},
        payload::{
            decode_bootstrap_event_payload, decode_source_event_payload, BootstrapEventPayload,
            SourceEventPayload,
        },
        secret_store::FfiGetSecretResult,
        FfiBootstrapEvent, FfiBootstrapReceiver, FfiBootstrapResult, FfiBootstrapResultReceiver,
        FfiChangeReceiver, FfiComponentStatus, FfiLifecycleEvent, FfiLogEntry, FfiRuntimeContext,
        FfiSourceEvent, FfiStr, SourceVtable,
    },
};
use sha2::{Digest, Sha256};
use std::{ffi::c_void, io::Read, sync::OnceLock};
use transport::{MAX_FRAME, MAX_PAYLOAD};

struct Outgoing {
    frame: Frame,
    _budget: Budget,
}

struct Pipe {
    binding: OnceLock<Binding>,
    output: std::sync::mpsc::SyncSender<Outgoing>,
    bytes: Arc<AtomicUsize>,
    next: AtomicU64,
    resolutions: Mutex<BTreeMap<u64, std::sync::mpsc::SyncSender<Option<String>>>>,
    positions: Mutex<BTreeMap<String, Arc<AtomicU64>>>,
    stopping: AtomicBool,
}

impl Pipe {
    fn phase(&self, phase: ConstructionPhase) {
        self.send_or_exit(Command::ConstructionPhase(phase));
    }

    fn send(&self, command: Command) -> anyhow::Result<()> {
        let binding = self
            .binding
            .get()
            .ok_or_else(|| failed("worker handshake is missing"))?;
        let frame = Frame::new(binding, command);
        let mut bytes =
            serde_json::to_vec(&frame).map_err(|_| failed("worker frame encoding failed"))?;
        let size = bytes.len();
        bytes.fill(0);
        anyhow::ensure!(size <= MAX_FRAME, "worker frame exceeds capacity");
        let budget = Budget::reserve(&self.bytes, size)?;
        self.output
            .try_send(Outgoing {
                frame,
                _budget: budget,
            })
            .map_err(|_| failed("worker output capacity exceeded"))
    }

    fn send_or_exit(&self, command: Command) {
        if self.send(command).is_err() {
            std::process::exit(72);
        }
    }
}

pub(super) fn main() -> i32 {
    let (output, outgoing) = std::sync::mpsc::sync_channel::<Outgoing>(QUEUE_RECORDS);
    let pipe = Arc::new(Pipe {
        binding: OnceLock::new(),
        output,
        bytes: Arc::new(AtomicUsize::new(0)),
        next: AtomicU64::new(1),
        resolutions: Mutex::default(),
        positions: Mutex::default(),
        stopping: AtomicBool::new(false),
    });
    // This thread is installed before dlopen and never calls foreign code. EOF
    // terminates even when factories, stop, destructors, or the SDK runtime hang.
    let (commands, requests) = std::sync::mpsc::sync_channel(128);
    let reader_pipe = pipe.clone();
    std::thread::spawn(move || {
        let mut input = std::io::stdin().lock();
        loop {
            let frame = match transport::read(&mut input) {
                Ok(frame) => frame,
                Err(_) => std::process::exit(73),
            };
            if let Some(binding) = reader_pipe.binding.get() {
                if frame.validate(binding).is_err() {
                    std::process::exit(73);
                }
            } else {
                if frame.validate(&frame.binding).is_err()
                    || frame.binding.identity.is_empty()
                    || frame.binding.identity.len() > 4096
                    || uuid::Uuid::parse_str(&frame.binding.nonce).is_err()
                    || !matches!(frame.command, Command::Request { .. })
                {
                    std::process::exit(73);
                }
                if reader_pipe.binding.set(frame.binding.clone()).is_err() {
                    std::process::exit(73);
                }
            }
            match frame.command {
                Command::Resolved { id, value } => {
                    let sender = reader_pipe
                        .resolutions
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&id);
                    match sender {
                        Some(sender) if sender.try_send(value).is_ok() => {}
                        _ => std::process::exit(73),
                    }
                }
                Command::Ack { query, sequence } => {
                    let positions = reader_pipe
                        .positions
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let Some(handle) = positions.get(&query) else {
                        std::process::exit(73);
                    };
                    let old = handle.load(Ordering::Acquire);
                    if sequence == u64::MAX || (old != u64::MAX && sequence < old) {
                        std::process::exit(73);
                    }
                    handle.store(sequence, Ordering::Release);
                }
                Command::Stop => {
                    reader_pipe.stopping.store(true, Ordering::Release);
                    if commands.try_send(Command::Stop).is_err() {
                        std::process::exit(73);
                    }
                }
                command @ (Command::Request { .. } | Command::Unsubscribe { .. }) => {
                    if commands.try_send(command).is_err() {
                        std::process::exit(73);
                    }
                }
                _ => std::process::exit(73),
            }
        }
    });
    let (finished, finish) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut output = std::io::stdout().lock();
        for outgoing in outgoing {
            let stopping = matches!(outgoing.frame.command, Command::Stopped);
            if transport::write(&mut output, &outgoing.frame).is_err() {
                std::process::exit(74);
            }
            if stopping {
                let _ = finished.send(());
                return;
            }
        }
    });
    let mut worker = Worker {
        pipe,
        source: None,
        spec: None,
        subscriptions: BTreeMap::new(),
        retired: Vec::new(),
    };
    for command in requests {
        let result = match command {
            Command::Request { id, request } => {
                let response = worker.request(request);
                match response {
                    Ok(response) => worker.pipe.send(Command::Response { id, response }),
                    Err(_) => {
                        worker.pipe.send_or_exit(Command::Response {
                            id,
                            response: Response::Failed,
                        });
                        // Exit without dropping potentially live foreign handles.
                        std::process::exit(75);
                    }
                }
            }
            Command::Unsubscribe { subscription } => worker.unsubscribe(subscription),
            Command::Stop => {
                if let Some(source) = &worker.source {
                    let result = (source.stop_fn)(source.state);
                    if unsafe { result.into_result() }.is_err() {
                        std::process::exit(76);
                    }
                }
                worker.pipe.send_or_exit(Command::Stopped);
                if finish.recv_timeout(Duration::from_secs(1)).is_err() {
                    std::process::exit(74);
                }
                // Stopped is not completion. The parent's wait is the completion.
                std::process::exit(0);
            }
            _ => Err(failed("worker command is invalid")),
        };
        if result.is_err() {
            std::process::exit(75);
        }
    }
    73
}

struct Worker {
    pipe: Arc<Pipe>,
    source: Option<SourceVtable>,
    spec: Option<WorkerSpec>,
    subscriptions: BTreeMap<u64, Subscription>,
    retired: Vec<Subscription>,
}

impl Worker {
    fn request(&mut self, request: Request) -> anyhow::Result<Response> {
        match request {
            Request::Probe(artifact) => {
                anyhow::ensure!(
                    self.source.is_none() && self.spec.is_none(),
                    "worker probe mode is exclusive"
                );
                let result = load(&artifact).and_then(|loaded| descriptors(&loaded));
                match result {
                    Ok(descriptors) => Ok(Response::Descriptor(Box::new(WorkerDescriptor {
                        artifact,
                        descriptors,
                    }))),
                    Err(error) => {
                        let message = error.to_string();
                        Ok(Response::ProbeFailed(if message.len() <= 4096 {
                            message
                        } else {
                            "foreign plugin probe failed; diagnostic exceeds byte limit".into()
                        }))
                    }
                }
            }
            Request::Construct(spec) => {
                let spec = *spec;
                anyhow::ensure!(
                    self.source.is_none()
                        && self.spec.is_none()
                        && self
                            .pipe
                            .binding
                            .get()
                            .is_some_and(|binding| binding.identity == spec.identity),
                    "worker construction identity conflict"
                );
                self.pipe.phase(ConstructionPhase::SourceLibrary);
                let source = load(&spec.source_artifact)?;
                let bootstrap = if spec.source_artifact.path == spec.bootstrap_artifact.path {
                    anyhow::ensure!(
                        serde_json::to_vec(&spec.source_artifact)?
                            == serde_json::to_vec(&spec.bootstrap_artifact)?,
                        "worker artifact aliases conflict"
                    );
                    source.clone()
                } else {
                    self.pipe.phase(ConstructionPhase::BootstrapLibrary);
                    load(&spec.bootstrap_artifact)?
                };
                self.pipe.phase(ConstructionPhase::ResolverInjection);
                install_resolver(
                    &source,
                    &spec.source_artifact.binary_sha256,
                    self.pipe.clone(),
                );
                install_resolver(
                    &bootstrap,
                    &spec.bootstrap_artifact.binary_sha256,
                    self.pipe.clone(),
                );
                self.pipe.phase(ConstructionPhase::SourceFactory);
                let inner = source.worker_source(
                    &spec.recipe.source.plugin.component,
                    &spec.source_id,
                    &spec.source_config,
                )?;
                self.source = Some(inner);
                self.pipe.phase(ConstructionPhase::SourceContract);
                let inner = self.source.as_ref().expect("owned source");
                let id = bounded_str(&(inner.id_fn)(inner.state), 256)?;
                let kind = bounded_str(&(inner.type_name_fn)(inner.state), 256)?;
                anyhow::ensure!(
                    id == spec.source_id
                        && kind == spec.recipe.source.plugin.component
                        && !(inner.auto_start_fn)(inner.state)
                        && (inner.supports_replay_fn)(inner.state),
                    "foreign source requires administrator-reviewed native replay without host WAL"
                );
                self.pipe.phase(ConstructionPhase::BootstrapFactory);
                let provider = bootstrap.worker_bootstrap(
                    &spec.recipe.bootstrap.plugin.component,
                    &spec.bootstrap_config,
                    &spec.bootstrap_source_config,
                )?;
                self.pipe.phase(ConstructionPhase::AttachBootstrap);
                (inner.set_bootstrap_provider_fn)(inner.state, Box::into_raw(Box::new(provider)));
                self.pipe.phase(ConstructionPhase::StateDirectory);
                validate_state_directory(&spec.state_directory)?;
                self.pipe.phase(ConstructionPhase::StateStore);
                let store = Arc::new(drasi_state_store_redb::RedbStateStoreProvider::new(
                    spec.state_directory.join("source-state.redb"),
                )?);
                self.pipe.phase(ConstructionPhase::StateBridge);
                let store = StateStoreVtableBuilder::build(store);
                let context = FfiRuntimeContext {
                    instance_id: FfiStr::from_str(&spec.identity),
                    component_id: FfiStr::from_str(&spec.source_id),
                    state_store: Box::into_raw(Box::new(store)),
                    log_callback: Some(silent_log),
                    log_ctx: std::ptr::null_mut(),
                    lifecycle_callback: Some(silent_lifecycle),
                    lifecycle_ctx: std::ptr::null_mut(),
                    identity_provider: std::ptr::null(),
                    snapshot_fetcher: std::ptr::null(),
                    wal_provider: std::ptr::null(),
                };
                self.pipe.phase(ConstructionPhase::Initialize);
                (inner.initialize_fn)(inner.state, &context);
                self.spec = Some(spec);
                self.pipe.phase(ConstructionPhase::Ready);
                Ok(Response::Constructed)
            }
            Request::Start => {
                let source = self
                    .source
                    .as_ref()
                    .ok_or_else(|| failed("worker source is not constructed"))?;
                let result = (source.start_fn)(source.state);
                Ok(if unsafe { result.into_result() }.is_ok() {
                    Response::Ok
                } else {
                    Response::Failed
                })
            }
            Request::Status => {
                let source = self
                    .source
                    .as_ref()
                    .ok_or_else(|| failed("worker source is not constructed"))?;
                let status = match (source.status_fn)(source.state) {
                    FfiComponentStatus::Starting => ComponentStatus::Starting,
                    FfiComponentStatus::Running => ComponentStatus::Running,
                    FfiComponentStatus::Stopping => ComponentStatus::Stopping,
                    FfiComponentStatus::Stopped => ComponentStatus::Stopped,
                    FfiComponentStatus::Reconfiguring => ComponentStatus::Reconfiguring,
                    FfiComponentStatus::Error => ComponentStatus::Error,
                    FfiComponentStatus::Added => ComponentStatus::Added,
                    FfiComponentStatus::Removed => ComponentStatus::Removed,
                };
                Ok(Response::Status(status))
            }
            Request::Deprovision => {
                let source = self
                    .source
                    .as_ref()
                    .ok_or_else(|| failed("worker source is not constructed"))?;
                let result = (source.deprovision_fn)(source.state);
                Ok(if unsafe { result.into_result() }.is_ok() {
                    Response::Ok
                } else {
                    Response::Failed
                })
            }
            Request::RemovePin { query } => {
                let subscriptions: Vec<_> = self
                    .subscriptions
                    .iter()
                    .filter_map(|(id, subscription)| (subscription.query == query).then_some(*id))
                    .collect();
                for id in subscriptions {
                    self.unsubscribe(id)?;
                }
                let source = self
                    .source
                    .as_ref()
                    .ok_or_else(|| failed("worker source is not constructed"))?;
                let result =
                    (source.remove_position_handle_fn)(source.state, FfiStr::from_str(&query));
                unsafe { result.into_result() }
                    .map_err(|_| failed("foreign source pin cleanup failed"))?;
                self.pipe
                    .positions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&query);
                Ok(Response::Ok)
            }
            Request::Subscribe {
                subscription,
                settings,
            } => self.subscribe(subscription, settings),
        }
    }

    fn subscribe(&mut self, id: u64, settings: transport::Settings) -> anyhow::Result<Response> {
        let spec = self
            .spec
            .as_ref()
            .ok_or_else(|| failed("worker source is not constructed"))?;
        position(settings.position.as_deref())?;
        anyhow::ensure!(
            self.subscriptions.len() < MAX_SUBSCRIPTIONS
                && !self.subscriptions.contains_key(&id)
                && settings.source_id == spec.source_id
                && !self
                    .subscriptions
                    .values()
                    .any(|value| value.query == settings.query_id),
            "worker subscription identity or capacity conflict"
        );
        let source = self.source.as_ref().expect("owned source");
        let nodes = serde_json::to_string(&settings.nodes)?;
        let relations = serde_json::to_string(&settings.relations)?;
        let (position_ptr, position_len) = settings
            .position
            .as_ref()
            .map_or((std::ptr::null(), 0), |value| {
                (value.as_ptr(), value.len() as u32)
            });
        let pointer = (source.subscribe_fn)(
            source.state,
            FfiStr::from_str(&settings.source_id),
            settings.bootstrap,
            FfiStr::from_str(&settings.query_id),
            FfiStr::from_str(&nodes),
            FfiStr::from_str(&relations),
            position_ptr,
            position_len,
            settings.position_handle,
            settings.sequence.unwrap_or(0),
        );
        if pointer.is_null() {
            return Ok(Response::Failed);
        }
        let response = unsafe { *Box::from_raw(pointer) };
        let query = unsafe { response.query_id.into_string() };
        let source_id = unsafe { response.source_id.into_string() };
        anyhow::ensure!(
            query == settings.query_id
                && source_id == spec.source_id
                && !response.receiver.is_null()
                && response.bootstrap_receiver.is_null() != settings.bootstrap
                && response.bootstrap_result_receiver.is_null() != settings.bootstrap
                && response.position_handle_ptr.is_null() != settings.position_handle,
            "foreign source subscription contract is incomplete"
        );
        let position_handle = if response.position_handle_ptr.is_null() {
            None
        } else {
            Some(unsafe { Arc::from_raw(response.position_handle_ptr.cast::<AtomicU64>()) })
        };
        if let Some(handle) = &position_handle {
            let mut positions = self
                .pipe
                .positions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            anyhow::ensure!(
                positions.len() < MAX_SUBSCRIPTIONS || positions.contains_key(&query),
                "foreign source retained pin capacity exceeded"
            );
            if let Some(previous) = positions.get(&query) {
                anyhow::ensure!(
                    Arc::ptr_eq(previous, handle),
                    "foreign source replaced a retained position handle"
                );
            } else {
                let initial = handle.load(Ordering::Acquire);
                anyhow::ensure!(
                    initial == u64::MAX || Some(initial) == settings.sequence,
                    "foreign source position handle is already acknowledged"
                );
                positions.insert(query.clone(), handle.clone());
            }
        }
        let live = unsafe { *Box::from_raw(response.receiver) };
        let bootstrap = if response.bootstrap_receiver.is_null() {
            None
        } else {
            Some(unsafe { *Box::from_raw(response.bootstrap_receiver) })
        };
        let result = if response.bootstrap_result_receiver.is_null() {
            None
        } else {
            Some(unsafe { *Box::from_raw(response.bootstrap_result_receiver) })
        };
        let state = Arc::new(CallbackState {
            pipe: self.pipe.clone(),
            subscription: id,
            source: spec.source_id.clone(),
            cancelled: AtomicBool::new(false),
            done: (
                Mutex::new(Done {
                    live: false,
                    bootstrap: !settings.bootstrap,
                    result: !settings.bootstrap,
                }),
                Condvar::new(),
            ),
        });
        (live.start_push_fn)(
            live.state,
            live_callback,
            Arc::as_ptr(&state) as *mut c_void,
        );
        if let Some(bootstrap) = &bootstrap {
            (bootstrap.start_push_fn)(
                bootstrap.state,
                bootstrap_callback,
                Arc::as_ptr(&state) as *mut c_void,
            );
        }
        if let Some(result) = &result {
            (result.start_fn)(
                result.state,
                result_callback,
                Arc::as_ptr(&state) as *mut c_void,
            );
        }
        self.subscriptions.insert(
            id,
            Subscription {
                query,
                live,
                bootstrap,
                result,
                callbacks: state,
                _position: position_handle,
            },
        );
        Ok(Response::Subscribed {
            bootstrap: settings.bootstrap,
            position_handle: settings.position_handle,
        })
    }

    fn unsubscribe(&mut self, id: u64) -> anyhow::Result<()> {
        if let Some(subscription) = self.subscriptions.get_mut(&id) {
            anyhow::ensure!(
                self.retired.len() < MAX_RETIRED_CALLBACKS,
                "foreign source retired callback capacity exceeded"
            );
            subscription
                .callbacks
                .cancelled
                .store(true, Ordering::Release);
            (subscription.live.drop_fn)(subscription.live.state);
            if let Some(bootstrap) = subscription.bootstrap.take() {
                (bootstrap.drop_fn)(bootstrap.state);
            }
            if let Some(result) = subscription.result.take() {
                (result.drop_fn)(result.state);
            }
            let (done, changed) = &subscription.callbacks.done;
            let deadline = Instant::now() + STOP_GRACE;
            let mut done = done
                .lock()
                .map_err(|_| failed("worker callback join failed"))?;
            while !(done.live && done.bootstrap && done.result) {
                let remaining = deadline.saturating_duration_since(Instant::now());
                anyhow::ensure!(
                    !remaining.is_zero(),
                    "foreign source receiver cleanup did not complete"
                );
                let result = changed
                    .wait_timeout(done, remaining)
                    .map_err(|_| failed("worker callback join failed"))?;
                done = result.0;
            }
            drop(done);
            // Sentinels end delivery, but the SDK's task/TLS destructors are not
            // joinable. Keep every callback address valid until the OS wait.
            self.retired
                .push(self.subscriptions.remove(&id).expect("owned subscription"));
        }
        self.pipe.send(Command::Unsubscribed { subscription: id })
    }
}

struct Subscription {
    query: String,
    live: FfiChangeReceiver,
    bootstrap: Option<FfiBootstrapReceiver>,
    result: Option<FfiBootstrapResultReceiver>,
    callbacks: Arc<CallbackState>,
    _position: Option<Arc<AtomicU64>>,
}

struct Done {
    live: bool,
    bootstrap: bool,
    result: bool,
}
struct CallbackState {
    pipe: Arc<Pipe>,
    subscription: u64,
    source: String,
    cancelled: AtomicBool,
    done: (Mutex<Done>, Condvar),
}

impl CallbackState {
    fn event(&self, event: Event) -> bool {
        if self.cancelled.load(Ordering::Acquire) {
            return false;
        }
        self.pipe.send_or_exit(Command::Event {
            subscription: self.subscription,
            event,
        });
        true
    }
    fn finish(&self, kind: u8) {
        let mut done = self
            .done
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match kind {
            0 => done.live = true,
            1 => done.bootstrap = true,
            _ => done.result = true,
        }
        self.done.1.notify_all();
    }
}

extern "C" fn live_callback(context: *mut c_void, event: *mut FfiSourceEvent) -> bool {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let state = unsafe { &*context.cast::<CallbackState>() };
        if event.is_null() {
            state.event(Event::LiveEnd);
            state.finish(0);
            return false;
        }
        let event = unsafe { Box::from_raw(event) };
        let payload = copy_payload(event.payload_ptr, event.payload_len, event.payload_drop_fn);
        let payload = match payload {
            Ok(payload) => payload,
            Err(_) => std::process::exit(77),
        };
        let value = decode_source_event_payload(&payload).unwrap_or_else(|| std::process::exit(77));
        if value.source_id != state.source
            || value.sequence == 0
            || position(value.source_position.as_deref()).is_err()
        {
            std::process::exit(77);
        }
        let payload = SourceEventPayload::from_wrapper(&value);
        let bytes = serde_json::to_vec(&payload).unwrap_or_else(|_| std::process::exit(77));
        state.event(Event::Live(bytes))
    }));
    result.unwrap_or_else(|_| std::process::exit(77))
}

extern "C" fn bootstrap_callback(context: *mut c_void, event: *mut FfiBootstrapEvent) -> bool {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let state = unsafe { &*context.cast::<CallbackState>() };
        if event.is_null() {
            state.event(Event::BootstrapEnd);
            state.finish(1);
            return false;
        }
        let event = unsafe { Box::from_raw(event) };
        let payload = copy_payload(event.payload_ptr, event.payload_len, event.payload_drop_fn);
        let payload = match payload {
            Ok(payload) => payload,
            Err(_) => std::process::exit(77),
        };
        let value =
            decode_bootstrap_event_payload(&payload).unwrap_or_else(|| std::process::exit(77));
        if value.source_id != state.source {
            std::process::exit(77);
        }
        let payload = BootstrapEventPayload::from_event(&value);
        let bytes = serde_json::to_vec(&payload).unwrap_or_else(|_| std::process::exit(77));
        state.event(Event::Bootstrap(bytes))
    }));
    result.unwrap_or_else(|_| std::process::exit(77))
}

extern "C" fn result_callback(context: *mut c_void, result: *mut FfiBootstrapResult) {
    let guarded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let state = unsafe { &*context.cast::<CallbackState>() };
        if result.is_null() {
            state.event(Event::BootstrapFailed);
            state.finish(2);
            return;
        }
        let result = unsafe { Box::from_raw(result) };
        if !result.error_ptr.is_null() {
            if let Some(drop) = result.error_drop_fn {
                drop(result.error_ptr as *mut u8, result.error_len);
            } else {
                std::process::exit(77);
            }
        }
        let cut = if result.source_position_ptr.is_null() {
            if result.source_position_len != 0 {
                std::process::exit(77);
            }
            None
        } else {
            let value = copy_payload(
                result.source_position_ptr,
                result.source_position_len,
                result.source_position_drop_fn,
            )
            .unwrap_or_else(|_| std::process::exit(77));
            if position(Some(&value)).is_err() {
                std::process::exit(77);
            }
            Some(value)
        };
        if result.event_count < 0 {
            state.event(Event::BootstrapFailed);
        } else {
            state.event(Event::BootstrapResult {
                count: result.event_count as usize,
                position: cut,
            });
        }
        state.finish(2);
    }));
    if guarded.is_err() {
        std::process::exit(77);
    }
}

fn copy_payload(
    pointer: *const u8,
    size: usize,
    drop: Option<extern "C" fn(*mut u8, usize)>,
) -> anyhow::Result<Vec<u8>> {
    let valid = !pointer.is_null() && size > 0 && size <= MAX_PAYLOAD && drop.is_some();
    let value = if valid {
        Some(unsafe { std::slice::from_raw_parts(pointer, size).to_vec() })
    } else {
        None
    };
    if !pointer.is_null() {
        if let Some(drop) = drop {
            drop(pointer as *mut u8, size);
        }
    }
    value.ok_or_else(|| failed("foreign source payload violates the byte ABI"))
}

struct ResolverContext {
    pipe: Arc<Pipe>,
    artifact: String,
}

fn install_resolver(plugin: &LoadedPlugin, artifact: &str, pipe: Arc<Pipe>) {
    let context = Box::leak(Box::new(ResolverContext {
        pipe,
        artifact: artifact.into(),
    }));
    plugin.inject_config_resolver((context as *mut ResolverContext).cast(), resolve);
}

extern "C" fn resolve(context: *const c_void, request: FfiStr) -> FfiGetSecretResult {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() {
            return None;
        }
        let context = unsafe { &*context.cast::<ResolverContext>() };
        let request = bounded_str(&request, 512).ok()?;
        let id = context.pipe.next.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        {
            let mut requests = context.pipe.resolutions.lock().ok()?;
            if requests.len() >= 128 {
                return None;
            }
            requests.insert(id, sender);
        }
        context
            .pipe
            .send(Command::Resolve {
                id,
                artifact: context.artifact.clone(),
                request: request.into_bytes(),
            })
            .ok()?;
        receiver
            .recv_timeout(Duration::from_secs(5))
            .ok()
            .flatten()
            .filter(|value| value.len() <= 16 << 10 && !value.as_bytes().contains(&0))
    }));
    match result {
        Ok(Some(value)) => FfiGetSecretResult::ok(value),
        _ => FfiGetSecretResult::err("scoped worker credential is unavailable".into()),
    }
}

extern "C" fn silent_log(_: *mut c_void, _: *const FfiLogEntry) {}
extern "C" fn silent_lifecycle(_: *mut c_void, _: *const FfiLifecycleEvent) {}

fn bounded_str(value: &FfiStr, limit: usize) -> anyhow::Result<String> {
    anyhow::ensure!(
        !value.ptr.is_null() && value.len <= limit,
        "foreign plugin string exceeds capacity"
    );
    let value = unsafe { std::slice::from_raw_parts(value.ptr.cast::<u8>(), value.len) };
    Ok(std::str::from_utf8(value)
        .map_err(|_| failed("foreign plugin string is invalid"))?
        .into())
}

fn validate_state_directory(path: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        path.is_absolute(),
        "worker state directory must be absolute"
    );
    std::fs::create_dir_all(path)
        .map_err(|_| failed("worker state directory could not be created"))?;
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| failed("worker state directory is unavailable"))?;
    anyhow::ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "worker state directory is not a private directory"
    );
    let file = path.join("source-state.redb");
    if let Ok(metadata) = std::fs::symlink_metadata(&file) {
        anyhow::ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "worker state store is not a regular file"
        );
    }
    Ok(())
}

fn load(artifact: &TrustedArtifact) -> anyhow::Result<Arc<LoadedPlugin>> {
    anyhow::ensure!(
        artifact.path.is_absolute()
            && artifact.binary_sha256.len() == 64
            && artifact
                .binary_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            && artifact.sdk_version == drasi_plugin_sdk::SDK_VERSION
            && artifact.abi_version == FFI_SDK_VERSION
            && artifact.core_version == env!("TDRASI_CORE_VERSION")
            && artifact.lib_version == env!("TDRASI_LIB_VERSION")
            && artifact.target_triple == TARGET_TRIPLE,
        "incompatible plugin artifact SDK, ABI, core, lib or target"
    );
    let metadata = std::fs::symlink_metadata(&artifact.path)
        .map_err(|_| failed("worker artifact is unavailable"))?;
    anyhow::ensure!(
        metadata.is_file() && metadata.len() > 0 && metadata.len() <= 256 << 20,
        "worker artifact is not a bounded regular file"
    );
    let mut file = std::fs::File::open(&artifact.path)
        .map_err(|_| failed("worker artifact could not be opened"))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| failed("worker artifact could not be read"))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    anyhow::ensure!(
        format!("{:x}", hasher.finalize()) == artifact.binary_sha256,
        "worker artifact digest mismatch"
    );
    use std::os::fd::AsRawFd;
    #[cfg(target_os = "linux")]
    let path = PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()));
    #[cfg(not(target_os = "linux"))]
    let path = PathBuf::from(format!("/dev/fd/{}", file.as_raw_fd()));
    let library = Arc::new(
        unsafe { libloading::Library::new(&path) }
            .map_err(|_| failed("worker verified library could not be opened"))?,
    );
    // No foreign TLS, vtable, callback, or runtime may outlive its machine code.
    std::mem::forget(library.clone());
    let function = unsafe {
        library.get::<unsafe extern "C" fn() -> *const PluginMetadata>(b"drasi_plugin_metadata")
    }
    .map_err(|_| failed("worker plugin ABI metadata is missing"))?;
    let pointer = unsafe { function() };
    anyhow::ensure!(!pointer.is_null(), "worker plugin ABI metadata is null");
    let metadata = unsafe { &*pointer };
    let abi = bounded_str(&metadata.sdk_version, 128)?;
    let core = bounded_str(&metadata.core_version, 128)?;
    let lib = bounded_str(&metadata.lib_version, 128)?;
    let target = bounded_str(&metadata.target_triple, 256)?;
    let version = bounded_str(&metadata.plugin_version, 128)?;
    anyhow::ensure!(
        abi == artifact.abi_version
            && core == artifact.core_version
            && lib == artifact.lib_version
            && target == artifact.target_triple
            && version == artifact.plugin_version,
        "incompatible plugin binary metadata: ABI {abi:?} (require {:?}), core {core:?} (expected {:?}), lib {lib:?} (expected {:?}), target {target:?} (expected {:?}), version {version:?} (expected {:?})",
        artifact.abi_version, artifact.core_version, artifact.lib_version,
        artifact.target_triple, artifact.plugin_version
    );
    let loaded =
        Arc::new(native::load(library).map_err(|_| failed("worker plugin registration failed"))?);
    std::mem::forget(loaded.clone());
    // Keep the hashed inode open through registration; dlopen used that inode,
    // not a mutable pathname that could be replaced between verification/load.
    std::mem::forget(file);
    Ok(loaded)
}

fn descriptors(plugin: &LoadedPlugin) -> anyhow::Result<Vec<PluginDescriptor>> {
    let mut result = Vec::new();
    macro_rules! describe {
        ($field:ident, $category:expr, $supported:expr) => {
            for descriptor in &plugin.$field {
                let schema = descriptor.config_schema_json();
                anyhow::ensure!(
                    schema.len() <= 64 << 10 && result.len() < 128,
                    "worker descriptor capacity exceeded"
                );
                let schema: Value = serde_json::from_str(&schema)
                    .map_err(|_| failed("worker descriptor schema is invalid"))?;
                anyhow::ensure!(
                    schema.is_object()
                        && descriptor.kind().len() <= 256
                        && descriptor.config_version().len() <= 128
                        && descriptor.config_schema_name().len() <= 256,
                    "worker descriptor is invalid"
                );
                result.push(PluginDescriptor {
                    plugin: String::new(),
                    component: descriptor.kind().into(),
                    category: $category,
                    config_version: descriptor.config_version().into(),
                    schema_name: descriptor.config_schema_name().into(),
                    configuration_schema: schema,
                    supported: $supported,
                });
            }
        };
    }
    describe!(source_plugins, PluginCategory::Source, true);
    describe!(bootstrap_plugins, PluginCategory::Bootstrap, true);
    describe!(reaction_plugins, PluginCategory::Reaction, false);
    describe!(identity_provider_plugins, PluginCategory::Identity, false);
    describe!(secret_store_plugins, PluginCategory::SecretStore, false);
    Ok(result)
}
