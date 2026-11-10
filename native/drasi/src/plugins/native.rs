use drasi_host_sdk::{
    callbacks::{default_lifecycle_callback, default_log_callback},
    proxies::{
        bootstrap_provider::BootstrapPluginProxy, identity_provider::IdentityProviderPluginProxy,
        reaction::ReactionPluginProxy, secret_store::SecretStorePluginProxy,
        source::SourcePluginProxy,
    },
};
use drasi_plugin_sdk::ffi::{
    callbacks::{ConfigResolverFn, FfiLogLevel, LifecycleCallbackFn, LogCallbackFn},
    FfiCreateResult, FfiLifecycleEvent, FfiLogEntry, FfiPluginRegistration, FfiRuntimeContext,
    FfiStr, SourcePluginVtable, SourceVtable,
};
use libloading::Library;
use std::{
    collections::BTreeMap,
    ffi::c_void,
    sync::{Arc, Mutex, OnceLock},
};

pub(crate) struct LoadedPlugin {
    pub source_plugins: Vec<SourcePluginProxy>,
    pub reaction_plugins: Vec<ReactionPluginProxy>,
    pub bootstrap_plugins: Vec<BootstrapPluginProxy>,
    pub identity_provider_plugins: Vec<IdentityProviderPluginProxy>,
    pub secret_store_plugins: Vec<SecretStorePluginProxy>,
    worker_sources: Vec<WorkerFactory>,
    worker_bootstrap: Vec<WorkerFactory>,
    resolver: extern "C" fn(*mut c_void, ConfigResolverFn),
    _library: Arc<Library>,
}

impl LoadedPlugin {
    pub fn inject_config_resolver(&self, context: *mut c_void, callback: ConfigResolverFn) {
        (self.resolver)(context, callback);
    }

    #[cfg(test)]
    pub fn config_resolver_injection_fn(&self) -> extern "C" fn(*mut c_void, ConfigResolverFn) {
        self.resolver
    }

    pub(crate) fn worker_source(
        &self,
        kind: &str,
        id: &str,
        configuration: &serde_json::Value,
    ) -> anyhow::Result<SourceVtable> {
        let factory = self
            .worker_sources
            .iter()
            .find(|value| value.kind == kind)
            .ok_or_else(|| anyhow::anyhow!("worker source factory is unavailable"))?;
        let configuration = serde_json::to_string(configuration)?;
        let result = (factory.source.expect("source factory"))(
            factory.state as *mut c_void,
            FfiStr::from_str(id),
            FfiStr::from_str(&configuration),
            false,
        );
        anyhow::ensure!(
            result.error_code == 0 && !result.ptr.is_null(),
            "worker source factory failed"
        );
        Ok(unsafe { *Box::from_raw(result.ptr.cast::<SourceVtable>()) })
    }

    pub(crate) fn worker_bootstrap(
        &self,
        kind: &str,
        configuration: &serde_json::Value,
        source_configuration: &serde_json::Value,
    ) -> anyhow::Result<drasi_plugin_sdk::ffi::BootstrapProviderVtable> {
        let factory = self
            .worker_bootstrap
            .iter()
            .find(|value| value.kind == kind)
            .ok_or_else(|| anyhow::anyhow!("worker bootstrap factory is unavailable"))?;
        let configuration = serde_json::to_string(configuration)?;
        let source_configuration = serde_json::to_string(source_configuration)?;
        let result = (factory.bootstrap.expect("bootstrap factory"))(
            factory.state as *mut c_void,
            FfiStr::from_str(&configuration),
            FfiStr::from_str(&source_configuration),
        );
        anyhow::ensure!(
            result.error_code == 0 && !result.ptr.is_null(),
            "worker bootstrap factory failed"
        );
        Ok(unsafe {
            *Box::from_raw(
                result
                    .ptr
                    .cast::<drasi_plugin_sdk::ffi::BootstrapProviderVtable>(),
            )
        })
    }
}

type DropFn = extern "C" fn(*mut c_void);
type CreateFn = extern "C" fn(*mut c_void, FfiStr, FfiStr, bool) -> FfiCreateResult;
type InitializeFn = extern "C" fn(*mut c_void, *const FfiRuntimeContext);
type BootstrapCreateFn = extern "C" fn(*mut c_void, FfiStr, FfiStr) -> FfiCreateResult;

struct WorkerFactory {
    kind: String,
    state: usize,
    source: Option<CreateFn>,
    bootstrap: Option<BootstrapCreateFn>,
}

#[derive(Clone, Copy)]
struct Factory {
    create: CreateFn,
    drop: DropFn,
}

#[derive(Clone, Copy)]
struct Source {
    initialize: InitializeFn,
    drop: DropFn,
}

fn factories() -> &'static Mutex<BTreeMap<usize, Factory>> {
    static FACTORIES: OnceLock<Mutex<BTreeMap<usize, Factory>>> = OnceLock::new();
    FACTORIES.get_or_init(Mutex::default)
}

fn sources() -> &'static Mutex<BTreeMap<usize, Source>> {
    static SOURCES: OnceLock<Mutex<BTreeMap<usize, Source>>> = OnceLock::new();
    SOURCES.get_or_init(Mutex::default)
}

struct Callbacks {
    instance: String,
    component: String,
    log: Option<LogCallbackFn>,
    log_context: usize,
    lifecycle: Option<LifecycleCallbackFn>,
    lifecycle_context: usize,
}

fn report(message: &'static str) {
    default_log_callback(
        std::ptr::null_mut(),
        &FfiLogEntry {
            level: FfiLogLevel::Error,
            plugin_id: FfiStr::from_str("native"),
            target: FfiStr::from_str("temporal_drasi"),
            message: FfiStr::from_str(message),
            timestamp_us: 0,
            instance_id: FfiStr::from_str(""),
            component_id: FfiStr::from_str(""),
        },
    );
}

pub(crate) extern "C" fn redacted_log_callback(context: *mut c_void, entry: *const FfiLogEntry) {
    if entry.is_null() {
        return;
    }
    let entry = unsafe { &*entry };
    let callbacks = (!context.is_null()).then(|| unsafe { &*context.cast::<Callbacks>() });
    let sanitized = FfiLogEntry {
        level: entry.level,
        plugin_id: FfiStr::from_str("native"),
        target: FfiStr::from_str("temporal_drasi"),
        message: FfiStr::from_str("Native plugin diagnostic redacted"),
        timestamp_us: entry.timestamp_us,
        instance_id: FfiStr::from_str(callbacks.map_or("", |value| &value.instance)),
        component_id: FfiStr::from_str(callbacks.map_or("", |value| &value.component)),
    };
    let (callback, target) = callbacks
        .and_then(|value| value.log.map(|callback| (callback, value.log_context)))
        .unwrap_or((default_log_callback, 0));
    callback(target as *mut c_void, &sanitized);
}

pub(crate) extern "C" fn redacted_lifecycle_callback(
    context: *mut c_void,
    event: *const FfiLifecycleEvent,
) {
    if event.is_null() {
        return;
    }
    let event = unsafe { &*event };
    let callbacks = (!context.is_null()).then(|| unsafe { &*context.cast::<Callbacks>() });
    let sanitized = FfiLifecycleEvent {
        component_id: FfiStr::from_str(callbacks.map_or("native", |value| &value.component)),
        component_type: FfiStr::from_str("source"),
        event_type: event.event_type,
        message: FfiStr::from_str("Native plugin lifecycle diagnostic redacted"),
        timestamp_us: event.timestamp_us,
    };
    let (callback, target) = callbacks
        .and_then(|value| {
            value
                .lifecycle
                .map(|callback| (callback, value.lifecycle_context))
        })
        .unwrap_or((default_lifecycle_callback, 0));
    callback(target as *mut c_void, &sanitized);
}

extern "C" fn initialize(state: *mut c_void, context: *const FfiRuntimeContext) {
    let Ok(registered) = sources().lock() else {
        report("Native source callback registry lock poisoned");
        return;
    };
    let source = registered.get(&(state as usize)).copied();
    drop(registered);
    let Some(source) = source else {
        report("Native source callback registration is missing");
        return;
    };
    if context.is_null() {
        report("Native source runtime context is missing");
        return;
    }
    let context = unsafe { &*context };
    // The stock SDK retains callback contexts after source drop for late callbacks.
    let callbacks = Box::leak(Box::new(Callbacks {
        instance: unsafe { context.instance_id.to_string() },
        component: unsafe { context.component_id.to_string() },
        log: context.log_callback,
        log_context: context.log_ctx as usize,
        lifecycle: context.lifecycle_callback,
        lifecycle_context: context.lifecycle_ctx as usize,
    }));
    let pointer = (callbacks as *mut Callbacks).cast::<c_void>();
    let sanitized = FfiRuntimeContext {
        instance_id: FfiStr::from_str(&callbacks.instance),
        component_id: FfiStr::from_str(&callbacks.component),
        state_store: context.state_store,
        log_callback: Some(redacted_log_callback),
        log_ctx: pointer,
        lifecycle_callback: Some(redacted_lifecycle_callback),
        lifecycle_ctx: pointer,
        identity_provider: context.identity_provider,
        snapshot_fetcher: context.snapshot_fetcher,
        // The published WAL bridge exchanges Rust-layout SourceChange values.
        wal_provider: std::ptr::null(),
    };
    (source.initialize)(state, &sanitized);
}

extern "C" fn drop_source(state: *mut c_void) {
    let Ok(mut registered) = sources().lock() else {
        report("Native source callback registry lock poisoned");
        return;
    };
    let source = registered.remove(&(state as usize));
    drop(registered);
    let Some(source) = source else {
        report("Native source callback registration is missing during drop");
        return;
    };
    (source.drop)(state);
}

extern "C" fn create_source(
    state: *mut c_void,
    id: FfiStr,
    configuration: FfiStr,
    auto_start: bool,
) -> FfiCreateResult {
    let Ok(registered) = factories().lock() else {
        return FfiCreateResult::err("native factory callback registry lock poisoned".into());
    };
    let factory = registered.get(&(state as usize)).copied();
    drop(registered);
    let Some(factory) = factory else {
        return FfiCreateResult::err("native factory callback registration is missing".into());
    };
    let result = (factory.create)(state, id, configuration, auto_start);
    if result.error_code != 0 || result.ptr.is_null() {
        return result;
    }
    let source = unsafe { &mut *result.ptr.cast::<SourceVtable>() };
    let Ok(mut registered) = sources().lock() else {
        let owned = unsafe { Box::from_raw(result.ptr.cast::<SourceVtable>()) };
        (owned.drop_fn)(owned.state);
        return FfiCreateResult::err("native source callback registry lock poisoned".into());
    };
    if registered.contains_key(&(source.state as usize)) {
        drop(registered);
        drop(unsafe { Box::from_raw(result.ptr.cast::<SourceVtable>()) });
        return FfiCreateResult::err("native source callback identity conflict".into());
    }
    if registered.len() >= 4096 {
        drop(registered);
        let owned = unsafe { Box::from_raw(result.ptr.cast::<SourceVtable>()) };
        (owned.drop_fn)(owned.state);
        return FfiCreateResult::err("native source callback capacity exceeded".into());
    }
    registered.insert(
        source.state as usize,
        Source {
            initialize: source.initialize_fn,
            drop: source.drop_fn,
        },
    );
    source.initialize_fn = initialize;
    source.drop_fn = drop_source;
    result
}

extern "C" fn drop_factory(state: *mut c_void) {
    let Ok(mut registered) = factories().lock() else {
        report("Native factory callback registry lock poisoned");
        return;
    };
    let factory = registered.remove(&(state as usize));
    drop(registered);
    let Some(factory) = factory else {
        report("Native factory callback registration is missing during drop");
        return;
    };
    (factory.drop)(state);
}

fn protect(mut factory: SourcePluginVtable) -> anyhow::Result<SourcePluginVtable> {
    let mut registered = factories()
        .lock()
        .map_err(|_| anyhow::anyhow!("native factory callback registry lock poisoned"))?;
    anyhow::ensure!(
        registered.len() < 8192 && !registered.contains_key(&(factory.state as usize)),
        "native factory callback capacity or identity conflict"
    );
    registered.insert(
        factory.state as usize,
        Factory {
            create: factory.create_source_fn,
            drop: factory.drop_fn,
        },
    );
    factory.create_source_fn = create_source;
    factory.drop_fn = drop_factory;
    Ok(factory)
}

unsafe fn take<T>(pointer: *mut T, count: usize) -> anyhow::Result<Vec<T>> {
    anyhow::ensure!(count <= 128, "native plugin factory capacity exceeded");
    if count == 0 {
        return Ok(Vec::new());
    }
    anyhow::ensure!(!pointer.is_null(), "native plugin factory array is missing");
    // The public SDK registration contract transfers boxed-slice ownership.
    Ok(unsafe { Vec::from_raw_parts(pointer, count, count) })
}

pub(crate) fn load(library: Arc<Library>) -> anyhow::Result<LoadedPlugin> {
    let initialize = unsafe {
        library.get::<unsafe extern "C" fn() -> *mut FfiPluginRegistration>(b"drasi_plugin_init")?
    };
    let pointer = unsafe { initialize() };
    anyhow::ensure!(!pointer.is_null(), "native plugin registration is missing");
    let registration = unsafe { Box::from_raw(pointer) };
    (registration.set_log_callback)(std::ptr::null_mut(), redacted_log_callback);
    (registration.set_lifecycle_callback)(std::ptr::null_mut(), redacted_lifecycle_callback);
    let source = unsafe {
        take(
            registration.source_plugins,
            registration.source_plugin_count,
        )?
    };
    let reaction = unsafe {
        take(
            registration.reaction_plugins,
            registration.reaction_plugin_count,
        )?
    };
    let bootstrap = unsafe {
        take(
            registration.bootstrap_plugins,
            registration.bootstrap_plugin_count,
        )?
    };
    let identity = unsafe {
        take(
            registration.identity_provider_plugins,
            registration.identity_provider_plugin_count,
        )?
    };
    let secret = unsafe {
        take(
            registration.secret_store_plugins,
            registration.secret_store_plugin_count,
        )?
    };
    let source = source
        .into_iter()
        .map(protect)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let worker_sources = source
        .iter()
        .map(|value| WorkerFactory {
            kind: unsafe { (value.kind_fn)(value.state).to_string() },
            state: value.state as usize,
            source: Some(value.create_source_fn),
            bootstrap: None,
        })
        .collect();
    let worker_bootstrap = bootstrap
        .iter()
        .map(|value| WorkerFactory {
            kind: unsafe { (value.kind_fn)(value.state).to_string() },
            state: value.state as usize,
            source: None,
            bootstrap: Some(value.create_bootstrap_provider_fn),
        })
        .collect();
    let source_plugins = source
        .into_iter()
        .map(|value| SourcePluginProxy::new(value, library.clone()))
        .collect();
    Ok(LoadedPlugin {
        source_plugins,
        reaction_plugins: reaction
            .into_iter()
            .map(|value| ReactionPluginProxy::new(value, library.clone()))
            .collect(),
        bootstrap_plugins: bootstrap
            .into_iter()
            .map(|value| BootstrapPluginProxy::new(value, library.clone()))
            .collect(),
        identity_provider_plugins: identity
            .into_iter()
            .map(|value| IdentityProviderPluginProxy::new(value, library.clone()))
            .collect(),
        secret_store_plugins: secret
            .into_iter()
            .map(|value| SecretStorePluginProxy::new(value, library.clone()))
            .collect(),
        worker_sources,
        worker_bootstrap,
        resolver: registration.set_config_resolver,
        _library: library,
    })
}
