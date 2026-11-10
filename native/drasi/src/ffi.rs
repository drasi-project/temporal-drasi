use crate::{protocol::*, runtime::Host};
use futures_util::FutureExt;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

mod admission;
mod plugin;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Buffer {
    pub allocation: u64,
    pub data: *const u8,
    pub len: usize,
}

#[repr(C)]
pub struct Status {
    pub code: i32,
    pub reserved: u32,
    pub buffer: Buffer,
}

type RuntimeHandle = Arc<RuntimeEntry>;

struct RuntimeEntry {
    host: tokio::sync::RwLock<Host>,
    plugins: Arc<plugin::Gate>,
    startup: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
}

impl RuntimeEntry {
    fn new(host: Host, startup: Option<tokio::sync::OwnedSemaphorePermit>) -> Self {
        Self {
            host: tokio::sync::RwLock::new(host),
            plugins: Arc::default(),
            startup: Mutex::new(startup),
        }
    }
}

struct Delivery(Option<Result<Value>>);

impl Drop for Delivery {
    fn drop(&mut self) {
        if let Some(Ok(value)) = self.0.as_ref() {
            if let Some(handle) = value.get("handle").and_then(Value::as_u64) {
                if let Ok(orphan) = host(handle) {
                    executor().spawn(async move {
                        if orphan.host.write().await.close().await.is_ok() {
                            lock(hosts()).remove(&handle);
                        }
                    });
                }
            }
        }
    }
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static HOSTS: OnceLock<Mutex<HashMap<u64, RuntimeHandle>>> = OnceLock::new();
static BUFFERS: OnceLock<Mutex<HashMap<u64, Box<[u8]>>>> = OnceLock::new();
static CANCELLATIONS: OnceLock<Mutex<HashMap<u64, Arc<AtomicBool>>>> = OnceLock::new();
static TOKIO: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn hosts() -> &'static Mutex<HashMap<u64, RuntimeHandle>> {
    HOSTS.get_or_init(Mutex::default)
}
fn buffers() -> &'static Mutex<HashMap<u64, Box<[u8]>>> {
    BUFFERS.get_or_init(Mutex::default)
}
fn cancellations() -> &'static Mutex<HashMap<u64, Arc<AtomicBool>>> {
    CANCELLATIONS.get_or_init(Mutex::default)
}
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn executor() -> &'static tokio::runtime::Runtime {
    TOKIO.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("cannot create Drasi Tokio runtime")
    })
}

fn admission_slots() -> &'static Arc<tokio::sync::Semaphore> {
    static SLOTS: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(8)))
}

fn status(result: Result<Value>) -> Status {
    let (code, value) = match result {
        Ok(value) => (0, value),
        Err(error) => (error.code, json!(error)),
    };
    let bytes = serde_json::to_vec(&value)
        .unwrap_or_else(|_| br#"{"code":7,"message":"JSON encoding failed"}"#.to_vec());
    if bytes.len() > MAX_OUTPUT {
        return status(Err(Error::new(
            3,
            "native response exceeds the ABI byte limit",
        )));
    }
    let bytes = bytes.into_boxed_slice();
    let buffer = Buffer {
        allocation: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        data: bytes.as_ptr(),
        len: bytes.len(),
    };
    lock(buffers()).insert(buffer.allocation, bytes);
    Status {
        code,
        reserved: 0,
        buffer,
    }
}

fn boundary(operation: impl FnOnce() -> Result<Value>) -> Status {
    status(
        catch_unwind(AssertUnwindSafe(operation))
            .unwrap_or_else(|_| Err(Error::new(7, "panic contained at the native ABI boundary"))),
    )
}

// Inputs are copied before scheduling; the ABI never retains caller memory.
unsafe fn input<T: DeserializeOwned>(pointer: *const u8, length: usize) -> Result<T> {
    if pointer.is_null() || length == 0 || length > MAX_INPUT || length > isize::MAX as usize {
        return Err(Error::new(1, "invalid input pointer or byte length"));
    }
    let bytes = unsafe { std::slice::from_raw_parts(pointer, length) };
    Ok(serde_json::from_slice(bytes)?)
}

fn host(handle: u64) -> Result<RuntimeHandle> {
    lock(hosts())
        .get(&handle)
        .cloned()
        .ok_or_else(|| Error::new(2, "unknown runtime handle"))
}

fn cancellation(handle: u64) -> Result<Arc<AtomicBool>> {
    lock(cancellations())
        .get(&handle)
        .cloned()
        .ok_or_else(|| Error::new(2, "unknown cancellation handle"))
}

fn wait(
    cancellation: Arc<AtomicBool>,
    timeout_ms: u32,
    future: impl Future<Output = Result<Value>> + Send + 'static,
) -> Result<Value> {
    check_wait(&cancellation, timeout_ms)?;
    let (sender, receiver) = mpsc::sync_channel(1);
    executor().spawn(async move {
        let result = AssertUnwindSafe(future)
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(Error::new(7, "panic contained in native operation")));
        let _ = sender.send(Delivery(Some(result)));
    });
    let deadline = Instant::now() + Duration::from_millis(timeout_ms.into());
    loop {
        if cancellation.load(Ordering::Acquire) {
            return Err(Error::new(
                4,
                "native operation cancelled; an already-started mutation may still complete",
            ));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::new(
                5,
                "native deadline exceeded; an already-started mutation may still complete",
            ));
        }
        match receiver.recv_timeout(remaining.min(Duration::from_millis(5))) {
            Ok(mut delivery) => {
                let result = delivery.0.take().expect("native delivery has a result");
                if let Ok(value) = &result {
                    if let Some(handle) = value.get("handle").and_then(Value::as_u64) {
                        if let Ok(host) = host(handle) {
                            lock(&host.startup).take();
                        }
                    }
                }
                return result;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(Error::new(7, "native worker disconnected"))
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
}

fn check_wait(cancellation: &AtomicBool, timeout_ms: u32) -> Result<()> {
    if timeout_ms == 0 || timeout_ms > 30_000 {
        return Err(Error::new(
            1,
            "native timeout must be in 1..30000 milliseconds",
        ));
    }
    if cancellation.load(Ordering::Acquire) {
        return Err(Error::new(4, "native operation cancelled"));
    }
    Ok(())
}

#[no_mangle]
pub extern "C" fn temporal_drasi_v1_abi_version() -> u32 {
    VERSION
}

/// Compiles a query without opening persistent runtime state.
///
/// # Safety
/// `data` must point to `length` readable bytes for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn temporal_drasi_v1_validate_query(
    data: *const u8,
    length: usize,
    cancel: u64,
    timeout_ms: u32,
) -> Status {
    boundary(|| {
        let compilation = unsafe { input::<crate::query::Request>(data, length)? }.prepare()?;
        wait(cancellation(cancel)?, timeout_ms, compilation.validate())
    })
}

#[no_mangle]
pub extern "C" fn temporal_drasi_v1_cancel_new() -> u64 {
    let handle = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    lock(cancellations()).insert(handle, Arc::new(AtomicBool::new(false)));
    handle
}

#[no_mangle]
pub extern "C" fn temporal_drasi_v1_cancel(handle: u64) -> i32 {
    match cancellation(handle) {
        Ok(token) => {
            token.store(true, Ordering::Release);
            0
        }
        Err(error) => error.code,
    }
}

#[no_mangle]
pub extern "C" fn temporal_drasi_v1_cancel_free(handle: u64) -> i32 {
    if lock(cancellations()).remove(&handle).is_some() {
        0
    } else {
        2
    }
}

#[no_mangle]
pub extern "C" fn temporal_drasi_v1_buffer_free(allocation: u64) -> i32 {
    if allocation == 0 || lock(buffers()).remove(&allocation).is_some() {
        0
    } else {
        2
    }
}

/// # Safety
/// `data` must point to `length` readable bytes for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn temporal_drasi_v1_open(
    data: *const u8,
    length: usize,
    cancel: u64,
    timeout_ms: u32,
) -> Status {
    boundary(|| {
        let config: Config = unsafe { input(data, length)? };
        let token = cancellation(cancel)?;
        check_wait(&token, timeout_ms)?;
        let startup = if config.plugins.is_empty() {
            None
        } else {
            Some(plugin::permit()?)
        };
        wait(token, timeout_ms, async move {
            let runtime = Host::open(config).await?;
            let handle = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            lock(hosts()).insert(handle, Arc::new(RuntimeEntry::new(runtime, startup)));
            Ok(json!({ "handle": handle }))
        })
    })
}

/// # Safety
/// `data` must point to `length` readable bytes for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn temporal_drasi_v1_apply(
    handle: u64,
    data: *const u8,
    length: usize,
    cancel: u64,
    timeout_ms: u32,
) -> Status {
    boundary(|| {
        let request: Request = unsafe { input(data, length)? };
        let host = host(handle)?;
        let token = cancellation(cancel)?;
        check_wait(&token, timeout_ms)?;
        match request {
            Request::Checkpoint { destination } => {
                host.plugins.seal();
                wait(token, timeout_ms, async move {
                    host.plugins.drain().await?;
                    host.host.write().await.checkpoint(&destination).await
                })
            }
            Request::Command(command) if command.is_plugin_management() => {
                let flight = host.plugins.admit()?;
                wait(token, timeout_ms, async move {
                    tokio::task::spawn_blocking(move || plugin::run(host, command, flight))
                        .await
                        .map_err(|_| Error::new(7, "native plugin worker failed"))?
                })
            }
            Request::Command(command) => wait(token, timeout_ms, async move {
                host.host.read().await.apply(command).await
            }),
        }
    })
}

/// Runs one management operation to completion without a detached caller wait.
///
/// # Safety
/// `data` must point to `length` readable bytes for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn temporal_drasi_v1_plugin_run_joined(
    handle: u64,
    data: *const u8,
    length: usize,
) -> Status {
    boundary(|| {
        let command: Command = unsafe { input(data, length)? };
        if !command.is_plugin_management() {
            return Err(Error::new(
                1,
                "joined plugin call requires a management command",
            ));
        }
        let host = host(handle)?;
        let flight = host.plugins.admit()?;
        plugin::run(host, command, flight)
    })
}

/// Admits a managed Source with a separate, non-JSON credential packet.
///
/// # Safety
/// Both pointers must reference their declared readable byte lengths for this call.
#[no_mangle]
pub unsafe extern "C" fn temporal_drasi_v1_admit_plugin_source(
    handle: u64,
    definition: *const u8,
    definition_length: usize,
    values: *const u8,
    values_length: usize,
    cancel: u64,
    timeout_ms: u32,
) -> Status {
    boundary(|| {
        let definition: NodeDefinition = unsafe { input(definition, definition_length)? };
        let host = host(handle)?;
        let token = cancellation(cancel)?;
        let permit = admission_slots()
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::new(3, "plugin credential transfer capacity exhausted"))?;
        if values.is_null() || values_length == 0 || values_length > MAX_INPUT {
            return Err(Error::new(
                1,
                "invalid plugin credential pointer or byte length",
            ));
        }
        let values =
            admission::decode(unsafe { std::slice::from_raw_parts(values, values_length) })?;
        wait(token, timeout_ms, async move {
            let _permit = permit;
            let node = host
                .host
                .read()
                .await
                .admit_plugin_source(definition, values)
                .await?;
            Ok(json!({ "node": node }))
        })
    })
}

#[no_mangle]
pub extern "C" fn temporal_drasi_v1_poll(
    handle: u64,
    limit: u32,
    wait_ms: u32,
    cancel: u64,
    timeout_ms: u32,
) -> Status {
    boundary(|| {
        if wait_ms > 25_000 {
            return Err(Error::new(1, "poll wait exceeds 25000 milliseconds"));
        }
        let host = host(handle)?;
        let token = cancellation(cancel)?;
        let poll_token = token.clone();
        wait(token, timeout_ms, async move {
            let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms.into());
            loop {
                if poll_token.load(Ordering::Acquire) {
                    return Err(Error::new(4, "poll cancelled"));
                }
                let events = host.host.read().await.poll(limit as usize).await?;
                if !events.is_empty() || tokio::time::Instant::now() >= deadline {
                    return Ok(json!({ "events": events }));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    })
}

/// # Safety
/// `data` must point to `length` readable bytes for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn temporal_drasi_v1_ack(
    handle: u64,
    data: *const u8,
    length: usize,
    cancel: u64,
    timeout_ms: u32,
) -> Status {
    boundary(|| {
        let receipt: Receipt = unsafe { input(data, length)? };
        let host = host(handle)?;
        wait(cancellation(cancel)?, timeout_ms, async move {
            host.host.read().await.ack(receipt).await?;
            Ok(json!({}))
        })
    })
}

#[no_mangle]
pub extern "C" fn temporal_drasi_v1_close(handle: u64, cancel: u64, timeout_ms: u32) -> Status {
    boundary(|| {
        let host = host(handle)?;
        let token = cancellation(cancel)?;
        check_wait(&token, timeout_ms)?;
        host.plugins.seal();
        wait(token, timeout_ms, async move {
            let drained = host.plugins.drain().await;
            // Failed handles retain their directory lock and shutdown evidence.
            let closed = host.host.write().await.close().await;
            drained?;
            closed?;
            lock(hosts()).remove(&handle);
            Ok(json!({}))
        })
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn decode(status: Status) -> (i32, Value) {
        let bytes = unsafe { std::slice::from_raw_parts(status.buffer.data, status.buffer.len) };
        let value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(temporal_drasi_v1_buffer_free(status.buffer.allocation), 0);
        assert_eq!(temporal_drasi_v1_buffer_free(status.buffer.allocation), 2);
        (status.code, value)
    }

    pub(crate) async fn exercise_sensitive_admission(
        runtime: Host,
        definition: NodeDefinition,
        packet: Vec<u8>,
    ) {
        let fresh = runtime
            .apply(Command::CheckPluginSource {
                definition: definition.clone(),
            })
            .await
            .unwrap();
        assert_eq!(fresh, json!({"materialization_required": true}));
        let handle = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let runtime = Arc::new(RuntimeEntry::new(runtime, None));
        lock(hosts()).insert(handle, runtime.clone());
        let blocker = runtime.host.write().await;
        let mut cleared_inputs = Vec::new();
        for _ in 0..8 {
            let mut input = packet.clone();
            let mut metadata = serde_json::to_vec(&definition).unwrap();
            let (result, input, metadata) = tokio::task::spawn_blocking(move || {
                let cancel = temporal_drasi_v1_cancel_new();
                let result = decode(unsafe {
                    temporal_drasi_v1_admit_plugin_source(
                        handle,
                        metadata.as_ptr(),
                        metadata.len(),
                        input.as_ptr(),
                        input.len(),
                        cancel,
                        2,
                    )
                });
                input.fill(0);
                metadata.fill(0);
                assert_eq!(temporal_drasi_v1_cancel_free(cancel), 0);
                (result, input, metadata)
            })
            .await
            .unwrap();
            assert_eq!(result.0, 5);
            cleared_inputs.push((input, metadata));
        }
        assert_eq!(admission_slots().available_permits(), 0);
        let metadata = serde_json::to_vec(&definition).unwrap();
        let cancel = temporal_drasi_v1_cancel_new();
        let full = decode(unsafe {
            temporal_drasi_v1_admit_plugin_source(
                handle,
                metadata.as_ptr(),
                metadata.len(),
                packet.as_ptr(),
                packet.len(),
                cancel,
                1000,
            )
        });
        assert_eq!(full.0, 3);
        assert_eq!(temporal_drasi_v1_cancel_free(cancel), 0);
        drop(blocker);
        tokio::time::timeout(Duration::from_secs(10), async {
            while admission_slots().available_permits() != 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sensitive admission jobs did not finish");
        for (input, metadata) in &cleared_inputs {
            assert!(input.iter().chain(metadata).all(|byte| *byte == 0));
        }
        let observed = runtime
            .host
            .read()
            .await
            .apply(Command::ObserveNodes)
            .await
            .unwrap();
        assert_eq!(observed["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(observed["nodes"][0]["status"], "running");
        let warm = runtime
            .host
            .read()
            .await
            .apply(Command::CheckPluginSource { definition })
            .await
            .unwrap();
        assert_eq!(warm, json!({"materialization_required": false}));
        let closed = tokio::task::spawn_blocking(move || {
            let cancel = temporal_drasi_v1_cancel_new();
            let result = decode(temporal_drasi_v1_close(handle, cancel, 30_000));
            assert_eq!(temporal_drasi_v1_cancel_free(cancel), 0);
            result
        })
        .await
        .unwrap();
        assert_eq!(closed.0, 0);
    }

    #[test]
    fn abi_ownership_errors_and_cancellation() {
        assert_eq!(temporal_drasi_v1_abi_version(), 1);
        assert_eq!(temporal_drasi_v1_buffer_free(0), 0);
        let token = temporal_drasi_v1_cancel_new();
        let invalid = unsafe { temporal_drasi_v1_open(std::ptr::null(), 8, token, 1000) };
        assert_eq!(decode(invalid).0, 1);
        let invalid = unsafe { temporal_drasi_v1_open(b"{}".as_ptr(), MAX_INPUT + 1, token, 1000) };
        assert_eq!(decode(invalid).0, 1);
        assert_eq!(decode(temporal_drasi_v1_close(u64::MAX, token, 1000)).0, 2);
        assert_eq!(temporal_drasi_v1_cancel(token), 0);
        assert_eq!(
            wait(cancellation(token).unwrap(), 1000, async { Ok(json!({})) })
                .unwrap_err()
                .code,
            4
        );
        assert_eq!(temporal_drasi_v1_cancel_free(token), 0);
        assert_eq!(temporal_drasi_v1_cancel_free(token), 2);
        assert_eq!(temporal_drasi_v1_cancel(token), 2);
        assert_eq!(decode(boundary(|| panic!("boundary test"))).0, 7);
    }

    #[test]
    fn plugin_completion_rejects_retry_before_host_access() {
        let workspace = crate::runtime::tests::Workspace::new();
        let runtime = executor().block_on(Host::open(workspace.config())).unwrap();
        let runtime = Arc::new(RuntimeEntry::new(runtime, None));
        let handle = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        lock(hosts()).insert(handle, runtime.clone());
        let blocker = executor().block_on(runtime.host.write());
        let token = temporal_drasi_v1_cancel_new();
        let command = br#"{"op":"list_catalogs"}"#;
        let first = decode(unsafe {
            temporal_drasi_v1_apply(handle, command.as_ptr(), command.len(), token, 2)
        });
        let repeated = decode(unsafe {
            temporal_drasi_v1_apply(handle, command.as_ptr(), command.len(), token, 2)
        });
        drop(blocker);
        let closed = decode(temporal_drasi_v1_close(handle, token, 30_000));
        assert_eq!(temporal_drasi_v1_cancel_free(token), 0);
        assert_eq!(closed.0, 0);
        assert_eq!(first.0, 5);
        assert_eq!(
            repeated.0, 3,
            "a timed-out plugin call admitted another detached operation"
        );
    }

    #[test]
    fn plugin_completion_close_drains_unscheduled_flight() {
        let workspace = crate::runtime::tests::Workspace::new();
        let runtime = executor().block_on(Host::open(workspace.config())).unwrap();
        let runtime = Arc::new(RuntimeEntry::new(runtime, None));
        let handle = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        lock(hosts()).insert(handle, runtime.clone());
        let flight = runtime.plugins.admit().unwrap();
        let token = temporal_drasi_v1_cancel_new();

        let closing = decode(temporal_drasi_v1_close(handle, token, 2));
        let live = executor()
            .block_on(async { runtime.host.read().await.apply(Command::ListCatalogs).await });
        let command = br#"{"op":"list_catalogs"}"#;
        let sealed = decode(unsafe {
            temporal_drasi_v1_plugin_run_joined(handle, command.as_ptr(), command.len())
        });
        let finished = plugin::run(runtime, Command::ListCatalogs, flight);
        let closed = decode(temporal_drasi_v1_close(handle, token, 30_000));
        assert_eq!(temporal_drasi_v1_cancel_free(token), 0);

        assert_eq!(closing.0, 5);
        assert!(
            live.is_ok(),
            "close reached Host before admitted work finished"
        );
        assert_eq!(sealed.0, 3);
        assert!(finished.is_ok());
        assert!(
            closed.0 == 0 || closed.0 == 2,
            "joined close failed: {closed:?}"
        );
    }

    #[test]
    fn plugin_completion_unwind_quarantines_checkpoint() {
        let workspace = crate::runtime::tests::Workspace::new();
        let runtime = executor().block_on(Host::open(workspace.config())).unwrap();
        let runtime = Arc::new(RuntimeEntry::new(runtime, None));
        let handle = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        lock(hosts()).insert(handle, runtime.clone());
        let flight = runtime.plugins.admit().unwrap();
        let failed = decode(boundary(|| {
            let _flight = flight;
            panic!("plugin completion fixture");
        }));
        let token = temporal_drasi_v1_cancel_new();
        let closed = decode(temporal_drasi_v1_close(handle, token, 30_000));
        let destination = workspace.0.join("uncertain-checkpoint");
        let command = serde_json::to_vec(&json!({
            "op": "checkpoint", "destination": destination,
        }))
        .unwrap();
        let checkpoint = decode(unsafe {
            temporal_drasi_v1_apply(handle, command.as_ptr(), command.len(), token, 30_000)
        });
        assert_eq!(temporal_drasi_v1_cancel_free(token), 0);
        let retained = host(handle).is_ok();
        lock(hosts()).remove(&handle);

        assert_eq!(failed.0, 7);
        assert_eq!(closed.0, 7);
        assert_eq!(checkpoint, closed);
        assert!(retained);
        assert!(!destination.exists());
    }

    #[test]
    fn abi_failed_close_retains_failure_and_refuses_checkpoint() {
        let workspace = crate::runtime::tests::Workspace::new();
        let mut runtime = executor().block_on(Host::open(workspace.config())).unwrap();
        crate::runtime::shutdown_tests::replace_sync(&mut runtime, None);
        let handle = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        lock(hosts()).insert(handle, Arc::new(RuntimeEntry::new(runtime, None)));
        let token = temporal_drasi_v1_cancel_new();
        let first = decode(temporal_drasi_v1_close(handle, token, 30_000));
        assert_eq!(first.0, 7);
        let repeated = decode(temporal_drasi_v1_close(handle, token, 30_000));
        assert_eq!(repeated, first, "failed close became NotFound or success");
        let reopened = executor()
            .block_on(Host::open(workspace.config()))
            .err()
            .expect("failed close released its owned directory");
        assert!(reopened
            .message
            .contains("already owned by another runtime"));
        let destination = workspace.0.join("forbidden-checkpoint");
        let command = serde_json::to_vec(&json!({
            "op": "checkpoint", "destination": destination,
        }))
        .unwrap();
        let checkpoint = decode(unsafe {
            temporal_drasi_v1_apply(handle, command.as_ptr(), command.len(), token, 30_000)
        });
        assert_eq!(checkpoint, first);
        assert!(!destination.exists());
        assert_eq!(temporal_drasi_v1_cancel_free(token), 0);
        lock(hosts()).remove(&handle);
    }
}
