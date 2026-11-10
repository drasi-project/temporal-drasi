use super::*;
use crate::protocol::{ComponentRecipe, PluginComponentRef};
use std::io::Cursor;
use std::os::unix::fs::PermissionsExt;

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(".verification")
            .join(format!("worker-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn driver(&self) -> PathBuf {
        let path = self.0.join("fixture-worker");
        std::fs::write(&path, include_bytes!("fixture_driver.py")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        path
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn artifact(path: PathBuf) -> TrustedArtifact {
    TrustedArtifact {
        path,
        binary_sha256: "0".repeat(64),
        sdk_version: drasi_plugin_sdk::SDK_VERSION.into(),
        core_version: env!("TDRASI_CORE_VERSION").into(),
        lib_version: env!("TDRASI_LIB_VERSION").into(),
        target_triple: drasi_plugin_sdk::ffi::metadata::TARGET_TRIPLE.into(),
        plugin_version: "1.0.0".into(),
        abi_version: drasi_plugin_sdk::ffi::metadata::FFI_SDK_VERSION.into(),
    }
}

fn actual_artifact(path: PathBuf) -> TrustedArtifact {
    use sha2::{Digest, Sha256};
    let mut result = artifact(path.clone());
    result.binary_sha256 = format!("{:x}", Sha256::digest(std::fs::read(path).unwrap()));
    result
}

fn actual_binary() -> PathBuf {
    std::fs::canonicalize(
        std::env::var("TEMPORAL_DRASI_WORKER_TEST_BINARY")
            .expect("set TEMPORAL_DRASI_WORKER_TEST_BINARY to the built companion"),
    )
    .unwrap()
}

fn actual_plugin() -> PathBuf {
    std::fs::canonicalize(
        std::env::var("TEMPORAL_DRASI_WORKER_TEST_PLUGIN")
            .expect("set TEMPORAL_DRASI_WORKER_TEST_PLUGIN to the built fixture library"),
    )
    .unwrap()
}

fn spec(directory: &Directory, mode: &str) -> WorkerSpec {
    let recipe = ComponentRecipe {
        plugin: PluginComponentRef {
            digest: format!("sha256:{}", "0".repeat(64)),
            component: "test-source".into(),
        },
        configuration: serde_json::json!({}),
        secrets: BTreeMap::new(),
    };
    WorkerSpec {
        identity: format!("graph/source/{}", uuid::Uuid::new_v4()),
        source_id: "fixture".into(),
        recipe: PluginSourceRecipe {
            source: recipe.clone(),
            bootstrap: recipe,
        },
        source_artifact: artifact(directory.0.join("unused-source")),
        bootstrap_artifact: artifact(directory.0.join("unused-bootstrap")),
        source_config: serde_json::json!({ "fixture_mode": mode }),
        bootstrap_config: serde_json::json!({}),
        bootstrap_source_config: serde_json::json!({}),
        state_directory: directory.0.join("state"),
    }
}

#[test]
fn frames_reject_oversize_version_and_identity_before_dispatch() {
    let binding = Binding {
        nonce: uuid::Uuid::new_v4().to_string(),
        identity: "graph/source/revision".into(),
    };
    let frame = Frame::new(&binding, Command::Stop);
    let mut encoded = Vec::new();
    transport::write(&mut encoded, &frame).unwrap();
    transport::read(&mut Cursor::new(encoded.clone()))
        .unwrap()
        .validate(&binding)
        .unwrap();
    let wrong = Binding {
        identity: "another-source".into(),
        ..binding.clone()
    };
    assert!(transport::read(&mut Cursor::new(encoded))
        .unwrap()
        .validate(&wrong)
        .is_err());
    let mut oversized = b"TDW1".to_vec();
    oversized.extend_from_slice(&((transport::MAX_FRAME + 1) as u32).to_be_bytes());
    assert!(transport::read(&mut Cursor::new(oversized)).is_err());
    let mut bad = serde_json::to_value(Frame::new(&binding, Command::Stop)).unwrap();
    bad["version"] = serde_json::json!(2);
    let bad: Frame = serde_json::from_value(bad).unwrap();
    assert!(bad.validate(&binding).is_err());
}

#[test]
fn opaque_positions_and_byte_credits_fail_closed() {
    assert!(position(None).is_ok());
    for bytes in [vec![0], vec![255, 0, 9], vec![1; MAX_POSITION]] {
        position(Some(&bytes)).unwrap();
    }
    assert!(position(Some(&[])).is_err());
    assert!(position(Some(&vec![1; MAX_POSITION + 1])).is_err());
    let bytes = Arc::new(AtomicUsize::new(0));
    let budget = Budget::reserve(&bytes, QUEUE_BYTES).unwrap();
    assert!(Budget::reserve(&bytes, 1).is_err());
    drop(budget);
    assert_eq!(bytes.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn stop_waits_and_joins_even_when_worker_hangs_or_lies() {
    for mode in ["normal", "hang", "lie"] {
        let directory = Directory::new();
        let resolver: Arc<WorkerResolver> =
            Arc::new(|_, _| Err(failed("fixture has no credentials")));
        let source = WorkerSource::spawn(
            &directory.driver(),
            spec(&directory, mode),
            resolver.clone(),
        )
        .await
        .unwrap();
        source.start().await.unwrap();
        assert!(!source.is_joined());
        let result = tokio::time::timeout(Duration::from_secs(8), source.close())
            .await
            .unwrap();
        assert_eq!(result.is_ok(), mode == "normal");
        assert!(source.is_joined());
        assert!(source.owner().supervisor.lock().unwrap().is_none());
        assert!(source.owner().shared.pending.lock().unwrap().is_empty());
        assert!(source.owner().shared.lanes.lock().unwrap().is_empty());
        assert_eq!(source.close().await.is_ok(), mode == "normal");
        drop(source);
        assert_eq!(Arc::strong_count(&resolver), 1);
    }
}

#[tokio::test]
async fn stdout_misuse_is_redacted_and_worker_is_reaped() {
    let directory = Directory::new();
    let source = WorkerSource::spawn(
        &directory.driver(),
        spec(&directory, "stdout"),
        Arc::new(|_, _| Err(failed("fixture has no credentials"))),
    )
    .await
    .unwrap();
    let error = source.start().await.unwrap_err();
    assert!(!error.to_string().contains("private-foreign-output"));
    assert!(source.close().await.is_err());
    assert!(source.is_joined());
}

#[test]
fn fixture_eof_watchdog_is_independent_of_hung_main() {
    let directory = Directory::new();
    let mut child = std::process::Command::new(directory.driver())
        .env_clear()
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = child.stdout.take().unwrap();
    let binding = Binding {
        nonce: uuid::Uuid::new_v4().to_string(),
        identity: "fixture-eof".into(),
    };
    let mut configuration = spec(&directory, "hang");
    configuration.identity = binding.identity.clone();
    transport::write(
        &mut input,
        &Frame::new(
            &binding,
            Command::Request {
                id: 1,
                request: Request::Construct(Box::new(configuration)),
            },
        ),
    )
    .unwrap();
    assert!(matches!(
        transport::read(&mut output).unwrap().command,
        Command::Response { .. }
    ));
    transport::write(&mut input, &Frame::new(&binding, Command::Stop)).unwrap();
    drop(input);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(73));
            break;
        }

        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("EOF watchdog did not reap the hung worker");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test]
#[ignore = "build companion and fixture, set TEMPORAL_DRASI_WORKER_TEST_BINARY and TEMPORAL_DRASI_WORKER_TEST_PLUGIN"]
async fn actual_worker_validates_loads_bootstraps_and_reuses_pins() {
    use sha2::{Digest, Sha256};
    let binary = actual_binary();
    let artifact = actual_artifact(actual_plugin());
    let descriptor = probe(&binary, artifact.clone()).await.unwrap();
    assert_eq!(descriptor.descriptors.len(), 2);
    for mutation in ["hash", "abi", "exported_version"] {
        let mut invalid = artifact.clone();
        match mutation {
            "hash" => invalid.binary_sha256 = "0".repeat(64),
            "abi" => invalid.abi_version = "invalid".into(),
            _ => invalid.plugin_version = "invalid".into(),
        }
        assert!(probe(&binary, invalid).await.is_err(), "{mutation}");
    }
    let directory = Directory::new();
    let mut spec = spec(&directory, "normal");
    spec.recipe.bootstrap.plugin.component = "test-bootstrap".into();
    spec.source_artifact = artifact.clone();
    spec.bootstrap_artifact = artifact.clone();
    let value = "worker-scoped-credential";
    let digest = format!("{:x}", Sha256::digest(value.as_bytes()));
    spec.source_config = serde_json::json!({
        "connection":{"kind":"Secret","name":"fixture-token"}, "expected_digest":digest
    });
    spec.bootstrap_config = spec.source_config.clone();
    spec.bootstrap_source_config = spec.source_config.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let resolved = calls.clone();
    let binary_hash = artifact.binary_sha256.clone();
    let source = WorkerSource::spawn(
        &binary,
        spec,
        Arc::new(move |actual, bytes| {
            anyhow::ensure!(actual == binary_hash, "fixture audience mismatch");
            let request: Value = serde_json::from_slice(bytes)?;
            anyhow::ensure!(
                request == serde_json::json!({"kind":"Secret","name":"fixture-token"}),
                "fixture capability mismatch"
            );
            resolved.fetch_add(1, Ordering::AcqRel);
            Ok(value.into())
        }),
    )
    .await
    .unwrap();
    source.start().await.unwrap();
    assert_eq!(source.status().await, ComponentStatus::Running);
    let settings = SourceSubscriptionSettings {
        source_id: source.id().into(),
        query_id: "query".into(),
        enable_bootstrap: true,
        nodes: Default::default(),
        relations: Default::default(),
        resume_from: None,
        resume_sequence: None,
        request_position_handle: true,
    };
    let mut subscription = source.subscribe(settings.clone()).await.unwrap();
    let result = subscription
        .bootstrap_result_receiver
        .take()
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.event_count, 0);
    assert_eq!(
        result.source_position.as_deref(),
        Some([0_u8; 8].as_slice())
    );
    assert!(subscription
        .bootstrap_receiver
        .take()
        .unwrap()
        .recv()
        .await
        .is_none());
    let pin = subscription.position_handle.take().unwrap();
    assert_eq!(pin.load(Ordering::Acquire), u64::MAX);
    drop(subscription.receiver);
    let mut replay = source
        .subscribe(SourceSubscriptionSettings {
            enable_bootstrap: false,
            resume_from: result.source_position.clone(),
            ..settings.clone()
        })
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&pin, replay.position_handle.as_ref().unwrap()));
    assert!(calls.load(Ordering::Acquire) >= 3);
    drop(replay.receiver);
    let epoch = source.owner().shared.binding.nonce.clone();
    source.stop().await.unwrap();
    assert!(source.is_joined());
    source.start().await.unwrap();
    assert_ne!(source.owner().shared.binding.nonce, epoch);
    let after_restart = source
        .subscribe(SourceSubscriptionSettings {
            enable_bootstrap: false,
            resume_from: result.source_position,
            ..settings
        })
        .await
        .unwrap();
    assert!(Arc::ptr_eq(
        &pin,
        after_restart.position_handle.as_ref().unwrap()
    ));
    drop(after_restart.receiver);
    source.remove_position_handle("query").await;
    replay.position_handle.take();
    assert!(source.owner().shared.pins.lock().unwrap().is_empty());
    source.close().await.unwrap();
    assert!(source.is_joined());
}

#[tokio::test]
#[ignore = "build companion and fixture, set TEMPORAL_DRASI_WORKER_TEST_BINARY and TEMPORAL_DRASI_WORKER_TEST_PLUGIN"]
async fn actual_sdk_operation_failures_are_redacted_but_clean_stop_is_recoverable() {
    let binary = actual_binary();
    let artifact = actual_artifact(actual_plugin());
    for mode in [
        "diagnostic_probe",
        "fail_first_start",
        "fail_subscribe",
        "fail_bootstrap",
        "fail_stop",
        "without_position_handle",
    ] {
        let directory = Directory::new();
        let mut configuration = spec(&directory, "normal");
        configuration.recipe.bootstrap.plugin.component = "test-bootstrap".into();
        configuration.source_artifact = artifact.clone();
        configuration.bootstrap_artifact = artifact.clone();
        configuration.source_config = serde_json::json!({
            "connection": {"kind":"Secret","name":"fixture-token"},
        });
        configuration.bootstrap_config = configuration.source_config.clone();
        configuration.bootstrap_source_config = configuration.source_config.clone();
        if mode == "fail_bootstrap" {
            configuration.bootstrap_config[mode] = serde_json::json!(true);
        } else {
            configuration.source_config[mode] = serde_json::json!(true);
        }
        let hash = artifact.binary_sha256.clone();
        let resolver: Arc<WorkerResolver> = Arc::new(move |actual, bytes| {
            anyhow::ensure!(actual == hash, "fixture audience mismatch");
            let request: Value = serde_json::from_slice(bytes)?;
            anyhow::ensure!(
                request == serde_json::json!({"kind":"Secret","name":"fixture-token"}),
                "fixture capability mismatch"
            );
            Ok("private-worker-operation-credential".into())
        });
        let source = WorkerSource::spawn(&binary, configuration, resolver.clone())
            .await
            .unwrap();
        let started = source.start().await;
        if matches!(mode, "diagnostic_probe" | "fail_first_start") {
            assert_eq!(
                started.unwrap_err().to_string(),
                "foreign source operation failed",
                "{mode}"
            );
            assert_eq!(source.status().await, ComponentStatus::Error, "{mode}");
            assert!(!source.process.failed.load(Ordering::Acquire), "{mode}");
            if mode == "fail_first_start" {
                source.start().await.unwrap();
                assert_eq!(source.status().await, ComponentStatus::Running);
            }
        } else {
            started.unwrap();
            if mode != "fail_stop" {
                let subscribed = source
                    .subscribe(SourceSubscriptionSettings {
                        source_id: source.id().into(),
                        query_id: "query".into(),
                        enable_bootstrap: true,
                        nodes: Default::default(),
                        relations: Default::default(),
                        resume_from: None,
                        resume_sequence: None,
                        request_position_handle: mode != "without_position_handle",
                    })
                    .await;
                if mode == "fail_subscribe" {
                    assert_eq!(
                        subscribed.err().unwrap().to_string(),
                        "foreign source operation failed"
                    );
                } else {
                    let mut subscription = subscribed.unwrap();
                    let result = subscription
                        .bootstrap_result_receiver
                        .take()
                        .unwrap()
                        .await
                        .unwrap();
                    if mode == "fail_bootstrap" {
                        assert_eq!(
                            result.unwrap_err().to_string(),
                            "foreign source bootstrap failed"
                        );
                    } else {
                        assert_eq!(result.unwrap().event_count, 0);
                        assert!(subscription.position_handle.is_none());
                        assert!(source.process.pins.lock().unwrap().is_empty());
                    }
                    drop(subscription.receiver);
                }
            }
        }
        let stopped = source.stop().await;
        assert_eq!(stopped.is_ok(), mode != "fail_stop", "{mode}");
        assert!(source.is_joined(), "{mode}");
        assert_eq!(source.close().await.is_ok(), mode != "fail_stop", "{mode}");
        drop(source);
        assert_eq!(Arc::strong_count(&resolver), 1, "{mode}");
    }
}

#[tokio::test]
#[ignore = "build workspace, set TEMPORAL_DRASI_WORKER_TEST_BINARY and TEMPORAL_DRASI_WORKER_TEST_PLUGIN"]
async fn mixed_binary_factory_failures_report_only_static_phase_and_join_before_returning() {
    let binary = actual_binary();
    let source_artifact = actual_artifact(actual_plugin());
    let bootstrap_artifact = actual_artifact(actual_plugin().with_file_name(format!(
        "{}temporal_drasi_other_test_plugin{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    )));
    assert_ne!(
        source_artifact.binary_sha256,
        bootstrap_artifact.binary_sha256
    );
    for (flag, phase, audience) in [
        (
            "fail_factory",
            "SourceFactory",
            source_artifact.binary_sha256.clone(),
        ),
        (
            "fail_bootstrap_factory",
            "BootstrapFactory",
            bootstrap_artifact.binary_sha256.clone(),
        ),
    ] {
        let directory = Directory::new();
        let mut configuration = spec(&directory, "normal");
        configuration.recipe.bootstrap.plugin.component = "test-bootstrap".into();
        configuration.source_artifact = source_artifact.clone();
        configuration.bootstrap_artifact = bootstrap_artifact.clone();
        configuration.source_config = serde_json::json!({
            "connection": {"kind":"Secret","name":"private-factory-input-token"},
        });
        configuration.bootstrap_config = configuration.source_config.clone();
        configuration.bootstrap_source_config = configuration.source_config.clone();
        if flag == "fail_factory" {
            configuration.source_config[flag] = serde_json::json!(true);
        } else {
            configuration.bootstrap_config[flag] = serde_json::json!(true);
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let resolved = calls.clone();
        let resolver: Arc<WorkerResolver> = Arc::new(move |actual, bytes| {
            anyhow::ensure!(actual == audience, "fixture audience mismatch");
            let request: Value = serde_json::from_slice(bytes)?;
            anyhow::ensure!(
                request
                    == serde_json::json!({"kind":"Secret","name":"private-factory-input-token"}),
                "fixture capability mismatch"
            );
            resolved.fetch_add(1, Ordering::AcqRel);
            Ok("private-factory-reply-value".into())
        });
        let error = WorkerSource::spawn(&binary, configuration, resolver.clone())
            .await
            .err()
            .expect("fixture factory must fail")
            .to_string();
        assert!(error.contains(&format!("phase: {phase}")), "{error}");
        assert!(!error.contains("private-factory"));
        assert!(!error.contains(directory.0.to_str().unwrap()));
        assert!(error.len() < 512);
        assert_eq!(calls.load(Ordering::Acquire), 1, "{flag}");
        assert_eq!(Arc::strong_count(&resolver), 1, "{flag}");
    }
}

#[test]
#[ignore = "build companion and set TEMPORAL_DRASI_WORKER_TEST_BINARY"]
fn actual_worker_eof_exits_while_foreign_registration_is_hung() {
    let directory = Directory::new();
    let ready = directory.0.join("foreign-entered");
    let library = directory.0.join(if cfg!(target_os = "macos") {
        "hung.dylib"
    } else {
        "hung.so"
    });
    let source = directory.0.join("hung.c");
    std::fs::write(&source, include_bytes!("hung_registration.c")).unwrap();
    let compiled = std::process::Command::new("cc")
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror", "-fPIC"])
        .arg(if cfg!(target_os = "macos") {
            "-dynamiclib"
        } else {
            "-shared"
        })
        .arg(format!(
            "-DTARGET_TRIPLE={:?}",
            drasi_plugin_sdk::ffi::metadata::TARGET_TRIPLE
        ))
        .arg(format!("-DREADY_PATH={:?}", ready.to_str().unwrap()))
        .arg(&source)
        .arg("-o")
        .arg(&library)
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "C watchdog fixture failed to compile"
    );
    let mut child = std::process::Command::new(actual_binary())
        .env_clear()
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let binding = Binding {
        nonce: uuid::Uuid::new_v4().to_string(),
        identity: "hung-registration".into(),
    };
    transport::write(
        &mut input,
        &Frame::new(
            &binding,
            Command::Request {
                id: 1,
                request: Request::Probe(actual_artifact(library)),
            },
        ),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready.exists() {
        if Instant::now() >= deadline || child.try_wait().unwrap().is_some() {
            let _ = child.kill();
            child.wait().unwrap();
            panic!("foreign registration did not enter the owned worker");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(input);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(73));
            break;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("actual watchdog did not exit while foreign registration was hung");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test]
async fn clean_stop_restarts_same_source_but_failure_and_retirement_are_sticky() {
    let directory = Directory::new();
    let source = WorkerSource::spawn(
        &directory.driver(),
        spec(&directory, "normal"),
        Arc::new(|_, _| Err(failed("fixture has no credentials"))),
    )
    .await
    .unwrap()
    .with_auto_start(true);
    assert!(source.auto_start());
    let mut previous = source.owner().shared.binding.nonce.clone();
    for _ in 0..3 {
        source.start().await.unwrap();
        source.stop().await.unwrap();
        assert!(source.is_joined());
        source.start().await.unwrap();
        let current = source.owner().shared.binding.nonce.clone();
        assert_ne!(current, previous);
        previous = current;
    }
    source.close().await.unwrap();
    assert!(source.start().await.is_err());
    let failed_source = WorkerSource::spawn(
        &directory.driver(),
        spec(&directory, "hang"),
        Arc::new(|_, _| Err(failed("fixture has no credentials"))),
    )
    .await
    .unwrap();
    failed_source.start().await.unwrap();
    assert!(failed_source.stop().await.is_err());
    assert!(failed_source.is_joined());
    assert!(failed_source.start().await.is_err());
    assert!(failed_source.close().await.is_err());
}

#[test]
fn committed_ack_is_exact_monotonic_and_frozen_by_unsealed_peer() {
    let (commands, _receiver) = std::sync::mpsc::sync_channel(1);
    let shared = Shared {
        binding: Binding {
            nonce: uuid::Uuid::new_v4().to_string(),
            identity: "ack-test".into(),
        },
        commands,
        pending: Mutex::default(),
        lanes: Mutex::default(),
        pins: Arc::new(Mutex::new(BTreeMap::new())),
        next: AtomicU64::new(1),
        failed: Arc::new(AtomicBool::new(false)),
        closing: AtomicBool::new(false),
        reaped: AtomicBool::new(false),
        stopped: AtomicBool::new(false),
        joined: (Mutex::new(false), Condvar::new()),
        bytes: Arc::new(AtomicUsize::new(0)),
        status: Mutex::new(ComponentStatus::Running),
        diagnostics: Mutex::default(),
        _resolver: Arc::new(|_, _| Ok(String::new())),
    };
    let peer = Arc::new(AtomicU64::new(2));
    let seed = Arc::new(AtomicU64::new(u64::MAX));
    {
        let mut pins = shared.pins.lock().unwrap();
        pins.insert(
            "peer".into(),
            Pin {
                position: peer.clone(),
                sent: 1,
                delivered: 8,
                removing: false,
                attached: true,
            },
        );
        pins.insert(
            "seed".into(),
            Pin {
                position: seed.clone(),
                sent: u64::MAX,
                delivered: 3,
                removing: false,
                attached: true,
            },
        );
    }
    assert!(transport::acknowledgements(&shared).unwrap().is_empty());
    peer.store(4, Ordering::Release);
    assert!(transport::acknowledgements(&shared).unwrap().is_empty());
    seed.store(3, Ordering::Release);
    assert_eq!(
        transport::acknowledgements(&shared).unwrap(),
        vec![("peer".into(), 4), ("seed".into(), 3)]
    );
    assert!(transport::acknowledgements(&shared).unwrap().is_empty());
    peer.store(3, Ordering::Release);
    assert!(transport::acknowledgements(&shared).is_err());
}
