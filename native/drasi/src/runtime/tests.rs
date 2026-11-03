use super::*;
use drasi_core::interface::IndexBackendPlugin;
use std::{path::PathBuf, time::Duration};

pub(crate) struct Workspace(pub(crate) PathBuf);

impl Workspace {
    pub(crate) fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(".verification")
            .join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    pub(crate) fn config(&self) -> Config {
        serde_json::from_value(json!({
            "version": VERSION,
            "state_dir": self.0.join("owner"),
            "owner_epoch": 1,
            "instance_id": "graph-test",
            "sources": [{"id": "feed", "kind": "temporal"}],
            "queries": [{
                "id": "q1",
                "revision": 1,
                "language": "cypher",
                "text": "MATCH (n:Item) RETURN n.amount AS amount",
                "sources": ["feed"]
            }]
        }))
        .unwrap()
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct QueryState {
    pub(super) config_hash: u64,
    pub(super) generation: u64,
    pub(super) sequence: u64,
    pub(super) rows: BTreeMap<String, Value>,
}

pub(crate) async fn query_state(host: &Host, id: &str, expected: &[Value]) -> QueryState {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let query = host
                .core()
                .unwrap()
                .query_manager()
                .get_query_instance(id)
                .await
                .unwrap();
            let snapshot = query.fetch_snapshot().await.unwrap();
            let state = QueryState {
                config_hash: snapshot.config_hash,
                generation: snapshot.output_generation,
                sequence: snapshot.as_of_sequence,
                rows: snapshot
                    .stream_keyed()
                    .map(|(key, value)| (key.to_string(), value))
                    .collect()
                    .await,
            };
            if state.rows.values().cloned().collect::<Vec<_>>() == expected {
                return state;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("query did not reach expected results")
}

pub(super) async fn pending(host: &Host, id: &str) -> Event {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(event) = host
                .poll(16)
                .await
                .unwrap()
                .into_iter()
                .find(|event| event.subscription_id == id)
            {
                return event;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("subscription did not publish output")
}

async fn attach(host: &mut Host, id: &str) -> Event {
    host.apply(Command::Subscribe {
        subscription: Subscription {
            id: id.into(),
            query_id: "q1".into(),
            query_revision: 1,
        },
    })
    .await
    .unwrap();
    pending(host, id).await
}

pub(super) async fn accept(host: &Host, event: &Event) {
    host.ack(Receipt {
        subscription_id: event.subscription_id.clone(),
        query_revision: event.query_revision,
        incarnation: event.incarnation.clone(),
        sequence: event.sequence,
        kind: event.kind.clone(),
        acceptance_id: format!("{}-{}", event.subscription_id, event.sequence),
    })
    .await
    .unwrap();
}

async fn feed(host: &mut Host, position: u64, amount: i64) {
    host.apply(Command::Feed {
        source_id: "feed".into(),
        position,
        change: Change {
            kind: ChangeKind::Insert,
            element_id: "item".into(),
            labels: vec!["Item".into()],
            effective_from: position,
            properties: BTreeMap::from([("amount".into(), json!(amount))]),
            relation: None,
        },
    })
    .await
    .unwrap();
}

async fn add_second_query(host: &Host, recovery: drasi_lib::RecoveryPolicy) {
    host.core()
        .unwrap()
        .add_query(
            Query::cypher("q2")
                .query("MATCH (n:Item) RETURN n.amount AS amount")
                .from_source("feed")
                .with_recovery_policy(recovery)
                .with_outbox_capacity(64)
                .build(),
        )
        .await
        .unwrap();
}

pub(super) async fn stop_query(host: &Host, id: &str) {
    let core = host.core().unwrap();
    core.stop_query(id).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let graph = core.get_graph().await;
            let query = graph.nodes.iter().find(|node| node.id == id).unwrap();
            if query.status == drasi_lib::channels::ComponentStatus::Stopped {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("query stop did not complete");
}

#[tokio::test]
async fn late_query_first_snapshot_contains_accepted_source_state() {
    let workspace = Workspace::new();
    let mut configuration = workspace.config();
    configuration.queries.clear();
    let mut host = Host::open(configuration.clone()).await.unwrap();
    for position in 1..=3 {
        feed(&mut host, position, position as i64).await;
    }
    add_second_query(&host, drasi_lib::RecoveryPolicy::Strict).await;
    let query = host
        .core()
        .unwrap()
        .query_manager()
        .get_query_instance("q2")
        .await
        .unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(10), query.fetch_snapshot())
        .await
        .expect("initial query snapshot did not become ready")
        .unwrap();
    assert_eq!(snapshot.as_of_sequence, 0);
    let initial: Vec<_> = snapshot.stream_keyed().map(|(_, row)| row).collect().await;
    assert_eq!(initial, vec![json!({"amount": 3})]);
    assert!(query.fetch_outbox(0).await.unwrap().results.is_empty());
    stop_query(&host, "q2").await;
    host.core()
        .unwrap()
        .start_query_existing("q2")
        .await
        .unwrap();
    let recovered = query.fetch_snapshot().await.unwrap();
    assert_eq!(recovered.as_of_sequence, 0);
    let recovered_rows: Vec<_> = recovered.stream_keyed().map(|(_, row)| row).collect().await;
    assert_eq!(recovered_rows, initial);
    assert!(query.fetch_outbox(0).await.unwrap().results.is_empty());
    drop(query);
    host.close().await.unwrap();
    drop(host);
    let mut host = Host::open(configuration).await.unwrap();
    add_second_query(&host, drasi_lib::RecoveryPolicy::ExistingOnly).await;
    let query = host
        .core()
        .unwrap()
        .query_manager()
        .get_query_instance("q2")
        .await
        .unwrap();
    let cold = tokio::time::timeout(Duration::from_secs(10), query.fetch_snapshot())
        .await
        .expect("cold query recovery did not become ready")
        .unwrap();
    assert_eq!(cold.as_of_sequence, 0);
    let cold_rows: Vec<_> = cold.stream_keyed().map(|(_, row)| row).collect().await;
    assert_eq!(cold_rows, initial);
    assert!(query.fetch_outbox(0).await.unwrap().results.is_empty());
    feed(&mut host, 4, 4).await;
    query_state(&host, "q2", &[json!({"amount": 4})]).await;
    stop_query(&host, "q2").await;
    feed(&mut host, 5, 5).await;
    host.core()
        .unwrap()
        .start_query_existing("q2")
        .await
        .unwrap();
    query_state(&host, "q2", &[json!({"amount": 5})]).await;
    drop(query);
    host.close().await.unwrap();
}

#[tokio::test]
async fn sdk_query_admission_preserves_existing_output_and_recovery() {
    let workspace = Workspace::new();
    let configuration = workspace.config();
    let mut host = Host::open(configuration.clone()).await.unwrap();
    let initial = attach(&mut host, "watch-1").await;
    accept(&host, &initial).await;
    feed(&mut host, 1, 12).await;
    let first_state = query_state(&host, "q1", &[json!({"amount": 12})]).await;
    let first = pending(&host, "watch-1").await;
    let first_bytes = serde_json::to_vec(&first).unwrap();

    add_second_query(&host, drasi_lib::RecoveryPolicy::Strict).await;
    let second_state = query_state(&host, "q2", &[json!({"amount": 12})]).await;
    assert_eq!(
        query_state(&host, "q1", &[json!({"amount": 12})]).await,
        first_state
    );
    assert_eq!(
        serde_json::to_vec(&pending(&host, "watch-1").await).unwrap(),
        first_bytes
    );
    let late = attach(&mut host, "watch-2").await;
    assert_eq!(late.sequence, first.sequence);
    assert_eq!(late.incarnation, first.incarnation);
    assert_eq!(late.rows.len(), 1);
    accept(&host, &late).await;

    stop_query(&host, "q2").await;
    host.core().unwrap().start_query("q2").await.unwrap();
    assert_eq!(
        query_state(&host, "q2", &[json!({"amount": 12})]).await,
        second_state
    );
    stop_query(&host, "q1").await;
    host.core().unwrap().start_query("q1").await.unwrap();
    assert_eq!(
        query_state(&host, "q1", &[json!({"amount": 12})]).await,
        first_state
    );
    assert_eq!(
        serde_json::to_vec(&pending(&host, "watch-1").await).unwrap(),
        first_bytes
    );
    host.close().await.unwrap();
    drop(host);

    let mut host = Host::open(configuration).await.unwrap();
    assert_eq!(
        serde_json::to_vec(&pending(&host, "watch-1").await).unwrap(),
        first_bytes
    );
    add_second_query(&host, drasi_lib::RecoveryPolicy::ExistingOnly).await;
    assert_eq!(
        query_state(&host, "q2", &[json!({"amount": 12})]).await,
        second_state
    );
    accept(&host, &first).await;

    feed(&mut host, 2, 14).await;
    query_state(&host, "q1", &[json!({"amount": 14})]).await;
    query_state(&host, "q2", &[json!({"amount": 14})]).await;
    let next = pending(&host, "watch-1").await;
    assert_eq!(next.sequence, first.sequence + 1);
    assert_eq!(next.incarnation, first.incarnation);
    let next_bytes = serde_json::to_vec(&next).unwrap();
    host.core()
        .unwrap()
        .add_query(
            Query::cypher("invalid")
                .query("THIS IS NOT CYPHER")
                .from_source("feed")
                .auto_start(false)
                .build(),
        )
        .await
        .unwrap();
    host.core()
        .unwrap()
        .start_query("invalid")
        .await
        .unwrap_err();
    feed(&mut host, 3, 20).await;
    query_state(&host, "q1", &[json!({"amount": 20})]).await;
    query_state(&host, "q2", &[json!({"amount": 20})]).await;
    assert_eq!(
        serde_json::to_vec(&pending(&host, "watch-1").await).unwrap(),
        next_bytes
    );
    accept(&host, &next).await;
    let last = pending(&host, "watch-1").await;
    assert_eq!(last.sequence, next.sequence + 1);
    assert_eq!(last.incarnation, next.incarnation);
    host.close().await.unwrap();
}

#[tokio::test]
async fn changed_query_generation_cannot_advance_existing_subscription() {
    let workspace = Workspace::new();
    let configuration = workspace.config();
    let mut host = Host::open(configuration.clone()).await.unwrap();
    let initial = attach(&mut host, "watch-1").await;
    accept(&host, &initial).await;
    feed(&mut host, 1, 12).await;
    let state = query_state(&host, "q1", &[json!({"amount": 12})]).await;
    let first = pending(&host, "watch-1").await;
    let first_bytes = serde_json::to_vec(&first).unwrap();
    host.close().await.unwrap();
    drop(host);

    let provider = RocksDbIndexProvider::new(workspace.0.join("owner/indexes"), true, false)
        .with_memory_budget_bytes(256 << 20)
        .unwrap();
    let indexes = provider.create_indexes("q1").await.unwrap();
    indexes
        .checkpoint_store
        .as_ref()
        .unwrap()
        .write_output_generation("q1", state.generation + 1)
        .await
        .unwrap();
    drop(indexes);
    drop(provider);

    let mut host = Host::open(configuration).await.unwrap();
    assert_eq!(
        serde_json::to_vec(&pending(&host, "watch-1").await).unwrap(),
        first_bytes
    );
    accept(&host, &first).await;
    feed(&mut host, 2, 14).await;
    let changed = query_state(&host, "q1", &[json!({"amount": 14})]).await;
    assert_eq!(changed.generation, state.generation + 1);
    let error = host.poll(16).await.unwrap_err();
    assert_eq!(error.code, 6);
    host.close().await.unwrap();
}

#[derive(Debug)]
enum RecoveryDamage {
    MissingHash,
    ChangedHash,
    ResetMarker,
    CorruptOutbox,
}

#[tokio::test]
async fn host_recovery_rejects_native_damage_without_rebuilding() {
    for damage in [
        RecoveryDamage::MissingHash,
        RecoveryDamage::ChangedHash,
        RecoveryDamage::ResetMarker,
        RecoveryDamage::CorruptOutbox,
    ] {
        let workspace = Workspace::new();
        let configuration = workspace.config();
        let mut host = Host::open(configuration.clone()).await.unwrap();
        let initial = attach(&mut host, "watch-1").await;
        accept(&host, &initial).await;
        feed(&mut host, 1, 12).await;
        let state = query_state(&host, "q1", &[json!({"amount": 12})]).await;
        let first = pending(&host, "watch-1").await;
        let pending_bytes = serde_json::to_vec(&first).unwrap();
        host.close().await.unwrap();
        drop(host);

        let provider = RocksDbIndexProvider::new(workspace.0.join("owner/indexes"), true, false)
            .with_memory_budget_bytes(256 << 20)
            .unwrap();
        let indexes = provider.create_indexes("q1").await.unwrap();
        let checkpoint = indexes.checkpoint_store.as_ref().unwrap();
        match damage {
            RecoveryDamage::MissingHash => checkpoint.clear_checkpoints().await.unwrap(),
            RecoveryDamage::ChangedHash => checkpoint
                .write_config_hash(state.config_hash.wrapping_add(1))
                .await
                .unwrap(),
            RecoveryDamage::ResetMarker => checkpoint.write_config_hash(0).await.unwrap(),
            RecoveryDamage::CorruptOutbox => indexes
                .outbox_writer
                .as_ref()
                .unwrap()
                .append("q1", state.sequence, b"not a serialized query output")
                .await
                .unwrap(),
        }
        let expected = persisted_query(&indexes).await;
        drop(indexes);
        drop(provider);

        match Host::open(configuration.clone()).await {
            Err(error) => assert!(!error.message.is_empty(), "{damage:?}"),
            Ok(mut host) => {
                host.close().await.unwrap();
                panic!("{damage:?}: recovery accepted damaged native state");
            }
        }

        let provider = RocksDbIndexProvider::new(workspace.0.join("owner/indexes"), true, false)
            .with_memory_budget_bytes(256 << 20)
            .unwrap();
        let indexes = provider.create_indexes("q1").await.unwrap();
        assert_eq!(persisted_query(&indexes).await, expected, "{damage:?}");
        let store = RedbStateStoreProvider::new(workspace.0.join("owner/state.redb")).unwrap();
        let handoff: Handoff =
            serde_json::from_slice(&store.get(SUBSCRIPTIONS, "watch-1").await.unwrap().unwrap())
                .unwrap();
        assert_eq!(
            serde_json::to_vec(handoff.pending.as_ref().unwrap()).unwrap(),
            pending_bytes,
            "{damage:?}"
        );
        drop(store);
        if matches!(damage, RecoveryDamage::ChangedHash) {
            indexes
                .checkpoint_store
                .as_ref()
                .unwrap()
                .write_config_hash(state.config_hash)
                .await
                .unwrap();
            drop(indexes);
            drop(provider);
            let mut host = Host::open(configuration).await.unwrap();
            assert_eq!(
                serde_json::to_vec(&pending(&host, "watch-1").await).unwrap(),
                pending_bytes
            );
            accept(&host, &first).await;
            feed(&mut host, 2, 14).await;
            query_state(&host, "q1", &[json!({"amount": 14})]).await;
            let next = pending(&host, "watch-1").await;
            assert_eq!(next.sequence, first.sequence + 1);
            assert_eq!(next.incarnation, first.incarnation);
            host.close().await.unwrap();
        }
    }
}

#[derive(Debug, PartialEq)]
struct PersistedQuery {
    hash: Option<u64>,
    generation: Option<u64>,
    sequence: Option<u64>,
    checkpoints: std::collections::HashMap<String, drasi_core::interface::SourceCheckpoint>,
    rows: BTreeMap<u64, Vec<u8>>,
    outbox: Vec<(u64, Vec<u8>)>,
}

async fn persisted_query(indexes: &drasi_core::interface::CreatedIndexes) -> PersistedQuery {
    let checkpoint = indexes.checkpoint_store.as_ref().unwrap();
    PersistedQuery {
        hash: checkpoint.read_config_hash().await.unwrap(),
        generation: checkpoint.read_output_generation("q1").await.unwrap(),
        sequence: checkpoint.read_result_sequence("q1").await.unwrap(),
        checkpoints: checkpoint.read_all_checkpoints().await.unwrap(),
        rows: indexes
            .live_results_writer
            .as_ref()
            .unwrap()
            .read_snapshot("q1")
            .await
            .unwrap()
            .into_iter()
            .collect(),
        outbox: indexes
            .outbox_writer
            .as_ref()
            .unwrap()
            .read_from("q1", 0)
            .await
            .unwrap(),
    }
}

#[tokio::test]
async fn incompatible_build_state_is_refused_before_sdk_or_retirement_writes() {
    for upstream in [
        None,
        Some("207ef6f3c4cd12d854dbe512c9b670282f996284+development.7bf4530999d24a435db70a6ac6e07c27b26253ac"),
    ] {
        let workspace = Workspace::new();
        let mut configuration = workspace.config();
        configuration.sources.clear();
        configuration.queries.clear();
        std::fs::create_dir_all(&configuration.state_dir).unwrap();
        let mut definition: Value =
            serde_json::from_slice(&configuration.durable_definition().unwrap()).unwrap();
        if let Some(upstream) = upstream {
            definition["upstream"] = json!(upstream);
        } else {
            definition.as_object_mut().unwrap().remove("upstream");
        }
        let definition = serde_json::to_vec(&definition).unwrap();
        let state = Path::new(&configuration.state_dir).join("state.redb");
        let store = RedbStateStoreProvider::new(&state).unwrap();
        let identity = b"e36c8971-e3fb-4401-a265-08915b10ad07";
        let epoch = configuration.owner_epoch.to_be_bytes();
        let catalog = serde_json::to_vec(&AdmissionLedger::default()).unwrap();
        store
            .set_many(
                CONTROL,
                &[
                    ("definition", &definition),
                    ("identity", identity),
                    ("owner_epoch", &epoch),
                    ("catalog", &catalog),
                ],
            )
            .await
            .unwrap();
        store.sync().await.unwrap();
        drop(store);
        let indexes = Path::new(&configuration.state_dir).join("indexes");
        std::fs::create_dir(&indexes).unwrap();
        std::fs::write(indexes.join("retained-old-format"), b"retained").unwrap();
        for mode in [
            StartMode::Automatic,
            StartMode::Dormant,
            StartMode::CheckpointRecovery,
        ] {
            let mut attempted = configuration.clone();
            attempted.start_mode = mode;
            match Host::open(attempted).await {
                Err(error) => {
                    assert_eq!(error.code, 3);
                    assert!(
                        error.message.contains("definition"),
                        "wrong build must fail before SDK recovery: {error}"
                    );
                }
                Ok(mut host) => {
                    host.close().await.unwrap();
                    panic!("incompatible native build opened retained state");
                }
            }
            let store = RedbStateStoreProvider::new(&state).unwrap();
            for (key, expected) in [
                ("definition", definition.as_slice()),
                ("identity", identity.as_slice()),
                ("owner_epoch", epoch.as_slice()),
                ("catalog", catalog.as_slice()),
            ] {
                assert_eq!(
                    store.get(CONTROL, key).await.unwrap().as_deref(),
                    Some(expected),
                    "refusal changed retained {key}"
                );
            }
            drop(store);
            assert_eq!(std::fs::read_dir(&indexes).unwrap().count(), 1);
            assert_eq!(
                std::fs::read(indexes.join("retained-old-format")).unwrap(),
                b"retained"
            );
        }
    }
}

#[tokio::test]
async fn host_does_not_initialize_an_unidentified_nonempty_directory() {
    let workspace = Workspace::new();
    let configuration = workspace.config();
    let state = Path::new(&configuration.state_dir);
    std::fs::create_dir_all(state).unwrap();
    std::fs::write(state.join("unknown-native-state"), b"retained").unwrap();
    match Host::open(configuration).await {
        Err(error) => assert_eq!(error.code, 7),
        Ok(mut host) => {
            host.close().await.unwrap();
            panic!("unidentified persisted state was admitted as a new query");
        }
    }
}

#[tokio::test]
async fn interrupted_initial_admission_does_not_grant_creation_again() {
    let workspace = Workspace::new();
    let configuration = workspace.config();
    std::fs::create_dir_all(&configuration.state_dir).unwrap();
    let store = RedbStateStoreProvider::new(Path::new(&configuration.state_dir).join("state.redb"))
        .unwrap();
    store
        .set_many(
            CONTROL,
            &[
                ("definition", &configuration.durable_definition().unwrap()),
                ("identity", b"e36c8971-e3fb-4401-a265-08915b10ad07"),
                ("owner_epoch", &configuration.owner_epoch.to_be_bytes()),
                (
                    "catalog",
                    &serde_json::to_vec(&AdmissionLedger::default()).unwrap(),
                ),
            ],
        )
        .await
        .unwrap();
    store.sync().await.unwrap();
    drop(store);

    for _ in 0..2 {
        match Host::open(configuration.clone()).await {
            Err(error) => assert!(error.message.contains("failed native admission")),
            Ok(mut host) => {
                host.close().await.unwrap();
                panic!("an uncertain first start granted fresh query initialization");
            }
        }
        let provider = RocksDbIndexProvider::new(workspace.0.join("owner/indexes"), true, false)
            .with_memory_budget_bytes(256 << 20)
            .unwrap();
        let indexes = provider.create_indexes("q1").await.unwrap();
        assert_eq!(
            persisted_query(&indexes).await,
            PersistedQuery {
                hash: None,
                generation: None,
                sequence: None,
                checkpoints: std::collections::HashMap::new(),
                rows: BTreeMap::new(),
                outbox: Vec::new(),
            }
        );
    }
}

#[tokio::test]
async fn missing_control_identity_or_definition_cannot_be_recreated() {
    for key in ["definition", "identity", "catalog"] {
        let workspace = Workspace::new();
        let configuration = workspace.config();
        let mut host = Host::open(configuration.clone()).await.unwrap();
        host.close().await.unwrap();
        drop(host);
        let store = RedbStateStoreProvider::new(workspace.0.join("owner/state.redb")).unwrap();
        store.delete(CONTROL, key).await.unwrap();
        store.sync().await.unwrap();
        drop(store);
        match Host::open(configuration).await {
            Err(error) => assert_eq!(error.code, 7, "{key}"),
            Ok(mut host) => {
                host.close().await.unwrap();
                panic!("missing {key} was silently recreated");
            }
        }
    }
}
