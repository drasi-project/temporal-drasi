use super::*;
use crate::{
    protocol::{Change, ChangeKind, Limits},
    source::{BootstrapLimits, Journal, TemporalSource},
};
use drasi_core::evaluation::variable_value::{integer::Integer, VariableValue};
use drasi_index_rocksdb::RocksDbIndexProvider;
use drasi_lib::{channels::SourceEventWrapper, sources::Source};
use drasi_state_store_redb::RedbStateStoreProvider;
use futures_util::StreamExt;
use serde_json::json;
use std::path::{Path, PathBuf};

struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/owned_query")
            .join(format!(".test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

struct Harness {
    core: DrasiLib,
    indexes: Arc<RocksDbIndexProvider>,
    sources: Vec<TemporalSource>,
    store: Arc<dyn drasi_lib::StateStoreProvider>,
}

impl Harness {
    async fn new(path: &Path) -> Self {
        let store = Arc::new(RedbStateStoreProvider::new(path.join("source.redb")).unwrap());
        let journal = Arc::new(Journal::new(store.clone(), 100));
        let core = DrasiLib::builder()
            .with_id("owned-test")
            .build()
            .await
            .unwrap();
        let mut sources = Vec::new();
        for id in ["left", "right"] {
            let source = TemporalSource::new(
                id,
                journal.clone(),
                true,
                BootstrapLimits::new(100, 1 << 20).unwrap(),
            )
            .unwrap();
            core.add_source(source.shared()).await.unwrap();
            sources.push(source);
        }
        core.start().await.unwrap();
        let indexes = Arc::new(
            RocksDbIndexProvider::new(path.join("indexes"), true, false)
                .with_memory_budget_bytes(32 << 20)
                .unwrap(),
        );
        Self {
            core,
            indexes,
            sources,
            store,
        }
    }

    async fn query(&self, id: &str, text: &str) -> Arc<dyn Query> {
        self.query_with_expectation(id, text, false).await
    }

    async fn query_with_expectation(
        &self,
        id: &str,
        text: &str,
        expected_existing: bool,
    ) -> Arc<dyn Query> {
        let config = drasi_lib::Query::cypher(id)
            .query(text)
            .from_source("left")
            .from_source("right")
            .auto_start(false)
            .build();
        install(
            &self.core,
            self.indexes.clone(),
            config,
            Limits::default(),
            self.store.clone(),
            expected_existing,
        )
        .await
        .unwrap();
        self.core
            .query_manager()
            .get_query_instance(id)
            .await
            .unwrap()
    }

    async fn feed(&self, source: usize, sequence: u64, value: i64) {
        self.sources[source]
            .feed(
                sequence,
                Change {
                    kind: if sequence == 1 {
                        ChangeKind::Insert
                    } else {
                        ChangeKind::Update
                    },
                    element_id: format!("item-{source}"),
                    labels: vec!["Item".into()],
                    effective_from: sequence,
                    properties: serde_json::from_value(json!({"value": value})).unwrap(),
                    relation: None,
                },
            )
            .await
            .unwrap();
    }

    async fn close(&self, query: &Arc<dyn Query>) {
        query.stop().await.unwrap();
        query.release_persistent_handles().await;
        self.core.stop().await.unwrap();
        let graph = self.core.component_graph();
        let mut graph = graph.write().await;
        let query = graph.take_runtime::<Arc<dyn Query>>(&query.get_config().id);
        let sources: Vec<_> = ["left", "right", "__component_graph__"]
            .into_iter()
            .filter_map(|id| graph.take_runtime::<Arc<dyn Source>>(id))
            .collect();
        drop(graph);
        drop((query, sources));
    }
}

async fn snapshot(query: &Arc<dyn Query>) -> (u64, u64, BTreeMap<u64, serde_json::Value>) {
    let response = query.fetch_snapshot().await.unwrap();
    (
        response.as_of_sequence,
        response.output_generation,
        response.stream_keyed().collect().await,
    )
}

async fn wait_sequence(query: &Arc<dyn Query>, sequence: u64) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if snapshot(query).await.0 == sequence {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned query did not commit expected sequence");
}

#[test]
fn reaction_codec_preserves_public_value_semantics() {
    let values = [
        (VariableValue::Null, json!(null)),
        (VariableValue::Bool(true), json!(true)),
        (
            VariableValue::Integer(Integer::from(u64::MAX)),
            json!(u64::MAX),
        ),
        (VariableValue::Awaiting, json!("Awaiting")),
        (
            VariableValue::Float(
                drasi_core::evaluation::variable_value::float::Float::from_f64(1.25).unwrap(),
            ),
            json!(1.25),
        ),
        (
            VariableValue::Float(f64::INFINITY.into()),
            json!("inf"),
        ),
        (
            VariableValue::Float(f64::NAN.into()),
            json!("NaN"),
        ),
        (
            VariableValue::Date(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()),
            json!("2026-01-02"),
        ),
        (
            VariableValue::LocalTime(chrono::NaiveTime::from_hms_opt(3, 4, 5).unwrap()),
            json!("03:04:05"),
        ),
        (
            VariableValue::List(vec![
                VariableValue::Integer(Integer::from(-1i64)),
                VariableValue::Object(BTreeMap::from([(
                    "ready".to_string(),
                    VariableValue::Bool(true),
                )])),
            ]),
            json!([-1, {"ready": true}]),
        ),
        (
            VariableValue::ElementReference(drasi_core::models::ElementReference {
                source_id: Arc::from("source"),
                element_id: Arc::from("item"),
            }),
            json!("ElementReference(ElementReference { source_id: \"source\", element_id: \"item\" })"),
        ),
        (
            VariableValue::ZonedDateTime(
                drasi_core::evaluation::variable_value::zoned_datetime::ZonedDateTime::new(
                    chrono::DateTime::parse_from_rfc3339("2026-01-02T03:04:05+02:00").unwrap(),
                    None,
                ),
            ),
            json!("2026-01-02T03:04:05+02:00"),
        ),
    ];
    for (input, expected) in values {
        assert_eq!(codec::value_json(&input), expected);
    }
}

#[tokio::test]
async fn every_source_must_finish_bootstrap_before_any_index_seed_is_written() {
    use drasi_core::models::ElementReference;
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    harness.feed(0, 1, 4).await;
    harness.feed(1, 1, 7).await;
    harness.sources[1].stop().await.unwrap();
    let query = harness
        .query("complete-only", "MATCH (n:Item) RETURN n.value AS value")
        .await;
    assert!(query
        .start()
        .await
        .unwrap_err()
        .to_string()
        .contains("not running"));
    query.stop().await.unwrap();
    let indexes = harness
        .indexes
        .create_indexes("complete-only")
        .await
        .unwrap();
    assert!(indexes
        .checkpoint_store
        .as_ref()
        .unwrap()
        .read_config_hash()
        .await
        .unwrap()
        .is_none());
    let guard = SessionGuard::begin(indexes.set.session_control.clone())
        .await
        .unwrap();
    assert!(indexes
        .set
        .element_index
        .get_element(&ElementReference {
            source_id: Arc::from("left"),
            element_id: Arc::from("item-0"),
        })
        .await
        .unwrap()
        .is_none());
    guard.commit().await.unwrap();
    drop(indexes);
    harness.sources[1].start().await.unwrap();
    query.start().await.unwrap();
    assert_eq!(snapshot(&query).await.0, 0);
    assert_eq!(snapshot(&query).await.2.len(), 2);
    harness.close(&query).await;
}

#[tokio::test]
async fn invalid_query_install_is_lazy_and_start_exposes_the_error() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    let query = harness.query("invalid", "THIS IS NOT CYPHER").await;
    assert_eq!(query.status().await, ComponentStatus::Stopped);
    assert!(query.start().await.is_err());
    assert_eq!(query.status().await, ComponentStatus::Error);
    query.stop().await.unwrap();
    assert_eq!(query.status().await, ComponentStatus::Stopped);
    harness.close(&query).await;
}

#[tokio::test]
async fn complete_multi_source_seed_is_sequence_zero_and_survives_warm_and_cold_start() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    harness.feed(0, 1, 4).await;
    harness.feed(1, 1, 7).await;
    let query = harness
        .query("seeded", "MATCH (n:Item) RETURN n.value AS value")
        .await;
    query.start().await.unwrap();
    let initial = snapshot(&query).await;
    assert_eq!((initial.0, initial.1), (0, 0));
    assert_eq!(
        initial.2.values().cloned().collect::<HashSet<_>>(),
        HashSet::from([json!({"value": 4}), json!({"value": 7})])
    );
    assert!(query.fetch_outbox(0).await.unwrap().results.is_empty());
    query.stop().await.unwrap();
    assert_eq!(query.subscription_count().await, 0);
    query.start().await.unwrap();
    assert_eq!(snapshot(&query).await, initial);
    harness.close(&query).await;
    drop(query);
    drop(harness);

    let harness = Harness::new(&workspace.0).await;
    let query = harness
        .query("seeded", "MATCH (n:Item) RETURN n.value AS value")
        .await;
    query.start().await.unwrap();
    assert_eq!(snapshot(&query).await, initial);
    let mut subscription = query.subscribe("live".into()).await.unwrap();
    assert_eq!(subscription.as_of_sequence, 0);
    harness.feed(0, 2, 9).await;
    let live = tokio::time::timeout(Duration::from_secs(10), subscription.receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live.sequence, 1);
    let value = serde_json::to_value(live.as_ref()).unwrap();
    assert_eq!(
        serde_json::to_value(query.fetch_outbox(0).await.unwrap().results[0].as_ref()).unwrap(),
        value
    );
    let updated = snapshot(&query).await;
    query.stop().await.unwrap();
    let indexes = harness.indexes.create_indexes("seeded").await.unwrap();
    let durable_bytes = indexes
        .outbox_writer
        .as_ref()
        .unwrap()
        .read_from("seeded", 0)
        .await
        .unwrap();
    drop(indexes);
    query.start().await.unwrap();
    assert_eq!(snapshot(&query).await, updated);
    assert_eq!(
        serde_json::to_value(query.fetch_outbox(0).await.unwrap().results[0].as_ref()).unwrap(),
        value
    );
    query.stop().await.unwrap();
    let indexes = harness.indexes.create_indexes("seeded").await.unwrap();
    assert_eq!(
        indexes
            .outbox_writer
            .as_ref()
            .unwrap()
            .read_from("seeded", 0)
            .await
            .unwrap(),
        durable_bytes
    );
    drop(indexes);
    harness.close(&query).await;
    drop(query);
    drop(harness);

    let harness = Harness::new(&workspace.0).await;
    let query = harness
        .query("seeded", "MATCH (n:Item) RETURN n.value AS value")
        .await;
    query.start().await.unwrap();
    assert_eq!(snapshot(&query).await, updated);
    assert_eq!(
        serde_json::to_value(query.fetch_outbox(0).await.unwrap().results[0].as_ref()).unwrap(),
        value
    );
    query.stop().await.unwrap();
    let indexes = harness.indexes.create_indexes("seeded").await.unwrap();
    assert_eq!(
        indexes
            .outbox_writer
            .as_ref()
            .unwrap()
            .read_from("seeded", 0)
            .await
            .unwrap(),
        durable_bytes
    );
    drop(indexes);
    harness.close(&query).await;
}

#[tokio::test]
async fn aggregate_state_spans_sources_and_continues_after_cold_rehydrate() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    harness.feed(0, 1, 4).await;
    harness.feed(1, 1, 7).await;
    let text = "MATCH (n:Item) RETURN count(n) AS count, sum(n.value) AS total";
    let query = harness.query("aggregate", text).await;
    query.start().await.unwrap();
    let initial = snapshot(&query).await;
    assert_eq!(
        initial.2.values().cloned().collect::<Vec<_>>(),
        vec![json!({"count": 2, "total": 11.0})]
    );
    harness.close(&query).await;
    drop(query);
    drop(harness);

    let harness = Harness::new(&workspace.0).await;
    let query = harness.query("aggregate", text).await;
    query.start().await.unwrap();
    assert_eq!(snapshot(&query).await, initial);
    harness.feed(1, 2, 10).await;
    wait_sequence(&query, 1).await;
    let after = snapshot(&query).await;
    assert_eq!(
        after.2.values().cloned().collect::<Vec<_>>(),
        vec![json!({"count": 2, "total": 14.0})]
    );
    assert_eq!(
        after.2.keys().collect::<Vec<_>>(),
        initial.2.keys().collect::<Vec<_>>()
    );
    harness.close(&query).await;
}

#[tokio::test]
async fn tampered_generation_is_refused_without_reset_and_error_owner_joins() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    harness.feed(0, 1, 4).await;
    let query = harness
        .query("sealed", "MATCH (n:Item) RETURN n.value AS value")
        .await;
    query.start().await.unwrap();
    let initial = snapshot(&query).await;
    query.stop().await.unwrap();
    let storage = harness.indexes.create_indexes("sealed").await.unwrap();
    storage
        .checkpoint_store
        .as_ref()
        .unwrap()
        .write_output_generation("sealed", 1)
        .await
        .unwrap();
    drop(storage);
    assert!(query
        .start()
        .await
        .unwrap_err()
        .to_string()
        .contains("generation"));
    query.stop().await.unwrap();
    assert_eq!(query.status().await, ComponentStatus::Stopped);
    assert_eq!(query.subscription_count().await, 0);
    let storage = harness.indexes.create_indexes("sealed").await.unwrap();
    assert_eq!(
        storage
            .live_results_writer
            .as_ref()
            .unwrap()
            .row_count("sealed")
            .await
            .unwrap(),
        initial.2.len()
    );
    storage
        .checkpoint_store
        .as_ref()
        .unwrap()
        .write_output_generation("sealed", 0)
        .await
        .unwrap();
    drop(storage);
    query.start().await.unwrap();
    assert_eq!(snapshot(&query).await, initial);
    harness.close(&query).await;
}

#[tokio::test]
async fn every_future_pop_checks_actual_rocksdb_due_time() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    let future = now_millis() + 60_000;
    harness.feed(0, 1, (now_millis() - 1) as i64).await;
    harness.feed(1, 1, future as i64).await;
    let query = harness
        .query(
            "timed",
            "MATCH (n:Item) WHERE drasi.trueLater(true, n.value) RETURN n.value AS value",
        )
        .await;
    query.start().await.unwrap();
    wait_sequence(&query, 1).await;
    let state = snapshot(&query).await;
    assert_eq!(state.2.len(), 1);
    assert!(state.2.values().all(|row| row["value"] != json!(future)));
    query.stop().await.unwrap();
    let indexes = harness.indexes.create_indexes("timed").await.unwrap();
    assert_eq!(
        indexes.set.future_queue.peek_due_time().await.unwrap(),
        Some(future)
    );
    drop(indexes);
    query.start().await.unwrap();
    assert_eq!(snapshot(&query).await, state);
    query.stop().await.unwrap();
    let indexes = harness.indexes.create_indexes("timed").await.unwrap();
    assert_eq!(
        indexes.set.future_queue.peek_due_time().await.unwrap(),
        Some(future)
    );
    drop(indexes);
    harness.close(&query).await;
}

#[tokio::test]
async fn seed_limit_failure_rolls_back_all_indexes_and_can_retry_without_reset() {
    use drasi_core::models::ElementReference;
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    harness.feed(0, 1, 4).await;
    harness.feed(1, 1, 7).await;
    let config = drasi_lib::Query::cypher("bounded")
        .query("MATCH (n:Item) RETURN n.value AS value")
        .from_source("left")
        .from_source("right")
        .auto_start(false)
        .build();
    install(
        &harness.core,
        harness.indexes.clone(),
        config,
        Limits {
            snapshot_rows: 1,
            ..Limits::default()
        },
        harness.store.clone(),
        false,
    )
    .await
    .unwrap();
    let query = harness
        .core
        .query_manager()
        .get_query_instance("bounded")
        .await
        .unwrap();
    assert!(query
        .start()
        .await
        .unwrap_err()
        .to_string()
        .contains("subscription results exceed configured limits"));
    query.stop().await.unwrap();
    let indexes = harness.indexes.create_indexes("bounded").await.unwrap();
    let guard = SessionGuard::begin(indexes.set.session_control.clone())
        .await
        .unwrap();
    assert!(indexes
        .set
        .element_index
        .get_element(&ElementReference {
            source_id: Arc::from("left"),
            element_id: Arc::from("item-0"),
        })
        .await
        .unwrap()
        .is_none());
    guard.commit().await.unwrap();
    assert_eq!(
        indexes
            .live_results_writer
            .as_ref()
            .unwrap()
            .row_count("bounded")
            .await
            .unwrap(),
        0
    );
    assert!(indexes
        .checkpoint_store
        .as_ref()
        .unwrap()
        .read_result_sequence("bounded")
        .await
        .unwrap()
        .is_none());
    drop(indexes);
    harness.sources[1]
        .feed(
            2,
            Change {
                kind: ChangeKind::Delete,
                element_id: "item-1".into(),
                labels: vec!["Item".into()],
                effective_from: 2,
                properties: BTreeMap::new(),
                relation: None,
            },
        )
        .await
        .unwrap();
    query.start().await.unwrap();
    let state = snapshot(&query).await;
    assert_eq!(state.0, 0);
    assert_eq!(
        state.2.values().cloned().collect::<Vec<_>>(),
        vec![json!({"value": 4})]
    );
    assert!(query.fetch_outbox(0).await.unwrap().results.is_empty());
    harness.close(&query).await;
}

struct ZeroSequenceSource {
    base: drasi_lib::sources::SourceBase,
    settings: Arc<Mutex<Vec<drasi_lib::config::SourceSubscriptionSettings>>>,
    failure: Option<Arc<Notify>>,
}

const PRIVATE_SOURCE_FAILURE: &str = "source failed at postgres://fixture:PRIVATE_SOURCE_CREDENTIAL@fixture.invalid/db; subscription results exceed configured limits";

struct FailingSourceReceiver {
    inner: Box<dyn ChangeReceiver<SourceEventWrapper>>,
    failure: Arc<Notify>,
}

#[async_trait]
impl ChangeReceiver<SourceEventWrapper> for FailingSourceReceiver {
    async fn recv(&mut self) -> Result<Arc<SourceEventWrapper>> {
        tokio::select! {
            biased;
            _ = self.failure.notified() => bail!(PRIVATE_SOURCE_FAILURE),
            event = self.inner.recv() => event,
        }
    }
}

#[async_trait]
impl Source for ZeroSequenceSource {
    fn id(&self) -> &str {
        "zero"
    }
    fn type_name(&self) -> &str {
        "sequence-zero-fixture"
    }
    fn auto_start(&self) -> bool {
        false
    }
    fn properties(&self) -> HashMap<String, serde_json::Value> {
        HashMap::new()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    async fn initialize(&self, context: drasi_lib::context::SourceRuntimeContext) {
        self.base.initialize(context).await;
    }
    async fn start(&self) -> Result<()> {
        self.base.set_status(ComponentStatus::Running, None).await;
        Ok(())
    }
    async fn stop(&self) -> Result<()> {
        self.base.stop_common().await
    }
    async fn status(&self) -> ComponentStatus {
        self.base.get_status().await
    }
    async fn remove_position_handle(&self, query: &str) {
        self.base.remove_position_handle(query).await;
    }
    async fn subscribe(
        &self,
        settings: drasi_lib::config::SourceSubscriptionSettings,
    ) -> Result<drasi_lib::channels::SubscriptionResponse> {
        use drasi_core::models::{
            Element, ElementMetadata, ElementPropertyMap, ElementReference, ElementValue,
            SourceChange,
        };
        use drasi_lib::{
            bootstrap::BootstrapResult,
            channels::{ReplayThenLiveReceiver, SourceEventWrapper},
        };
        self.settings.lock().await.push(settings.clone());
        let live = self.base.create_streaming_receiver().await?;
        let mut replay = std::collections::VecDeque::new();
        let (bootstrap_receiver, bootstrap_result_receiver) = if settings.enable_bootstrap {
            let (events, receiver) = tokio::sync::mpsc::channel(1);
            drop(events);
            let (result, complete) = tokio::sync::oneshot::channel();
            result
                .send(Ok(BootstrapResult {
                    event_count: 0,
                    source_position: Some(bytes::Bytes::from_static(b"initial-boundary")),
                }))
                .unwrap();
            (Some(receiver), Some(complete))
        } else {
            if settings.resume_sequence.is_none() {
                replay.push_back(Arc::new(SourceEventWrapper::new(
                    "zero".into(),
                    SourceEvent::Control(drasi_lib::channels::SourceControl::FuturesDue),
                    chrono::Utc::now(),
                    999,
                )));
                let mut properties = ElementPropertyMap::new();
                properties.insert("value", ElementValue::Integer(42));
                replay.push_back(Arc::new(
                    SourceEventWrapper::new(
                        "zero".into(),
                        SourceEvent::Change(SourceChange::Insert {
                            element: Element::Node {
                                metadata: ElementMetadata {
                                    reference: ElementReference::new("zero", "item"),
                                    labels: Arc::from([Arc::from("Item")]),
                                    effective_from: 1,
                                },
                                properties,
                            },
                        }),
                        chrono::Utc::now(),
                        0,
                    )
                    .with_source_position(bytes::Bytes::from_static(b"opaque-position-for-zero")),
                ));
            }
            (None, None)
        };
        let receiver: Box<dyn ChangeReceiver<SourceEventWrapper>> =
            Box::new(ReplayThenLiveReceiver::new(replay, live));
        let receiver: Box<dyn ChangeReceiver<SourceEventWrapper>> = match &self.failure {
            Some(failure) => Box::new(FailingSourceReceiver {
                inner: receiver,
                failure: failure.clone(),
            }),
            None => receiver,
        };
        Ok(drasi_lib::channels::SubscriptionResponse {
            query_id: settings.query_id.clone(),
            source_id: self.id().into(),
            receiver,
            bootstrap_receiver,
            bootstrap_result_receiver,
            position_handle: Some(self.base.create_position_handle(&settings.query_id).await),
        })
    }
}

#[tokio::test]
async fn upstream_sequence_zero_is_committed_and_resumed_without_reinterpreting_position() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    let settings = Arc::new(Mutex::new(Vec::new()));
    harness
        .core
        .add_source(ZeroSequenceSource {
            base: drasi_lib::sources::SourceBase::new(drasi_lib::sources::SourceBaseParams::new(
                "zero",
            ))
            .unwrap(),
            settings: settings.clone(),
            failure: None,
        })
        .await
        .unwrap();
    harness.core.start_source("zero").await.unwrap();
    let config = drasi_lib::Query::cypher("zero-query")
        .query("MATCH (n:Item) RETURN n.value AS value")
        .from_source("zero")
        .auto_start(false)
        .build();
    install(
        &harness.core,
        harness.indexes.clone(),
        config,
        Limits::default(),
        harness.store.clone(),
        false,
    )
    .await
    .unwrap();
    let query = harness
        .core
        .query_manager()
        .get_query_instance("zero-query")
        .await
        .unwrap();
    query.start().await.unwrap();
    wait_sequence(&query, 1).await;
    assert_eq!(
        snapshot(&query)
            .await
            .2
            .values()
            .cloned()
            .collect::<Vec<_>>(),
        vec![json!({"value": 42})]
    );
    query.stop().await.unwrap();
    query.start().await.unwrap();
    let captured = settings.lock().await;
    let resumed = captured.last().unwrap();
    assert_eq!(resumed.resume_sequence, Some(0));
    assert_eq!(
        resumed.resume_from.as_deref(),
        Some(b"opaque-position-for-zero".as_slice())
    );
    drop(captured);
    assert_eq!(snapshot(&query).await.0, 1);
    harness.close(&query).await;
}

#[tokio::test]
async fn retained_output_capacity_errors_are_public_without_advancing_durable_output() {
    for (id, rows, bytes, value) in [
        (
            "row-capacity",
            1,
            Limits::default().pending_bytes,
            "alpha".to_string(),
        ),
        ("byte-capacity", 100, 1024, "x".repeat(600)),
    ] {
        let workspace = Workspace::new();
        let harness = Harness::new(&workspace.0).await;
        let config = drasi_lib::Query::cypher(id)
            .query("MATCH (n:Item) RETURN n.value AS value")
            .from_source("left")
            .from_source("right")
            .auto_start(false)
            .build();
        install(
            &harness.core,
            harness.indexes.clone(),
            config,
            Limits {
                snapshot_rows: rows,
                pending_bytes: bytes,
                ..Limits::default()
            },
            harness.store.clone(),
            false,
        )
        .await
        .unwrap();
        let query = harness
            .core
            .query_manager()
            .get_query_instance(id)
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            check_output_failure(query.as_ref()),
        )
        .await
        .unwrap()
        .unwrap();
        query.start().await.unwrap();
        harness.sources[0]
            .feed(
                1,
                Change {
                    kind: ChangeKind::Insert,
                    element_id: "first".into(),
                    labels: vec!["Item".into()],
                    effective_from: 1,
                    properties: BTreeMap::from([("value".into(), json!(value))]),
                    relation: None,
                },
            )
            .await
            .unwrap();
        wait_sequence(&query, 1).await;
        check_output_failure(query.as_ref()).await.unwrap();
        let output = query.fetch_snapshot().await.unwrap();
        assert_eq!((output.as_of_sequence, output.len()), (1, 1));
        let accepted = query.fetch_outbox(0).await.unwrap();
        let accepted_bytes = serde_json::to_vec(&accepted.results[0]).unwrap();
        harness.sources[0]
            .feed(
                2,
                Change {
                    kind: ChangeKind::Insert,
                    element_id: "second".into(),
                    labels: vec!["Item".into()],
                    effective_from: 2,
                    properties: BTreeMap::from([("value".into(), json!(value))]),
                    relation: None,
                },
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while query.status().await != ComponentStatus::Error {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for _ in 0..2 {
            let error = crate::protocol::Error::from(
                check_output_failure(query.as_ref()).await.unwrap_err(),
            );
            assert_eq!(error.code, 3, "{id}");
            assert_eq!(
                error.message, "subscription results exceed configured limits",
                "{id}"
            );
        }
        assert!(matches!(
            query.fetch_snapshot().await,
            Err(FetchError::NotRunning {
                status: ComponentStatus::Error
            })
        ));
        query.stop().await.unwrap();
        check_output_failure(query.as_ref()).await.unwrap();
        assert!(matches!(
            query.fetch_snapshot().await,
            Err(FetchError::NotRunning {
                status: ComponentStatus::Stopped
            })
        ));
        let indexes = harness.indexes.create_indexes(id).await.unwrap();
        let checkpoints = indexes.checkpoint_store.as_ref().unwrap();
        assert_eq!(
            checkpoints.read_result_sequence(id).await.unwrap(),
            Some(1),
            "{id}"
        );
        assert_eq!(
            checkpoints.read_all_checkpoints().await.unwrap()["left"].sequence,
            1,
            "{id}"
        );
        assert_eq!(
            indexes
                .outbox_writer
                .as_ref()
                .unwrap()
                .read_from(id, 0)
                .await
                .unwrap(),
            vec![(1, accepted_bytes)],
            "{id}"
        );
        assert_eq!(
            indexes
                .live_results_writer
                .as_ref()
                .unwrap()
                .row_count(id)
                .await
                .unwrap(),
            1
        );
        drop(indexes);
        harness.close(&query).await;
    }
}

#[tokio::test]
async fn source_failure_does_not_expose_credentials_or_old_output_as_a_capacity_error() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    let failure = Arc::new(Notify::new());
    harness
        .core
        .add_source(ZeroSequenceSource {
            base: drasi_lib::sources::SourceBase::new(drasi_lib::sources::SourceBaseParams::new(
                "zero",
            ))
            .unwrap(),
            settings: Arc::new(Mutex::new(Vec::new())),
            failure: Some(failure.clone()),
        })
        .await
        .unwrap();
    harness.core.start_source("zero").await.unwrap();
    let config = drasi_lib::Query::cypher("private-source-failure")
        .query("MATCH (n:Item) RETURN n.value AS value")
        .from_source("zero")
        .auto_start(false)
        .build();
    install(
        &harness.core,
        harness.indexes.clone(),
        config,
        Limits::default(),
        harness.store.clone(),
        false,
    )
    .await
    .unwrap();
    let query = harness
        .core
        .query_manager()
        .get_query_instance("private-source-failure")
        .await
        .unwrap();
    query.start().await.unwrap();
    wait_sequence(&query, 1).await;
    check_output_failure(query.as_ref()).await.unwrap();
    assert_eq!(query.fetch_snapshot().await.unwrap().len(), 1);
    failure.notify_one();
    tokio::time::timeout(Duration::from_secs(10), async {
        while query.status().await != ComponentStatus::Error {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let owned = query.as_any().downcast_ref::<OwnedQuery>().unwrap();
    assert!(owned
        .failure
        .lock()
        .await
        .as_ref()
        .unwrap()
        .message
        .contains("PRIVATE_SOURCE_CREDENTIAL"));
    assert!(owned.publication.lock().await.output.is_some());
    for _ in 0..2 {
        check_output_failure(query.as_ref()).await.unwrap();
        assert_eq!(
            query.fetch_snapshot().await.unwrap_err().to_string(),
            "Query is not running (status: Error)"
        );
    }
    harness.close(&query).await;
}

struct FailSeedCommit {
    inner: Arc<RocksDbIndexProvider>,
    remaining: Arc<std::sync::atomic::AtomicUsize>,
    commit_before_failure: bool,
}

struct FailSeedSession {
    inner: Arc<dyn drasi_core::interface::SessionControl>,
    remaining: Arc<std::sync::atomic::AtomicUsize>,
    depth: std::sync::Mutex<usize>,
    commit_before_failure: bool,
}

#[async_trait]
impl IndexBackendPlugin for FailSeedCommit {
    fn is_volatile(&self) -> bool {
        false
    }
    async fn create_indexes(
        &self,
        id: &str,
    ) -> std::result::Result<drasi_core::interface::CreatedIndexes, drasi_core::interface::IndexError>
    {
        let mut indexes = self.inner.create_indexes(id).await?;
        indexes.set.session_control = Arc::new(FailSeedSession {
            inner: indexes.set.session_control,
            remaining: self.remaining.clone(),
            depth: std::sync::Mutex::new(0),
            commit_before_failure: self.commit_before_failure,
        });
        Ok(indexes)
    }
}

#[async_trait]
impl drasi_core::interface::SessionControl for FailSeedSession {
    async fn begin(&self) -> std::result::Result<(), drasi_core::interface::IndexError> {
        self.inner.begin().await?;
        *self.depth.lock().unwrap() += 1;
        Ok(())
    }
    async fn commit(&self) -> std::result::Result<(), drasi_core::interface::IndexError> {
        if *self.depth.lock().unwrap() == 1
            && self.remaining.fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |remaining| remaining.checked_sub(1),
            ) == Ok(1)
        {
            if self.commit_before_failure {
                self.inner.commit().await?;
                *self.depth.lock().unwrap() -= 1;
            }
            return Err(index_error(anyhow::anyhow!("injected seed commit failure")));
        }
        self.inner.commit().await?;
        *self.depth.lock().unwrap() -= 1;
        Ok(())
    }
    fn rollback(&self) -> std::result::Result<(), drasi_core::interface::IndexError> {
        *self.depth.lock().unwrap() = 0;
        self.inner.rollback()
    }
}

#[tokio::test]
async fn failed_outer_seed_commit_rolls_back_elements_results_and_actual_future_queue() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    let future = now_millis() + 60_000;
    harness.feed(0, 1, future as i64).await;
    harness.feed(1, 1, future as i64).await;
    let indexes = Arc::new(FailSeedCommit {
        inner: harness.indexes.clone(),
        remaining: Arc::new(std::sync::atomic::AtomicUsize::new(2)),
        commit_before_failure: false,
    });
    let config = drasi_lib::Query::cypher("failed-commit")
        .query("MATCH (n:Item) WHERE drasi.trueLater(true, n.value) RETURN n.value AS value")
        .from_source("left")
        .from_source("right")
        .auto_start(false)
        .build();
    install(
        &harness.core,
        indexes,
        config,
        Limits::default(),
        harness.store.clone(),
        false,
    )
    .await
    .unwrap();
    let query = harness
        .core
        .query_manager()
        .get_query_instance("failed-commit")
        .await
        .unwrap();
    assert!(query
        .start()
        .await
        .unwrap_err()
        .to_string()
        .contains("injected seed commit failure"));
    query.stop().await.unwrap();
    let indexes = harness
        .indexes
        .create_indexes("failed-commit")
        .await
        .unwrap();
    assert!(indexes
        .set
        .future_queue
        .peek_due_time()
        .await
        .unwrap()
        .is_none());
    let guard = SessionGuard::begin(indexes.set.session_control.clone())
        .await
        .unwrap();
    assert!(indexes
        .set
        .element_index
        .get_element(&drasi_core::models::ElementReference::new("left", "item-0"))
        .await
        .unwrap()
        .is_none());
    guard.commit().await.unwrap();
    assert!(indexes
        .checkpoint_store
        .as_ref()
        .unwrap()
        .read_result_sequence("failed-commit")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        indexes
            .live_results_writer
            .as_ref()
            .unwrap()
            .row_count("failed-commit")
            .await
            .unwrap(),
        0
    );
    assert!(indexes
        .outbox_writer
        .as_ref()
        .unwrap()
        .read_from("failed-commit", 0)
        .await
        .unwrap()
        .is_empty());
    drop(indexes);
    assert!(query
        .start()
        .await
        .unwrap_err()
        .to_string()
        .contains("committed state is missing"));
    harness.close(&query).await;
}

#[tokio::test]
async fn committed_seed_with_lost_commit_response_is_recovered_from_outer_fence() {
    for lose_index in [false, true] {
        let workspace = Workspace::new();
        let harness = Harness::new(&workspace.0).await;
        harness.feed(0, 1, 4).await;
        harness.feed(1, 1, 7).await;
        let config = drasi_lib::Query::cypher("uncertain-commit")
            .query("MATCH (n:Item) RETURN n.value AS value")
            .from_source("left")
            .from_source("right")
            .auto_start(false)
            .build();
        install(
            &harness.core,
            Arc::new(FailSeedCommit {
                inner: harness.indexes.clone(),
                remaining: Arc::new(std::sync::atomic::AtomicUsize::new(2)),
                commit_before_failure: true,
            }),
            config,
            Limits::default(),
            harness.store.clone(),
            false,
        )
        .await
        .unwrap();
        let query = harness
            .core
            .query_manager()
            .get_query_instance("uncertain-commit")
            .await
            .unwrap();
        assert!(query
            .start()
            .await
            .unwrap_err()
            .to_string()
            .contains("injected seed commit failure"));
        harness.close(&query).await;
        drop((query, harness));
        if lose_index {
            std::fs::remove_dir_all(workspace.0.join("indexes/uncertain-commit")).unwrap();
        }

        let harness = Harness::new(&workspace.0).await;
        let query = harness
            .query("uncertain-commit", "MATCH (n:Item) RETURN n.value AS value")
            .await;
        if lose_index {
            assert!(query
                .start()
                .await
                .unwrap_err()
                .to_string()
                .contains("committed state is missing"));
        } else {
            query.start().await.unwrap();
            let actual = snapshot(&query).await;
            assert_eq!((actual.0, actual.1, actual.2.len()), (0, 0, 2));
            assert!(query.fetch_outbox(0).await.unwrap().results.is_empty());
        }
        harness.close(&query).await;
    }
}

#[tokio::test]
async fn complete_query_directory_loss_is_refused_warm_without_losing_peer_query() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    let future = now_millis() + 60_000;
    harness.feed(0, 1, future as i64).await;
    harness.feed(1, 1, future as i64).await;
    let query = harness
        .query(
            "lost-warm",
            "MATCH (n:Item) WHERE drasi.trueLater(true, n.value) RETURN n.value AS value",
        )
        .await;
    let peer = harness
        .query("peer", "MATCH (n:Item) RETURN n.value AS value")
        .await;
    query.start().await.unwrap();
    peer.start().await.unwrap();
    assert!(snapshot(&query).await.2.is_empty());
    let peer_before = snapshot(&peer).await;
    query.stop().await.unwrap();
    let indexes = harness.indexes.create_indexes("lost-warm").await.unwrap();
    assert_eq!(
        indexes.set.future_queue.peek_due_time().await.unwrap(),
        Some(future)
    );
    drop(indexes);
    std::fs::remove_dir_all(workspace.0.join("indexes/lost-warm")).unwrap();
    assert!(query
        .start()
        .await
        .unwrap_err()
        .to_string()
        .contains("committed state is missing"));
    assert_eq!(peer.status().await, ComponentStatus::Running);
    assert_eq!(snapshot(&peer).await, peer_before);
    peer.stop().await.unwrap();
    peer.release_persistent_handles().await;
    harness
        .core
        .component_graph()
        .write()
        .await
        .take_runtime::<Arc<dyn Query>>("peer");
    harness.close(&query).await;
}

#[tokio::test]
async fn complete_query_directory_loss_is_refused_cold_for_rows_and_future_only_state() {
    for (id, text, rows) in [
        ("lost-rows", "MATCH (n:Item) RETURN n.value AS value", 2),
        (
            "lost-futures",
            "MATCH (n:Item) WHERE drasi.trueLater(true, n.value) RETURN n.value AS value",
            0,
        ),
    ] {
        let workspace = Workspace::new();
        let harness = Harness::new(&workspace.0).await;
        harness.feed(0, 1, (now_millis() + 60_000) as i64).await;
        harness.feed(1, 1, (now_millis() + 60_000) as i64).await;
        let query = harness.query(id, text).await;
        query.start().await.unwrap();
        assert_eq!(snapshot(&query).await.2.len(), rows, "{id}");
        harness.close(&query).await;
        drop((query, harness));
        std::fs::remove_dir_all(workspace.0.join("indexes").join(id)).unwrap();

        let harness = Harness::new(&workspace.0).await;
        let query = harness.query(id, text).await;
        assert!(
            query
                .start()
                .await
                .unwrap_err()
                .to_string()
                .contains("committed state is missing"),
            "{id}"
        );
        query.stop().await.unwrap();
        let indexes = harness.indexes.create_indexes(id).await.unwrap();
        assert!(
            indexes
                .checkpoint_store
                .as_ref()
                .unwrap()
                .read_result_sequence(id)
                .await
                .unwrap()
                .is_none(),
            "{id}"
        );
        assert_eq!(
            indexes
                .live_results_writer
                .as_ref()
                .unwrap()
                .row_count(id)
                .await
                .unwrap(),
            0,
            "{id}"
        );
        drop(indexes);
        harness.close(&query).await;
    }
}

#[tokio::test]
async fn known_uncommitted_seed_can_retry_cold_after_its_index_directory_is_lost() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    harness.feed(0, 1, 4).await;
    harness.feed(1, 1, 7).await;
    let config = drasi_lib::Query::cypher("uncommitted")
        .query("MATCH (n:Item) RETURN n.value AS value")
        .from_source("left")
        .from_source("right")
        .auto_start(false)
        .build();
    install(
        &harness.core,
        harness.indexes.clone(),
        config,
        Limits {
            snapshot_rows: 1,
            ..Limits::default()
        },
        harness.store.clone(),
        false,
    )
    .await
    .unwrap();
    let query = harness
        .core
        .query_manager()
        .get_query_instance("uncommitted")
        .await
        .unwrap();
    assert!(query
        .start()
        .await
        .unwrap_err()
        .to_string()
        .contains("subscription results exceed configured limits"));
    harness.close(&query).await;
    drop((query, harness));
    std::fs::remove_dir_all(workspace.0.join("indexes/uncommitted")).unwrap();

    let harness = Harness::new(&workspace.0).await;
    let query = harness
        .query_with_expectation(
            "uncommitted",
            "MATCH (n:Item) RETURN n.value AS value",
            true,
        )
        .await;
    query.start().await.unwrap();
    let actual = snapshot(&query).await;
    assert_eq!((actual.0, actual.1, actual.2.len()), (0, 0, 2));
    harness.close(&query).await;
}

#[tokio::test]
async fn unwitnessed_existing_admission_requires_a_complete_seal() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    let query = harness
        .query_with_expectation(
            "unwitnessed",
            "MATCH (n:Item) RETURN n.value AS value",
            true,
        )
        .await;
    assert!(query
        .start()
        .await
        .unwrap_err()
        .to_string()
        .contains("committed state is missing"));
    harness.close(&query).await;
}

#[tokio::test]
async fn unwitnessed_existing_seal_is_validated_and_adopted_before_readiness() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    harness.feed(0, 1, 4).await;
    let query = harness
        .query("adopted", "MATCH (n:Item) RETURN n.value AS value")
        .await;
    query.start().await.unwrap();
    let before = snapshot(&query).await;
    let key = serde_json::to_string(&("adopted", compute_config_hash(query.get_config()))).unwrap();
    harness.close(&query).await;
    assert!(harness
        .store
        .delete(admission::PARTITION, &key)
        .await
        .unwrap());
    drop((query, harness));

    let harness = Harness::new(&workspace.0).await;
    let query = harness
        .query_with_expectation("adopted", "MATCH (n:Item) RETURN n.value AS value", true)
        .await;
    query.start().await.unwrap();
    assert_eq!(snapshot(&query).await, before);
    assert!(harness
        .store
        .contains_key(admission::PARTITION, &key)
        .await
        .unwrap());
    harness.close(&query).await;
}

#[tokio::test]
async fn sdk_config_hash_without_owned_admission_never_authorizes_rebootstrap() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    let query = harness
        .query("unsealed", "MATCH (n:Item) RETURN n.value AS value")
        .await;
    let indexes = harness.indexes.create_indexes("unsealed").await.unwrap();
    indexes
        .checkpoint_store
        .as_ref()
        .unwrap()
        .write_config_hash(compute_config_hash(query.get_config()))
        .await
        .unwrap();
    drop(indexes);
    assert!(query
        .start()
        .await
        .unwrap_err()
        .to_string()
        .contains("unsealed SDK"));
    query.stop().await.unwrap();
    let indexes = harness.indexes.create_indexes("unsealed").await.unwrap();
    assert!(indexes
        .checkpoint_store
        .as_ref()
        .unwrap()
        .read_all_checkpoints()
        .await
        .unwrap()
        .is_empty());
    assert!(indexes
        .checkpoint_store
        .as_ref()
        .unwrap()
        .read_result_sequence("unsealed")
        .await
        .unwrap()
        .is_none());
    drop(indexes);
    harness.close(&query).await;
}

#[tokio::test]
async fn lagged_subscription_closes_for_outbox_replay_without_blocking_evaluation() {
    let workspace = Workspace::new();
    let harness = Harness::new(&workspace.0).await;
    let mut config = drasi_lib::Query::cypher("lagged")
        .query("MATCH (n:Item) RETURN n.value AS value")
        .from_source("left")
        .from_source("right")
        .auto_start(false)
        .build();
    config.dispatch_buffer_capacity = Some(1);
    install(
        &harness.core,
        harness.indexes.clone(),
        config,
        Limits {
            subscriptions: 1,
            ..Limits::default()
        },
        harness.store.clone(),
        false,
    )
    .await
    .unwrap();
    let query = harness
        .core
        .query_manager()
        .get_query_instance("lagged")
        .await
        .unwrap();
    query.start().await.unwrap();
    let mut lagged = query.subscribe("lagged".into()).await.unwrap();
    assert!(query.subscribe("over-limit".into()).await.is_err());
    harness.feed(0, 1, 1).await;
    wait_sequence(&query, 1).await;
    harness.feed(0, 2, 2).await;
    wait_sequence(&query, 2).await;
    assert!(lagged
        .receiver
        .recv()
        .await
        .unwrap_err()
        .to_string()
        .contains("lagged"));
    assert!(lagged.receiver.recv().await.is_err());
    assert_eq!(query.status().await, ComponentStatus::Running);
    let replay = query.fetch_outbox(0).await.unwrap();
    assert_eq!(
        replay
            .results
            .iter()
            .map(|result| result.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let mut resumed = query.subscribe("resumed".into()).await.unwrap();
    assert_eq!(resumed.as_of_sequence, 2);
    harness.feed(0, 3, 3).await;
    let next = tokio::time::timeout(Duration::from_secs(10), resumed.receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.sequence, 3);
    harness.close(&query).await;
}
