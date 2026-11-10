use super::*;
use crate::protocol::{
    Command, Language, NodeDefinition, NodeRecipe, ResourceLifetime, StartMode, Subscription,
};
use crate::runtime::{
    tests::{query_state, Workspace},
    Host,
};
use drasi_lib::state_store::{StateStoreError, StateStoreResult};
use drasi_state_store_redb::RedbStateStoreProvider;
use serde_json::json;
use std::collections::BTreeMap;

fn scope(shards: u32) -> HistoryScope {
    HistoryScope {
        namespace_id: "00000000-0000-4000-8000-000000000001".into(),
        shard_count: shards,
    }
}

fn bootstrap(shard: u32) -> HistoryStep {
    HistoryStep {
        request_id: uuid::Uuid::from_u128(u128::from(shard)).to_string(),
        shard_id: shard,
        expected_revision: 0,
        page: HistoryPage::Bootstrap {
            capture_epoch: 7,
            reconstruction_start: 10,
            next_token: String::new(),
            cutoff: 11,
        },
    }
}

fn record(position: i64, value: Option<i64>) -> HistoryRecord {
    HistoryRecord {
        position,
        capture_epoch: 7,
        namespace_id: scope(1).namespace_id,
        execution_key: "a".repeat(64),
        effective_from: position as u64,
        state: value
            .map(|value| {
                vec![ElementImage {
                    element_id: format!("x:{}:state:workflow", "a".repeat(64)),
                    labels: vec!["Workflow".into()],
                    effective_from: position as u64,
                    properties: BTreeMap::from([("value".into(), json!(value))]),
                    relation: None,
                }]
            })
            .unwrap_or_default(),
        facts: Vec::new(),
    }
}

fn page(
    shard: u32,
    revision: u64,
    start: i64,
    next: i64,
    records: Vec<HistoryRecord>,
) -> HistoryStep {
    HistoryStep {
        request_id: uuid::Uuid::from_u128((u128::from(shard) << 64) | (u128::from(revision) + 1))
            .to_string(),
        shard_id: shard,
        expected_revision: revision,
        page: HistoryPage::Records {
            capture_epoch: 7,
            start_position: start,
            next_position: next,
            published_cutoff: next,
            records,
        },
    }
}

async fn dump(store: &dyn StateStoreProvider) -> BTreeMap<String, Vec<u8>> {
    let partition = Journal::partition("source");
    let mut result = BTreeMap::new();
    for key in store.list_keys(&partition).await.unwrap() {
        result.insert(
            key.clone(),
            store.get(&partition, &key).await.unwrap().unwrap(),
        );
    }
    result
}

#[tokio::test]
async fn atomic_history_acceptance_preserves_receipts_scope_and_replacement() {
    let workspace = Workspace::new();
    let original_receipt;
    {
        let store = Arc::new(RedbStateStoreProvider::new(workspace.0.join("state.redb")).unwrap());
        let journal = Journal::new(store.clone(), 20);
        let limits = BootstrapLimits::new(20, 1 << 20).unwrap();
        journal
            .prepare_history("source", &scope(2), true)
            .await
            .unwrap();
        journal
            .accept_history("source", &bootstrap(1), limits)
            .await
            .unwrap();
        let first = page(1, 1, 10, 11, vec![record(10, Some(1))]);
        let accepted = journal
            .accept_history("source", &first, limits)
            .await
            .unwrap();
        original_receipt = accepted.receipt.clone();
        assert_eq!(accepted.events.len(), 1);
        assert!(accepted.fresh);
        journal
            .accept_history("source", &bootstrap(2), limits)
            .await
            .unwrap();
        journal
            .accept_history("source", &page(2, 1, 10, 11, Vec::new()), limits)
            .await
            .unwrap();
        let before = dump(store.as_ref()).await;
        let retried = journal
            .accept_history("source", &first, limits)
            .await
            .unwrap();
        assert_eq!(retried.receipt, original_receipt);
        assert!(!retried.fresh);
        assert!(retried.events.is_empty());
        assert_eq!(dump(store.as_ref()).await, before);
        let conflicting = page(1, 1, 10, 11, vec![record(10, Some(2))]);
        assert!(journal
            .accept_history("source", &conflicting, limits)
            .await
            .is_err());
        assert_eq!(dump(store.as_ref()).await, before);
        assert!(journal
            .prepare_history("source", &scope(3), false)
            .await
            .is_err());
    }
    let store = Arc::new(RedbStateStoreProvider::new(workspace.0.join("state.redb")).unwrap());
    let journal = Journal::new(store.clone(), 20);
    let limits = BootstrapLimits::new(20, 1 << 20).unwrap();
    journal
        .prepare_history("source", &scope(2), false)
        .await
        .unwrap();
    let header = journal.history_header("source").await.unwrap();
    let progress = journal
        .history_progress("source", 1, &header)
        .await
        .unwrap();
    assert_eq!(progress.receipt, Some(original_receipt));
    let updated = journal
        .accept_history(
            "source",
            &page(1, 2, 11, 12, vec![record(11, Some(2))]),
            limits,
        )
        .await
        .unwrap();
    assert!(matches!(updated.events[0].1, SourceChange::Update { .. }));
    let deleted = journal
        .accept_history(
            "source",
            &page(1, 3, 12, 13, vec![record(12, None)]),
            limits,
        )
        .await
        .unwrap();
    assert!(matches!(deleted.events[0].1, SourceChange::Delete { .. }));
    assert!(journal
        .snapshot_at("source", 3, limits)
        .await
        .unwrap()
        .elements
        .is_empty());
}

#[tokio::test]
async fn invalid_history_pages_never_modify_real_storage() {
    let workspace = Workspace::new();
    let store = Arc::new(RedbStateStoreProvider::new(workspace.0.join("state.redb")).unwrap());
    let journal = Journal::new(store.clone(), 2);
    let limits = BootstrapLimits::new(20, 1 << 20).unwrap();
    journal
        .prepare_history("source", &scope(1), true)
        .await
        .unwrap();
    journal
        .accept_history("source", &bootstrap(1), limits)
        .await
        .unwrap();
    let mut foreign = record(10, Some(1));
    foreign.namespace_id = "00000000-0000-4000-8000-000000000002".into();
    let mut wrong_epoch = record(10, Some(1));
    wrong_epoch.capture_epoch = 8;
    let mut duplicate = record(10, Some(1));
    duplicate.state.push(duplicate.state[0].clone());
    let mut oversized = record(10, Some(1));
    oversized.state[0]
        .properties
        .insert("large".into(), json!("x".repeat(MAX_INPUT)));
    let mut future = record(10, Some(1));
    future.state[0].effective_from = u64::MAX;
    for records in [
        vec![foreign],
        vec![wrong_epoch],
        vec![duplicate],
        vec![oversized],
        vec![future],
        vec![record(11, Some(2)), record(10, Some(1))],
        vec![record(10, Some(1)), record(10, Some(2))],
    ] {
        let before = dump(store.as_ref()).await;
        assert!(journal
            .accept_history("source", &page(1, 1, 10, 12, records), limits)
            .await
            .is_err());
        assert_eq!(dump(store.as_ref()).await, before);
    }
    let before = dump(store.as_ref()).await;
    assert!(journal
        .accept_history("source", &page(1, 1, 11, 12, Vec::new()), limits)
        .await
        .is_err());
    assert!(journal
        .accept_history("source", &page(2, 1, 10, 12, Vec::new()), limits)
        .await
        .is_err());
    assert_eq!(dump(store.as_ref()).await, before);
    journal
        .accept_history(
            "source",
            &page(1, 1, 10, 13, vec![record(10, Some(1)), record(11, Some(2))]),
            limits,
        )
        .await
        .unwrap();
    let before = dump(store.as_ref()).await;
    assert!(journal
        .accept_history(
            "source",
            &page(1, 2, 13, 14, vec![record(13, Some(3))]),
            limits
        )
        .await
        .is_err());
    assert_eq!(dump(store.as_ref()).await, before);
}

#[derive(Clone, Copy)]
enum WriteFault {
    Before,
    After,
    WaitAfter,
}

struct FaultStore {
    inner: Arc<RedbStateStoreProvider>,
    fault: std::sync::Mutex<Option<WriteFault>>,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[async_trait]
impl StateStoreProvider for FaultStore {
    async fn get(&self, store: &str, key: &str) -> StateStoreResult<Option<Vec<u8>>> {
        self.inner.get(store, key).await
    }
    async fn set(&self, store: &str, key: &str, value: Vec<u8>) -> StateStoreResult<()> {
        self.inner.set(store, key, value).await
    }
    async fn delete(&self, store: &str, key: &str) -> StateStoreResult<bool> {
        self.inner.delete(store, key).await
    }
    async fn contains_key(&self, store: &str, key: &str) -> StateStoreResult<bool> {
        self.inner.contains_key(store, key).await
    }
    async fn get_many(
        &self,
        store: &str,
        keys: &[&str],
    ) -> StateStoreResult<HashMap<String, Vec<u8>>> {
        self.inner.get_many(store, keys).await
    }
    async fn set_many(&self, store: &str, entries: &[(&str, &[u8])]) -> StateStoreResult<()> {
        let fault = self.fault.lock().unwrap().take();
        if matches!(fault, Some(WriteFault::Before)) {
            return Err(StateStoreError::Other("injected write failure".into()));
        }
        self.inner.set_many(store, entries).await?;
        if matches!(fault, Some(WriteFault::WaitAfter)) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        if matches!(fault, Some(WriteFault::After)) {
            return Err(StateStoreError::Other(
                "injected lost write response".into(),
            ));
        }
        Ok(())
    }
    async fn delete_many(&self, store: &str, keys: &[&str]) -> StateStoreResult<usize> {
        self.inner.delete_many(store, keys).await
    }
    async fn clear_store(&self, store: &str) -> StateStoreResult<usize> {
        self.inner.clear_store(store).await
    }
    async fn list_keys(&self, store: &str) -> StateStoreResult<Vec<String>> {
        self.inner.list_keys(store).await
    }
    async fn store_exists(&self, store: &str) -> StateStoreResult<bool> {
        self.inner.store_exists(store).await
    }
    async fn key_count(&self, store: &str) -> StateStoreResult<usize> {
        self.inner.key_count(store).await
    }
    async fn sync(&self) -> StateStoreResult<()> {
        self.inner.sync().await
    }
    fn is_durable(&self) -> bool {
        self.inner.is_durable()
    }
}

#[tokio::test]
async fn ambiguous_acceptance_is_resolved_by_actual_progress_not_dispatch() {
    for fault in [WriteFault::Before, WriteFault::After] {
        let workspace = Workspace::new();
        let store = Arc::new(FaultStore {
            inner: Arc::new(RedbStateStoreProvider::new(workspace.0.join("state.redb")).unwrap()),
            fault: std::sync::Mutex::new(None),
            entered: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        let journal = Arc::new(Journal::new(store.clone(), 20));
        let source = TemporalSource::new(
            "source",
            journal.clone(),
            true,
            BootstrapLimits::new(20, 1 << 20).unwrap(),
        )
        .unwrap()
        .with_history(scope(1))
        .unwrap();
        source.prepare_history(true).await.unwrap();
        source.start().await.unwrap();
        source.accept_history(bootstrap(1)).await.unwrap();
        let before = dump(store.as_ref()).await;
        *store.fault.lock().unwrap() = Some(fault);
        assert!(source
            .accept_history(page(1, 1, 10, 11, vec![record(10, Some(1))]))
            .await
            .is_err());
        assert!(!source.input_ready().await);
        assert!(source.input_blocked().await);
        let progress = source.history_progress(1).await.unwrap();
        match fault {
            WriteFault::Before => {
                assert_eq!(progress.revision, 1);
                assert_eq!(dump(store.as_ref()).await, before);
            }
            WriteFault::After => {
                assert_eq!(progress.revision, 2);
                assert_eq!(progress.receipt.unwrap().head, 1);
                assert_eq!(journal.head_sequence("source").await.unwrap(), 1);
                assert_eq!(
                    journal
                        .snapshot_at("source", 1, source.bootstrap_limits)
                        .await
                        .unwrap()
                        .elements
                        .len(),
                    1
                );
            }
            WriteFault::WaitAfter => panic!("unexpected gate in immediate fault matrix"),
        }
        source.stop().await.unwrap();
    }
}

#[tokio::test]
async fn cancellation_after_acceptance_blocks_output_until_joined_recovery() {
    use std::future::Future;
    use std::task::Poll;

    let workspace = Workspace::new();
    let store = Arc::new(FaultStore {
        inner: Arc::new(RedbStateStoreProvider::new(workspace.0.join("state.redb")).unwrap()),
        fault: std::sync::Mutex::new(None),
        entered: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    let journal = Arc::new(Journal::new(store.clone(), 20));
    let source = TemporalSource::new(
        "source",
        journal.clone(),
        true,
        BootstrapLimits::new(20, 1 << 20).unwrap(),
    )
    .unwrap()
    .with_history(scope(1))
    .unwrap();
    source.prepare_history(true).await.unwrap();
    source.start().await.unwrap();
    source.accept_history(bootstrap(1)).await.unwrap();
    source
        .accept_history(page(1, 1, 10, 11, Vec::new()))
        .await
        .unwrap();
    assert!(source.input_ready().await);
    *store.fault.lock().unwrap() = Some(WriteFault::WaitAfter);
    let retained = source.shared();
    let acceptance = tokio::spawn(async move {
        retained
            .accept_history(page(1, 2, 11, 12, vec![record(11, Some(2))]))
            .await
    });
    store.entered.notified().await;
    {
        let stopping = source.stop();
        tokio::pin!(stopping);
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(stopping.as_mut().poll(cx).is_pending())).await
        );
        let observation = source.input_status();
        tokio::pin!(observation);
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(observation.as_mut().poll(cx).is_pending()))
                .await
        );
    }
    acceptance.abort();
    assert!(acceptance.await.unwrap_err().is_cancelled());
    assert!(matches!(source.input_status().await, InputStatus::Blocked));
    assert_eq!(source.history_progress(1).await.unwrap().revision, 3);
    assert_eq!(journal.head_sequence("source").await.unwrap(), 1);
    assert_eq!(
        journal
            .snapshot_at("source", 1, source.bootstrap_limits)
            .await
            .unwrap()
            .elements
            .len(),
        1
    );
    source.stop().await.unwrap();
    source.start().await.unwrap();
    assert!(!source.input_ready().await);
    source
        .accept_history(page(1, 3, 12, 12, Vec::new()))
        .await
        .unwrap();
    assert!(source.input_ready().await);
    source.stop().await.unwrap();
}

#[tokio::test]
async fn bootstrap_continuation_and_missing_control_fail_closed() {
    let workspace = Workspace::new();
    let store = Arc::new(RedbStateStoreProvider::new(workspace.0.join("state.redb")).unwrap());
    let journal = Arc::new(Journal::new(store.clone(), 20));
    let source = TemporalSource::new(
        "source",
        journal.clone(),
        true,
        BootstrapLimits::new(20, 1 << 20).unwrap(),
    )
    .unwrap()
    .with_history(scope(1))
    .unwrap();
    source.prepare_history(true).await.unwrap();
    source.start().await.unwrap();
    let mut first = bootstrap(1);
    if let HistoryPage::Bootstrap { next_token, .. } = &mut first.page {
        *next_token = "opaque-token".into();
    }
    source.accept_history(first).await.unwrap();
    let accepted = source.history_progress(1).await.unwrap();
    source.stop().await.unwrap();
    source.start().await.unwrap();
    assert_eq!(source.history_progress(1).await.unwrap(), accepted);
    assert!(!source.input_ready().await);
    let mut next = bootstrap(1);
    next.expected_revision = 1;
    next.request_id = uuid::Uuid::from_u128(2).to_string();
    source.accept_history(next).await.unwrap();
    source
        .accept_history(page(1, 2, 10, 11, vec![record(10, Some(1))]))
        .await
        .unwrap();
    assert!(source.input_ready().await);
    store
        .delete(&Journal::partition("source"), "history:shard:1")
        .await
        .unwrap();
    let before = dump(store.as_ref()).await;
    assert!(source.history_progress(1).await.is_err());
    assert!(matches!(source.input_status().await, InputStatus::Blocked));
    assert_eq!(dump(store.as_ref()).await, before);
    assert!(journal
        .prepare_history("source", &scope(1), false)
        .await
        .is_ok());
    assert!(source
        .accept_history(page(1, 3, 11, 11, Vec::new()))
        .await
        .is_err());
    source.stop().await.unwrap();
    store
        .delete(&Journal::partition("source"), HEADER)
        .await
        .unwrap();
    assert!(source.prepare_history(false).await.is_err());
}

#[tokio::test]
async fn managed_sources_gate_queries_and_revalidate_after_warm_and_cold_start() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    config.start_mode = StartMode::Dormant;
    let definition = NodeDefinition {
        activation: 1,
        lifetime: ResourceLifetime {
            namespace_id: scope(2).namespace_id,
            resource_id: "executions".into(),
            incarnation: "source-generation".into(),
        },
        recipe: NodeRecipe::HistorySource { scope: scope(2) },
    };
    let mut host = Host::open(config.clone()).await.unwrap();
    let admitted = host
        .apply(Command::AdmitNode {
            definition: definition.clone(),
        })
        .await
        .unwrap();
    assert_eq!(admitted["node"]["status"], "starting");
    let id = admitted["node"]["native_id"].as_str().unwrap().to_string();
    let query = NodeDefinition {
        activation: 1,
        lifetime: ResourceLifetime {
            namespace_id: scope(2).namespace_id,
            resource_id: "query".into(),
            incarnation: "query-generation".into(),
        },
        recipe: NodeRecipe::Query {
            language: Language::Cypher,
            text: "MATCH (w:Workflow) RETURN w.value AS value".into(),
            sources: vec![id.clone()],
        },
    };
    assert!(host
        .apply(Command::AdmitNode {
            definition: query.clone()
        })
        .await
        .is_err());
    for shard in 1..=2 {
        host.apply(Command::AcceptHistory {
            source_id: id.clone(),
            step: bootstrap(shard),
        })
        .await
        .unwrap();
        host.apply(Command::AcceptHistory {
            source_id: id.clone(),
            step: page(
                shard,
                1,
                10,
                11,
                if shard == 1 {
                    vec![record(10, Some(1))]
                } else {
                    Vec::new()
                },
            ),
        })
        .await
        .unwrap();
    }
    let admitted = host
        .apply(Command::AdmitNode {
            definition: query.clone(),
        })
        .await
        .unwrap();
    let query_id = admitted["node"]["native_id"].as_str().unwrap().to_string();
    let original = query_state(&host, &query_id, &[json!({"value": 1})]).await;
    host.apply(Command::Subscribe {
        subscription: Subscription {
            id: "watch".into(),
            query_id: query_id.clone(),
            query_revision: 1,
        },
    })
    .await
    .unwrap();
    assert_eq!(host.poll(1).await.unwrap().len(), 1);
    host.close().await.unwrap();
    drop(host);

    let mut host = Host::open(config).await.unwrap();
    host.apply(Command::AdmitNode {
        definition: definition.clone(),
    })
    .await
    .unwrap();
    assert!(host
        .apply(Command::AdmitNode {
            definition: query.clone()
        })
        .await
        .is_err());
    for shard in 1..=2 {
        host.apply(Command::AcceptHistory {
            source_id: id.clone(),
            step: page(
                shard,
                1,
                10,
                11,
                if shard == 1 {
                    vec![record(10, Some(1))]
                } else {
                    Vec::new()
                },
            ),
        })
        .await
        .unwrap();
    }
    assert!(
        host.apply(Command::AdmitNode {
            definition: query.clone()
        })
        .await
        .is_err(),
        "old immutable receipts cannot certify fresh upstream readiness"
    );
    for shard in 1..=2 {
        host.apply(Command::AcceptHistory {
            source_id: id.clone(),
            step: page(shard, 2, 11, 11, Vec::new()),
        })
        .await
        .unwrap();
    }
    host.apply(Command::AdmitNode {
        definition: query.clone(),
    })
    .await
    .unwrap();
    assert_eq!(
        query_state(&host, &query_id, &[json!({"value": 1})]).await,
        original
    );
    host.apply(Command::BlockHistory {
        source_id: id.clone(),
        problem: "capture gap".into(),
    })
    .await
    .unwrap();
    assert!(host.poll(1).await.is_err());
    assert!(host.apply(Command::Snapshot { query_id }).await.is_err());
    host.close().await.unwrap();
}
