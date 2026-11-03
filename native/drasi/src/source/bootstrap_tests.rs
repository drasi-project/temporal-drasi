use super::*;
use crate::{protocol::Relation, runtime::tests::Workspace};
use drasi_lib::state_store::{StateStoreError, StateStoreResult};
use drasi_state_store_redb::RedbStateStoreProvider;
use serde_json::{json, Value};
use std::collections::HashSet;

fn node(id: &str, kind: ChangeKind, labels: &[&str], properties: Value) -> Change {
    Change {
        kind,
        element_id: id.into(),
        labels: labels.iter().map(|label| (*label).into()).collect(),
        effective_from: 1,
        properties: serde_json::from_value(properties).unwrap(),
        relation: None,
    }
}

fn settings() -> SourceSubscriptionSettings {
    SourceSubscriptionSettings {
        source_id: "source".into(),
        query_id: "query".into(),
        enable_bootstrap: true,
        nodes: HashSet::new(),
        relations: HashSet::new(),
        resume_from: None,
        resume_sequence: None,
        request_position_handle: false,
    }
}

fn fixture() -> (Workspace, TemporalSource, Arc<RedbStateStoreProvider>) {
    let workspace = Workspace::new();
    let store = Arc::new(RedbStateStoreProvider::new(workspace.0.join("state.redb")).unwrap());
    let source = TemporalSource::new(
        "source",
        Arc::new(Journal::new(store.clone(), 100)),
        true,
        BootstrapLimits::new(100, 1 << 20).unwrap(),
    )
    .unwrap();
    (workspace, source, store)
}

async fn bootstrap(
    source: &TemporalSource,
    settings: SourceSubscriptionSettings,
) -> (Vec<SourceChange>, BootstrapResult) {
    let mut subscription = source.subscribe(settings).await.unwrap();
    let mut events = subscription.bootstrap_receiver.take().unwrap();
    let mut changes = Vec::new();
    while let Some(event) = events.recv().await {
        changes.push(event.change);
    }
    let boundary = subscription
        .bootstrap_result_receiver
        .take()
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(boundary.event_count, changes.len());
    (changes, boundary)
}

#[tokio::test]
async fn snapshot_folds_patches_and_deletions_before_filtering() {
    let (_workspace, source, _) = fixture();
    source.start().await.unwrap();
    let relation = Change {
        relation: Some(Relation {
            in_source: "source".into(),
            in_id: "z".into(),
            out_source: "another-source".into(),
            out_id: "remote".into(),
        }),
        ..node("a", ChangeKind::Insert, &["Links"], json!({}))
    };
    let changes = [
        node(
            "z",
            ChangeKind::Insert,
            &["Old"],
            json!({"retained": 1, "patched": 2}),
        ),
        node("removed", ChangeKind::Insert, &["Current"], json!({})),
        node(
            "z",
            ChangeKind::Update,
            &["Current"],
            json!({"patched": null}),
        ),
        node("removed", ChangeKind::Delete, &["Different"], json!({})),
        node("hidden", ChangeKind::Insert, &["Hidden"], json!({})),
        relation.clone(),
    ];
    for (index, change) in changes.into_iter().enumerate() {
        source.feed(index as u64 + 1, change).await.unwrap();
    }
    let (selected, boundary) = bootstrap(
        &source,
        SourceSubscriptionSettings {
            nodes: HashSet::from(["Current".into()]),
            relations: HashSet::from(["Links".into()]),
            ..settings()
        },
    )
    .await;
    assert_eq!(
        selected,
        vec![
            to_change(
                "source",
                node(
                    "z",
                    ChangeKind::Insert,
                    &["Current"],
                    json!({"retained": 1, "patched": null})
                )
            )
            .unwrap(),
            to_change("source", relation).unwrap(),
        ]
    );
    assert_eq!(
        boundary.source_position,
        Some(Bytes::copy_from_slice(&6u64.to_be_bytes()))
    );
    let (empty, _) = bootstrap(
        &source,
        SourceSubscriptionSettings {
            nodes: HashSet::from(["Old".into()]),
            relations: HashSet::from(["Missing".into()]),
            ..settings()
        },
    )
    .await;
    assert!(empty.is_empty());
    let (all, _) = bootstrap(&source, settings()).await;
    assert_eq!(all.len(), 3);
    source.stop().await.unwrap();
}

#[tokio::test]
async fn empty_projection_keeps_an_exact_bootstrap_boundary() {
    let (_workspace, source, _) = fixture();
    source.start().await.unwrap();
    let (empty, boundary) = bootstrap(&source, settings()).await;
    assert!(empty.is_empty());
    assert_eq!(
        boundary.source_position,
        Some(Bytes::copy_from_slice(&0u64.to_be_bytes()))
    );
    for (position, kind) in [(1, ChangeKind::Insert), (2, ChangeKind::Delete)] {
        source
            .feed(position, node("item", kind, &["Item"], json!({})))
            .await
            .unwrap();
    }
    let (empty, boundary) = bootstrap(&source, settings()).await;
    assert!(empty.is_empty());
    assert_eq!(
        boundary.source_position,
        Some(Bytes::copy_from_slice(&2u64.to_be_bytes()))
    );
    source.stop().await.unwrap();
}

#[test]
fn projection_limits_account_for_replacements_and_deletions() {
    let first = to_change(
        "source",
        node("first", ChangeKind::Insert, &["Item"], json!({"value": 1})),
    )
    .unwrap();
    let second = to_change(
        "source",
        node("second", ChangeKind::Insert, &["Item"], json!({"value": 2})),
    )
    .unwrap();
    let encoded = |change: &SourceChange| match change {
        SourceChange::Insert { element } => serde_json::to_vec(element).unwrap().len(),
        _ => unreachable!(),
    };
    let exact = encoded(&first) + encoded(&second);
    let mut projection = Projection::default();
    let limits = BootstrapLimits::new(2, exact).unwrap();
    projection.apply(first.clone(), limits).unwrap();
    projection.apply(second.clone(), limits).unwrap();
    assert_eq!(projection.bytes, exact);
    assert_eq!(projection.elements.len(), 2);
    projection.apply(first.clone(), limits).unwrap();
    assert_eq!(projection.bytes, exact);
    let delete = to_change("source", node("second", ChangeKind::Delete, &[], json!({}))).unwrap();
    projection.apply(delete, limits).unwrap();
    assert_eq!(projection.bytes, encoded(&first));
    for (limits, expected) in [
        (BootstrapLimits::new(2, exact - 1).unwrap(), "byte limit"),
        (BootstrapLimits::new(1, exact).unwrap(), "element limit"),
    ] {
        let mut projection = Projection::default();
        projection.apply(first.clone(), limits).unwrap();
        assert!(projection
            .apply(second.clone(), limits)
            .unwrap_err()
            .to_string()
            .contains(expected));
        assert_eq!(projection.elements.len(), 1);
        assert_eq!(projection.bytes, encoded(&first));
    }
}

#[test]
fn projection_upserts_updates_and_replaces_inserts_without_kind_panics() {
    let mut projection = Projection::default();
    let limits = BootstrapLimits::new(1, 1 << 20).unwrap();
    projection
        .apply(
            to_change(
                "source",
                node("item", ChangeKind::Update, &["Item"], json!({"old": 1})),
            )
            .unwrap(),
            limits,
        )
        .unwrap();
    let replacement = to_change(
        "source",
        node("item", ChangeKind::Insert, &["New"], json!({"new": 2})),
    )
    .unwrap();
    projection.apply(replacement.clone(), limits).unwrap();
    let SourceChange::Insert { element } = replacement else {
        unreachable!()
    };
    assert_eq!(
        projection.elements.get(element.get_reference()).unwrap().0,
        element
    );
    let relation = Change {
        relation: Some(Relation {
            in_source: "source".into(),
            in_id: "item".into(),
            out_source: "source".into(),
            out_id: "target".into(),
        }),
        ..node("item", ChangeKind::Update, &["Link"], json!({}))
    };
    let error = projection
        .apply(to_change("source", relation.clone()).unwrap(), limits)
        .unwrap_err();
    assert!(error.to_string().contains("changes an element's kind"));
    assert_eq!(
        projection.elements.get(element.get_reference()).unwrap().0,
        element
    );
    projection
        .apply(
            to_change(
                "source",
                Change {
                    kind: ChangeKind::Insert,
                    ..relation
                },
            )
            .unwrap(),
            limits,
        )
        .unwrap();
    assert_eq!(projection.elements.len(), 1);
    assert!(matches!(
        projection.elements.values().next().unwrap().0,
        Element::Relation { .. }
    ));
    projection
        .apply(
            to_change(
                "source",
                node("item", ChangeKind::Delete, &["Different"], json!({})),
            )
            .unwrap(),
            limits,
        )
        .unwrap();
    assert!(projection.elements.is_empty());
    assert_eq!(projection.bytes, 0);
}

#[tokio::test]
async fn source_bootstrap_bounds_precede_label_selection_and_registration() {
    let (_workspace, mut source, _) = fixture();
    source.bootstrap_limits = BootstrapLimits::new(1, 1 << 20).unwrap();
    source.start().await.unwrap();
    for (position, label) in [(1, "Selected"), (2, "Unrelated")] {
        source
            .feed(
                position,
                node(label, ChangeKind::Insert, &[label], json!({})),
            )
            .await
            .unwrap();
    }
    let error = source
        .subscribe(SourceSubscriptionSettings {
            nodes: HashSet::from(["Selected".into()]),
            ..settings()
        })
        .await
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("element limit"));
    let registered = source.base.wait_for_subscribers();
    tokio::pin!(registered);
    assert!(futures_util::poll!(registered.as_mut()).is_pending());
    source.stop().await.unwrap();
}

#[tokio::test]
async fn corrupt_journal_does_not_register_or_rewrite_source_state() {
    for (name, entry) in [
        ("missing", None),
        ("corrupt", Some(b"not-json".to_vec())),
        ("oversized", Some(vec![b'x'; (1 << 20) + 1])),
        (
            "foreign source",
            Some(
                serde_json::to_vec(
                    &to_change(
                        "foreign",
                        node("item", ChangeKind::Insert, &["Item"], json!({})),
                    )
                    .unwrap(),
                )
                .unwrap(),
            ),
        ),
    ] {
        let (_workspace, source, store) = fixture();
        let head = 1u64.to_be_bytes();
        let key = format!("{:020}", 1);
        let mut writes: Vec<(&str, &[u8])> = vec![("head", &head)];
        if let Some(bytes) = &entry {
            writes.push((&key, bytes));
        }
        store
            .set_many(&Journal::partition("source"), &writes)
            .await
            .unwrap();
        source.start().await.unwrap();
        assert!(source.subscribe(settings()).await.is_err(), "{name}");
        assert_eq!(
            store
                .get(&Journal::partition("source"), "head")
                .await
                .unwrap(),
            Some(head.to_vec()),
            "{name}"
        );
        assert_eq!(
            store
                .get(&Journal::partition("source"), &key)
                .await
                .unwrap(),
            entry,
            "{name}"
        );
        let registered = source.base.wait_for_subscribers();
        tokio::pin!(registered);
        assert!(
            futures_util::poll!(registered.as_mut()).is_pending(),
            "{name}"
        );
        source.stop().await.unwrap();
    }
}

#[tokio::test]
async fn opaque_bootstrap_checkpoint_wins_over_zero_sequence_without_losing_other_retries() {
    let (_workspace, source, _) = fixture();
    source.start().await.unwrap();
    let initial = node("item", ChangeKind::Insert, &["Item"], json!({"value": 1}));
    source.feed(1, initial.clone()).await.unwrap();
    let mut recovered = source
        .subscribe(SourceSubscriptionSettings {
            resume_from: Some(Bytes::copy_from_slice(&1u64.to_be_bytes())),
            resume_sequence: Some(0),
            ..settings()
        })
        .await
        .unwrap();
    assert!(recovered.bootstrap_receiver.is_none());
    assert!(recovered.bootstrap_result_receiver.is_none());
    let mut earlier = source
        .subscribe(SourceSubscriptionSettings {
            query_id: "earlier".into(),
            resume_sequence: Some(0),
            ..settings()
        })
        .await
        .unwrap();
    source.feed(1, initial).await.unwrap();
    source
        .feed(
            2,
            node("item", ChangeKind::Update, &["Item"], json!({"value": 2})),
        )
        .await
        .unwrap();
    assert_eq!(recovered.receiver.recv().await.unwrap().sequence, 2);
    for sequence in [1, 1, 2] {
        assert_eq!(earlier.receiver.recv().await.unwrap().sequence, sequence);
    }
    for position in [
        Bytes::from_static(b"bad"),
        Bytes::copy_from_slice(&3u64.to_be_bytes()),
    ] {
        assert!(source
            .subscribe(SourceSubscriptionSettings {
                resume_from: Some(position),
                resume_sequence: Some(0),
                ..settings()
            })
            .await
            .is_err());
    }
    source.stop().await.unwrap();
}

#[tokio::test]
async fn invalid_journal_heads_do_not_fall_back_to_empty_bootstrap() {
    for head in [vec![1], 101u64.to_be_bytes().to_vec()] {
        let (_workspace, source, store) = fixture();
        store
            .set(&Journal::partition("source"), "head", head.clone())
            .await
            .unwrap();
        source.start().await.unwrap();
        assert!(source.subscribe(settings()).await.is_err());
        assert_eq!(
            store
                .get(&Journal::partition("source"), "head")
                .await
                .unwrap(),
            Some(head)
        );
        source.stop().await.unwrap();
    }
}

struct PausedReadStore {
    inner: Arc<RedbStateStoreProvider>,
    entered: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    resume: tokio::sync::Notify,
}

#[async_trait]
impl StateStoreProvider for PausedReadStore {
    async fn get(&self, store: &str, key: &str) -> StateStoreResult<Option<Vec<u8>>> {
        if store == Journal::partition("source") && key == "00000000000000000001" {
            let entered = self.entered.lock().await.take();
            if let Some(entered) = entered {
                entered
                    .send(())
                    .map_err(|_| StateStoreError::Other("snapshot observer dropped".into()))?;
                self.resume.notified().await;
            }
        }
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
        self.inner.set_many(store, entries).await
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
async fn snapshot_read_fences_concurrent_feed_and_stop_until_live_registration() {
    let (_workspace, original, inner) = fixture();
    original
        .journal
        .put(
            "source",
            1,
            &to_change(
                "source",
                node("item", ChangeKind::Insert, &["Item"], json!({"value": 1})),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let (entered, observed) = tokio::sync::oneshot::channel();
    let store = Arc::new(PausedReadStore {
        inner,
        entered: tokio::sync::Mutex::new(Some(entered)),
        resume: tokio::sync::Notify::new(),
    });
    let source = TemporalSource::new(
        "source",
        Arc::new(Journal::new(store.clone(), 100)),
        true,
        BootstrapLimits::new(100, 1 << 20).unwrap(),
    )
    .unwrap();
    source.start().await.unwrap();
    let subscription = source.subscribe(settings());
    tokio::pin!(subscription);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::select! {
            result = &mut subscription => panic!("subscription escaped paused snapshot read: {}", result.is_ok()),
            result = observed => result.unwrap(),
        }
    }).await.expect("snapshot read did not reach its deterministic pause");
    let feeding = source.feed(
        2,
        node("item", ChangeKind::Update, &["Item"], json!({"value": 2})),
    );
    let stopping = source.stop();
    tokio::pin!(feeding, stopping);
    assert!(futures_util::poll!(feeding.as_mut()).is_pending());
    assert!(futures_util::poll!(stopping.as_mut()).is_pending());
    store.resume.notify_one();
    let mut subscription = subscription.await.unwrap();
    feeding.await.unwrap();
    stopping.await.unwrap();
    let mut image = subscription.bootstrap_receiver.take().unwrap();
    assert_eq!(
        image.recv().await.unwrap().change,
        to_change(
            "source",
            node("item", ChangeKind::Insert, &["Item"], json!({"value": 1}))
        )
        .unwrap()
    );
    assert!(image.recv().await.is_none());
    let boundary = subscription
        .bootstrap_result_receiver
        .take()
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        boundary.source_position,
        Some(Bytes::copy_from_slice(&1u64.to_be_bytes()))
    );
    assert_eq!(subscription.receiver.recv().await.unwrap().sequence, 2);
    assert_eq!(source.status().await, ComponentStatus::Stopped);
}
