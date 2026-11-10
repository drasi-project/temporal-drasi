use serde_json::{json, Value};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use temporal_drasi::{protocol::*, runtime::Host};

struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(".verification")
            .join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self, name: &str) -> String {
        self.0.join(name).to_str().unwrap().into()
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn config(directory: String) -> Config {
    Config {
        version: VERSION,
        state_dir: directory,
        owner_epoch: 1,
        instance_id: "native-test".into(),
        start_mode: StartMode::Automatic,
        restore_from: None,
        sources: vec![SourceConfig {
            id: "feed".into(),
            kind: SourceKind::Temporal,
            plugin: None,
            plugin_kind: None,
            config: Value::Null,
            bootstrap: None,
        }],
        queries: vec![QueryConfig {
            id: "q".into(),
            revision: 1,
            language: Language::Cypher,
            text:
                "MATCH (n:Item) WHERE n.amount >= 10 RETURN n.name AS name, n.amount * 2 AS doubled"
                    .into(),
            sources: vec!["feed".into()],
        }],
        plugins: Vec::new(),
        catalogs: Vec::new(),
        managed_plugins: Vec::new(),
        limits: Limits::default(),
    }
}

fn feed(position: u64, kind: ChangeKind, amount: i64) -> Command {
    Command::Feed {
        source_id: "feed".into(),
        position,
        change: Change {
            kind,
            element_id: "item-1".into(),
            labels: vec!["Item".into()],
            effective_from: position,
            properties: BTreeMap::from([
                ("name".into(), json!("alpha")),
                ("amount".into(), json!(amount)),
            ]),
            relation: None,
        },
    }
}

async fn snapshot(host: &mut Host, expected: Value) -> Event {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result = host
                .apply(Command::Snapshot {
                    query_id: "q".into(),
                })
                .await
                .unwrap();
            let event: Event = serde_json::from_value(result["snapshot"].clone()).unwrap();
            let rows: Vec<_> = event.rows.iter().map(|row| row.value.clone()).collect();
            if json!(rows) == expected {
                return event;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("query did not evaluate the committed source change")
}

async fn next(host: &Host) -> Event {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let events = host.poll(1).await.unwrap();
            if let Some(event) = events.into_iter().next() {
                return event;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("query did not publish its durable result")
}

fn receipt(event: &Event, accepted: &str) -> Receipt {
    Receipt {
        subscription_id: event.subscription_id.clone(),
        query_revision: event.query_revision,
        incarnation: event.incarnation.clone(),
        sequence: event.sequence,
        kind: event.kind.clone(),
        acceptance_id: accepted.into(),
    }
}

async fn subscribe(host: &mut Host, id: &str) -> Event {
    host.apply(Command::Subscribe {
        subscription: Subscription {
            id: id.into(),
            query_id: "q".into(),
            query_revision: 1,
        },
    })
    .await
    .unwrap();
    next(host).await
}

#[tokio::test]
async fn evaluates_real_query_and_replays_unaccepted_output_after_restart() {
    let workspace = Workspace::new();
    let configuration = config(workspace.path("owner-1"));
    let mut host = Host::open(configuration.clone()).await.unwrap();
    let initial = subscribe(&mut host, "watch-1").await;
    assert_eq!(initial.kind, EventKind::Snapshot);
    assert!(initial.rows.is_empty());
    host.ack(receipt(&initial, "initial-commit")).await.unwrap();

    host.apply(feed(1, ChangeKind::Insert, 12)).await.unwrap();
    let expected = json!([{"name":"alpha","doubled":24}]);
    snapshot(&mut host, expected.clone()).await;
    let event = next(&host).await;
    assert_eq!(event.kind, EventKind::Delta);
    assert_eq!(event.sequence, initial.sequence + 1);
    assert!(
        matches!(event.changes.as_slice(), [OutputDiff::Add { data, .. }] if data == &expected[0])
    );
    assert_eq!(
        serde_json::to_value(next(&host).await).unwrap(),
        serde_json::to_value(&event).unwrap()
    );
    host.close().await.unwrap();
    drop(host);

    let mut host = Host::open(configuration.clone()).await.unwrap();
    snapshot(&mut host, expected).await;
    assert_eq!(
        serde_json::to_value(next(&host).await).unwrap(),
        serde_json::to_value(&event).unwrap()
    );
    let mut skipped = receipt(&event, "bad-commit");
    skipped.sequence += 1;
    assert_eq!(host.ack(skipped).await.unwrap_err().code, 6);
    let accepted = receipt(&event, "durable-commit");
    host.ack(accepted.clone()).await.unwrap();
    host.ack(accepted).await.unwrap();
    assert!(host.poll(1).await.unwrap().is_empty());

    host.apply(feed(1, ChangeKind::Insert, 12)).await.unwrap();
    host.apply(feed(2, ChangeKind::Update, 17)).await.unwrap();
    snapshot(&mut host, json!([{"name":"alpha","doubled":34}])).await;
    let updated = next(&host).await;
    assert_eq!(updated.sequence, event.sequence + 1);
    assert!(matches!(
        updated.changes.as_slice(),
        [OutputDiff::Update { .. }]
    ));
    host.ack(receipt(&updated, "updated-commit")).await.unwrap();

    host.apply(feed(3, ChangeKind::Update, 1)).await.unwrap();
    snapshot(&mut host, json!([])).await;
    let deleted = next(&host).await;
    assert!(matches!(
        deleted.changes.as_slice(),
        [OutputDiff::Delete { .. }]
    ));
    assert_eq!(
        host.apply(feed(5, ChangeKind::Insert, 11))
            .await
            .unwrap_err()
            .code,
        6
    );
    assert_eq!(
        host.apply(feed(1, ChangeKind::Insert, 14))
            .await
            .unwrap_err()
            .code,
        3
    );
    assert!(Host::open(configuration).await.is_err());
    host.close().await.unwrap();
}

#[tokio::test]
async fn checkpoint_restores_query_source_and_pending_receipt_to_isolated_owner() {
    let workspace = Workspace::new();
    let configuration = config(workspace.path("owner-1"));
    let mut host = Host::open(configuration.clone()).await.unwrap();
    host.apply(feed(1, ChangeKind::Insert, 21)).await.unwrap();
    let expected = json!([{"name":"alpha","doubled":42}]);
    snapshot(&mut host, expected.clone()).await;
    let initial = subscribe(&mut host, "watch-1").await;
    assert_eq!(initial.rows[0].value, expected[0]);
    let destination = workspace.path("checkpoint");
    let result = host.checkpoint(&destination).await.unwrap();
    assert!(result["checkpoint"]["files"]["state.redb"].is_string());
    assert!(host.poll(1).await.is_err());
    drop(host);

    let mut restored_config = configuration;
    restored_config.owner_epoch = 2;
    restored_config.state_dir = workspace.path("owner-2");
    restored_config.restore_from = Some(destination);
    let mut restored = Host::open(restored_config).await.unwrap();
    snapshot(&mut restored, expected).await;
    assert_eq!(
        serde_json::to_value(next(&restored).await).unwrap(),
        serde_json::to_value(&initial).unwrap()
    );
    restored
        .ack(receipt(&initial, "restored-commit"))
        .await
        .unwrap();
    restored
        .apply(feed(2, ChangeKind::Update, 22))
        .await
        .unwrap();
    snapshot(&mut restored, json!([{"name":"alpha","doubled":44}])).await;
    restored.close().await.unwrap();
}

#[tokio::test]
async fn each_watch_has_its_own_durable_snapshot_boundary_and_receipts() {
    let workspace = Workspace::new();
    let mut host = Host::open(config(workspace.path("owner"))).await.unwrap();
    let first = subscribe(&mut host, "watch-1").await;
    host.ack(receipt(&first, "initial-1")).await.unwrap();
    host.apply(feed(1, ChangeKind::Insert, 13)).await.unwrap();
    snapshot(&mut host, json!([{"name":"alpha","doubled":26}])).await;
    host.apply(Command::Subscribe {
        subscription: Subscription {
            id: "watch-2".into(),
            query_id: "q".into(),
            query_revision: 1,
        },
    })
    .await
    .unwrap();
    let pending = host.poll(2).await.unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].kind, EventKind::Delta);
    assert_eq!(pending[1].kind, EventKind::Snapshot);
    assert_eq!(pending[0].sequence, pending[1].sequence);
    host.ack(receipt(&pending[0], "accept-watch-1"))
        .await
        .unwrap();
    let remaining = host.poll(2).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].subscription_id, "watch-2");
    host.close().await.unwrap();
}

#[tokio::test]
async fn outbox_gaps_do_not_implicitly_reset_or_skip() {
    let workspace = Workspace::new();
    let mut configuration = config(workspace.path("owner"));
    configuration.limits.outbox_entries = 1;
    let mut host = Host::open(configuration).await.unwrap();
    let initial = subscribe(&mut host, "watch-1").await;
    host.ack(receipt(&initial, "initial")).await.unwrap();
    for (position, amount) in [(1, 11), (2, 12), (3, 13)] {
        host.apply(feed(
            position,
            if position == 1 {
                ChangeKind::Insert
            } else {
                ChangeKind::Update
            },
            amount,
        ))
        .await
        .unwrap();
        snapshot(&mut host, json!([{"name":"alpha","doubled":amount * 2}])).await;
    }
    assert_eq!(host.poll(1).await.unwrap_err().code, 6);
    host.close().await.unwrap();
}

#[tokio::test]
async fn retained_results_obey_row_and_byte_limits_without_advancing_output() {
    for (row_limit, byte_limit, name) in [
        (1, Limits::default().pending_bytes, "alpha".to_string()),
        (100, 1024, "x".repeat(600)),
    ] {
        let workspace = Workspace::new();
        let mut configuration = config(workspace.path("owner"));
        configuration.limits.snapshot_rows = row_limit;
        configuration.limits.pending_bytes = byte_limit;
        let mut host = Host::open(configuration).await.unwrap();
        let initial = subscribe(&mut host, "watch-1").await;
        host.ack(receipt(&initial, "initial")).await.unwrap();

        let mut first = feed(1, ChangeKind::Insert, 11);
        if let Command::Feed { change, .. } = &mut first {
            change.properties.insert("name".into(), json!(name));
        }
        host.apply(first).await.unwrap();
        snapshot(&mut host, json!([{"name": name, "doubled": 22}])).await;
        let accepted = next(&host).await;
        host.ack(receipt(&accepted, "first")).await.unwrap();

        let mut second = feed(2, ChangeKind::Insert, 11);
        if let Command::Feed { change, .. } = &mut second {
            change.element_id = "item-2".into();
            change.properties.insert("name".into(), json!(name));
        }
        host.apply(second).await.unwrap();
        for _ in 0..2 {
            let error = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    match host.poll(1).await {
                        Err(error) => return error,
                        Ok(events) => {
                            assert!(events.is_empty());
                            tokio::task::yield_now().await;
                        }
                    }
                }
            })
            .await
            .unwrap();
            assert_eq!(error.code, 3);
            assert_eq!(
                error.message,
                "subscription results exceed configured limits"
            );
        }
        host.close().await.unwrap();
    }
}
