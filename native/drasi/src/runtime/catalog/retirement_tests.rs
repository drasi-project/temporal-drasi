use super::*;
use crate::runtime::{admission_tests::*, tests::*};
use drasi_lib::queries::output_state::{FetchError, OutboxResponse, SnapshotResponse};
use tokio::sync::Notify;

struct DelayedSnapshot {
    query: Arc<dyn drasi_lib::queries::Query>,
    captured: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait::async_trait]
impl drasi_lib::queries::Query for DelayedSnapshot {
    async fn start(&self) -> anyhow::Result<()> {
        self.query.start().await
    }
    async fn stop(&self) -> anyhow::Result<()> {
        self.query.stop().await
    }
    async fn status(&self) -> ComponentStatus {
        self.query.status().await
    }
    fn get_config(&self) -> &drasi_lib::config::QueryConfig {
        self.query.get_config()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    async fn subscribe(
        &self,
        id: String,
    ) -> anyhow::Result<drasi_lib::channels::QuerySubscriptionResponse> {
        self.query.subscribe(id).await
    }
    async fn fetch_snapshot(&self) -> std::result::Result<SnapshotResponse, FetchError> {
        let snapshot = self.query.fetch_snapshot().await?;
        self.captured.notify_one();
        self.release.notified().await;
        Ok(snapshot)
    }
    async fn fetch_outbox(&self, after: u64) -> std::result::Result<OutboxResponse, FetchError> {
        self.query.fetch_outbox(after).await
    }
    async fn output_generation(&self) -> u64 {
        self.query.output_generation().await
    }
    fn is_volatile(&self) -> bool {
        self.query.is_volatile()
    }
    async fn release_persistent_handles(&self) {
        self.query.release_persistent_handles().await;
    }
    async fn prepare_for_removal_preserving_state(&self) -> anyhow::Result<()> {
        self.query.prepare_for_removal_preserving_state().await
    }
}

async fn retire(host: &Host, id: &str) -> Result<Value> {
    host.apply(serde_json::from_value(
        json!({"op": "retire_node", "native_id": id}),
    )?)
    .await
}

#[tokio::test]
async fn retired_queries_release_runtime_and_refuse_stale_readmission() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    config.start_mode = StartMode::Dormant;
    let mut host = Host::open(config.clone()).await.unwrap();
    let source = admit(&host, &source_definition()).await.unwrap();
    let definition = query_definition(source["native_id"].as_str().unwrap(), "query", "3");
    let query = admit(&host, &definition).await.unwrap();
    let id = query["native_id"].as_str().unwrap();
    stop_node(&host, id).await;
    let result = retire(&host, id).await.unwrap();
    assert_eq!(result["node"]["status"], "retired");
    assert!(!host
        .core()
        .unwrap()
        .get_graph()
        .await
        .nodes
        .iter()
        .any(|node| node.id == id));
    assert!(admit(&host, &definition)
        .await
        .unwrap_err()
        .message
        .contains("retired"));
    host.close().await.unwrap();
    drop(host);
    let mut reopened = Host::open(config).await.unwrap();
    assert!(!reopened
        .core()
        .unwrap()
        .get_graph()
        .await
        .nodes
        .iter()
        .any(|node| node.id == id));
    assert!(admit(&reopened, &definition)
        .await
        .unwrap_err()
        .message
        .contains("retired"));
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn retirement_refuses_running_bindings_and_retained_dependents_without_mutation() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    config.start_mode = StartMode::Dormant;
    let mut host = Host::open(config.clone()).await.unwrap();
    let source = admit(&host, &source_definition()).await.unwrap();
    let source_id = source["native_id"].as_str().unwrap();
    let definition = query_definition(source_id, "query", "3");
    let query = admit(&host, &definition).await.unwrap();
    let id = query["native_id"].as_str().unwrap();
    let binding = WatchBinding {
        watch: ResourceLifetime {
            namespace_id: "00000000-0000-4000-8000-000000000001".into(),
            resource_id: "watch".into(),
            incarnation: "4".into(),
        },
        watch_revision: 4,
        query: serde_json::from_value(definition["lifetime"].clone()).unwrap(),
        query_revision: 3,
    };
    let subscribed = host
        .apply(Command::BindWatch {
            binding,
            query_id: id.into(),
        })
        .await
        .unwrap();
    let subscription = subscribed["subscription"]["definition"]["id"]
        .as_str()
        .unwrap();
    let catalog = host.store().unwrap().get(CONTROL, "catalog").await.unwrap();
    let pending = host
        .store()
        .unwrap()
        .get(SUBSCRIPTIONS, subscription)
        .await
        .unwrap();
    assert!(retire(&host, id).await.is_err());
    assert_eq!(
        host.store().unwrap().get(CONTROL, "catalog").await.unwrap(),
        catalog
    );
    stop_node(&host, id).await;
    assert!(retire(&host, id).await.is_err());
    assert_eq!(
        host.store().unwrap().get(CONTROL, "catalog").await.unwrap(),
        catalog
    );
    assert_eq!(
        host.store()
            .unwrap()
            .get(SUBSCRIPTIONS, subscription)
            .await
            .unwrap(),
        pending
    );
    stop_node(&host, source_id).await;
    assert!(retire(&host, source_id).await.is_err());
    assert_eq!(
        host.store().unwrap().get(CONTROL, "catalog").await.unwrap(),
        catalog
    );
    host.apply(Command::Unsubscribe {
        subscription_id: subscription.into(),
    })
    .await
    .unwrap();
    let retired = retire(&host, id).await.unwrap();
    let catalog = host.store().unwrap().get(CONTROL, "catalog").await.unwrap();
    assert_eq!(retire(&host, id).await.unwrap(), retired);
    assert_eq!(
        host.store().unwrap().get(CONTROL, "catalog").await.unwrap(),
        catalog
    );
    assert_eq!(
        retire(&host, source_id).await.unwrap()["node"]["status"],
        "retired"
    );
    let closed = host
        .store()
        .unwrap()
        .get(SUBSCRIPTIONS, subscription)
        .await
        .unwrap();
    host.close().await.unwrap();
    drop(host);
    let mut reopened = Host::open(config).await.unwrap();
    assert_eq!(
        reopened
            .store()
            .unwrap()
            .get(SUBSCRIPTIONS, subscription)
            .await
            .unwrap(),
        closed
    );
    let subscriptions = reopened
        .apply(Command::ListSubscriptions {
            after: String::new(),
            limit: 64,
        })
        .await
        .unwrap();
    assert_eq!(subscriptions["subscriptions"][0]["deleted"], true);
    assert!(!reopened
        .core()
        .unwrap()
        .get_graph()
        .await
        .nodes
        .iter()
        .any(|node| node.id == id || node.id == source_id));
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn retiring_one_query_preserves_unrelated_pending_output_and_successor_replay() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    config.start_mode = StartMode::Dormant;
    let mut host = Host::open(config.clone()).await.unwrap();
    let source = admit(&host, &source_definition()).await.unwrap();
    let source_id = source["native_id"].as_str().unwrap();
    let healthy_definition = query_definition(source_id, "healthy", "3");
    let healthy = admit(&host, &healthy_definition).await.unwrap();
    let healthy_id = healthy["native_id"].as_str().unwrap();
    let expired_definition = query_definition(source_id, "expired", "4");
    let expired = admit(&host, &expired_definition).await.unwrap();
    let expired_id = expired["native_id"].as_str().unwrap();
    let initial = attach_query(&host, "pending", healthy_id).await;
    accept(&host, &initial).await;
    feed_source(&host, source_id, 1, 10).await;
    query_state(&host, healthy_id, &[json!({"amount": 10})]).await;
    let output = pending(&host, "pending").await;
    let bytes = serde_json::to_vec(&output).unwrap();
    stop_node(&host, expired_id).await;
    retire(&host, expired_id).await.unwrap();
    assert_eq!(
        serde_json::to_vec(&pending(&host, "pending").await).unwrap(),
        bytes
    );
    let checkpoint = workspace.0.join("retirement-cut");
    host.checkpoint(checkpoint.to_str().unwrap()).await.unwrap();
    drop(host);
    config.owner_epoch += 1;
    config.state_dir = workspace.0.join("successor").to_str().unwrap().into();
    config.restore_from = Some(checkpoint.to_str().unwrap().into());
    let mut successor = Host::open(config).await.unwrap();
    assert_eq!(
        serde_json::to_vec(&pending(&successor, "pending").await).unwrap(),
        bytes
    );
    assert!(admit(&successor, &expired_definition).await.is_err());
    admit(&successor, &source_definition()).await.unwrap();
    admit(&successor, &healthy_definition).await.unwrap();
    feed_source(&successor, source_id, 2, 20).await;
    query_state(&successor, healthy_id, &[json!({"amount": 20})]).await;
    accept(&successor, &output).await;
    let next = pending(&successor, "pending").await;
    assert_eq!(next.sequence, output.sequence + 1);
    assert_eq!(next.incarnation, output.incarnation);
    successor.close().await.unwrap();
}

#[tokio::test]
async fn subscribe_cannot_commit_a_snapshot_captured_before_retirement() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    config.start_mode = StartMode::Dormant;
    let host = Arc::new(Host::open(config).await.unwrap());
    let source = admit(&host, &source_definition()).await.unwrap();
    let definition = query_definition(source["native_id"].as_str().unwrap(), "query", "3");
    let node = admit(&host, &definition).await.unwrap();
    let id = node["native_id"].as_str().unwrap();
    query_state(&host, id, &[]).await;
    let captured = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let query = host
        .core()
        .unwrap()
        .query_manager()
        .get_query_instance(id)
        .await
        .unwrap();
    let delayed: Arc<dyn drasi_lib::queries::Query> = Arc::new(DelayedSnapshot {
        query,
        captured: captured.clone(),
        release: release.clone(),
    });
    host.core()
        .unwrap()
        .component_graph()
        .write()
        .await
        .set_runtime(id, Box::new(delayed))
        .unwrap();
    let waiter = host.clone();
    let subscription = Subscription {
        id: "late".into(),
        query_id: id.into(),
        query_revision: 1,
    };
    let pending =
        tokio::spawn(async move { waiter.apply(Command::Subscribe { subscription }).await });
    tokio::time::timeout(std::time::Duration::from_secs(10), captured.notified())
        .await
        .unwrap();
    stop_node(&host, id).await;
    retire(&host, id).await.unwrap();
    release.notify_one();
    assert!(pending.await.unwrap().is_err());
    assert_eq!(
        host.store()
            .unwrap()
            .get(SUBSCRIPTIONS, "late")
            .await
            .unwrap(),
        None
    );
    let mut host =
        Arc::try_unwrap(host).unwrap_or_else(|_| panic!("subscription retained the host"));
    host.close().await.unwrap();
}

#[test]
fn only_completed_retirement_reclaims_exact_catalog_capacity() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    for query_nodes in [false, true] {
        let mut ledger = AdmissionLedger::default();
        let source: NodeDefinition = serde_json::from_value(source_definition()).unwrap();
        let source_id = ledger.insert(source, &config).unwrap();
        ledger.nodes.get_mut(&source_id).unwrap().phase = Phase::Provisioned;
        let limit = if query_nodes { 256 } else { 64 };
        let begin = if query_nodes { 0 } else { 1 };
        let definition = |index| -> NodeDefinition {
            let mut value = if query_nodes {
                query_definition(&source_id, "query", "3")
            } else {
                source_definition()
            };
            value["lifetime"]["resource_id"] = json!(format!("node-{index}"));
            serde_json::from_value(value).unwrap()
        };
        for index in begin..limit {
            ledger.insert(definition(index), &config).unwrap();
        }

        let candidate = definition(limit);
        assert!(ledger
            .clone()
            .insert(candidate.clone(), &config)
            .unwrap_err()
            .message
            .contains("capacity exhausted"));
        let retired_id = definition(limit - 1).native_id().unwrap();
        ledger.version = 2;
        ledger.nodes.get_mut(&retired_id).unwrap().retirement = Some(Retirement::Retiring);
        ledger.validate(&config).unwrap();
        assert!(ledger
            .clone()
            .insert(candidate.clone(), &config)
            .unwrap_err()
            .message
            .contains("capacity exhausted"));
        ledger.nodes.get_mut(&retired_id).unwrap().retirement = Some(Retirement::Retired);
        ledger.validate(&config).unwrap();
        ledger.insert(candidate, &config).unwrap();
        let mut stale = definition(limit - 1);
        stale.activation += 1;
        assert!(ledger
            .clone()
            .insert(stale, &config)
            .unwrap_err()
            .message
            .contains("permanently retired"));
        let mut legacy = ledger.clone();
        legacy.version = 1;
        assert!(legacy
            .validate(&config)
            .unwrap_err()
            .message
            .contains("identity is invalid"));
    }
}

#[tokio::test]
async fn stopped_retirement_intent_is_completed_before_successor_construction() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    config.start_mode = StartMode::Dormant;
    let mut host = Host::open(config.clone()).await.unwrap();
    let definition: NodeDefinition = serde_json::from_value(source_definition()).unwrap();
    let node = admit(&host, &source_definition()).await.unwrap();
    let id = node["native_id"].as_str().unwrap();
    stop_node(&host, id).await;
    let mut ledger = host.catalog().unwrap().lock().await.clone();
    ledger.version = 2;
    ledger.nodes.get_mut(id).unwrap().retirement = Some(Retirement::Retiring);
    host.save_catalog(ledger).await.unwrap();
    host.store().unwrap().sync().await.unwrap();
    assert!(host
        .core()
        .unwrap()
        .get_graph()
        .await
        .nodes
        .iter()
        .any(|node| node.id == id));
    host.close().await.unwrap();
    drop(host);
    let mut reopened = Host::open(config).await.unwrap();
    assert!(!reopened
        .core()
        .unwrap()
        .get_graph()
        .await
        .nodes
        .iter()
        .any(|node| node.id == id));
    let observation = reopened.apply(Command::ObserveNodes).await.unwrap();
    assert_eq!(observation["nodes"][0]["status"], "retired");
    let retained: AdmissionLedger = serde_json::from_slice(
        &reopened
            .store()
            .unwrap()
            .get(CONTROL, "catalog")
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(retained.nodes[id].retirement == Some(Retirement::Retired));
    let mut changed = definition;
    changed.activation += 1;
    assert!(reopened
        .apply(Command::AdmitNode {
            definition: changed
        })
        .await
        .unwrap_err()
        .message
        .contains("permanently retired"));
    reopened.close().await.unwrap();
}
