use super::{tests::Workspace, *};
use drasi_lib::state_store::{StateStoreError, StateStoreResult};
use drasi_lib::{channels::ComponentStatus, Source};
use std::{collections::HashMap, time::Duration};
use tokio::sync::Notify;

struct ShutdownStore {
    inner: Arc<dyn StateStoreProvider>,
    entered: Option<Arc<Notify>>,
}

#[async_trait::async_trait]
impl StateStoreProvider for ShutdownStore {
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
        if let Some(entered) = &self.entered {
            entered.notify_one();
            std::future::pending::<()>().await;
        }
        Err(StateStoreError::StorageError(
            "injected shutdown sync failure".into(),
        ))
    }

    fn is_durable(&self) -> bool {
        self.inner.is_durable()
    }
}

pub(crate) fn replace_sync(host: &mut Host, entered: Option<Arc<Notify>>) {
    host.store = Some(Arc::new(ShutdownStore {
        inner: host.store.take().unwrap(),
        entered,
    }));
}

struct FailedStopSource {
    inner: Arc<dyn Source>,
    fail: Mutex<bool>,
}

#[async_trait::async_trait]
impl Source for FailedStopSource {
    fn id(&self) -> &str {
        self.inner.id()
    }
    fn type_name(&self) -> &str {
        self.inner.type_name()
    }
    fn properties(&self) -> HashMap<String, Value> {
        self.inner.properties()
    }
    fn auto_start(&self) -> bool {
        self.inner.auto_start()
    }
    fn supports_replay(&self) -> bool {
        self.inner.supports_replay()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_any()
    }
    async fn initialize(&self, context: drasi_lib::context::SourceRuntimeContext) {
        self.inner.initialize(context).await;
    }
    async fn start(&self) -> anyhow::Result<()> {
        self.inner.start().await
    }
    async fn stop(&self) -> anyhow::Result<()> {
        let fail = std::mem::take(&mut *self.fail.lock().await);
        anyhow::ensure!(!fail, "injected source stop failure");
        self.inner.stop().await
    }
    async fn status(&self) -> ComponentStatus {
        self.inner.status().await
    }
    async fn subscribe(
        &self,
        settings: drasi_lib::config::SourceSubscriptionSettings,
    ) -> anyhow::Result<drasi_lib::channels::SubscriptionResponse> {
        self.inner.subscribe(settings).await
    }
    async fn remove_position_handle(&self, query: &str) {
        self.inner.remove_position_handle(query).await;
    }
}

#[tokio::test]
async fn failed_node_stop_cannot_be_cleaned_into_a_checkpoint() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    let mut host = Host::open(config).await.unwrap();
    let definition = super::admission_tests::source_definition();
    let node = super::admission_tests::admit(&host, &definition)
        .await
        .unwrap();
    let id = node["native_id"].as_str().unwrap();
    let core = host.core().unwrap();
    {
        let graph = core.component_graph();
        let mut graph = graph.write().await;
        let inner = graph.get_runtime::<Arc<dyn Source>>(id).unwrap().clone();
        let source: Arc<dyn Source> = Arc::new(FailedStopSource {
            inner,
            fail: Mutex::new(true),
        });
        graph.set_runtime(id, Box::new(source)).unwrap();
    }
    let first = host
        .apply(Command::StopNode {
            native_id: id.into(),
        })
        .await
        .unwrap_err();
    assert!(first.message.contains("injected source stop failure"));
    host.apply(Command::StopNode {
        native_id: id.into(),
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let graph = host.core().unwrap().get_graph().await;
            if graph
                .nodes
                .iter()
                .find(|node| node.id == id)
                .unwrap()
                .status
                == ComponentStatus::Stopped
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source cleanup did not settle");
    let observation = host.apply(Command::ObserveNodes).await.unwrap();
    assert_eq!(observation["nodes"][0]["status"], "stopped");
    assert_eq!(observation["cleanup_failure"], first.message);
    let destination = workspace.0.join("forbidden-checkpoint");
    let error = host
        .checkpoint(destination.to_str().unwrap())
        .await
        .expect_err("node stop failure became a successful checkpoint");
    assert_eq!((error.code, error.message), (first.code, first.message));
    assert!(!destination.exists());
}

#[tokio::test]
async fn failed_close_cannot_be_retried_into_a_checkpoint() {
    let workspace = Workspace::new();
    let config = workspace.config();
    let mut host = Host::open(config).await.unwrap();
    host.apply(Command::Subscribe {
        subscription: Subscription {
            id: "watch".into(),
            query_id: "q1".into(),
            query_revision: 1,
        },
    })
    .await
    .unwrap();
    let before = host
        .store()
        .unwrap()
        .get(SUBSCRIPTIONS, "watch")
        .await
        .unwrap();
    assert!(before.is_some());
    replace_sync(&mut host, None);
    let first = host.close().await.unwrap_err();
    assert!(first.message.contains("injected shutdown sync failure"));
    let repeated = host.close().await.expect_err("failed close became success");
    assert_eq!(
        (repeated.code, repeated.message),
        (first.code, first.message.clone())
    );
    let destination = workspace.0.join("forbidden-checkpoint");
    let checkpoint = host
        .checkpoint(destination.to_str().unwrap())
        .await
        .expect_err("failed close published a checkpoint");
    assert_eq!(
        (checkpoint.code, checkpoint.message),
        (first.code, first.message)
    );
    assert!(!destination.exists());
    let state_dir = host.config.state_dir.clone();
    drop(host);
    let store = RedbStateStoreProvider::new(Path::new(&state_dir).join("state.redb")).unwrap();
    assert_eq!(store.get(SUBSCRIPTIONS, "watch").await.unwrap(), before);
}

#[tokio::test]
async fn cancelled_close_cannot_be_retried_into_a_checkpoint() {
    let workspace = Workspace::new();
    let mut host = Host::open(workspace.config()).await.unwrap();
    let entered = Arc::new(Notify::new());
    replace_sync(&mut host, Some(entered.clone()));
    tokio::time::timeout(Duration::from_secs(10), async {
        let closing = host.close();
        tokio::pin!(closing);
        tokio::select! {
            result = &mut closing => panic!("blocked shutdown completed: {result:?}"),
            _ = entered.notified() => {}
        }
    })
    .await
    .expect("shutdown did not reach the storage sync");
    let error = host
        .close()
        .await
        .expect_err("cancelled close became success");
    assert_eq!(error.code, 3);
    assert!(error.message.contains("shutdown did not complete"));
    let destination = workspace.0.join("forbidden-checkpoint");
    let error = host
        .checkpoint(destination.to_str().unwrap())
        .await
        .expect_err("cancelled close published a checkpoint");
    assert!(error.message.contains("shutdown did not complete"));
    assert!(!destination.exists());
}

#[tokio::test]
async fn failed_sdk_stop_retains_graph_and_source_ownership_until_cleanup() {
    let workspace = Workspace::new();
    let mut host = Host::open(workspace.config()).await.unwrap();
    host.snapshot("q1", "before-close").await.unwrap();
    {
        let graph = host.core().unwrap().component_graph();
        let mut graph = graph.write().await;
        let inner = graph
            .get_runtime::<Arc<dyn Source>>("feed")
            .unwrap()
            .clone();
        let source: Arc<dyn Source> = Arc::new(FailedStopSource {
            inner,
            fail: Mutex::new(true),
        });
        graph.set_runtime("feed", Box::new(source)).unwrap();
    }
    let first = host.close().await.unwrap_err();
    assert!(first.message.contains("injected source stop failure"));
    assert!(host.core.is_some());
    assert!(host.store.is_some());
    assert!(host.sources.lock().await.contains_key("feed"));
    let repeated = host.close().await.unwrap_err();
    assert_eq!(
        (repeated.code, repeated.message),
        (first.code, first.message.clone())
    );
    assert!(host.core.is_none());
    assert!(host.sources.lock().await.is_empty());
    let destination = workspace.0.join("forbidden-checkpoint");
    let error = host
        .checkpoint(destination.to_str().unwrap())
        .await
        .unwrap_err();
    assert_eq!((error.code, error.message), (first.code, first.message));
    assert!(!destination.exists());
}

#[tokio::test]
async fn successful_close_stops_builtin_source_before_checkpoint() {
    let workspace = Workspace::new();
    let mut host = Host::open(workspace.config()).await.unwrap();
    host.snapshot("q1", "before-close").await.unwrap();
    let source = host.sources.lock().await["feed"].shared();
    assert_eq!(source.status().await, ComponentStatus::Running);
    host.close().await.unwrap();
    assert_eq!(source.status().await, ComponentStatus::Stopped);
    let error = source
        .feed(
            1,
            Change {
                kind: ChangeKind::Insert,
                element_id: "after-stop".into(),
                labels: vec!["Item".into()],
                effective_from: 1,
                properties: BTreeMap::from([("amount".into(), json!(10))]),
                relation: None,
            },
        )
        .await
        .expect_err("closed built-in source accepted producer work");
    assert!(error.message.contains("not running"));
    drop(source);
    host.close().await.unwrap();
    let destination = workspace.0.join("checkpoint");
    let published = host
        .checkpoint(destination.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(published["checkpoint"]["owner_epoch"], 1);
    assert_eq!(published["checkpoint"]["instance_id"], "graph-test");
    drop(host);
    let mut config = workspace.config();
    config.state_dir = workspace.0.join("restored").to_str().unwrap().into();
    config.owner_epoch = 2;
    config.restore_from = Some(destination.to_str().unwrap().into());
    let mut successor = Host::open(config).await.unwrap();
    successor.close().await.unwrap();
}
