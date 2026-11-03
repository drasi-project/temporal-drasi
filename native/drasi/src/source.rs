use crate::protocol::{Change, ChangeKind, Error, Result};
use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use drasi_core::models::{
    Element, ElementMetadata, ElementPropertyMap, ElementReference, SourceChange,
};
use drasi_lib::{
    bootstrap::BootstrapResult,
    channels::{
        BootstrapEvent, ChangeReceiver, ComponentStatus, SourceEvent, SourceEventWrapper,
        SubscriptionResponse,
    },
    config::SourceSubscriptionSettings,
    context::SourceRuntimeContext,
    sources::{SourceBase, SourceBaseParams},
    wal::{WalError, WalProvider, WriteAheadLogConfig},
    Source, StateStoreProvider,
};
use std::{collections::HashMap, io::Write, sync::Arc};

pub(crate) mod history;

#[derive(Clone, Copy)]
pub(crate) struct BootstrapLimits {
    elements: usize,
    bytes: usize,
}

impl BootstrapLimits {
    pub(crate) fn new(elements: usize, bytes: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(
            elements > 0 && bytes > 0,
            "bootstrap limits must be positive"
        );
        Ok(Self { elements, bytes })
    }
}

#[derive(Default)]
struct Projection {
    elements: HashMap<ElementReference, (Element, usize)>,
    bytes: usize,
}

impl Projection {
    fn apply(&mut self, change: SourceChange, limits: BootstrapLimits) -> anyhow::Result<()> {
        let element = match change {
            SourceChange::Insert { element } => element,
            SourceChange::Update { mut element } => {
                if let Some((previous, _)) = self.elements.get(element.get_reference()) {
                    anyhow::ensure!(
                        std::mem::discriminant(&element) == std::mem::discriminant(previous),
                        "feed journal changes an element's kind without an insert"
                    );
                    element.merge_missing_properties(previous);
                }
                element
            }
            SourceChange::Delete { metadata } => {
                if let Some((_, size)) = self.elements.remove(&metadata.reference) {
                    self.bytes -= size;
                }
                return Ok(());
            }
            SourceChange::Future { .. } => {
                anyhow::bail!("feed journal contains an unsupported future event")
            }
        };
        let reference = element.get_reference().clone();
        let previous_bytes = self.elements.get(&reference).map_or(0, |(_, size)| *size);
        anyhow::ensure!(
            self.elements.len() + usize::from(!self.elements.contains_key(&reference))
                <= limits.elements,
            "Temporal source bootstrap exceeds the configured element limit"
        );
        let mut size = EncodedSize {
            bytes: 0,
            limit: limits.bytes,
        };
        serde_json::to_writer(&mut size, &element)?;
        let bytes = (self.bytes - previous_bytes)
            .checked_add(size.bytes)
            .context("Temporal source bootstrap byte count overflow")?;
        anyhow::ensure!(
            bytes <= limits.bytes,
            "Temporal source bootstrap exceeds the configured byte limit"
        );
        self.elements.insert(reference, (element, size.bytes));
        self.bytes = bytes;
        Ok(())
    }
}

struct EncodedSize {
    bytes: usize,
    limit: usize,
}

impl Write for EncodedSize {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len())
            .filter(|next| *next <= self.limit)
            .ok_or_else(|| std::io::Error::other("bootstrap element exceeds the byte limit"))?;
        self.bytes = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct AfterPositionReceiver {
    floor: u64,
    inner: Box<dyn ChangeReceiver<SourceEventWrapper>>,
}

#[async_trait]
impl ChangeReceiver<SourceEventWrapper> for AfterPositionReceiver {
    async fn recv(&mut self) -> anyhow::Result<Arc<SourceEventWrapper>> {
        loop {
            let event = self.inner.recv().await?;
            if event.sequence > self.floor {
                return Ok(event);
            }
        }
    }
}

pub struct Journal {
    store: Arc<dyn StateStoreProvider>,
    capacity: u64,
}

impl Journal {
    pub fn new(store: Arc<dyn StateStoreProvider>, capacity: u64) -> Self {
        Self { store, capacity }
    }

    fn partition(source: &str) -> String {
        format!("temporal-feed:{source}")
    }

    pub async fn put(&self, source: &str, position: u64, change: &SourceChange) -> Result<()> {
        let partition = Self::partition(source);
        let key = format!("{position:020}");
        let bytes = serde_json::to_vec(change)?;
        if let Some(existing) = self.store.get(&partition, &key).await.map_err(storage)? {
            if existing != bytes {
                return Err(Error::new(
                    3,
                    "feed position was already used for a different change",
                ));
            }
            return Ok(());
        }
        let head = self.head_sequence(source).await.map_err(storage)?;
        if head.checked_add(1) != Some(position) {
            return Err(Error::new(
                6,
                format!(
                    "feed gap: expected {}, received {position}",
                    head.saturating_add(1)
                ),
            ));
        }
        if head >= self.capacity {
            return Err(Error::new(3, "feed retention capacity exhausted; explicit source snapshot/retention management is required"));
        }
        // The event and its externally assigned position commit in one redb transaction.
        self.store
            .set_many(
                &partition,
                &[
                    (&key, bytes.as_slice()),
                    ("head", position.to_be_bytes().as_slice()),
                ],
            )
            .await
            .map_err(storage)?;
        Ok(())
    }

    async fn snapshot_at(
        &self,
        source: &str,
        head: u64,
        limits: BootstrapLimits,
    ) -> anyhow::Result<Projection> {
        anyhow::ensure!(
            head <= self.capacity,
            "feed head exceeds retention capacity"
        );
        let mut projection = Projection::default();
        for position in 1..=head {
            let key = format!("{position:020}");
            let bytes = self
                .store
                .get(&Self::partition(source), &key)
                .await?
                .with_context(|| format!("feed journal entry {source}/{position} is missing"))?;
            anyhow::ensure!(
                bytes.len() <= limits.bytes,
                "feed journal entry {source}/{position} exceeds the bootstrap byte limit"
            );
            let change: SourceChange = serde_json::from_slice(&bytes)
                .with_context(|| format!("invalid feed journal entry {source}/{position}"))?;
            anyhow::ensure!(
                change.get_reference().source_id.as_ref() == source,
                "feed journal entry {source}/{position} belongs to another source"
            );
            drasi_core::models::validate_effective_from(change.get_transaction_time())
                .map_err(anyhow::Error::msg)?;
            projection.apply(change, limits).with_context(|| {
                format!("cannot bootstrap feed journal entry {source}/{position}")
            })?;
        }
        Ok(projection)
    }
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::new(7, error.to_string())
}

fn wal_storage(error: impl std::fmt::Display) -> WalError {
    WalError::StorageError(error.to_string())
}

#[async_trait]
impl WalProvider for Journal {
    async fn register(&self, _: &str, _: WriteAheadLogConfig) -> std::result::Result<(), WalError> {
        Err(WalError::InvalidConfig(
            "Temporal feed positions are assigned by the committed feed, not the generic WAL API"
                .into(),
        ))
    }

    async fn append(&self, _: &str, _: &SourceChange) -> std::result::Result<u64, WalError> {
        Err(WalError::InvalidEvent(
            "use the position-bearing feed command".into(),
        ))
    }

    async fn read_from(
        &self,
        source: &str,
        sequence: u64,
    ) -> std::result::Result<Vec<(u64, SourceChange)>, WalError> {
        let head = self.head_sequence(source).await?;
        let mut events = Vec::new();
        for position in sequence..=head {
            let key = format!("{position:020}");
            let bytes = self
                .store
                .get(&Self::partition(source), &key)
                .await
                .map_err(wal_storage)?
                .ok_or_else(|| WalError::PositionUnavailable {
                    source_id: source.into(),
                    requested: position,
                    oldest_available: Some(1),
                })?;
            events.push((
                position,
                serde_json::from_slice(&bytes).map_err(wal_storage)?,
            ));
        }
        Ok(events)
    }

    async fn prune_up_to(&self, _: &str, _: u64) -> std::result::Result<u64, WalError> {
        Err(WalError::InvalidConfig("feed pruning requires an external consistent baseline and is not supported by this adapter".into()))
    }

    async fn head_sequence(&self, source: &str) -> std::result::Result<u64, WalError> {
        match self
            .store
            .get(&Self::partition(source), "head")
            .await
            .map_err(wal_storage)?
        {
            None => Ok(0),
            Some(bytes) => Ok(u64::from_be_bytes(
                bytes
                    .try_into()
                    .map_err(|_| wal_storage("invalid feed head"))?,
            )),
        }
    }

    async fn oldest_sequence(&self, source: &str) -> std::result::Result<Option<u64>, WalError> {
        Ok((self.head_sequence(source).await? > 0).then_some(1))
    }

    async fn event_count(&self, source: &str) -> std::result::Result<u64, WalError> {
        self.head_sequence(source).await
    }

    async fn delete_wal(&self, _: &str) -> std::result::Result<(), WalError> {
        Err(WalError::InvalidConfig(
            "feed deletion requires an explicit source generation reset".into(),
        ))
    }
}

pub struct TemporalSource {
    base: SourceBase,
    journal: Arc<Journal>,
    feed: Arc<tokio::sync::Mutex<()>>,
    bootstrap_limits: BootstrapLimits,
    history: Option<crate::protocol::HistoryScope>,
    input: Arc<tokio::sync::Mutex<history::InputState>>,
}

impl TemporalSource {
    pub fn new(
        id: &str,
        journal: Arc<Journal>,
        auto_start: bool,
        bootstrap_limits: BootstrapLimits,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            base: SourceBase::new(SourceBaseParams::new(id).with_auto_start(auto_start))?,
            journal,
            feed: Arc::default(),
            bootstrap_limits,
            history: None,
            input: Arc::default(),
        })
    }

    pub fn shared(&self) -> Self {
        Self {
            base: self.base.clone_shared(),
            journal: self.journal.clone(),
            feed: self.feed.clone(),
            bootstrap_limits: self.bootstrap_limits,
            history: self.history.clone(),
            input: self.input.clone(),
        }
    }

    pub async fn feed(&self, position: u64, change: Change) -> Result<()> {
        let _feed = self.feed.lock().await;
        if self.history.is_some() {
            return Err(Error::new(
                3,
                "managed History input does not accept manual changes",
            ));
        }
        if self.status().await != ComponentStatus::Running {
            return Err(Error::new(3, "Temporal source is not running"));
        }
        let change = to_change(self.id(), change)?;
        self.journal.put(self.id(), position, &change).await?;
        self.base
            .dispatch_event(
                SourceEventWrapper::new(
                    self.id().into(),
                    SourceEvent::Change(change),
                    chrono::Utc::now(),
                    position,
                )
                .with_source_position(Bytes::copy_from_slice(&position.to_be_bytes())),
            )
            .await
            .map_err(Error::from)
    }

    async fn bootstrap(
        &self,
        settings: &SourceSubscriptionSettings,
        head: u64,
    ) -> anyhow::Result<SubscriptionResponse> {
        let projection = self
            .journal
            .snapshot_at(self.id(), head, self.bootstrap_limits)
            .await?;
        let mut elements: Vec<_> = projection
            .elements
            .into_values()
            .map(|(element, _)| element)
            .filter(|element| {
                let labels = match element {
                    Element::Node { .. } => &settings.nodes,
                    Element::Relation { .. } => &settings.relations,
                };
                labels.is_empty()
                    || element
                        .get_metadata()
                        .labels
                        .iter()
                        .any(|label| labels.contains(label.as_ref()))
            })
            .collect();
        elements.sort_by(|left, right| {
            matches!(left, Element::Relation { .. })
                .cmp(&matches!(right, Element::Relation { .. }))
                .then_with(|| {
                    left.get_reference()
                        .source_id
                        .cmp(&right.get_reference().source_id)
                })
                .then_with(|| {
                    left.get_reference()
                        .element_id
                        .cmp(&right.get_reference().element_id)
                })
        });
        let count = elements.len();
        let (sender, receiver) = tokio::sync::mpsc::channel(count.max(1));
        for element in elements {
            sender.try_send(BootstrapEvent {
                source_id: self.id().into(),
                change: SourceChange::Insert { element },
                timestamp: chrono::Utc::now(),
                sequence: 0,
            })?;
        }
        drop(sender);
        let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
        result_sender
            .send(Ok(BootstrapResult {
                event_count: count,
                source_position: Some(Bytes::copy_from_slice(&head.to_be_bytes())),
            }))
            .map_err(|_| {
                anyhow::anyhow!("bootstrap completion receiver closed before registration")
            })?;
        let position_handle = if settings.request_position_handle {
            Some(self.base.create_position_handle(&settings.query_id).await)
        } else {
            None
        };
        let live = self.base.create_streaming_receiver().await?;
        Ok(SubscriptionResponse {
            query_id: settings.query_id.clone(),
            source_id: self.id().into(),
            receiver: Box::new(AfterPositionReceiver {
                floor: head,
                inner: live,
            }),
            bootstrap_receiver: Some(receiver),
            position_handle,
            bootstrap_result_receiver: Some(result_receiver),
        })
    }
}

#[async_trait]
impl Source for TemporalSource {
    fn id(&self) -> &str {
        self.base.get_id()
    }
    fn type_name(&self) -> &str {
        "temporal"
    }
    fn properties(&self) -> HashMap<String, serde_json::Value> {
        HashMap::new()
    }
    fn supports_replay(&self) -> bool {
        true
    }
    fn auto_start(&self) -> bool {
        self.base.auto_start
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    async fn initialize(&self, context: SourceRuntimeContext) {
        self.base.initialize(context).await;
    }
    async fn start(&self) -> anyhow::Result<()> {
        let _feed = self.feed.lock().await;
        if let Some(scope) = &self.history {
            self.journal
                .prepare_history(self.id(), scope, false)
                .await?;
            *self.input.lock().await = history::InputState::default();
            self.input.lock().await.blocked = self.journal.history_blocked(self.id()).await?;
        }
        self.base.set_status(ComponentStatus::Running, None).await;
        Ok(())
    }
    async fn stop(&self) -> anyhow::Result<()> {
        let _feed = self.feed.lock().await;
        *self.input.lock().await = history::InputState::default();
        self.base.stop_common().await
    }
    async fn status(&self) -> ComponentStatus {
        self.base.status_handle().get_status().await
    }
    async fn subscribe(
        &self,
        settings: SourceSubscriptionSettings,
    ) -> anyhow::Result<SubscriptionResponse> {
        let position = match settings.resume_from.as_ref() {
            Some(bytes) => {
                Some(u64::from_be_bytes(bytes.as_ref().try_into().map_err(
                    |_| anyhow::anyhow!("invalid Temporal source position"),
                )?))
            }
            None => settings.resume_sequence,
        };
        let _feed = self.feed.lock().await;
        anyhow::ensure!(
            self.status().await == ComponentStatus::Running,
            "Temporal source is not running"
        );
        anyhow::ensure!(self.input_ready().await, "History input is not ready");
        let head = self.journal.head_sequence(self.id()).await?;
        if settings.enable_bootstrap && position.is_none() {
            return self.bootstrap(&settings, head).await;
        }
        let position = position.unwrap_or(0);
        anyhow::ensure!(
            position <= head,
            "Temporal source position {position} is beyond retained head {head}"
        );
        let mut response = self
            .base
            .subscribe_with_replay(&settings, self.journal.as_ref(), position, "Temporal")
            .await?;
        response.receiver = Box::new(AfterPositionReceiver {
            floor: position,
            inner: response.receiver,
        });
        Ok(response)
    }
    async fn remove_position_handle(&self, query: &str) {
        self.base.remove_position_handle(query).await;
    }
}

fn to_change(source: &str, change: Change) -> Result<SourceChange> {
    if change.element_id.is_empty()
        || change.element_id.len() > 4096
        || change.labels.len() > 64
        || change
            .labels
            .iter()
            .any(|label| label.is_empty() || label.len() > 256)
    {
        return Err(Error::new(1, "invalid graph element identity or labels"));
    }

    drasi_core::models::validate_effective_from(change.effective_from)
        .map_err(|error| Error::new(1, error))?;
    let metadata = ElementMetadata {
        reference: ElementReference::new(source, &change.element_id),
        labels: change
            .labels
            .iter()
            .map(|label| Arc::from(label.as_str()))
            .collect(),
        effective_from: change.effective_from,
    };
    if matches!(change.kind, ChangeKind::Delete) {
        return Ok(SourceChange::Delete { metadata });
    }
    let properties = ElementPropertyMap::from(&serde_json::Map::from_iter(change.properties));
    let element = match change.relation {
        Some(relation) => Element::Relation {
            metadata,
            properties,
            in_node: ElementReference::new(&relation.in_source, &relation.in_id),
            out_node: ElementReference::new(&relation.out_source, &relation.out_id),
        },
        None => Element::Node {
            metadata,
            properties,
        },
    };
    Ok(match change.kind {
        ChangeKind::Insert => SourceChange::Insert { element },
        ChangeKind::Update => SourceChange::Update { element },
        ChangeKind::Delete => unreachable!(),
    })
}

#[cfg(test)]
mod bootstrap_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashSet};

    use drasi_state_store_redb::RedbStateStoreProvider;

    #[tokio::test]
    async fn new_subscription_bootstraps_current_accepted_state() {
        let workspace = crate::runtime::tests::Workspace::new();
        let store = Arc::new(RedbStateStoreProvider::new(workspace.0.join("state.redb")).unwrap());
        let source = TemporalSource::new(
            "source",
            Arc::new(Journal::new(store, 100)),
            true,
            BootstrapLimits::new(100, 1 << 20).unwrap(),
        )
        .unwrap();
        source.start().await.unwrap();
        let change = |position, kind, amount| Change {
            kind,
            element_id: "item".into(),
            labels: vec!["Item".into()],
            effective_from: position,
            properties: BTreeMap::from([("amount".into(), serde_json::json!(amount))]),
            relation: None,
        };
        source
            .feed(1, change(1, ChangeKind::Insert, 1))
            .await
            .unwrap();
        source
            .feed(2, change(2, ChangeKind::Update, 2))
            .await
            .unwrap();
        let mut subscription = source
            .subscribe(SourceSubscriptionSettings {
                source_id: source.id().into(),
                query_id: "query".into(),
                enable_bootstrap: true,
                nodes: HashSet::from(["Item".into()]),
                relations: HashSet::new(),
                resume_from: None,
                resume_sequence: None,
                request_position_handle: true,
            })
            .await
            .unwrap();
        let mut bootstrap = subscription
            .bootstrap_receiver
            .take()
            .expect("accepted history must be delivered behind the query bootstrap barrier");
        let initial = bootstrap.recv().await.unwrap();
        assert_eq!(
            serde_json::to_value(initial.change).unwrap(),
            serde_json::to_value(to_change(source.id(), change(2, ChangeKind::Insert, 2)).unwrap())
                .unwrap()
        );
        assert!(bootstrap.recv().await.is_none());
        let boundary = subscription
            .bootstrap_result_receiver
            .take()
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            boundary.source_position,
            Some(Bytes::copy_from_slice(&2u64.to_be_bytes()))
        );
        {
            let received = subscription.receiver.recv();
            tokio::pin!(received);
            assert!(futures_util::poll!(received.as_mut()).is_pending());
        }
        source
            .feed(1, change(1, ChangeKind::Insert, 1))
            .await
            .unwrap();
        source
            .feed(2, change(2, ChangeKind::Update, 2))
            .await
            .unwrap();
        source
            .feed(3, change(3, ChangeKind::Update, 3))
            .await
            .unwrap();
        let live = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            subscription.receiver.recv(),
        )
        .await
        .expect("live source update was lost at the bootstrap boundary")
        .unwrap();
        assert_eq!(live.sequence, 3);
        assert_eq!(
            live.source_position,
            Some(Bytes::copy_from_slice(&3u64.to_be_bytes()))
        );
        source.stop().await.unwrap();
    }

    #[tokio::test]
    async fn subscription_joins_the_feed_and_stop_boundary() {
        let workspace = crate::runtime::tests::Workspace::new();
        let store = Arc::new(RedbStateStoreProvider::new(workspace.0.join("state.redb")).unwrap());
        let source = TemporalSource::new(
            "source",
            Arc::new(Journal::new(store, 100)),
            true,
            BootstrapLimits::new(100, 1 << 20).unwrap(),
        )
        .unwrap();
        source.start().await.unwrap();
        let settings = SourceSubscriptionSettings {
            source_id: source.id().into(),
            query_id: "query".into(),
            enable_bootstrap: true,
            nodes: HashSet::new(),
            relations: HashSet::new(),
            resume_from: None,
            resume_sequence: None,
            request_position_handle: false,
        };
        let feeding = source.feed.lock().await;
        {
            let subscription = source.subscribe(settings.clone());
            tokio::pin!(subscription);
            assert!(
                futures_util::poll!(subscription.as_mut()).is_pending(),
                "subscription raced a feed commit/dispatch or source stop"
            );
            drop(feeding);
            subscription.await.unwrap();
        }
        source.stop().await.unwrap();
        assert!(source.subscribe(settings).await.is_err());
    }

    #[tokio::test]
    async fn zero_checkpoint_resumes_without_bootstrap() {
        let workspace = crate::runtime::tests::Workspace::new();
        let store = Arc::new(RedbStateStoreProvider::new(workspace.0.join("state.redb")).unwrap());
        let source = TemporalSource::new(
            "source",
            Arc::new(Journal::new(store, 100)),
            true,
            BootstrapLimits::new(100, 1 << 20).unwrap(),
        )
        .unwrap();
        source.start().await.unwrap();
        source
            .feed(
                1,
                Change {
                    kind: ChangeKind::Insert,
                    element_id: "item".into(),
                    labels: vec!["Item".into()],
                    effective_from: 1,
                    properties: BTreeMap::new(),
                    relation: None,
                },
            )
            .await
            .unwrap();
        let mut subscription = source
            .subscribe(SourceSubscriptionSettings {
                source_id: source.id().into(),
                query_id: "query".into(),
                enable_bootstrap: true,
                nodes: HashSet::new(),
                relations: HashSet::new(),
                resume_from: None,
                resume_sequence: Some(0),
                request_position_handle: true,
            })
            .await
            .unwrap();
        assert!(subscription.bootstrap_receiver.is_none());
        assert!(subscription.bootstrap_result_receiver.is_none());
        assert_eq!(subscription.receiver.recv().await.unwrap().sequence, 1);
        source.stop().await.unwrap();
    }

    #[tokio::test]
    async fn stop_waits_for_the_feed_critical_section() {
        let workspace = crate::runtime::tests::Workspace::new();
        let store = Arc::new(RedbStateStoreProvider::new(workspace.0.join("state.redb")).unwrap());
        let source = TemporalSource::new(
            "source",
            Arc::new(Journal::new(store, 100)),
            true,
            BootstrapLimits::new(100, 1 << 20).unwrap(),
        )
        .unwrap();
        let feeding = source.feed.lock().await;
        let stopped = source.stop();
        tokio::pin!(stopped);
        assert!(
            futures_util::poll!(stopped.as_mut()).is_pending(),
            "source stopped before in-flight feed work finished"
        );
        drop(feeding);
        stopped.await.unwrap();
        assert_eq!(source.status().await, ComponentStatus::Stopped);
    }
}
