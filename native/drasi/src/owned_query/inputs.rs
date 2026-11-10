use super::{Cut, OwnedQuery};
use anyhow::{bail, ensure, Context, Result};
use drasi_lib::{
    channels::{BootstrapEvent, SourceEventWrapper, SubscriptionResponse},
    config::SourceSubscriptionSettings,
    queries::{LabelExtractor, SubscriptionSettingsBuilder},
    sources::{Source, SourceManager},
};
use futures_util::{future::select_all, FutureExt};
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::sync::watch;

pub(super) struct Port {
    pub id: String,
    source: Arc<dyn Source>,
    settings: SourceSubscriptionSettings,
    subscription: Option<SubscriptionResponse>,
    position_handle: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl Port {
    pub fn ack(&self, sequence: u64) {
        if let Some(handle) = &self.position_handle {
            handle.store(sequence, Ordering::Release);
        }
    }

    async fn detach(&mut self, query: &str) {
        self.subscription.take();
        self.source.remove_position_handle(query).await;
    }
}

pub(super) struct Inputs {
    pub ports: Vec<Port>,
    pending: Vec<(usize, Arc<SourceEventWrapper>)>,
    pending_bytes: usize,
    max_pending: usize,
    max_bytes: usize,
}

pub(super) struct Seed {
    pub records: Vec<BootstrapEvent>,
    pub cuts: BTreeMap<String, Cut>,
}

impl Inputs {
    pub async fn new(query: &OwnedQuery, manager: &SourceManager) -> Result<Self> {
        let config = &query.base.config;
        let labels = LabelExtractor::extract_labels(&config.query, &config.query_language)?;
        let settings = SubscriptionSettingsBuilder::build_subscription_settings(config, &labels)?;
        let mut ports = Vec::new();
        for mut settings in settings {
            let source = manager
                .get_source_instance(&settings.source_id)
                .await
                .with_context(|| format!("source '{}' is unavailable", settings.source_id))?;
            ensure!(
                source.supports_replay(),
                "source '{}' cannot honor durable replay",
                settings.source_id
            );
            settings.request_position_handle = true;
            ports.push(Port {
                id: settings.source_id.clone(),
                source,
                settings,
                subscription: None,
                position_handle: None,
            });
        }
        Ok(Self {
            ports,
            pending: Vec::new(),
            pending_bytes: 0,
            max_pending: config
                .priority_queue_capacity
                .unwrap_or(10_000)
                .max(1)
                .min(query.limits.feed_events.min(usize::MAX as u64) as usize),
            max_bytes: query.limits.pending_bytes,
        })
    }

    pub async fn seed(
        &mut self,
        query: &OwnedQuery,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<Seed> {
        ensure!(
            query.base.config.enable_bootstrap,
            "first owned query start requires a complete bootstrap"
        );
        let mut records = Vec::new();
        let mut cuts = BTreeMap::new();
        let mut bytes = 0usize;
        for port in &mut self.ports {
            if *stop.borrow() {
                bail!("owned query bootstrap cancelled");
            }
            let mut settings = port.settings.clone();
            settings.enable_bootstrap = true;
            let response =
                port.source.subscribe(settings).await.with_context(|| {
                    format!("source '{}' bootstrap subscription failed", port.id)
                })?;
            port.subscription = Some(response);
            let response = port.subscription.as_mut().expect("subscription installed");
            port.position_handle = response.position_handle.clone();
            let mut bootstrap = response.bootstrap_receiver.take().with_context(|| {
                format!(
                    "source '{}' did not provide a complete bootstrap stream",
                    port.id
                )
            })?;
            let mut completion = response.bootstrap_result_receiver.take().with_context(|| {
                format!(
                    "source '{}' did not certify its bootstrap boundary",
                    port.id
                )
            })?;
            let mut result = None;
            let mut ended = false;
            let mut count = 0usize;
            let mut discarded_live = 0u64;
            let deadline = tokio::time::sleep(Duration::from_secs(
                query.base.config.bootstrap_timeout_secs,
            ));
            tokio::pin!(deadline);
            while !ended || result.is_none() {
                tokio::select! {
                    biased;
                    _ = stop.changed() => bail!("owned query bootstrap cancelled"),
                    _ = &mut deadline => bail!("source '{}' bootstrap timed out", port.id),
                    received = &mut completion, if result.is_none() => {
                        result = Some(received
                            .with_context(|| format!("source '{}' bootstrap completion was dropped", port.id))??);
                    }
                    event = bootstrap.recv(), if !ended => {
                        match event {
                            Some(event) => {
                                ensure!(event.source_id == port.id, "bootstrap source identity does not match subscription");
                                ensure!(!matches!(event.change, drasi_core::models::SourceChange::Future { .. }), "bootstrap stream cannot evaluate a future before the complete seed");
                                bytes = bytes.checked_add(change_bytes(&event.change)?)
                                    .context("owned query bootstrap byte count overflow")?;
                                count = count.checked_add(1).context("owned query bootstrap record count overflow")?;
                                ensure!(
                                    records.len() < query.limits.feed_events.min(usize::MAX as u64) as usize
                                        && bytes <= query.limits.pending_bytes,
                                    "owned query complete bootstrap exceeds seed limits"
                                );
                                records.push(event);
                            }
                            None => ended = true,
                        }
                    }
                    event = response.receiver.recv() => {
                        let event = event.with_context(|| format!("source '{}' live stream failed during bootstrap", port.id))?;
                        ensure!(event.source_id == port.id, "bootstrap live source identity mismatch");
                        ensure!(event_bytes(&event)? <= query.limits.pending_bytes, "owned query provisional live event exceeds byte limit");
                        discarded_live = discarded_live.checked_add(1).context("bootstrap live count overflow")?;
                        ensure!(discarded_live <= query.limits.feed_events, "owned query provisional live buffer limit exceeded");
                    }
                }
            }
            let result = result.context("bootstrap result is unavailable")?;
            ensure!(
                result.event_count == count,
                "source '{}' bootstrap count does not match successful completion",
                port.id
            );
            let position = result.source_position.with_context(|| {
                format!("source '{}' bootstrap has no replayable boundary", port.id)
            })?;
            ensure!(
                !position.is_empty() && position.len() <= 65536,
                "source '{}' bootstrap position is empty or oversized",
                port.id
            );
            bytes = bytes
                .checked_add(position.len())
                .and_then(|bytes| bytes.checked_add(port.id.len()))
                .context("owned query bootstrap cut byte count overflow")?;
            ensure!(
                bytes <= query.limits.pending_bytes,
                "owned query complete bootstrap exceeds seed limits"
            );
            cuts.insert(
                port.id.clone(),
                Cut {
                    upstream: None,
                    position: position.to_vec(),
                    offset: 0,
                },
            );
            port.subscription.take();
        }
        Ok(Seed { records, cuts })
    }

    pub async fn attach(
        &mut self,
        query: &OwnedQuery,
        cuts: &BTreeMap<String, Cut>,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        for port in &mut self.ports {
            if *stop.borrow() {
                return Ok(());
            }
            let cut = cuts
                .get(&port.id)
                .context("owned query has no cut for source")?;
            // Keep the source's existing position registration during the handover.
            // The new receiver replays from the sealed cut; provisional live data is discarded.
            port.subscription.take();
            let mut settings = port.settings.clone();
            settings.enable_bootstrap = false;
            settings.resume_from = Some(bytes::Bytes::copy_from_slice(&cut.position));
            settings.resume_sequence = cut.upstream;
            let response = port.source.subscribe(settings).await.with_context(|| {
                format!(
                    "source '{}' replay at position {:?} failed",
                    port.id, cut.position
                )
            })?;
            ensure!(
                response.bootstrap_receiver.is_none()
                    && response.bootstrap_result_receiver.is_none(),
                "source replay unexpectedly restarted bootstrap"
            );
            port.position_handle = response.position_handle.clone();
            port.subscription = Some(response);
            if let Some(sequence) = cut.upstream {
                port.ack(sequence);
            }
        }
        query
            .active_inputs
            .store(self.ports.len(), Ordering::Release);
        Ok(())
    }

    fn buffer(&mut self, rank: usize, event: Arc<SourceEventWrapper>) -> Result<()> {
        let bytes = event_bytes(&event)?;
        self.pending_bytes = self
            .pending_bytes
            .checked_add(bytes)
            .context("owned input byte count overflow")?;
        ensure!(
            self.pending.len() < self.max_pending && self.pending_bytes <= self.max_bytes,
            "owned query pending input limit exceeded"
        );
        self.pending.push((rank, event));
        Ok(())
    }

    pub async fn next(
        &mut self,
        stop: &mut watch::Receiver<bool>,
        due: Option<u64>,
    ) -> Result<Option<(usize, Arc<SourceEventWrapper>)>> {
        // Sample all ready inputs without waiting for a source which has not produced anything.
        for rank in 0..self.ports.len() {
            loop {
                let ready = self.ports[rank]
                    .subscription
                    .as_mut()
                    .context("owned source subscription is detached")?
                    .receiver
                    .recv()
                    .now_or_never();
                match ready {
                    Some(event) => self.buffer(rank, event?)?,
                    None => break,
                }
            }
        }
        if self.pending.is_empty() {
            let receives = self
                .ports
                .iter_mut()
                .enumerate()
                .map(|(rank, port)| {
                    async move {
                        let event = port
                            .subscription
                            .as_mut()
                            .context("owned source subscription is detached")?
                            .receiver
                            .recv()
                            .await;
                        Ok::<_, anyhow::Error>((rank, event?))
                    }
                    .boxed()
                })
                .collect::<Vec<_>>();
            let wait = due.map_or(Duration::from_millis(100), |due| {
                Duration::from_millis(due.saturating_sub(super::now_millis()).min(100))
            });
            let received = tokio::select! {
                biased;
                _ = stop.changed() => None,
                _ = tokio::time::sleep(wait) => None,
                (event, _, _) = select_all(receives) => Some(event),
            };
            if let Some(event) = received {
                let (rank, event) = event?;
                self.buffer(rank, event)?;
            } else {
                return Ok(None);
            }
        }
        self.pending
            .sort_by(|(left_rank, left), (right_rank, right)| {
                left.timestamp
                    .cmp(&right.timestamp)
                    .then(left_rank.cmp(right_rank))
                    .then(left.sequence.cmp(&right.sequence))
            });
        let event = self.pending.remove(0);
        self.pending_bytes =
            self.pending
                .iter()
                .try_fold(0usize, |total, (_, event)| -> Result<_> {
                    total
                        .checked_add(event_bytes(event)?)
                        .context("owned input byte count overflow")
                })?;
        Ok(Some(event))
    }

    pub async fn close(&mut self, query: &str) {
        self.detach();
        for port in &mut self.ports {
            port.detach(query).await;
        }
        self.ports.clear();
    }

    pub fn detach(&mut self) {
        self.pending.clear();
        self.pending_bytes = 0;
        for port in &mut self.ports {
            port.subscription.take();
        }
    }
}

fn change_bytes(change: &drasi_core::models::SourceChange) -> Result<usize> {
    Ok(serde_json::to_vec(change)?.len())
}

fn event_bytes(event: &SourceEventWrapper) -> Result<usize> {
    serde_json::to_vec(&event.event)?
        .len()
        .checked_add(
            event
                .source_position
                .as_ref()
                .map_or(0, |position| position.len()),
        )
        .context("owned input size overflow")
}
