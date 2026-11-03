use super::*;
use crate::protocol::{
    ElementImage, HistoryCursor, HistoryPage, HistoryProgress, HistoryReceipt, HistoryRecord,
    HistoryScan, HistoryScope, HistoryStep, MAX_INPUT,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

const HEADER: &str = "history:scope";
const CONTROL_BYTES: usize = 8 << 20;

#[derive(Default)]
pub(super) struct InputState {
    validated: BTreeSet<u32>,
    pub(super) blocked: bool,
}

pub(crate) enum InputStatus {
    Ready,
    Loading,
    Blocked,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    version: u32,
    scope: HistoryScope,
    shards: BTreeSet<u32>,
    progress_bytes: usize,
    blocked: Option<String>,
}

struct Accepted {
    receipt: HistoryReceipt,
    events: Vec<(u64, SourceChange)>,
    fresh: bool,
}

impl Journal {
    pub(super) async fn history_blocked(&self, source: &str) -> Result<bool> {
        Ok(self.history_header(source).await?.blocked.is_some())
    }

    async fn history_header(&self, source: &str) -> Result<Header> {
        let bytes = self
            .store
            .get(&Self::partition(source), HEADER)
            .await
            .map_err(storage)?
            .ok_or_else(|| Error::new(7, "History input scope is missing"))?;
        let header: Header = serde_json::from_slice(&bytes)?;
        header.scope.validate()?;
        if header.version != 1
            || header.progress_bytes > CONTROL_BYTES
            || header
                .shards
                .iter()
                .any(|id| *id == 0 || *id > header.scope.shard_count)
            || header
                .blocked
                .as_ref()
                .is_some_and(|problem| problem.len() > 2048)
        {
            return Err(Error::new(7, "History input scope is invalid"));
        }
        Ok(header)
    }

    pub(super) async fn prepare_history(
        &self,
        source: &str,
        scope: &HistoryScope,
        create: bool,
    ) -> Result<()> {
        scope.validate()?;
        let partition = Self::partition(source);
        if self
            .store
            .get(&partition, HEADER)
            .await
            .map_err(storage)?
            .is_some()
        {
            let header = self.history_header(source).await?;
            if header.scope != *scope {
                return Err(Error::new(3, "History input scope changed"));
            }
            return Ok(());
        }
        if !create || self.head_sequence(source).await.map_err(storage)? != 0 {
            return Err(Error::new(7, "existing History input has no durable scope"));
        }
        let header = Header {
            version: 1,
            scope: scope.clone(),
            shards: BTreeSet::new(),
            progress_bytes: 0,
            blocked: None,
        };
        self.store
            .set(&partition, HEADER, serde_json::to_vec(&header)?)
            .await
            .map_err(storage)
    }

    async fn history_progress(
        &self,
        source: &str,
        shard: u32,
        header: &Header,
    ) -> Result<HistoryProgress> {
        if shard == 0 || shard > header.scope.shard_count {
            return Err(Error::new(
                1,
                "History shard is outside the configured scope",
            ));
        }
        let key = format!("history:shard:{shard}");
        let Some(bytes) = self
            .store
            .get(&Self::partition(source), &key)
            .await
            .map_err(storage)?
        else {
            if header.shards.contains(&shard) {
                return Err(Error::new(7, "accepted History shard progress is missing"));
            }
            return Ok(HistoryProgress::default());
        };
        if !header.shards.contains(&shard) {
            return Err(Error::new(7, "History shard progress is not registered"));
        }
        let progress: HistoryProgress = serde_json::from_slice(&bytes)?;
        let head = self.head_sequence(source).await.map_err(storage)?;
        let valid_cursor = match &progress.cursor {
            HistoryCursor::Unstarted => false,
            HistoryCursor::Capturing {
                capture_epoch,
                reconstruction_start,
                next_position,
                scan,
            } => {
                *capture_epoch > 0
                    && *reconstruction_start > 0
                    && *next_position >= *reconstruction_start
                    && match scan {
                        HistoryScan::Scanning { token, cutoff } => {
                            !token.is_empty()
                                && token.len() <= 24 << 10
                                && *cutoff >= *reconstruction_start
                        }
                        HistoryScan::Complete { cutoff } => *cutoff >= *reconstruction_start,
                    }
            }
        };
        if !valid_cursor
            || progress.revision == 0
            || !progress.receipt.as_ref().is_some_and(|receipt| {
                receipt.revision == progress.revision
                    && uuid::Uuid::parse_str(&receipt.request_id)
                        .is_ok_and(|id| !id.is_nil() && id.to_string() == receipt.request_id)
                    && receipt.head <= head
                    && receipt
                        .first_position
                        .is_none_or(|first| first > 0 && first <= receipt.head)
            })
        {
            return Err(Error::new(7, "accepted History shard progress is invalid"));
        }
        Ok(progress)
    }

    async fn accept_history(
        &self,
        source: &str,
        step: &HistoryStep,
        limits: BootstrapLimits,
    ) -> Result<Accepted> {
        let request_id = uuid::Uuid::parse_str(&step.request_id)
            .map_err(|_| Error::new(1, "History acceptance requires a canonical request UUID"))?;
        if request_id.is_nil() || request_id.to_string() != step.request_id {
            return Err(Error::new(
                1,
                "History acceptance requires a canonical nonzero request UUID",
            ));
        }
        let encoded = serde_json::to_vec(step)?;
        if encoded.len() > MAX_INPUT {
            return Err(Error::new(1, "History page exceeds the native input limit"));
        }
        let digest: [u8; 32] = Sha256::digest(encoded).into();
        let mut header = self.history_header(source).await?;
        if header.blocked.is_some() {
            return Err(Error::new(
                3,
                "History input requires a new source incarnation",
            ));
        }
        let previous = self
            .history_progress(source, step.shard_id, &header)
            .await?;
        if previous.revision != step.expected_revision {
            if previous.revision.checked_sub(1) == Some(step.expected_revision) {
                if let Some(receipt) = &previous.receipt {
                    if receipt.digest == digest {
                        return Ok(Accepted {
                            receipt: receipt.clone(),
                            events: Vec::new(),
                            fresh: false,
                        });
                    }
                }
                return Err(Error::new(
                    3,
                    "History step identity was already used for different content",
                ));
            }
            return Err(Error::new(
                3,
                "History predecessor changed; reload accepted progress",
            ));
        }
        let cursor = successor(&previous.cursor, &step.page)?;
        let head = self.head_sequence(source).await.map_err(storage)?;
        let mut changes = Vec::new();
        if let HistoryPage::Records {
            start_position,
            next_position,
            capture_epoch,
            records,
            ..
        } = &step.page
        {
            if records.len() > 100 {
                return Err(Error::new(1, "History page exceeds 100 records"));
            }
            let mut last = None;
            let mut projection = self
                .snapshot_at(source, head, limits)
                .await
                .map_err(storage)?;
            for record in records {
                if record.namespace_id != header.scope.namespace_id
                    || record.capture_epoch != *capture_epoch
                    || record.position < *start_position
                    || record.position >= *next_position
                    || last.is_some_and(|position| record.position <= position)
                {
                    return Err(Error::new(
                        1,
                        "History record does not belong to the accepted interval and scope",
                    ));
                }
                last = Some(record.position);
                materialize(source, record, &mut projection, limits, &mut changes)?;
            }
        }
        let count = u64::try_from(changes.len()).map_err(storage)?;
        let next_head = head
            .checked_add(count)
            .filter(|head| *head <= self.capacity)
            .ok_or_else(|| Error::new(3, "feed retention capacity exhausted"))?;
        let receipt = HistoryReceipt {
            request_id: step.request_id.clone(),
            revision: previous
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::new(3, "History revision exhausted"))?,
            first_position: (count > 0).then_some(head + 1),
            head: next_head,
            digest,
        };
        let progress = HistoryProgress {
            revision: receipt.revision,
            cursor,
            receipt: Some(receipt.clone()),
        };
        let old_bytes = if previous.revision == 0 {
            0
        } else {
            serde_json::to_vec(&previous)?.len()
        };
        let bytes = serde_json::to_vec(&progress)?;
        header.progress_bytes = header
            .progress_bytes
            .checked_sub(old_bytes)
            .and_then(|size| size.checked_add(bytes.len()))
            .ok_or_else(|| Error::new(7, "History control byte count is invalid"))?;
        header.shards.insert(step.shard_id);
        let header_bytes = serde_json::to_vec(&header)?;
        if header.progress_bytes + header_bytes.len() > CONTROL_BYTES {
            return Err(Error::new(3, "History control capacity exhausted"));
        }
        let mut entries = vec![
            (format!("history:shard:{}", step.shard_id), bytes),
            (HEADER.into(), header_bytes),
            ("head".into(), next_head.to_be_bytes().to_vec()),
        ];
        let mut events = Vec::with_capacity(changes.len());
        let mut event_bytes = 0usize;
        for (index, change) in changes.into_iter().enumerate() {
            let position = head + index as u64 + 1;
            let bytes = serde_json::to_vec(&change)?;
            event_bytes = event_bytes
                .checked_add(bytes.len())
                .filter(|size| *size <= 16 << 20)
                .ok_or_else(|| Error::new(3, "History acceptance exceeds the event byte limit"))?;
            entries.push((format!("{position:020}"), bytes));
            events.push((position, change));
        }
        let values: Vec<_> = entries
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_slice()))
            .collect();
        self.store
            .set_many(&Self::partition(source), &values)
            .await
            .map_err(storage)?;
        Ok(Accepted {
            receipt,
            events,
            fresh: true,
        })
    }
}

fn successor(previous: &HistoryCursor, page: &HistoryPage) -> Result<HistoryCursor> {
    match page {
        HistoryPage::Bootstrap {
            capture_epoch,
            reconstruction_start,
            next_token,
            cutoff,
        } => {
            if *capture_epoch <= 0
                || *reconstruction_start <= 0
                || cutoff < reconstruction_start
                || next_token.len() > 24 << 10
            {
                return Err(Error::new(1, "invalid History bootstrap scope or bounds"));
            }
            let next_position = match previous {
                HistoryCursor::Unstarted => *reconstruction_start,
                HistoryCursor::Capturing {
                    capture_epoch: epoch,
                    reconstruction_start: start,
                    next_position,
                    scan:
                        HistoryScan::Scanning {
                            cutoff: previous_cutoff,
                            ..
                        },
                } if epoch == capture_epoch
                    && start == reconstruction_start
                    && cutoff >= previous_cutoff =>
                {
                    *next_position
                }
                _ => {
                    return Err(Error::new(
                        3,
                        "History bootstrap does not continue the accepted scan",
                    ))
                }
            };
            Ok(HistoryCursor::Capturing {
                capture_epoch: *capture_epoch,
                reconstruction_start: *reconstruction_start,
                next_position,
                scan: if next_token.is_empty() {
                    HistoryScan::Complete { cutoff: *cutoff }
                } else {
                    HistoryScan::Scanning {
                        token: next_token.clone(),
                        cutoff: *cutoff,
                    }
                },
            })
        }
        HistoryPage::Records {
            capture_epoch,
            start_position,
            next_position,
            published_cutoff,
            ..
        } => {
            if let HistoryCursor::Capturing {
                capture_epoch: epoch,
                reconstruction_start,
                next_position: start,
                scan,
            } = previous
            {
                if capture_epoch == epoch
                    && start_position == start
                    && next_position >= start
                    && published_cutoff >= next_position
                {
                    return Ok(HistoryCursor::Capturing {
                        capture_epoch: *epoch,
                        reconstruction_start: *reconstruction_start,
                        next_position: *next_position,
                        scan: scan.clone(),
                    });
                }
            }
            Err(Error::new(
                3,
                "History page does not continue the accepted cursor and epoch",
            ))
        }
    }
}

fn materialize(
    source: &str,
    record: &HistoryRecord,
    projection: &mut Projection,
    limits: BootstrapLimits,
    changes: &mut Vec<SourceChange>,
) -> Result<()> {
    if record.execution_key.len() != 64
        || !record
            .execution_key
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::new(
            1,
            "History execution key requires a SHA-256 identity",
        ));
    }
    drasi_core::models::validate_effective_from(record.effective_from)
        .map_err(|error| Error::new(1, error))?;
    let prefix = format!("x:{}:state:", record.execution_key);
    let fact_prefix = format!("x:{}:fact:{}:", record.execution_key, record.position);
    let mut identities = BTreeSet::new();
    let mut incoming = Vec::new();
    for (images, prefix, immutable) in [
        (&record.state, &prefix, false),
        (&record.facts, &fact_prefix, true),
    ] {
        for image in images {
            if !image.element_id.starts_with(prefix)
                || image.effective_from != record.effective_from
                || !identities.insert(image.element_id.clone())
                || image.relation.as_ref().is_some_and(|relation| {
                    relation.in_source != source || relation.out_source != source
                })
            {
                return Err(Error::new(
                    1,
                    "History element identity, time or relation is invalid",
                ));
            }
            let reference = ElementReference::new(source, &image.element_id);
            let existing = projection.elements.contains_key(&reference);
            if immutable && existing {
                return Err(Error::new(3, "History fact identity was already accepted"));
            }
            incoming.push(to_change(
                source,
                image_change(
                    image,
                    if existing {
                        ChangeKind::Update
                    } else {
                        ChangeKind::Insert
                    },
                ),
            )?);
        }
    }
    let mut deletions: Vec<_> = projection
        .elements
        .values()
        .filter(|(element, _)| {
            element.get_reference().element_id.starts_with(&prefix)
                && !identities.contains(element.get_reference().element_id.as_ref())
        })
        .map(|(element, _)| {
            let mut metadata = element.get_metadata().clone();
            metadata.effective_from = record.effective_from;
            (matches!(element, Element::Relation { .. }), metadata)
        })
        .collect();
    deletions.sort_by(|left, right| {
        right.0.cmp(&left.0).then_with(|| {
            left.1
                .reference
                .element_id
                .cmp(&right.1.reference.element_id)
        })
    });
    incoming.sort_by(|left, right| {
        matches!(
            left,
            SourceChange::Insert {
                element: Element::Relation { .. }
            } | SourceChange::Update {
                element: Element::Relation { .. }
            }
        )
        .cmp(&matches!(
            right,
            SourceChange::Insert {
                element: Element::Relation { .. }
            } | SourceChange::Update {
                element: Element::Relation { .. }
            }
        ))
        .then_with(|| {
            left.get_reference()
                .element_id
                .cmp(&right.get_reference().element_id)
        })
    });
    for change in deletions
        .into_iter()
        .map(|(_, metadata)| SourceChange::Delete { metadata })
        .chain(incoming)
    {
        projection.apply(change.clone(), limits).map_err(storage)?;
        changes.push(change);
    }
    Ok(())
}

fn image_change(image: &ElementImage, kind: ChangeKind) -> Change {
    Change {
        kind,
        element_id: image.element_id.clone(),
        labels: image.labels.clone(),
        effective_from: image.effective_from,
        properties: image.properties.clone(),
        relation: image.relation.clone(),
    }
}

impl TemporalSource {
    pub(crate) fn with_history(mut self, scope: HistoryScope) -> Result<Self> {
        scope.validate()?;
        self.history = Some(scope);
        Ok(self)
    }

    pub(crate) async fn prepare_history(&self, create: bool) -> Result<()> {
        if let Some(scope) = &self.history {
            self.journal
                .prepare_history(self.id(), scope, create)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn input_ready(&self) -> bool {
        match &self.history {
            None => true,
            Some(scope) => {
                let state = self.input.lock().await;
                !state.blocked && state.validated.len() == scope.shard_count as usize
            }
        }
    }

    pub(crate) async fn input_blocked(&self) -> bool {
        self.input.lock().await.blocked
    }

    pub(crate) async fn input_status(&self) -> InputStatus {
        let _feed = self.feed.lock().await;
        if self.input_blocked().await {
            InputStatus::Blocked
        } else if self.input_ready().await {
            InputStatus::Ready
        } else {
            InputStatus::Loading
        }
    }

    pub(crate) async fn history_requires_reconstruction(&self) -> Result<bool> {
        if self.history.is_none() {
            return Ok(false);
        }
        let _feed = self.feed.lock().await;
        self.journal.history_blocked(self.id()).await
    }

    pub(crate) async fn require_input_ready(&self) -> Result<()> {
        if matches!(self.input_status().await, InputStatus::Ready) {
            Ok(())
        } else {
            Err(Error::new(3, "query History input is not ready"))
        }
    }

    pub(crate) async fn history_progress(&self, shard: u32) -> Result<HistoryProgress> {
        if self.history.is_none() {
            return Err(Error::new(3, "source is not managed History input"));
        }
        let _feed = self.feed.lock().await;
        let result = async {
            let header = self.journal.history_header(self.id()).await?;
            self.journal
                .history_progress(self.id(), shard, &header)
                .await
        }
        .await;
        if result.is_err() {
            self.input.lock().await.blocked = true;
        }
        result
    }

    pub(crate) async fn block_history(&self, problem: String) -> Result<()> {
        if self.history.is_none() || problem.is_empty() || problem.len() > 2048 {
            return Err(Error::new(
                1,
                "History input failure requires a managed source and a bounded problem",
            ));
        }
        let _feed = self.feed.lock().await;
        self.input.lock().await.blocked = true;
        let mut header = self.journal.history_header(self.id()).await?;
        header.blocked = Some(problem);
        self.journal
            .store
            .set(
                &Journal::partition(self.id()),
                HEADER,
                serde_json::to_vec(&header)?,
            )
            .await
            .map_err(storage)
    }

    pub(crate) async fn suspend_history(&self) -> Result<()> {
        if self.history.is_none() {
            return Err(Error::new(3, "source is not managed History input"));
        }
        let _feed = self.feed.lock().await;
        self.input.lock().await.blocked = true;
        Ok(())
    }

    pub(crate) async fn accept_history(&self, step: HistoryStep) -> Result<HistoryReceipt> {
        if self.history.is_none() {
            return Err(Error::new(3, "source is not managed History input"));
        }
        let _feed = self.feed.lock().await;
        if self.status().await != ComponentStatus::Running || self.input.lock().await.blocked {
            return Err(Error::new(
                3,
                "History input is not running or requires stop and recovery",
            ));
        }
        self.input.lock().await.blocked = true;
        let accepted = self
            .journal
            .accept_history(self.id(), &step, self.bootstrap_limits)
            .await?;
        for (position, change) in accepted.events {
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
                .map_err(Error::from)?;
        }
        let header = self.journal.history_header(self.id()).await?;
        let progress = self
            .journal
            .history_progress(self.id(), step.shard_id, &header)
            .await?;
        if let (
            HistoryCursor::Capturing {
                next_position,
                scan: HistoryScan::Complete { cutoff },
                ..
            },
            HistoryPage::Records {
                published_cutoff, ..
            },
        ) = (&progress.cursor, &step.page)
        {
            if accepted.fresh && next_position >= cutoff && next_position == published_cutoff {
                self.input.lock().await.validated.insert(step.shard_id);
            }
        }
        self.input.lock().await.blocked = false;
        Ok(accepted.receipt)
    }
}

#[cfg(test)]
mod tests;
