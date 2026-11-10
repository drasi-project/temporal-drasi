use super::{Cut, OwnedQuery};
use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use drasi_core::interface::{
    CheckpointStore, CreatedIndexes, IndexError, LiveResultsWriter, OutboxWriter, RowMutation,
};
use drasi_lib::{
    channels::{QueryResult, ResultDiff},
    queries::QueryOutputState,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};

const SEAL_KEY: &str = "\0temporal-owned-query-seal-v1";
const ADMISSION_KEY: &str = "\0temporal-owned-query-admission-v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Seal {
    format: u32,
    query: String,
    config_hash: u64,
    generation: u64,
    sequence: u64,
    cuts: BTreeMap<String, Cut>,
    rows_digest: Vec<u8>,
    outbox_digest: Vec<u8>,
}

pub(super) struct Storage {
    pub created: CreatedIndexes,
    pub checkpoints: Arc<dyn CheckpointStore>,
    pub live: Arc<dyn LiveResultsWriter>,
    pub outbox: Arc<dyn OutboxWriter>,
}

#[derive(Clone)]
pub(super) struct State {
    pub output: QueryOutputState,
    pub cuts: BTreeMap<String, Cut>,
    pub outbox_bytes: BTreeMap<u64, Vec<u8>>,
}

pub(super) struct Staged {
    pub state: State,
    pub result: Option<QueryResult>,
}

impl Storage {
    pub fn new(created: CreatedIndexes) -> Result<Self> {
        let checkpoints = created
            .checkpoint_store
            .clone()
            .context("owned query requires atomic persistent checkpoints")?;
        ensure!(
            checkpoints.is_persistent(),
            "owned query requires persistent indexes"
        );
        Ok(Self {
            checkpoints,
            live: created
                .live_results_writer
                .clone()
                .context("owned query requires persistent live rows")?,
            outbox: created
                .outbox_writer
                .clone()
                .context("owned query requires persistent outbox")?,
            created,
        })
    }

    pub async fn hydrate(&self, query: &OwnedQuery) -> Result<Option<State>> {
        let config = &query.base.config;
        let mut checkpoints = self.checkpoints.read_all_checkpoints().await?;
        let seal = checkpoints.remove(SEAL_KEY);
        ensure!(
            seal.is_some() || !query.admission.requires_seal().await,
            "owned query committed state is missing; explicit recovery is required"
        );
        if seal.is_some() {
            query.admission.observe_seal().await?;
        }
        let admission = checkpoints.remove(ADMISSION_KEY);
        if let Some(admission) = &admission {
            let bytes = admission
                .source_position
                .as_ref()
                .context("owned query admission payload is missing")?;
            ensure!(
                bytes.len() <= query.limits.pending_bytes,
                "owned query admission byte limit exceeded"
            );
            let identity: (u32, String, u64) =
                serde_json::from_slice(bytes).context("invalid owned query admission")?;
            ensure!(
                admission.sequence == 0 && identity == (1, config.id.clone(), query.config_hash),
                "owned query admission does not match query identity"
            );
        }
        let sequence = self.checkpoints.read_result_sequence(&config.id).await?;
        let generation = self
            .checkpoints
            .read_output_generation(&config.id)
            .await?
            .unwrap_or(0);
        let hash = self.checkpoints.read_config_hash().await?;
        let rows = self.live.read_snapshot(&config.id).await?;
        let outbox = self.outbox.read_from(&config.id, 0).await?;
        let latest = self.outbox.read_latest_sequence(&config.id).await?;
        let Some(seal) = seal else {
            let future = self.created.set.future_queue.peek_due_time().await?;
            ensure!(
                checkpoints.is_empty()
                    && sequence.is_none()
                    && generation == 0
                    && rows.is_empty()
                    && outbox.is_empty()
                    && latest.is_none()
                    && future.is_none(),
                "owned query refuses unsealed durable state"
            );
            ensure!(
                hash.is_none() || (admission.is_some() && hash == Some(query.config_hash)),
                "owned query refuses unsealed SDK state or configuration changes"
            );
            return Ok(None);
        };
        ensure!(
            admission.is_some(),
            "owned query seal is missing its format admission"
        );
        let bytes = seal
            .source_position
            .context("owned query seal is missing its payload")?;
        ensure!(
            bytes.len() <= query.limits.pending_bytes,
            "owned query seal exceeds byte limit"
        );
        let seal_data: Seal = serde_json::from_slice(&bytes).context("invalid owned query seal")?;
        ensure!(
            seal_data.format == 1
                && seal_data.query == config.id
                && seal_data.config_hash == query.config_hash
                && hash == Some(query.config_hash)
                && seal_data.generation == generation
                && sequence == Some(seal_data.sequence)
                && seal.sequence == seal_data.sequence,
            "owned query durable identity, generation or head does not match its seal"
        );
        ensure!(
            seal_data.cuts.len() == config.sources.len()
                && checkpoints.len() == config.sources.len(),
            "owned query durable source vector is incomplete"
        );
        for source in &config.sources {
            let cut = seal_data
                .cuts
                .get(&source.source_id)
                .context("owned query seal omits a configured source")?;
            let checkpoint = checkpoints
                .get(&source.source_id)
                .context("owned query checkpoint omits a configured source")?;
            ensure!(
                checkpoint.sequence == cut.upstream.unwrap_or(0)
                    && checkpoint.source_position.as_deref() == Some(cut.position.as_slice()),
                "owned query source checkpoint does not match its sealed cut"
            );
            ensure!(
                !cut.position.is_empty()
                    && cut.position.len() <= 65536
                    && (cut.upstream.is_some() == (cut.offset > 0)),
                "owned query cut has invalid position or offset"
            );
        }
        ensure!(
            rows.len() <= query.limits.snapshot_rows,
            super::OutputCapacityExceeded
        );
        let mut decoded_rows = BTreeMap::<u64, Value>::new();
        for (key, bytes) in rows {
            ensure!(
                bytes.len() <= query.limits.pending_bytes,
                super::OutputCapacityExceeded
            );
            let row = serde_json::from_slice(&bytes).context("invalid owned query live row")?;
            ensure!(
                decoded_rows.insert(key, row).is_none(),
                "duplicate durable live row signature"
            );
        }
        let mut outbox_bytes = BTreeMap::new();
        let mut decoded_outbox = Vec::new();
        let mut previous: Option<u64> = None;
        for (key, bytes) in outbox {
            ensure!(
                bytes.len() <= query.limits.pending_bytes,
                super::OutputCapacityExceeded
            );
            let result: QueryResult =
                serde_json::from_slice(&bytes).context("invalid owned query outbox JSON")?;
            ensure!(
                key > 0
                    && result.sequence == key
                    && result.query_id == config.id
                    && !result.results.is_empty()
                    && previous.is_none_or(|previous| previous.checked_add(1) == Some(key)),
                "owned query outbox is not a contiguous valid result history"
            );
            previous = Some(key);
            ensure!(
                outbox_bytes.insert(key, bytes).is_none(),
                "duplicate owned query outbox sequence"
            );
            decoded_outbox.push(Arc::new(result));
        }
        ensure!(
            previous == latest
                && ((seal_data.sequence == 0 && previous.is_none())
                    || previous == Some(seal_data.sequence))
                && decoded_outbox.len() <= query.capacity,
            "owned query outbox does not end at its sealed head"
        );
        ensure!(
            outbox_bytes
                .values()
                .try_fold(0usize, |total, bytes| total.checked_add(bytes.len()))
                .is_some_and(|total| total <= query.limits.pending_bytes),
            super::OutputCapacityExceeded
        );
        ensure!(
            digest(&decoded_rows)? == seal_data.rows_digest
                && digest(&outbox_bytes)? == seal_data.outbox_digest,
            "owned query durable rows or outbox do not match their seal"
        );
        let mut output = QueryOutputState::new(query.capacity);
        output.hydrate(
            decoded_rows.into_iter().collect(),
            decoded_outbox,
            seal_data.sequence,
            generation,
        );
        query.bound_output(&output)?;
        Ok(Some(State {
            output,
            cuts: seal_data.cuts,
            outbox_bytes,
        }))
    }

    pub async fn admit(&self, query: &OwnedQuery) -> Result<()> {
        // A failed owned seed may leave only this marker. An SDK config hash alone
        // cannot certify that its private bootstrap never populated the indexes.
        let bytes = Bytes::from(serde_json::to_vec(&(
            1u32,
            &query.base.config.id,
            query.config_hash,
        ))?);
        ensure!(
            bytes.len() <= query.limits.pending_bytes,
            "owned query admission byte limit exceeded"
        );
        let guard =
            drasi_core::interface::SessionGuard::begin(self.created.set.session_control.clone())
                .await?;
        self.checkpoints
            .stage_checkpoint(ADMISSION_KEY, 0, Some(&bytes))
            .await?;
        guard.commit().await?;
        self.checkpoints
            .write_config_hash(query.config_hash)
            .await?;
        Ok(())
    }

    pub async fn stage_seal(&self, query: &OwnedQuery, state: &State) -> Result<()> {
        let rows: BTreeMap<_, _> = state.output.clone_results().into_iter().collect();
        let seal = Seal {
            format: 1,
            query: query.base.config.id.clone(),
            config_hash: query.config_hash,
            generation: state.output.generation(),
            sequence: state.output.as_of_sequence(),
            cuts: state.cuts.clone(),
            rows_digest: digest(&rows)?,
            outbox_digest: digest(&state.outbox_bytes)?,
        };
        let bytes = Bytes::from(serde_json::to_vec(&seal)?);
        ensure!(
            bytes.len() <= query.limits.pending_bytes,
            "owned query seal exceeds byte limit"
        );
        self.checkpoints
            .stage_result_sequence(&query.base.config.id, seal.sequence)
            .await?;
        self.checkpoints
            .stage_checkpoint(SEAL_KEY, seal.sequence, Some(&bytes))
            .await?;
        Ok(())
    }

    pub async fn stage_cut(&self, source: &str, cut: &Cut) -> Result<()> {
        self.checkpoints
            .stage_checkpoint(
                source,
                cut.upstream.unwrap_or(0),
                Some(&Bytes::copy_from_slice(&cut.position)),
            )
            .await?;
        Ok(())
    }

    pub async fn stage_rows(&self, query: &str, diffs: &[ResultDiff]) -> Result<()> {
        let mut data = Vec::with_capacity(diffs.len());
        for diff in diffs {
            let (key, row) = match diff {
                ResultDiff::Add {
                    data,
                    row_signature,
                }
                | ResultDiff::Update {
                    after: data,
                    row_signature,
                    ..
                }
                | ResultDiff::Aggregation {
                    after: data,
                    row_signature,
                    ..
                } => (*row_signature, Some(serde_json::to_vec(data)?)),
                ResultDiff::Delete { row_signature, .. } => (*row_signature, None),
                ResultDiff::Noop => continue,
            };
            data.push((key, row));
        }
        let mutations: Vec<_> = data
            .iter()
            .map(|(row_signature, data)| RowMutation {
                row_signature: *row_signature,
                data: data.as_deref(),
            })
            .collect();
        if !mutations.is_empty() {
            self.live.apply_mutations(query, &mutations).await?;
        }
        Ok(())
    }

    pub async fn stage(
        &self,
        query: &OwnedQuery,
        state: &State,
        diffs: Vec<ResultDiff>,
        source: &str,
        cut: Option<Cut>,
    ) -> Result<Staged> {
        let mut next = state.clone();
        if let Some(cut) = cut {
            self.stage_cut(source, &cut).await?;
            next.cuts.insert(source.to_string(), cut);
        }
        let result = if diffs.is_empty() {
            None
        } else {
            let sequence = state
                .output
                .as_of_sequence()
                .checked_add(1)
                .context("owned query result sequence exhausted")?;
            let result = QueryResult::new(
                query.base.config.id.clone(),
                sequence,
                chrono::Utc::now(),
                diffs.clone(),
                [
                    ("source_id".to_string(), Value::String(source.to_string())),
                    (
                        "processed_by".to_string(),
                        Value::String("drasi-core".to_string()),
                    ),
                    ("result_count".to_string(), Value::from(diffs.len())),
                ]
                .into_iter()
                .collect(),
            );
            let bytes = serde_json::to_vec(&result)?;
            ensure!(
                bytes.len() <= query.limits.pending_bytes,
                super::OutputCapacityExceeded
            );
            let retain_from = sequence
                .saturating_sub(query.capacity as u64)
                .saturating_add(1);
            next.outbox_bytes
                .retain(|sequence, _| *sequence >= retain_from);
            next.outbox_bytes.insert(sequence, bytes.clone());
            ensure!(
                next.outbox_bytes
                    .values()
                    .try_fold(0usize, |total, bytes| total.checked_add(bytes.len()))
                    .is_some_and(|total| total <= query.limits.pending_bytes),
                super::OutputCapacityExceeded
            );
            next.output
                .apply_committed_sequence(sequence, &diffs, result.clone());
            query.bound_output(&next.output)?;
            self.outbox
                .append_and_trim(&query.base.config.id, sequence, &bytes, retain_from)
                .await?;
            self.stage_rows(&query.base.config.id, &diffs).await?;
            Some(result)
        };
        self.stage_seal(query, &next).await?;
        Ok(Staged {
            state: next,
            result,
        })
    }
}

fn digest(value: &impl Serialize) -> Result<Vec<u8>> {
    Ok(Sha256::digest(serde_json::to_vec(value)?).to_vec())
}

pub(super) fn index_error(error: anyhow::Error) -> IndexError {
    match error.downcast::<super::OutputCapacityExceeded>() {
        Ok(error) => IndexError::other(error),
        Err(error) => IndexError::other(std::io::Error::other(format!("{error:#}"))),
    }
}
