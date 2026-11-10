use anyhow::{ensure, Context, Result};
use drasi_lib::StateStoreProvider;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

pub(super) const PARTITION: &str = "temporal-owned-query-commit-witness-v1";

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Uncommitted,
    CommitMayExist,
    Committed,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Witness {
    format: u32,
    query: String,
    config_hash: u64,
    phase: Phase,
}

pub(super) struct Admission {
    store: Arc<dyn StateStoreProvider>,
    key: String,
    query: String,
    config_hash: u64,
    expected_existing: bool,
    phase: Mutex<Option<Phase>>,
}

impl Admission {
    pub async fn open(
        store: Arc<dyn StateStoreProvider>,
        query: &str,
        config_hash: u64,
        expected_existing: bool,
        max_bytes: usize,
    ) -> Result<Arc<Self>> {
        let key = serde_json::to_string(&(query, config_hash))?;
        let saved = store.get(PARTITION, &key).await?;
        let phase = if let Some(bytes) = saved {
            ensure!(
                bytes.len() <= max_bytes,
                "owned query outer admission exceeds byte limit"
            );
            let witness: Witness =
                serde_json::from_slice(&bytes).context("invalid owned query outer admission")?;
            ensure!(
                witness.format == 1 && witness.query == query && witness.config_hash == config_hash,
                "owned query outer admission does not match query identity"
            );
            Some(witness.phase)
        } else {
            None
        };
        Ok(Arc::new(Self {
            store,
            key,
            query: query.to_string(),
            config_hash,
            expected_existing,
            phase: Mutex::new(phase),
        }))
    }

    pub async fn requires_seal(&self) -> bool {
        match *self.phase.lock().await {
            Some(Phase::Uncommitted) => false,
            Some(Phase::CommitMayExist | Phase::Committed) => true,
            None => self.expected_existing,
        }
    }

    pub async fn before_commit(&self) -> Result<()> {
        self.write(Phase::CommitMayExist).await
    }

    pub async fn uncommitted(&self) -> Result<()> {
        if self.phase.lock().await.is_none() {
            ensure!(
                !self.expected_existing,
                "existing query cannot be admitted as new"
            );
            self.write(Phase::Uncommitted).await?;
        }
        Ok(())
    }

    pub async fn observe_seal(&self) -> Result<()> {
        if !self.requires_seal().await {
            self.before_commit().await?;
        }
        Ok(())
    }

    pub async fn committed(&self) -> Result<()> {
        self.write(Phase::Committed).await
    }

    async fn write(&self, phase: Phase) -> Result<()> {
        let bytes = serde_json::to_vec(&Witness {
            format: 1,
            query: self.query.clone(),
            config_hash: self.config_hash,
            phase,
        })?;
        // Keep the conservative phase even if the outer store's write result is uncertain.
        *self.phase.lock().await = Some(phase);
        self.store.set(PARTITION, &self.key, bytes).await?;
        Ok(())
    }
}
