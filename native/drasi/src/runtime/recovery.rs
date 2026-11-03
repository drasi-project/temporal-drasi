use super::*;

#[cfg(test)]
mod tests;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Retirement {
    retired_definition_v1: Vec<u8>,
    instance_id: String,
    owner_epoch: u64,
    identity: String,
    retirement_id: String,
}

impl Retirement {
    fn validate(&self, config: &Config, identity: &str, epoch: u64) -> Result<()> {
        if self.retired_definition_v1 != config.durable_definition()?
            || self.instance_id != config.instance_id
            || self.owner_epoch == 0
            || self.owner_epoch != epoch
            || self.identity != identity
            || uuid::Uuid::parse_str(&self.retirement_id)
                .ok()
                .is_none_or(|id| id.is_nil() || id.to_string() != self.retirement_id)
        {
            return Err(Error::new(
                7,
                "retired native admission identity is invalid",
            ));
        }
        Ok(())
    }
}

fn retained_file(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(io)?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(Error::new(
            7,
            "retained native state must be a nonempty regular file",
        ));
    }
    Ok(())
}

async fn metadata(store: &dyn StateStoreProvider, config: &Config) -> Result<(Vec<u8>, String)> {
    let definition = store
        .get(CONTROL, "definition")
        .await
        .map_err(io)?
        .ok_or_else(|| Error::new(7, "retained native definition is missing"))?;
    let epoch = store.get(CONTROL, "owner_epoch").await.map_err(io)?;
    if epoch.as_deref() != Some(config.owner_epoch.to_be_bytes().as_slice()) {
        return Err(Error::new(7, "retained native owner epoch is invalid"));
    }
    let identity = store
        .get(CONTROL, "identity")
        .await
        .map_err(io)?
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .filter(|value| uuid::Uuid::parse_str(value).is_ok())
        .ok_or_else(|| Error::new(7, "retained native identity is missing or invalid"))?;
    let catalog: AdmissionLedger = serde_json::from_slice(
        &store
            .get(CONTROL, "catalog")
            .await
            .map_err(io)?
            .ok_or_else(|| Error::new(7, "retained native catalog is missing"))?,
    )
    .map_err(|_| Error::new(7, "retained native catalog is invalid"))?;
    catalog.validate(config)?;
    bindings::validate_recovery(store, config, &catalog).await?;
    Ok((definition, identity))
}

pub(super) async fn retire(config: &Config) -> Result<String> {
    let path = Path::new(&config.state_dir).join("state.redb");
    retained_file(&path)?;
    let store = RedbStateStoreProvider::new(path).map_err(io)?;
    let (definition, identity) = metadata(&store, config).await?;
    if definition != config.durable_definition()? {
        let retired: Retirement = serde_json::from_slice(&definition)
            .map_err(|_| Error::new(3, "retained native definition is incompatible"))?;
        retired.validate(config, &identity, config.owner_epoch)?;
        store.sync().await.map_err(io)?;
        return Ok(retired.retirement_id);
    }
    let retirement_id = uuid::Uuid::new_v4().to_string();
    let retired = Retirement {
        retired_definition_v1: definition,
        instance_id: config.instance_id.clone(),
        owner_epoch: config.owner_epoch,
        identity,
        retirement_id: retirement_id.clone(),
    };
    // Legacy open compares this value before constructing plugins or SDK runtimes.
    store
        .set(CONTROL, "definition", serde_json::to_vec(&retired)?)
        .await
        .map_err(io)?;
    store.sync().await.map_err(io)?;
    Ok(retirement_id)
}

pub(super) async fn is_isolated_retired_restore(
    store: &dyn StateStoreProvider,
    config: &Config,
    definition: &[u8],
) -> Result<bool> {
    let Some(source) = config.restore_from.as_ref() else {
        return Ok(false);
    };
    let retired: Retirement = serde_json::from_slice(definition)
        .map_err(|_| Error::new(3, "restored native definition is incompatible"))?;
    if retired.owner_epoch.checked_add(1) != Some(config.owner_epoch) {
        return Err(Error::new(
            3,
            "retired state requires an isolated successor epoch",
        ));
    }
    let mut predecessor = config.clone();
    predecessor.owner_epoch = retired.owner_epoch;
    let (_, identity) = metadata(store, &predecessor).await?;
    retired.validate(config, &identity, predecessor.owner_epoch)?;
    let manifest = checkpoint::read_manifest(Path::new(source))?;
    if manifest.owner_epoch != retired.owner_epoch
        || manifest.retirement_id.as_deref() != Some(&retired.retirement_id)
    {
        return Err(Error::new(
            7,
            "retired state does not match the recovery cut",
        ));
    }
    Ok(true)
}

impl Host {
    pub(super) async fn open_checkpoint_recovery(config: Config) -> Result<Self> {
        if !cfg!(any(target_os = "linux", target_os = "macos")) {
            return Err(Error::new(
                8,
                "checkpoint recovery requires supported local filesystem fencing",
            ));
        }
        if config.restore_from.is_some() {
            return Err(Error::new(
                1,
                "checkpoint recovery cannot restore or initialize state",
            ));
        }
        let directory = Path::new(&config.state_dir);
        if directory.canonicalize().map_err(io)? != directory
            || !std::fs::symlink_metadata(directory.join("owner.lock"))
                .map_err(io)?
                .is_file()
        {
            return Err(Error::new(
                7,
                "recovery requires the original regular native directory and lock",
            ));
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.join("owner.lock"))
            .map_err(io)?;
        lock.try_lock_exclusive()
            .map_err(|_| Error::new(3, "native directory is already owned by another runtime"))?;
        checkpoint::validate_tree(directory)?;
        retire(&config).await?;
        let identity = {
            let store = RedbStateStoreProvider::new(directory.join("state.redb")).map_err(io)?;
            let (_, identity) = metadata(&store, &config).await?;
            identity
        };
        let credential_graph = uuid::Uuid::parse_str(&identity).map_err(io)?;
        Ok(Self {
            config,
            core: None,
            store: None,
            sources: Mutex::new(BTreeMap::new()),
            managed_sources: Mutex::new(BTreeMap::new()),
            deferred: Mutex::new(BTreeSet::new()),
            plugins: None,
            handoffs: Mutex::new(()),
            binding_gates: Mutex::new(BTreeMap::new()),
            catalog: None,
            topology: Mutex::new(()),
            cleanup_failure: Mutex::new(None),
            shutdown: Shutdown::Closed,
            identity,
            credential_graph,
            _lock: Some(lock),
        })
    }
}
