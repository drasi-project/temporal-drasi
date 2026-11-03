use crate::protocol::*;
use drasi_host_sdk::{
    callbacks::{redacted_lifecycle_callback, redacted_log_callback},
    loader::load_plugin_from_path,
    registry::{
        matches_trusted_identity, CosignVerifier, LocalDirRegistry, LocalPluginInfo,
        OciRegistryClient, PluginMetadataJson, RegistryConfig, SignatureStatus, TrustedIdentity,
        VerificationConfig, VerificationResult,
    },
    LoadedPlugin,
};
use drasi_plugin_sdk::{
    descriptor::{
        BootstrapPluginDescriptor, IdentityProviderPluginDescriptor, ReactionPluginDescriptor,
        SecretStorePluginDescriptor, SourcePluginDescriptor,
    },
    ffi::metadata::{PluginMetadata, FFI_SDK_VERSION, TARGET_TRIPLE},
    SDK_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) mod managed;
#[cfg(test)]
mod postgres_tests;
mod schema;

pub struct Plugins {
    loaded: BTreeMap<String, ActivePlugin>,
    catalogs: Vec<CatalogConfig>,
    trust: Trust,
    directory: PathBuf,
    host_id: String,
    observations: BTreeMap<String, PluginObservation>,
}

struct ActivePlugin {
    reference: String,
    handle: Arc<LoadedPlugin>,
    descriptors: Vec<PluginDescriptor>,
}

// A process may initialize one immutable artifact for each plugin identity.
type Resident = Arc<OnceLock<Result<Arc<LoadedPlugin>>>>;
static RESIDENT: OnceLock<Mutex<BTreeMap<String, (String, Resident)>>> = OnceLock::new();

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledArtifact {
    reference: String,
    binary_sha256: String,
    filename: String,
    metadata: PluginMetadataJson,
    signer: Identity,
}

struct Staging(PathBuf);

impl Drop for Staging {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn io(error: impl std::fmt::Display) -> Error {
    Error::new(7, error.to_string())
}

fn valid_repository(value: &str) -> bool {
    let Some((host, repository)) = value.split_once('/') else {
        return false;
    };
    (host.contains('.') || host == "localhost" || host.starts_with("localhost:"))
        && host
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b".-:".contains(&b))
        && repository.split('/').all(|segment| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
        })
}

fn hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub fn validate_policy(plugin: &PluginConfig, trust: &Trust) -> Result<()> {
    identifier(&plugin.id)?;
    identifier(plugin.filename.split('.').next().unwrap_or_default())?;
    if plugin.filename.contains('/')
        || plugin.filename.contains('\\')
        || plugin.filename.len() > 200
        || !drasi_host_sdk::is_plugin_binary(&plugin.filename)
    {
        return Err(Error::new(
            1,
            "plugin filename must be a native library basename",
        ));
    }
    let (repository, digest) = plugin.reference.rsplit_once("@sha256:").ok_or_else(|| {
        Error::new(
            1,
            "plugin reference must be pinned by SHA-256 manifest digest",
        )
    })?;
    if !valid_repository(repository)
        || !hex_digest(digest)
        || !hex_digest(&plugin.binary_sha256)
        || plugin
            .local_catalog_path
            .as_ref()
            .is_some_and(|p| !Path::new(p).is_absolute())
    {
        return Err(Error::new(1, "invalid plugin SHA-256 digest"));
    }
    if !trust
        .registry_prefixes
        .iter()
        .any(|prefix| valid_repository(prefix) && repository.starts_with(&format!("{prefix}/")))
        || trust.identities.is_empty()
        || trust
            .identities
            .iter()
            .any(|identity| identity.issuer.is_empty() || identity.subject.is_empty())
    {
        return Err(Error::new(
            1,
            "plugin registry and signing identities must be explicitly trusted",
        ));
    }
    Ok(())
}

pub fn validate_catalogs(catalogs: &[CatalogConfig], trust: &Trust) -> Result<()> {
    let mut ids = BTreeSet::new();
    let mut releases = BTreeSet::new();
    let mut digests = BTreeSet::new();
    let mut count = 0;
    for catalog in catalogs {
        identifier(&catalog.id)?;
        if catalog.id == "startup" || !ids.insert(&catalog.id) || catalog.display_name.len() > 256 {
            return Err(Error::new(
                1,
                "duplicate, reserved or invalid catalog identity",
            ));
        }
        for release in &catalog.plugins {
            count += 1;
            validate_policy(&release.artifact, trust)?;
            if release.version.is_empty()
                || release.version.len() > 128
                || !release
                    .version
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b".+-_".contains(&b))
                || !releases.insert((&catalog.id, &release.artifact.id, &release.version))
                || !digests.insert(digest(&release.artifact))
            {
                return Err(Error::new(1, "duplicate or invalid catalog release"));
            }
        }
    }
    if catalogs.len() > 32 || count > 256 {
        return Err(Error::new(1, "configured catalog capacity exceeded"));
    }
    Ok(())
}

fn verified(status: SignatureStatus, identities: &[TrustedIdentity]) -> Result<Identity> {
    match status {
        SignatureStatus::Verified(result) if matches_trusted_identity(&result, identities) => {
            Ok(Identity {
                issuer: result.issuer,
                subject: result.subject,
            })
        }
        SignatureStatus::Verified(_) => Err(Error::new(
            3,
            "plugin signature has an untrusted signing identity",
        )),
        SignatureStatus::Unsigned => Err(Error::new(3, "unsigned native plugins are not allowed")),
        SignatureStatus::Tampered(_) => {
            Err(Error::new(3, "native plugin signature verification failed"))
        }
    }
}

fn compatible(metadata: &PluginMetadataJson) -> bool {
    metadata.sdk_version == SDK_VERSION
        && metadata.core_version == drasi_core::VERSION
        && metadata.lib_version == drasi_lib::VERSION
        && metadata.target_triple == TARGET_TRIPLE
}

fn check_binary(path: &Path, expected: &str) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(io)?;
    if !metadata.is_file() || metadata.len() > 256 << 20 {
        return Err(Error::new(
            3,
            "plugin cache entry is not a bounded regular file",
        ));
    }
    let bytes = std::fs::read(path).map_err(io)?;
    if format!("{:x}", Sha256::digest(&bytes)) != expected {
        return Err(Error::new(
            3,
            "plugin binary digest does not match the administrator's pin",
        ));
    }
    Ok(())
}

fn digest(plugin: &PluginConfig) -> String {
    plugin
        .reference
        .rsplit_once('@')
        .expect("validated plugin reference")
        .1
        .into()
}

impl Plugins {
    pub async fn open(
        config: &[PluginConfig],
        catalogs: &[CatalogConfig],
        trust: &Trust,
        directory: &Path,
        host_id: &str,
    ) -> Result<Self> {
        validate_catalogs(catalogs, trust)?;
        let mut ids = BTreeSet::new();
        for artifact in config {
            validate_policy(artifact, trust)?;
            if !ids.insert(&artifact.id) {
                return Err(Error::new(1, "duplicate startup plugin id"));
            }
        }
        let mut catalogs = catalogs.to_vec();
        if !config.is_empty() {
            catalogs.push(CatalogConfig {
                id: "startup".into(),
                display_name: "Server startup plugins".into(),
                plugins: config
                    .iter()
                    .map(|artifact| CatalogRelease {
                        version: digest(artifact),
                        artifact: artifact.clone(),
                    })
                    .collect(),
            });
        }
        let mut plugins = Self {
            loaded: BTreeMap::new(),
            catalogs,
            trust: trust.clone(),
            directory: directory.into(),
            host_id: host_id.into(),
            observations: BTreeMap::new(),
        };
        for artifact in config {
            let selection = PluginSelection {
                catalog_id: "startup".into(),
                plugin_id: artifact.id.clone(),
                version: digest(artifact),
            };
            plugins.install(&selection, &digest(artifact)).await?;
            plugins.load_digest(&digest(artifact)).await?;
        }
        Ok(plugins)
    }

    fn identities(&self) -> Vec<TrustedIdentity> {
        self.trust
            .identities
            .iter()
            .map(|identity| TrustedIdentity {
                issuer: identity.issuer.clone(),
                subject_pattern: identity.subject.clone(),
            })
            .collect()
    }

    fn client(&self) -> OciRegistryClient {
        let verifier = CosignVerifier::new(VerificationConfig {
            enabled: true,
            trusted_identities: self.identities(),
        });
        OciRegistryClient::with_verifier(RegistryConfig::default(), verifier)
    }

    pub fn catalogs(&self) -> Vec<Catalog> {
        self.catalogs
            .iter()
            .map(|c| Catalog {
                id: c.id.clone(),
                display_name: c.display_name.clone(),
            })
            .collect()
    }

    fn release(&self, selection: &PluginSelection) -> Result<CatalogRelease> {
        self.catalogs
            .iter()
            .find(|c| c.id == selection.catalog_id)
            .and_then(|c| {
                c.plugins.iter().find(|p| {
                    p.artifact.id == selection.plugin_id && p.version == selection.version
                })
            })
            .cloned()
            .ok_or_else(|| Error::new(2, "plugin release is not in a configured catalog"))
    }

    fn by_digest(&self, value: &str) -> Result<(PluginSelection, CatalogRelease)> {
        for catalog in &self.catalogs {
            for release in &catalog.plugins {
                if digest(&release.artifact) == value {
                    return Ok((
                        PluginSelection {
                            catalog_id: catalog.id.clone(),
                            plugin_id: release.artifact.id.clone(),
                            version: release.version.clone(),
                        },
                        release.clone(),
                    ));
                }
            }
        }
        Err(Error::new(
            2,
            "plugin digest is not in a configured catalog",
        ))
    }

    fn description(
        &self,
        selection: &PluginSelection,
        release: &CatalogRelease,
        metadata: Option<&PluginMetadataJson>,
    ) -> DiscoveredPlugin {
        DiscoveredPlugin {
            catalog_id: selection.catalog_id.clone(),
            id: release.artifact.id.clone(),
            version: release.version.clone(),
            digest: digest(&release.artifact),
            platform: metadata
                .map(|m| m.target_triple.clone())
                .unwrap_or_default(),
            abi_version: self
                .loaded
                .get(&release.artifact.id)
                .filter(|p| p.reference == release.artifact.reference)
                .map(|_| FFI_SDK_VERSION.into())
                .unwrap_or_default(),
            sdk_version: metadata.map(|m| m.sdk_version.clone()).unwrap_or_default(),
            core_version: metadata.map(|m| m.core_version.clone()).unwrap_or_default(),
            lib_version: metadata.map(|m| m.lib_version.clone()).unwrap_or_default(),
            descriptors: self
                .loaded
                .get(&release.artifact.id)
                .filter(|p| p.reference == release.artifact.reference)
                .map(|p| p.descriptors.clone())
                .unwrap_or_default(),
            compatible: metadata.is_some_and(|m| {
                compatible(m) && (selection.catalog_id == "startup" || m.version == release.version)
            }),
            message: String::new(),
        }
    }

    pub async fn inspect(&self, selection: &PluginSelection) -> Result<DiscoveredPlugin> {
        let release = self.release(selection)?;
        let metadata = match self.cached(&release.artifact) {
            Ok(Some(installed)) => Ok(installed.metadata),
            Ok(None) => self
                .client()
                .fetch_metadata(&release.artifact.reference)
                .await
                .map_err(Error::from),
            Err(error) => Err(error),
        };
        let mut plugin = self.description(selection, &release, metadata.as_ref().ok());
        plugin.message = match metadata {
            Err(_) => "configured artifact metadata could not be fetched".into(),
            Ok(_) if !plugin.compatible => format!(
                "require SDK {SDK_VERSION}, core {}, lib {} and target {TARGET_TRIPLE}; binary ABI {FFI_SDK_VERSION} is checked before initialization",
                drasi_core::VERSION, drasi_lib::VERSION
            ),
            Ok(_) => String::new(),
        };
        Ok(plugin)
    }

    fn selections(&self, catalog_id: &str, search: &str) -> Result<Vec<PluginSelection>> {
        if search.len() > 256 {
            return Err(Error::new(1, "plugin search exceeds 256 bytes"));
        }
        if !catalog_id.is_empty() && !self.catalogs.iter().any(|c| c.id == catalog_id) {
            return Err(Error::new(2, "catalog is not configured"));
        }
        let mut result = Vec::new();
        for catalog in &self.catalogs {
            if !catalog_id.is_empty() && catalog.id != catalog_id {
                continue;
            }
            for release in &catalog.plugins {
                if !release.artifact.id.contains(search) {
                    continue;
                }
                result.push(PluginSelection {
                    catalog_id: catalog.id.clone(),
                    plugin_id: release.artifact.id.clone(),
                    version: release.version.clone(),
                });
            }
        }
        result.sort_by(|a, b| {
            (&a.catalog_id, &a.plugin_id, &a.version).cmp(&(
                &b.catalog_id,
                &b.plugin_id,
                &b.version,
            ))
        });
        Ok(result)
    }

    pub async fn discover_page(
        &self,
        catalog_id: &str,
        search: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<DiscoveredPlugin>, bool)> {
        if limit > 200 {
            return Err(Error::new(1, "plugin page exceeds 200 releases"));
        }
        let selections = self.selections(catalog_id, search)?;
        if offset > selections.len() {
            return Err(Error::new(1, "plugin page offset exceeds the catalog"));
        }
        let end = selections.len().min(offset.saturating_add(if limit == 0 {
            selections.len()
        } else {
            limit
        }));
        let mut result = Vec::with_capacity(end - offset);
        for selection in &selections[offset..end] {
            result.push(self.inspect(selection).await?);
        }
        Ok((result, end < selections.len()))
    }

    fn cache_directory(&self, plugin: &PluginConfig) -> PathBuf {
        self.directory
            .join(digest(plugin))
            .join(&plugin.binary_sha256)
    }

    fn cached(&self, plugin: &PluginConfig) -> Result<Option<InstalledArtifact>> {
        let directory = self.cache_directory(plugin);
        if !directory.exists() {
            return Ok(None);
        }
        let record = directory.join("verified.json");
        let metadata = std::fs::symlink_metadata(&record).map_err(io)?;
        if !metadata.is_file() || metadata.len() > MAX_INPUT as u64 {
            return Err(Error::new(
                3,
                "plugin cache metadata exceeds the byte limit",
            ));
        }
        let bytes = std::fs::read(record).map_err(io)?;
        let installed: InstalledArtifact = serde_json::from_slice(&bytes)?;
        if installed.reference != plugin.reference
            || installed.filename != plugin.filename
            || installed.binary_sha256 != plugin.binary_sha256
            || !compatible(&installed.metadata)
            || !matches_trusted_identity(
                &VerificationResult {
                    issuer: installed.signer.issuer.clone(),
                    subject: installed.signer.subject.clone(),
                },
                &self.identities(),
            )
        {
            return Err(Error::new(
                3,
                "plugin cache does not match the configured artifact and trust policy",
            ));
        }
        check_binary(&directory.join(&plugin.filename), &plugin.binary_sha256)?;
        Ok(Some(installed))
    }

    fn observe(
        &mut self,
        plugin: DiscoveredPlugin,
        state: PluginHostState,
        message: &str,
        signer: Option<Identity>,
    ) -> PluginObservation {
        let observation = PluginObservation {
            plugin,
            host_id: self.host_id.clone(),
            state,
            message: message.into(),
            updated_at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            verified_signer_identity: signer,
        };
        self.observations
            .insert(observation.plugin.digest.clone(), observation.clone());
        observation
    }

    async fn install_artifact(
        &self,
        plugin: &PluginConfig,
        version: Option<&str>,
    ) -> Result<InstalledArtifact> {
        let client = self.client();
        let signer = verified(
            client
                .verifier()
                .verify_plugin(&plugin.reference, &client.auth())
                .await,
            &self.identities(),
        )?;
        if client
            .get_digest(&plugin.reference)
            .await
            .map_err(|_| Error::new(7, "configured artifact manifest could not be fetched"))?
            != digest(plugin)
        {
            return Err(Error::new(
                3,
                "registry manifest differs from the configured digest",
            ));
        }
        let metadata = client
            .fetch_metadata(&plugin.reference)
            .await
            .map_err(|_| Error::new(7, "configured artifact metadata could not be fetched"))?;
        if !compatible(&metadata) || version.is_some_and(|v| v != metadata.version) {
            return Err(Error::new(
                3,
                "incompatible plugin release, SDK, core, lib or target",
            ));
        }
        if let Some(mut installed) = self.cached(plugin)? {
            if serde_json::to_vec(&installed.metadata)? != serde_json::to_vec(&metadata)? {
                return Err(Error::new(
                    3,
                    "cached metadata differs from the signed artifact",
                ));
            }
            installed.signer = signer;
            return Ok(installed);
        }
        let staging = Staging(
            self.directory
                .join(format!(".install-{}", uuid::Uuid::new_v4())),
        );
        std::fs::create_dir_all(&staging.0).map_err(io)?;
        let downloaded = client
            .download_plugin(&plugin.reference, &staging.0, &plugin.filename)
            .await
            .map_err(|_| Error::new(7, "configured plugin download failed"))?;
        verified(downloaded.verification, &self.identities())?;
        check_binary(&downloaded.path, &plugin.binary_sha256)?;
        if let Some(local) = &plugin.local_catalog_path {
            let path = Path::new(local);
            check_binary(path, &plugin.binary_sha256)?;
            let parent = path
                .parent()
                .ok_or_else(|| Error::new(1, "invalid local catalog file"))?;
            // SDK directory discovery dlopens unverified files, so only its copy operation is safe here.
            LocalDirRegistry::new(parent)
                .install(
                    &LocalPluginInfo {
                        reference: plugin.id.clone(),
                        filename: plugin.filename.clone(),
                        version: metadata.version.clone(),
                        sdk_version: metadata.sdk_version.clone(),
                        file_path: path.into(),
                    },
                    &staging.0,
                )
                .map_err(|_| Error::new(7, "configured local mirror could not be installed"))?;
            check_binary(&downloaded.path, &plugin.binary_sha256)?;
        }
        let installed = InstalledArtifact {
            reference: plugin.reference.clone(),
            binary_sha256: plugin.binary_sha256.clone(),
            filename: plugin.filename.clone(),
            metadata,
            signer,
        };
        publish_cache(&staging.0, &self.cache_directory(plugin), &installed)?;
        Ok(installed)
    }

    pub async fn install(
        &mut self,
        selection: &PluginSelection,
        expected_digest: &str,
    ) -> Result<PluginObservation> {
        let release = self.release(selection)?;
        if digest(&release.artifact) != expected_digest {
            return Err(Error::new(
                3,
                "installation digest differs from the configured catalog release",
            ));
        }
        let version = (selection.catalog_id != "startup").then_some(release.version.as_str());
        match self.install_artifact(&release.artifact, version).await {
            Ok(installed) => {
                let plugin = self.description(selection, &release, Some(&installed.metadata));
                let state = if self
                    .loaded
                    .get(&release.artifact.id)
                    .is_some_and(|p| p.reference == release.artifact.reference)
                {
                    PluginHostState::Loaded
                } else {
                    PluginHostState::Installed
                };
                Ok(self.observe(plugin, state, "", Some(installed.signer)))
            }
            Err(error) => {
                let plugin = self.description(selection, &release, None);
                let state = if error.message.starts_with("incompatible plugin") {
                    PluginHostState::Incompatible
                } else {
                    PluginHostState::Failed
                };
                self.observe(plugin, state, &error.message, None);
                Err(error)
            }
        }
    }

    pub async fn load_digest(&mut self, value: &str) -> Result<PluginObservation> {
        let (selection, release) = self.by_digest(value)?;
        let result: Result<PluginObservation> = async {
            let installed = self
                .cached(&release.artifact)?
                .ok_or_else(|| Error::new(2, "plugin artifact is not installed"))?;
            if selection.catalog_id != "startup" && installed.metadata.version != release.version {
                return Err(Error::new(
                    3,
                    "signed artifact version differs from the configured release",
                ));
            }
            // A cache receipt is not a substitute for cryptographic verification at load time.
            let client = self.client();
            let signer = verified(
                client
                    .verifier()
                    .verify_plugin(&release.artifact.reference, &client.auth())
                    .await,
                &self.identities(),
            )?;
            let metadata = client
                .fetch_metadata(&release.artifact.reference)
                .await
                .map_err(|_| Error::new(7, "configured artifact metadata could not be fetched"))?;
            if !compatible(&metadata)
                || serde_json::to_vec(&metadata)? != serde_json::to_vec(&installed.metadata)?
            {
                return Err(Error::new(
                    3,
                    "incompatible plugin cache metadata differs from the signed artifact",
                ));
            }
            let path = self
                .cache_directory(&release.artifact)
                .join(&release.artifact.filename);
            let loaded = resident(&release.artifact, &path, &installed.metadata)?;
            // Validate and bound schemas before exposing the factories to this runtime.
            let descriptors = descriptors(&release.artifact.id, &loaded)?;
            self.loaded.insert(
                release.artifact.id.clone(),
                ActivePlugin {
                    reference: release.artifact.reference.clone(),
                    handle: loaded,
                    descriptors,
                },
            );
            let plugin = self.description(&selection, &release, Some(&installed.metadata));
            Ok(self.observe(plugin, PluginHostState::Loaded, "", Some(signer)))
        }
        .await;
        if let Err(error) = &result {
            let plugin = self.description(&selection, &release, None);
            let state = if error.message.contains("restart required") {
                PluginHostState::RestartRequired
            } else if error.message.starts_with("incompatible plugin") {
                PluginHostState::Incompatible
            } else {
                PluginHostState::Failed
            };
            self.observe(plugin, state, &error.message, None);
        }
        result
    }

    pub fn installation(&mut self, value: &str) -> Result<PluginObservation> {
        let (selection, release) = self.by_digest(value)?;
        // Check the bytes even when this process has an earlier successful observation.
        match self.cached(&release.artifact) {
            Ok(Some(installed)) => {
                if let Some(previous) = self.observations.get(value) {
                    return Ok(previous.clone());
                }
                let plugin = self.description(&selection, &release, Some(&installed.metadata));
                let state = if plugin.compatible {
                    PluginHostState::Installed
                } else {
                    PluginHostState::Incompatible
                };
                Ok(self.observe(plugin, state, "", Some(installed.signer)))
            }
            Ok(None) => {
                let previous = self.observations.get(value).cloned().ok_or_else(|| {
                    Error::new(2, "plugin artifact has no installation observation")
                })?;
                if matches!(
                    previous.state,
                    PluginHostState::Installed | PluginHostState::Loaded
                ) {
                    let plugin = self.description(&selection, &release, None);
                    return Ok(self.observe(
                        plugin,
                        PluginHostState::Failed,
                        "previously installed plugin cache is missing",
                        None,
                    ));
                }
                Ok(previous)
            }
            Err(_) => {
                let plugin = self.description(&selection, &release, None);
                Ok(self.observe(
                    plugin,
                    PluginHostState::Failed,
                    "plugin cache validation failed",
                    None,
                ))
            }
        }
    }

    pub fn installations(&mut self) -> Result<Vec<PluginObservation>> {
        let values: BTreeSet<_> = self
            .catalogs
            .iter()
            .flat_map(|c| c.plugins.iter().map(|p| digest(&p.artifact)))
            .collect();
        let mut result = Vec::new();
        for value in values {
            match self.installation(&value) {
                Ok(observation) => result.push(observation),
                Err(error) if error.code == 2 => {}
                Err(error) => return Err(error),
            }
        }
        Ok(result)
    }

    pub fn descriptors(&self) -> Result<Value> {
        let mut result = Vec::new();
        for plugin in self.loaded.values() {
            result.extend(plugin.descriptors.clone());
        }
        Ok(json!({ "sdk_abi": FFI_SDK_VERSION, "target": TARGET_TRIPLE, "descriptors": result }))
    }

    pub async fn source(
        &self,
        source: &SourceConfig,
        auto_start: bool,
    ) -> Result<Box<dyn drasi_lib::Source>> {
        let plugin = source
            .plugin
            .as_ref()
            .and_then(|id| self.loaded.get(id))
            .ok_or_else(|| Error::new(2, "source plugin is not installed"))?;
        let descriptor = plugin
            .handle
            .source_plugins
            .iter()
            .find(|descriptor| Some(descriptor.kind()) == source.plugin_kind.as_deref())
            .ok_or_else(|| {
                Error::new(2, "source descriptor is not provided by the pinned plugin")
            })?;
        let instance = descriptor
            .create_source(&source.id, &source.config, auto_start)
            .await
            .map_err(Error::from)?;
        if !instance.supports_replay() {
            return Err(Error::new(
                3,
                "workflow-triggering sources must support positional replay",
            ));
        }
        if let Some(bootstrap) = source.bootstrap.as_ref() {
            let plugin = self
                .loaded
                .get(&bootstrap.plugin)
                .ok_or_else(|| Error::new(2, "bootstrap plugin is not installed"))?;
            let descriptor = plugin
                .handle
                .bootstrap_plugins
                .iter()
                .find(|descriptor| descriptor.kind() == bootstrap.kind)
                .ok_or_else(|| {
                    Error::new(
                        2,
                        "bootstrap descriptor is not provided by the pinned plugin",
                    )
                })?;
            let provider = descriptor
                .create_bootstrap_provider(&bootstrap.config, &source.config)
                .await
                .map_err(Error::from)?;
            instance.set_bootstrap_provider(provider).await;
        }
        Ok(instance)
    }
}

fn publish_cache(staging: &Path, destination: &Path, installed: &InstalledArtifact) -> Result<()> {
    let binary = staging.join(&installed.filename);
    check_binary(&binary, &installed.binary_sha256)?;
    let record = staging.join("verified.json");
    std::fs::write(&record, serde_json::to_vec(installed)?).map_err(io)?;
    for path in [&binary, &record] {
        let file = std::fs::File::open(path).map_err(io)?;
        file.sync_all().map_err(io)?;
        let mut permissions = file.metadata().map_err(io)?.permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(path, permissions).map_err(io)?;
        file.sync_all().map_err(io)?;
    }
    std::fs::File::open(staging)
        .and_then(|d| d.sync_all())
        .map_err(io)?;
    std::fs::create_dir_all(destination.parent().expect("cache parent")).map_err(io)?;
    std::fs::File::open(
        destination
            .parent()
            .and_then(Path::parent)
            .expect("cache root"),
    )
    .and_then(|d| d.sync_all())
    .map_err(io)?;
    std::fs::rename(staging, destination).map_err(io)?;
    std::fs::File::open(destination.parent().expect("cache parent"))
        .and_then(|d| d.sync_all())
        .map_err(io)?;
    Ok(())
}

fn resident(
    plugin: &PluginConfig,
    path: &Path,
    metadata: &PluginMetadataJson,
) -> Result<Arc<LoadedPlugin>> {
    let slot = {
        let mut residents = RESIDENT
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let fingerprint = format!("{}:{}", plugin.reference, plugin.binary_sha256);
        let (pin, slot) = residents
            .entry(plugin.id.clone())
            .or_insert_with(|| (fingerprint.clone(), Arc::new(OnceLock::new())));
        if pin != &fingerprint {
            return Err(Error::new(
                3,
                "plugin replacement: process restart required before initialization",
            ));
        }
        slot.clone()
    };
    slot.get_or_init(|| {
        check_binary(path, &plugin.binary_sha256)?;
        // Pin even rejected libraries after dlopen; native TLS destructors must never outlive their code.
        let library = unsafe { libloading::Library::new(path) }
            .map_err(|_| Error::new(7, "verified plugin library could not be opened"))?;
        let library = Box::leak(Box::new(library));
        let metadata_fn = unsafe {
            library.get::<unsafe extern "C" fn() -> *const PluginMetadata>(b"drasi_plugin_metadata")
        }
        .map_err(|_| Error::new(3, "plugin does not export ABI metadata"))?;
        let pointer = unsafe { metadata_fn() };
        if pointer.is_null() {
            return Err(Error::new(3, "plugin returned null ABI metadata"));
        }
        let binary_metadata = unsafe { &*pointer };
        let (sdk, core, lib, target, version) = unsafe {
            (binary_metadata.sdk_version.to_string(), binary_metadata.core_version.to_string(),
            binary_metadata.lib_version.to_string(), binary_metadata.target_triple.to_string(),
            binary_metadata.plugin_version.to_string())
        };
        if sdk != FFI_SDK_VERSION || core != metadata.core_version || lib != metadata.lib_version
            || target != metadata.target_triple || version != metadata.version {
            return Err(Error::new(
                3,
                format!("incompatible plugin binary metadata: ABI {sdk} (require {FFI_SDK_VERSION}), core {core} (signed {}), lib {lib} (signed {}), target {target} (signed {}), version {version} (signed {})", metadata.core_version, metadata.lib_version, metadata.target_triple, metadata.version),
            ));
        }
        let plugin_handle = load_plugin_from_path(
            path,
            std::ptr::null_mut(),
            redacted_log_callback,
            std::ptr::null_mut(),
            redacted_lifecycle_callback,
        )
        .map_err(|_| {
            Error::new(
                7,
                "verified plugin could not be initialized by the host SDK",
            )
        })?;
        crate::credentials::install(&plugin_handle, &plugin.binary_sha256, &digest(plugin))?;
        Ok(Arc::new(plugin_handle))
    })
    .clone()
}

fn descriptors(id: &str, plugin: &LoadedPlugin) -> Result<Vec<PluginDescriptor>> {
    let mut result = Vec::new();
    macro_rules! describe {
        ($plugin:expr, $id:expr, $field:ident, $category:expr, $supported:expr) => {
            for descriptor in &$plugin.$field {
                let schema = descriptor.config_schema_json();
                if schema.len() > 64 << 10 || result.len() >= 128 {
                    return Err(Error::new(3, "plugin descriptor capacity exceeded"));
                }
                let schema: Value = serde_json::from_str(&schema)?;
                if !schema.is_object() {
                    return Err(Error::new(
                        3,
                        "plugin configuration schema must be a JSON object",
                    ));
                }
                result.push(PluginDescriptor {
                    plugin: $id.into(),
                    component: descriptor.kind().into(),
                    category: $category,
                    config_version: descriptor.config_version().into(),
                    schema_name: descriptor.config_schema_name().into(),
                    configuration_schema: schema,
                    supported: $supported,
                });
            }
        };
    }
    describe!(plugin, id, source_plugins, PluginCategory::Source, true);
    describe!(
        plugin,
        id,
        bootstrap_plugins,
        PluginCategory::Bootstrap,
        true
    );
    describe!(
        plugin,
        id,
        reaction_plugins,
        PluginCategory::Reaction,
        false
    );
    describe!(
        plugin,
        id,
        identity_provider_plugins,
        PluginCategory::Identity,
        false
    );
    describe!(
        plugin,
        id,
        secret_store_plugins,
        PluginCategory::SecretStore,
        false
    );
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Workspace(PathBuf);

    impl Workspace {
        fn new() -> Self {
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join(".verification")
                .join(format!("plugins-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Workspace {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn trust() -> Trust {
        Trust {
            registry_prefixes: vec!["registry.example/trusted".into()],
            identities: vec![Identity {
                issuer: "issuer".into(),
                subject: "subject".into(),
            }],
        }
    }

    fn artifact() -> PluginConfig {
        PluginConfig {
            id: "test-plugin".into(),
            reference: format!(
                "registry.example/trusted/source/test@sha256:{}",
                "a".repeat(64)
            ),
            filename: format!("libdrasi_test{}", std::env::consts::DLL_SUFFIX),
            binary_sha256: format!("{:x}", Sha256::digest(b"test-binary")),
            local_catalog_path: None,
        }
    }

    fn catalog(plugin: PluginConfig) -> CatalogConfig {
        CatalogConfig {
            id: "trusted".into(),
            display_name: "Trusted plugins".into(),
            plugins: vec![CatalogRelease {
                version: "1.0.0".into(),
                artifact: plugin,
            }],
        }
    }

    fn selection() -> PluginSelection {
        PluginSelection {
            catalog_id: "trusted".into(),
            plugin_id: "test-plugin".into(),
            version: "1.0.0".into(),
        }
    }

    fn metadata() -> PluginMetadataJson {
        PluginMetadataJson {
            name: "test".into(),
            kind: "test".into(),
            plugin_type: "source".into(),
            version: "1.0.0".into(),
            sdk_version: SDK_VERSION.into(),
            core_version: drasi_core::VERSION.into(),
            lib_version: drasi_lib::VERSION.into(),
            target_triple: TARGET_TRIPLE.into(),
            description: None,
            license: None,
        }
    }

    // Synthetic receipts exercise cache invariants, not signature verification.
    fn seed_cache(plugins: &Plugins, artifact: &PluginConfig, bytes: &[u8]) -> PathBuf {
        let staging = Staging(
            plugins
                .directory
                .join(format!(".test-{}", uuid::Uuid::new_v4())),
        );
        std::fs::create_dir_all(&staging.0).unwrap();
        std::fs::write(staging.0.join(&artifact.filename), bytes).unwrap();
        let installed = InstalledArtifact {
            reference: artifact.reference.clone(),
            binary_sha256: artifact.binary_sha256.clone(),
            filename: artifact.filename.clone(),
            metadata: metadata(),
            signer: Identity {
                issuer: "issuer".into(),
                subject: "subject".into(),
            },
        };
        let destination = plugins.cache_directory(artifact);
        publish_cache(&staging.0, &destination, &installed).unwrap();
        destination
    }

    #[test]
    fn catalog_policy_rejects_unpinned_untrusted_and_ambiguous_releases() {
        let plugin = artifact();
        assert!(validate_policy(&plugin, &trust()).is_ok());
        for reference in [
            "registry.example/trusted/source/test:latest".to_string(),
            format!(
                "registry.example/trusted-evil/source/test@sha256:{}",
                "a".repeat(64)
            ),
            format!("registry.example/trusted/../evil@sha256:{}", "a".repeat(64)),
            format!(
                "https://registry.example/trusted/test@sha256:{}",
                "a".repeat(64)
            ),
            format!("registry.example/trusted/test@sha256:{}", "A".repeat(64)),
        ] {
            assert!(validate_policy(
                &PluginConfig {
                    reference,
                    ..plugin.clone()
                },
                &trust()
            )
            .is_err());
        }
        for filename in ["../libdrasi_test.so", "nested/libdrasi_test.so", "test.txt"] {
            assert!(validate_policy(
                &PluginConfig {
                    filename: filename.into(),
                    ..plugin.clone()
                },
                &trust()
            )
            .is_err());
        }
        assert!(validate_policy(&plugin, &Trust::default()).is_err());
        assert!(validate_policy(
            &PluginConfig {
                local_catalog_path: Some("relative.so".into()),
                ..plugin.clone()
            },
            &trust()
        )
        .is_err());
        let c = catalog(plugin);
        assert!(validate_catalogs(std::slice::from_ref(&c), &trust()).is_ok());
        assert!(validate_catalogs(&[c.clone(), c.clone()], &trust()).is_err());
        assert!(validate_catalogs(
            &[CatalogConfig {
                id: "startup".into(),
                ..c
            }],
            &trust()
        )
        .is_err());
    }

    #[test]
    fn compatibility_is_checked_for_every_declared_boundary() {
        assert!(compatible(&metadata()));
        assert_ne!(SDK_VERSION, FFI_SDK_VERSION);
        let mut abi_as_release = metadata();
        abi_as_release.sdk_version = FFI_SDK_VERSION.into();
        assert!(!compatible(&abi_as_release));
        for field in [
            "sdk_version",
            "core_version",
            "lib_version",
            "target_triple",
        ] {
            let mut value = serde_json::to_value(metadata()).unwrap();
            value[field] = json!("incompatible");
            let metadata: PluginMetadataJson = serde_json::from_value(value).unwrap();
            assert!(!compatible(&metadata), "{field}");
        }
    }

    #[tokio::test]
    async fn catalog_commands_cannot_add_locations_or_policy() {
        let workspace = Workspace::new();
        let plugin = artifact();
        let mut plugins = Plugins::open(
            &[],
            &[catalog(plugin.clone())],
            &trust(),
            &workspace.0,
            "host",
        )
        .await
        .unwrap();
        assert_eq!(plugins.catalogs().len(), 1);
        assert_eq!(
            plugins
                .discover_page("unknown", "", 0, 0)
                .await
                .unwrap_err()
                .code,
            2
        );
        assert!(plugins
            .discover_page("trusted", "missing", 0, 0)
            .await
            .unwrap()
            .0
            .is_empty());
        let mut unknown = selection();
        unknown.plugin_id = "../arbitrary".into();
        assert_eq!(plugins.inspect(&unknown).await.unwrap_err().code, 2);
        assert_eq!(
            plugins
                .install(&selection(), &format!("sha256:{}", "b".repeat(64)))
                .await
                .unwrap_err()
                .code,
            3
        );
        assert_eq!(
            plugins
                .load_digest(&digest(&plugin))
                .await
                .unwrap_err()
                .code,
            2
        );
        let observed = plugins.installation(&digest(&plugin)).unwrap();
        assert_eq!(observed.state, PluginHostState::Failed);
        assert!(observed.verified_signer_identity.is_none());
        for op in [
            json!({"op":"discover_plugins","registry_prefix":"attacker.example/plugins"}),
            json!({"op":"install_plugin","selection":selection(),"digest":digest(&plugin),"path":"/arbitrary"}),
            json!({"op":"load_plugin","digest":digest(&plugin),"trust":trust()}),
        ] {
            assert!(serde_json::from_value::<Command>(op).is_err());
        }
        assert!(plugins.loaded.is_empty());
    }

    #[tokio::test]
    async fn discovery_selects_a_page_before_metadata_io() {
        let workspace = Workspace::new();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let registry = format!("{}/trusted", listener.local_addr().unwrap());
        let mut trust = trust();
        trust.registry_prefixes = vec![registry.clone()];
        let mut cached = artifact();
        cached.reference = format!("{registry}/source/test@sha256:{}", "a".repeat(64));
        let mut uncached = cached.clone();
        uncached.id = "z-uncached".into();
        uncached.reference = format!("{registry}/source/other@sha256:{}", "b".repeat(64));
        let mut catalog = catalog(uncached);
        catalog.plugins.push(CatalogRelease {
            version: "1.0.0".into(),
            artifact: cached.clone(),
        });
        let plugins = Plugins::open(&[], &[catalog], &trust, &workspace.0, "host")
            .await
            .unwrap();
        seed_cache(&plugins, &cached, b"test-binary");
        let (page, more) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            plugins.discover_page("trusted", "", 0, 1),
        )
        .await
        .expect("discovery fetched an excluded release")
        .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id, cached.id);
        assert!(page[0].compatible);
        assert!(more);
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(plugins
            .discover_page("trusted", "", 2, 1)
            .await
            .unwrap()
            .0
            .is_empty());
        for (offset, limit) in [(0, 201), (3, 1), (usize::MAX, 1)] {
            assert_eq!(
                plugins
                    .discover_page("trusted", "", offset, limit)
                    .await
                    .unwrap_err()
                    .code,
                1
            );
        }
        assert!(plugins.loaded.is_empty());
    }

    #[tokio::test]
    async fn cache_is_immutable_revalidated_and_observed_after_reopen() {
        let workspace = Workspace::new();
        let artifact = artifact();
        let catalogs = [catalog(artifact.clone())];
        let mut plugins = Plugins::open(&[], &catalogs, &trust(), &workspace.0, "host")
            .await
            .unwrap();
        assert!(plugins.installations().unwrap().is_empty());
        let directory = seed_cache(&plugins, &artifact, b"test-binary");
        let observation = plugins.installation(&digest(&artifact)).unwrap();
        assert_eq!(observation.state, PluginHostState::Installed);
        assert!(observation.plugin.compatible);
        assert!(observation.plugin.descriptors.is_empty());
        assert!(plugins.loaded.is_empty());
        let discovered = plugins
            .discover_page("trusted", "test", 0, 0)
            .await
            .unwrap()
            .0;
        assert_eq!(discovered.len(), 1);
        assert!(discovered[0].compatible);
        assert_eq!(discovered[0].sdk_version, SDK_VERSION);
        assert!(discovered[0].abi_version.is_empty());
        let bytes = std::fs::read(directory.join("verified.json")).unwrap();
        assert!(plugins.cached(&artifact).unwrap().is_some());
        let duplicate = Staging(workspace.0.join(".duplicate"));
        std::fs::create_dir_all(&duplicate.0).unwrap();
        std::fs::write(duplicate.0.join(&artifact.filename), b"test-binary").unwrap();
        assert!(publish_cache(
            &duplicate.0,
            &directory,
            &plugins.cached(&artifact).unwrap().unwrap()
        )
        .is_err());
        assert_eq!(
            std::fs::read(directory.join("verified.json")).unwrap(),
            bytes
        );
        drop(plugins);
        let mut reopened = Plugins::open(&[], &catalogs, &trust(), &workspace.0, "host-reopened")
            .await
            .unwrap();
        assert_eq!(
            reopened.installations().unwrap()[0].state,
            PluginHostState::Installed
        );
        let binary = directory.join(&artifact.filename);
        std::fs::remove_file(&binary).unwrap();
        std::fs::write(&binary, b"corrupted").unwrap();
        assert_eq!(
            reopened.installation(&digest(&artifact)).unwrap().state,
            PluginHostState::Failed
        );
        assert_eq!(
            reopened
                .load_digest(&digest(&artifact))
                .await
                .unwrap_err()
                .code,
            3
        );
        assert!(reopened.loaded.is_empty());
    }

    #[tokio::test]
    async fn missing_cache_cannot_replay_an_installed_success_observation() {
        let workspace = Workspace::new();
        let artifact = artifact();
        let mut plugins = Plugins::open(
            &[],
            &[catalog(artifact.clone())],
            &trust(),
            &workspace.0,
            "host",
        )
        .await
        .unwrap();
        let directory = seed_cache(&plugins, &artifact, b"test-binary");
        assert_eq!(
            plugins.installation(&digest(&artifact)).unwrap().state,
            PluginHostState::Installed
        );
        std::fs::remove_dir_all(directory).unwrap();
        let observed = plugins.installation(&digest(&artifact)).unwrap();
        assert_eq!(observed.state, PluginHostState::Failed);
        assert!(observed.verified_signer_identity.is_none());
    }

    #[tokio::test]
    async fn changed_signer_policy_does_not_trust_a_cache_receipt() {
        let workspace = Workspace::new();
        let artifact = artifact();
        let catalogs = [catalog(artifact.clone())];
        let plugins = Plugins::open(&[], &catalogs, &trust(), &workspace.0, "host")
            .await
            .unwrap();
        seed_cache(&plugins, &artifact, b"test-binary");
        let mut stricter = trust();
        stricter.identities[0].subject = "different-subject".into();
        let mut plugins = Plugins::open(&[], &catalogs, &stricter, &workspace.0, "host")
            .await
            .unwrap();
        assert_eq!(
            plugins
                .load_digest(&digest(&artifact))
                .await
                .unwrap_err()
                .code,
            3
        );
        let observed = plugins.installation(&digest(&artifact)).unwrap();
        assert_eq!(observed.state, PluginHostState::Failed);
        assert!(observed.verified_signer_identity.is_none());
    }

    #[tokio::test]
    async fn sdk_loader_is_idempotent_resident_and_returns_real_source_and_bootstrap_schemas() {
        let fixture = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .join(format!(
                "{}temporal_drasi_test_plugin{}",
                std::env::consts::DLL_PREFIX,
                std::env::consts::DLL_SUFFIX
            ));
        let bytes = std::fs::read(&fixture).expect("Cargo builds the fixture as a dev dependency");
        let artifact = PluginConfig {
            id: "resident-success-fixture".into(),
            binary_sha256: format!("{:x}", Sha256::digest(bytes)),
            ..artifact()
        };
        let library = Box::leak(Box::new(
            unsafe { libloading::Library::new(&fixture) }.unwrap(),
        ));
        let calls =
            unsafe { library.get::<extern "C" fn() -> u64>(b"temporal_test_descriptor_calls") }
                .unwrap();
        let before = calls();
        let mut wrong_metadata = metadata();
        wrong_metadata.core_version = "invalid".into();
        let rejected = PluginConfig {
            id: "resident-mismatch-fixture".into(),
            ..artifact.clone()
        };
        assert!(resident(&rejected, &fixture, &wrong_metadata)
            .err()
            .unwrap()
            .message
            .starts_with("incompatible plugin binary metadata"));
        assert_eq!(calls(), before);
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let barrier = barrier.clone();
                let artifact = artifact.clone();
                let fixture = fixture.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    resident(&artifact, &fixture, &metadata())
                })
            })
            .collect();
        let handles: Vec<_> = workers
            .into_iter()
            .map(|w| w.join().unwrap().unwrap())
            .collect();
        let first = handles[0].clone();
        assert!(handles.iter().all(|h| Arc::ptr_eq(h, &first)));
        let second = resident(&artifact, &fixture, &metadata()).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        let descriptors = descriptors(&artifact.id, &first).unwrap();
        assert_eq!(descriptors.len(), 2);
        assert_eq!(descriptors[0].category, PluginCategory::Source);
        assert_eq!(descriptors[0].component, "test-source");
        assert_eq!(
            descriptors[0].configuration_schema["source.test.Config"]["properties"]["connection"]
                ["x-ui"],
            json!({"secret":true})
        );
        assert_eq!(descriptors[1].category, PluginCategory::Bootstrap);
        assert_eq!(descriptors[1].component, "test-bootstrap");
        assert!(descriptors.iter().all(|d| d.supported));
        let configuration = json!({"connection":"literal-connection"});
        let plugins = Plugins {
            loaded: BTreeMap::from([(
                artifact.id.clone(),
                ActivePlugin {
                    reference: artifact.reference.clone(),
                    handle: first.clone(),
                    descriptors,
                },
            )]),
            catalogs: Vec::new(),
            trust: Trust::default(),
            directory: PathBuf::new(),
            host_id: "fixture-host".into(),
            observations: BTreeMap::new(),
        };
        let source_config = SourceConfig {
            id: "managed-fixture".into(),
            kind: SourceKind::Plugin,
            plugin: Some(artifact.id.clone()),
            plugin_kind: Some("test-source".into()),
            config: configuration.clone(),
            bootstrap: None,
        };
        let automatic = plugins.source(&source_config, true).await.unwrap();
        assert!(automatic.auto_start());
        assert_eq!(
            automatic.status().await,
            drasi_lib::channels::ComponentStatus::Stopped
        );
        drop(automatic);
        let source = plugins
            .source(&source_config, false)
            .await
            .expect("the fixture must construct a real Source across the plugin ABI");
        assert!(!source.auto_start());
        assert!(source.supports_replay());
        assert_eq!(
            source.status().await,
            drasi_lib::channels::ComponentStatus::Stopped
        );
        assert_eq!(
            serde_json::to_value(source.properties()).unwrap(),
            configuration
        );
        let provider = first.bootstrap_plugins[0]
            .create_bootstrap_provider(&json!({}), &configuration)
            .await
            .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let result = provider
            .bootstrap(
                drasi_lib::bootstrap::BootstrapRequest {
                    query_id: "fixture-query".into(),
                    node_labels: Vec::new(),
                    relation_labels: Vec::new(),
                    request_id: "fixture-bootstrap".into(),
                },
                &drasi_lib::bootstrap::BootstrapContext::new_minimal(
                    "fixture-host".into(),
                    source.id().into(),
                ),
                sender,
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.event_count, 0);
        assert_eq!(
            result.source_position.as_deref(),
            Some([0_u8; 8].as_slice())
        );
        assert!(receiver.recv().await.is_none());
        source.set_bootstrap_provider(provider).await;
        let core = drasi_lib::DrasiLib::builder()
            .with_id("managed-plugin-fixture")
            .with_source(source)
            .build()
            .await
            .unwrap();
        core.start().await.unwrap();
        let source = {
            let graph = core.component_graph();
            let graph = graph.read().await;
            graph
                .get_runtime::<Arc<dyn drasi_lib::Source>>("managed-fixture")
                .unwrap()
                .clone()
        };
        assert!(matches!(
            source.status().await,
            drasi_lib::channels::ComponentStatus::Added
                | drasi_lib::channels::ComponentStatus::Stopped
        ));
        core.start_source("managed-fixture").await.unwrap();
        assert_eq!(
            source.status().await,
            drasi_lib::channels::ComponentStatus::Running
        );
        core.stop_source("managed-fixture").await.unwrap();
        assert_eq!(
            source.status().await,
            drasi_lib::channels::ComponentStatus::Stopped
        );
        core.shutdown().await.unwrap();
        let other_fixture = fixture.with_file_name(format!(
            "{}temporal_drasi_other_test_plugin{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX,
        ));
        let other_artifact = PluginConfig {
            id: "resident-other-fixture".into(),
            reference: format!("registry.example/trusted/other@sha256:{}", "c".repeat(64)),
            binary_sha256: format!(
                "{:x}",
                Sha256::digest(std::fs::read(&other_fixture).unwrap())
            ),
            ..artifact.clone()
        };
        let other = resident(&other_artifact, &other_fixture, &metadata()).unwrap();
        crate::credentials::tests::exercise_real_plugin(
            &first,
            &digest(&artifact),
            &other,
            &digest(&other_artifact),
            || {
                resident(
                    &PluginConfig {
                        id: "resident-alias-fixture".into(),
                        ..artifact.clone()
                    },
                    &fixture,
                    &metadata(),
                )
                .unwrap()
            },
        )
        .await;
        let other_library = Box::leak(Box::new(
            unsafe { libloading::Library::new(&other_fixture) }.unwrap(),
        ));
        let source_calls =
            unsafe { library.get::<extern "C" fn() -> u64>(b"temporal_test_source_calls") }
                .unwrap();
        let bootstrap_calls = unsafe {
            other_library.get::<extern "C" fn() -> u64>(b"temporal_test_bootstrap_calls")
        }
        .unwrap();
        crate::runtime::plugin_tests::exercise(
            || Plugins {
                loaded: BTreeMap::from([
                    (
                        artifact.id.clone(),
                        ActivePlugin {
                            reference: artifact.reference.clone(),
                            handle: first.clone(),
                            descriptors: super::descriptors(&artifact.id, &first).unwrap(),
                        },
                    ),
                    (
                        other_artifact.id.clone(),
                        ActivePlugin {
                            reference: other_artifact.reference.clone(),
                            handle: other.clone(),
                            descriptors: super::descriptors(&other_artifact.id, &other).unwrap(),
                        },
                    ),
                ]),
                catalogs: Vec::new(),
                trust: Trust::default(),
                directory: PathBuf::new(),
                host_id: "managed-fixture-host".into(),
                observations: BTreeMap::new(),
            },
            &digest(&artifact),
            &digest(&other_artifact),
            || (source_calls(), bootstrap_calls()),
        )
        .await;
        let replacement = PluginConfig {
            reference: format!(
                "registry.example/trusted/source/test@sha256:{}",
                "b".repeat(64)
            ),
            ..artifact
        };
        assert!(resident(&replacement, &fixture, &metadata())
            .err()
            .unwrap()
            .message
            .contains("restart required"));
    }

    #[test]
    fn signature_policy_is_fail_closed() {
        let identities = vec![TrustedIdentity {
            issuer: "issuer".into(),
            subject_pattern: "subject".into(),
        }];
        assert!(verified(SignatureStatus::Unsigned, &identities).is_err());
        assert!(verified(SignatureStatus::Tampered("bad".into()), &identities).is_err());
        assert!(verified(
            SignatureStatus::Verified(drasi_host_sdk::registry::VerificationResult {
                issuer: "issuer".into(),
                subject: "attacker".into(),
            }),
            &identities
        )
        .is_err());
        assert!(verified(
            SignatureStatus::Verified(drasi_host_sdk::registry::VerificationResult {
                issuer: "issuer".into(),
                subject: "subject".into(),
            }),
            &identities
        )
        .is_ok());
    }
}
