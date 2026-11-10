use super::{
    companion, schema,
    worker::{TrustedArtifact, WorkerSource, WorkerSpec},
    Plugins,
};
use crate::{
    credentials::{AdmissionValues, CredentialLease, CredentialScope},
    protocol::*,
};
use std::path::PathBuf;

pub(crate) type ManagedSource = WorkerSource;

pub(crate) struct ManagedInstance {
    source: Option<ManagedSource>,
    lease: Option<CredentialLease>,
}

impl ManagedInstance {
    pub(crate) fn source(&self) -> ManagedSource {
        self.source.as_ref().expect("owned managed source").clone()
    }

    pub(crate) async fn close(mut self) -> Result<()> {
        let source = self.source.as_ref().expect("owned managed source");
        let result = source.close().await;
        if !source.is_joined() {
            return Err(Error::new(3, "managed source worker cleanup is unproved"));
        }
        self.source.take();
        self.lease
            .take()
            .expect("owned credential lease")
            .release_after_join()?;
        result.map_err(|_| Error::new(3, "managed source worker failed during cleanup"))
    }
}

impl Drop for ManagedInstance {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            lease.quarantine_with(self.source.take());
        }
    }
}

pub(crate) struct ManagedFactory {
    recipe: PluginSourceRecipe,
    source: TrustedArtifact,
    bootstrap: TrustedArtifact,
    state_root: PathBuf,
    worker: PathBuf,
}

impl Plugins {
    pub(crate) fn managed_factory(&self, recipe: &PluginSourceRecipe) -> Result<ManagedFactory> {
        let select = |component: &ComponentRecipe, category: PluginCategory| {
            let mut selected: Option<(&TrustedArtifact, &PluginDescriptor)> = None;
            for plugin in self.loaded.values().filter(|plugin| {
                plugin.reference.rsplit_once('@').map(|(_, pin)| pin)
                    == Some(component.plugin.digest.as_str())
            }) {
                for descriptor in plugin.descriptors.iter().filter(|descriptor| {
                    descriptor.category == category
                        && descriptor.component == component.plugin.component
                }) {
                    if let Some((previous, schema)) = selected {
                        if previous.binary_sha256 != plugin.artifact.binary_sha256
                            || schema.configuration_schema != descriptor.configuration_schema
                            || schema.schema_name != descriptor.schema_name
                            || schema.config_version != descriptor.config_version
                        {
                            return Err(Error::new(
                                3,
                                "managed plugin descriptor pin is ambiguous",
                            ));
                        }
                    } else {
                        selected = Some((&plugin.artifact, descriptor));
                    }
                }
            }
            let (artifact, descriptor) = selected.ok_or_else(|| {
                Error::new(
                    3,
                    "managed plugin descriptor is not loaded under its exact pin",
                )
            })?;
            schema::validate(descriptor, component)?;
            Ok(artifact.clone())
        };
        if !self.directory.is_absolute() {
            return Err(Error::new(3, "plugin state directory must be absolute"));
        }
        Ok(ManagedFactory {
            recipe: recipe.clone(),
            source: select(&recipe.source, PluginCategory::Source)?,
            bootstrap: select(&recipe.bootstrap, PluginCategory::Bootstrap)?,
            state_root: self
                .directory
                .parent()
                .ok_or_else(|| Error::new(3, "plugin state directory has no native root"))?
                .join("sources"),
            worker: companion::path()?,
        })
    }
}

impl ManagedFactory {
    pub(crate) async fn construct(
        self,
        scope: CredentialScope,
        values: AdmissionValues,
    ) -> Result<ManagedInstance> {
        identifier(&scope.native_id)?;
        let identity =
            serde_json::to_string(&(scope.graph, &scope.lifetime, &scope.native_id, &self.recipe))?;
        let id = scope.native_id.clone();
        let lease = values.bind_worker(
            scope,
            &self.recipe,
            &self.source.binary_sha256,
            &self.bootstrap.binary_sha256,
        )?;
        let resolver = lease.worker_resolver()?;
        let input = lease.inputs();
        let spec = WorkerSpec {
            identity,
            source_id: id.clone(),
            recipe: self.recipe,
            source_artifact: self.source,
            bootstrap_artifact: self.bootstrap,
            source_config: input.source.clone(),
            bootstrap_config: input.bootstrap.clone(),
            bootstrap_source_config: input.bootstrap_source.clone(),
            state_directory: self.state_root.join(id),
        };
        match WorkerSource::spawn(&self.worker, spec, resolver).await {
            Ok(source) => Ok(ManagedInstance {
                source: Some(source),
                lease: Some(lease),
            }),
            Err(error) => {
                // Failed construction returns only after the child and IPC owners are joined.
                lease.release_after_join()?;
                Err(Error::new(
                    3,
                    format!("managed source worker construction failed: {error}"),
                ))
            }
        }
    }
}
