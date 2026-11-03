use super::*;
use drasi_lib::{channels::ComponentStatus, Source};
use sha2::{Digest, Sha256};

#[cfg(test)]
mod retirement_tests;
#[cfg(test)]
mod tests;

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Phase {
    Intended,
    Provisioned,
    Ready,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Retirement {
    Retiring,
    Retired,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Admission {
    pub definition: NodeDefinition,
    pub phase: Phase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fault: Option<RecoveryFault>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retirement: Option<Retirement>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum RecoveryFault {
    Materialization,
    Cleanup,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AdmissionLedger {
    version: u32,
    pub nodes: BTreeMap<String, Admission>,
}

impl Default for AdmissionLedger {
    fn default() -> Self {
        Self {
            version: 1,
            nodes: BTreeMap::new(),
        }
    }
}

impl NodeDefinition {
    fn is_query(&self) -> bool {
        matches!(self.recipe, NodeRecipe::Query { .. })
    }

    fn native_id(&self) -> Result<String> {
        self.lifetime.validate()?;
        if let NodeRecipe::HistorySource { scope } = &self.recipe {
            scope.validate()?;
            if scope.namespace_id != self.lifetime.namespace_id {
                return Err(Error::new(
                    1,
                    "History input namespace must match its resource lifetime",
                ));
            }
        }
        if let NodeRecipe::PluginSource { source, bootstrap } = &self.recipe {
            source.plugin.validate()?;
            bootstrap.plugin.validate()?;
            crate::credentials::component(source)?;
            crate::credentials::component(bootstrap)?;
        }
        if self.activation == 0 {
            return Err(Error::new(1, "node activation must be nonzero"));
        }
        if let NodeRecipe::Query { text, sources, .. } = &self.recipe {
            if text.is_empty()
                || text.len() > 64 << 10
                || sources.is_empty()
                || sources.len() > 64
                || sources.iter().collect::<BTreeSet<_>>().len() != sources.len()
            {
                return Err(Error::new(
                    1,
                    "query recipe requires bounded text and distinct source bindings",
                ));
            }
            for source in sources {
                identifier(source)?;
            }
        }
        let mut digest = Sha256::new();
        digest.update(b"temporal-native-node-v1\0");
        digest.update(serde_json::to_vec(self)?);
        Ok(format!(
            "{}-{:x}",
            if self.is_query() { "q" } else { "s" },
            digest.finalize()
        ))
    }

    pub(super) fn query_config(&self, native_id: &str) -> Option<QueryConfig> {
        match &self.recipe {
            NodeRecipe::TemporalSource
            | NodeRecipe::HistorySource { .. }
            | NodeRecipe::PluginSource { .. } => None,
            NodeRecipe::Query {
                language,
                text,
                sources,
            } => Some(QueryConfig {
                id: native_id.into(),
                revision: 1,
                language: language.clone(),
                text: text.clone(),
                sources: sources.clone(),
            }),
        }
    }

    pub(super) fn source_config(&self, native_id: &str) -> Option<SourceConfig> {
        match self.recipe {
            NodeRecipe::TemporalSource => Some(SourceConfig {
                id: native_id.into(),
                kind: SourceKind::Temporal,
                plugin: None,
                plugin_kind: None,
                config: Value::Null,
                bootstrap: None,
            }),
            NodeRecipe::HistorySource { ref scope } => Some(SourceConfig {
                id: native_id.into(),
                kind: SourceKind::Temporal,
                plugin: None,
                plugin_kind: None,
                config: json!(scope),
                bootstrap: None,
            }),
            NodeRecipe::Query { .. } | NodeRecipe::PluginSource { .. } => None,
        }
    }
}

impl AdmissionLedger {
    pub(super) fn validate(&self, config: &Config) -> Result<()> {
        if self.version != 1 && self.version != 2 {
            return Err(Error::new(7, "native admission ledger version is invalid"));
        }
        if serde_json::to_vec(self)?.len() > 8 << 20 {
            return Err(Error::new(
                3,
                "native admission ledger byte capacity exhausted",
            ));
        }
        let mut sources = config.sources.len();
        let mut queries = config.queries.len();
        let mut lifetimes = BTreeSet::new();
        for (id, admission) in &self.nodes {
            let definition = &admission.definition;
            if definition.native_id().ok().as_ref() != Some(id)
                || config.sources.iter().any(|source| source.id == *id)
                || config.queries.iter().any(|query| query.id == *id)
                || !lifetimes.insert((
                    &definition.lifetime.namespace_id,
                    &definition.lifetime.resource_id,
                    &definition.lifetime.incarnation,
                    definition.activation,
                    definition.is_query(),
                ))
                || (admission.fault.is_some() && admission.phase == Phase::Intended)
                || (admission.retirement.is_some() && self.version != 2)
                || (admission.retirement.is_some() && admission.fault.is_some())
            {
                return Err(Error::new(7, "native admission ledger identity is invalid"));
            }

            self.validate_dependencies(admission)?;
            if admission.retirement == Some(Retirement::Retired) {
                continue;
            }
            if admission.definition.is_query() {
                queries += 1;
            } else {
                sources += 1;
            }
        }
        if sources > 64 || queries > 256 {
            return Err(Error::new(3, "native admission ledger capacity exhausted"));
        }
        Ok(())
    }

    fn validate_dependencies(&self, query: &Admission) -> Result<()> {
        if let NodeRecipe::Query { sources, .. } = &query.definition.recipe {
            for source in sources {
                let admission = self.nodes.get(source).ok_or_else(|| {
                    Error::new(3, "query source is not retained in the admission ledger")
                })?;
                if admission.definition.is_query()
                    || admission.definition.lifetime.namespace_id
                        != query.definition.lifetime.namespace_id
                    || admission.phase == Phase::Intended
                    || (query.retirement != Some(Retirement::Retired)
                        && admission.retirement.is_some())
                {
                    return Err(Error::new(
                        3,
                        "query source binding has the wrong namespace, kind or admission phase",
                    ));
                }
            }
        }
        Ok(())
    }

    fn insert(&mut self, definition: NodeDefinition, config: &Config) -> Result<String> {
        let id = definition.native_id()?;
        for (previous_id, previous) in &self.nodes {
            if previous.definition.lifetime == definition.lifetime
                && previous.definition.is_query() == definition.is_query()
                && previous.retirement.is_some()
            {
                return Err(Error::new(3, "resource lifetime is permanently retired"));
            }
            if previous.definition.lifetime == definition.lifetime
                && previous.definition.activation == definition.activation
                && previous.definition.is_query() == definition.is_query()
                && previous_id != &id
            {
                return Err(Error::new(
                    3,
                    "resource lifetime is already bound to another immutable recipe",
                ));
            }
        }
        self.nodes.entry(id.clone()).or_insert(Admission {
            definition,
            phase: Phase::Intended,
            fault: None,
            retirement: None,
        });
        self.validate(config)?;
        Ok(id)
    }
}

impl Host {
    pub(super) async fn save_catalog(&self, ledger: AdmissionLedger) -> Result<()> {
        ledger.validate(&self.config)?;
        self.store()?
            .set(CONTROL, "catalog", serde_json::to_vec(&ledger)?)
            .await
            .map_err(io)?;
        *self.catalog()?.lock().await = ledger;
        Ok(())
    }

    async fn node_status(&self, id: &str, admission: &Admission) -> Result<NodeObservation> {
        let core = self.core()?;
        if let Some(retirement) = admission.retirement {
            let graph = core.component_graph();
            let graph = graph.read().await;
            return Ok(NodeObservation {
                history_blocked: false,
                native_id: id.into(),
                query_revision: u64::from(admission.definition.is_query()),
                status: match retirement {
                    Retirement::Retiring => NodeStatus::Retiring,
                    Retirement::Retired if graph.get_component(id).is_none() => NodeStatus::Retired,
                    Retirement::Retired => NodeStatus::RecoveryRequired,
                },
                definition: admission.definition.clone(),
            });
        }
        let retained = self
            .sources
            .lock()
            .await
            .get(id)
            .map(TemporalSource::shared);
        let history_blocked = match &retained {
            Some(source) => source.history_requires_reconstruction().await?,
            None => false,
        };
        let mut status = if admission.definition.is_query() {
            let query = {
                let graph = core.component_graph();
                let graph = graph.read().await;
                graph
                    .get_runtime::<Arc<dyn drasi_lib::queries::Query>>(id)
                    .cloned()
            };
            match query {
                Some(query) => Some(query.status().await),
                None => None,
            }
        } else {
            let source = {
                let graph = core.component_graph();
                let graph = graph.read().await;
                graph.get_runtime::<Arc<dyn Source>>(id).cloned()
            };
            match source {
                Some(source) => Some(source.status().await),
                None => None,
            }
        };
        if matches!(status, Some(ComponentStatus::Running)) && !admission.definition.is_query() {
            let retained = self
                .sources
                .lock()
                .await
                .get(id)
                .map(TemporalSource::shared);
            if let Some(source) = retained {
                match source.input_status().await {
                    crate::source::history::InputStatus::Ready => {}
                    crate::source::history::InputStatus::Loading => {
                        status = Some(ComponentStatus::Starting)
                    }
                    crate::source::history::InputStatus::Blocked => {
                        status = Some(ComponentStatus::Error)
                    }
                }
            }
        }
        let deferred = self.deferred.lock().await.contains(id);
        let graph = core.component_graph();
        let graph = graph.read().await;
        if let Some(component) = graph.get_component(id) {
            if matches!(
                component.status,
                ComponentStatus::Stopping | ComponentStatus::Error
            ) {
                status = Some(component.status);
            }
        }
        Ok(NodeObservation {
            history_blocked,
            native_id: id.into(),
            query_revision: if admission.definition.is_query() {
                1
            } else {
                0
            },
            status: match status {
                _ if admission.fault.is_some() => NodeStatus::RecoveryRequired,
                Some(ComponentStatus::Added | ComponentStatus::Stopped) => NodeStatus::Stopped,
                Some(ComponentStatus::Starting) => NodeStatus::Starting,
                Some(ComponentStatus::Running) => NodeStatus::Running,
                Some(ComponentStatus::Stopping) => NodeStatus::Stopping,
                Some(ComponentStatus::Reconfiguring) => NodeStatus::Reconfiguring,
                Some(ComponentStatus::Error) => NodeStatus::Failed,
                Some(ComponentStatus::Removed) => NodeStatus::RecoveryRequired,
                None if admission.phase == Phase::Intended => NodeStatus::Intended,
                None if deferred => NodeStatus::Deferred,
                None => NodeStatus::RecoveryRequired,
            },
            definition: admission.definition.clone(),
        })
    }

    pub(super) async fn observe_nodes(&self) -> Result<Vec<NodeObservation>> {
        let nodes = self.catalog()?.lock().await.nodes.clone();
        let mut observations = Vec::with_capacity(nodes.len());
        for (id, admission) in nodes {
            observations.push(self.node_status(&id, &admission).await?);
        }
        Ok(observations)
    }

    pub(super) async fn stop_node(&self, id: &str) -> Result<NodeObservation> {
        identifier(id)?;
        let _topology = self.topology.lock().await;
        let mut ledger = self.catalog()?.lock().await.clone();
        let admission = ledger
            .nodes
            .get(id)
            .cloned()
            .ok_or_else(|| Error::new(2, "node is not in the admission catalog"))?;
        let observation = self.node_status(id, &admission).await?;
        let retry_resident = matches!(admission.fault, Some(RecoveryFault::Materialization))
            && matches!(admission.definition.recipe, NodeRecipe::PluginSource { .. })
            && self.managed_sources.lock().await.contains_key(id);
        match observation.status {
            NodeStatus::Intended
            | NodeStatus::Deferred
            | NodeStatus::Stopped
            | NodeStatus::Retired
            | NodeStatus::Stopping => {
                return Ok(observation);
            }
            NodeStatus::RecoveryRequired if retry_resident => {}
            NodeStatus::RecoveryRequired | NodeStatus::Reconfiguring | NodeStatus::Retiring => {
                return Err(Error::new(
                    3,
                    "node cannot stop in its current lifecycle state",
                ));
            }
            NodeStatus::Starting | NodeStatus::Running | NodeStatus::Failed => {}
        }
        let core = self.core()?;
        let stopped = if admission.definition.is_query() {
            core.stop_query(id).await.map_err(Error::from)
        } else {
            for (query_id, query) in &ledger.nodes {
                if matches!(&query.definition.recipe, NodeRecipe::Query { sources, .. } if sources.iter().any(|source| source == id))
                {
                    let dependent = self.node_status(query_id, query).await?;
                    if !matches!(
                        dependent.status,
                        NodeStatus::Intended | NodeStatus::Deferred | NodeStatus::Stopped
                    ) {
                        return Err(Error::new(
                            3,
                            "stop dependent queries before stopping their source",
                        ));
                    }
                }
            }
            if retry_resident {
                let source = self
                    .managed_sources
                    .lock()
                    .await
                    .get(id)
                    .expect("resident managed source")
                    .source();
                source.stop().await.map_err(Error::from)
            } else {
                core.stop_source(id).await.map_err(Error::from)
            }
        };
        if let Err(error) = stopped {
            if matches!(admission.definition.recipe, NodeRecipe::PluginSource { .. }) {
                self.plugin_fault(&mut ledger, id, true).await?;
            }
            self.cleanup_failure
                .lock()
                .await
                .get_or_insert_with(|| error.clone());
            return Err(error);
        }
        if retry_resident {
            ledger.nodes.get_mut(id).expect("retained admission").fault = None;
            self.save_catalog(ledger.clone()).await?;
        }
        self.node_status(id, &ledger.nodes[id]).await
    }

    pub(super) async fn retire_node(&self, id: &str) -> Result<NodeObservation> {
        identifier(id)?;
        let _topology = self.topology.lock().await;
        self.require_admission_safe().await?;
        let mut ledger = self.catalog()?.lock().await.clone();
        let admission = ledger
            .nodes
            .get(id)
            .cloned()
            .ok_or_else(|| Error::new(2, "node is not in the admission catalog"))?;
        if admission.retirement == Some(Retirement::Retired) {
            return self.node_status(id, &admission).await;
        }
        if admission.fault.is_some()
            || (admission.retirement.is_none()
                && !matches!(
                    self.node_status(id, &admission).await?.status,
                    NodeStatus::Stopped | NodeStatus::Intended | NodeStatus::Deferred
                ))
        {
            return Err(Error::new(
                3,
                "native retirement requires a clean stopped node",
            ));
        }
        if !admission.definition.is_query() && ledger.nodes.values().any(|query|
            query.retirement != Some(Retirement::Retired)
                && matches!(&query.definition.recipe, NodeRecipe::Query { sources, .. } if sources.iter().any(|source| source == id))) {
            return Err(Error::new(3, "retire dependent queries before retiring their source"));
        }
        let handoffs = self.handoffs.lock().await;
        for key in self.store()?.list_keys(SUBSCRIPTIONS).await.map_err(io)? {
            let handoff = self.handoff(&key).await?;
            if handoff.definition.query_id == id
                && (!handoff.deleted || handoff.pending.is_some() || handoff.results.is_some())
            {
                return Err(Error::new(
                    3,
                    "close all query subscriptions before native retirement",
                ));
            }
        }
        ledger.version = 2;
        ledger
            .nodes
            .get_mut(id)
            .expect("retained admission")
            .retirement = Some(Retirement::Retiring);
        let result = async {
            self.save_catalog(ledger.clone()).await?;
            self.store()?.sync().await.map_err(io)?;
            drop(handoffs);
            let graph = self.core()?.component_graph();
            let present = graph.read().await.get_component(id).is_some();
            if present {
                if admission.definition.is_query() {
                    self.core()?
                        .remove_query_preserving_state(id)
                        .await
                        .map_err(Error::from)?;
                } else {
                    self.core()?
                        .remove_source(id, false)
                        .await
                        .map_err(Error::from)?;
                }
            }
            if graph.read().await.get_component(id).is_some() {
                return Err(Error::new(
                    3,
                    "native retirement did not remove the SDK graph node",
                ));
            }
            self.sources.lock().await.remove(id);
            let managed = self.managed_sources.lock().await.remove(id);
            if let Some(instance) = managed {
                instance.close().await?;
            }
            self.deferred.lock().await.remove(id);
            ledger
                .nodes
                .get_mut(id)
                .expect("retained admission")
                .retirement = Some(Retirement::Retired);
            self.save_catalog(ledger.clone()).await?;
            self.store()?.sync().await.map_err(io)?;
            self.node_status(id, &ledger.nodes[id]).await
        }
        .await;
        if let Err(error) = &result {
            self.cleanup_failure
                .lock()
                .await
                .get_or_insert_with(|| error.clone());
        }
        result
    }

    pub(super) async fn admit_node(&self, definition: NodeDefinition) -> Result<NodeObservation> {
        if matches!(definition.recipe, NodeRecipe::PluginSource { .. }) {
            return Err(Error::new(
                3,
                "plugin admission requires host-authorized credential inputs",
            ));
        }
        let _topology = self.topology.lock().await;
        self.require_admission_safe().await?;
        let mut ledger = self.catalog()?.lock().await.clone();
        let id = ledger.insert(definition, &self.config)?;
        self.save_catalog(ledger.clone()).await?;
        let admission = ledger.nodes[&id].clone();
        if admission.fault.is_some() {
            return Err(Error::new(3, "node has a retained recovery fault"));
        }
        let core = self.core()?;
        if matches!(
            self.node_status(&id, &admission).await?.status,
            NodeStatus::Failed | NodeStatus::Stopping | NodeStatus::Reconfiguring
        ) {
            return Err(Error::new(
                3,
                "node admission cannot resume this runtime; stop cleanup and existing-state recovery are required",
            ));
        }
        match &admission.definition.recipe {
            NodeRecipe::PluginSource { .. } => unreachable!("separate sensitive admission"),
            NodeRecipe::TemporalSource | NodeRecipe::HistorySource { .. } => {
                let creation = admission.phase == Phase::Intended;
                let existing = {
                    let graph = core.component_graph();
                    let graph = graph.read().await;
                    graph.get_runtime::<Arc<dyn Source>>(&id).cloned()
                };
                let source = match existing {
                    Some(source) => source
                        .as_any()
                        .downcast_ref::<TemporalSource>()
                        .ok_or_else(|| Error::new(7, "admitted source runtime has the wrong type"))?
                        .shared(),
                    None => {
                        let mut source = TemporalSource::new(
                            &id,
                            Arc::new(Journal::new(
                                self.store()?.clone(),
                                self.config.limits.feed_events,
                            )),
                            true,
                            BootstrapLimits::new(
                                self.config.limits.snapshot_rows,
                                self.config.limits.pending_bytes,
                            )
                            .map_err(Error::from)?,
                        )
                        .map_err(Error::from)?;
                        if let NodeRecipe::HistorySource { scope } = &admission.definition.recipe {
                            source = source.with_history(scope.clone())?;
                        }
                        source.prepare_history(creation).await?;
                        ledger.nodes.get_mut(&id).expect("retained admission").phase =
                            Phase::Provisioned;
                        self.save_catalog(ledger.clone()).await?;
                        core.add_source(source.shared())
                            .await
                            .map_err(Error::from)?;
                        source
                    }
                };
                source.prepare_history(false).await?;
                match source.status().await {
                    ComponentStatus::Added | ComponentStatus::Stopped => {
                        core.start_source(&id).await.map_err(Error::from)?;
                    }
                    ComponentStatus::Running | ComponentStatus::Starting => {}
                    _ => {
                        return Err(Error::new(
                            3,
                            "source admission cannot resume this runtime; stop cleanup is required",
                        ));
                    }
                }
                self.sources.lock().await.insert(id.clone(), source);
            }
            NodeRecipe::Query { sources, .. } => {
                for source in sources {
                    let dependency = self.node_status(source, &ledger.nodes[source]).await?;
                    if !matches!(dependency.status, NodeStatus::Running) {
                        return Err(Error::new(3, "admitted query source is not running"));
                    }
                }
                let creation = admission.phase == Phase::Intended;
                let existing = {
                    let graph = core.component_graph();
                    let graph = graph.read().await;
                    graph
                        .get_runtime::<Arc<dyn drasi_lib::queries::Query>>(&id)
                        .cloned()
                };
                let new_runtime = existing.is_none();
                if new_runtime {
                    self.deferred.lock().await.remove(&id);
                    let config = admission
                        .definition
                        .query_config(&id)
                        .expect("query recipe");
                    core.add_query(config.native(
                        if creation {
                            drasi_lib::RecoveryPolicy::Strict
                        } else {
                            drasi_lib::RecoveryPolicy::ExistingOnly
                        },
                        self.config.limits.outbox_entries,
                        false,
                    ))
                    .await
                    .map_err(Error::from)?;
                }
                ledger.nodes.get_mut(&id).expect("retained admission").phase = Phase::Provisioned;
                self.save_catalog(ledger.clone()).await?;
                let query = core
                    .query_manager()
                    .get_query_instance(&id)
                    .await
                    .map_err(|error| Error::new(7, error))?;
                match query.status().await {
                    ComponentStatus::Running | ComponentStatus::Starting => {}
                    ComponentStatus::Added | ComponentStatus::Stopped if creation => {
                        core.start_query(&id).await.map_err(Error::from)?;
                    }
                    ComponentStatus::Added | ComponentStatus::Stopped => {
                        core.start_query_existing(&id)
                            .await
                            .map_err(Error::from)?;
                    }
                    _ => return Err(Error::new(3, "query admission cannot resume this runtime; existing-state recovery or explicit reset is required")),
                }
            }
        }
        let observation = self.node_status(&id, &ledger.nodes[&id]).await?;
        if !matches!(
            observation.status,
            NodeStatus::Starting | NodeStatus::Running
        ) {
            return Err(Error::new(
                3,
                "native admission did not reach starting or running state",
            ));
        }
        if matches!(observation.status, NodeStatus::Running) {
            ledger.nodes.get_mut(&id).expect("retained admission").phase = Phase::Ready;
            self.save_catalog(ledger).await?;
        }
        Ok(observation)
    }

    fn plugin_source_recipe(&self, definition: &NodeDefinition) -> Result<PluginSourceRecipe> {
        let NodeRecipe::PluginSource { source, bootstrap } = &definition.recipe else {
            return Err(Error::new(
                1,
                "managed plugin admission requires a plugin Source recipe",
            ));
        };
        let recipe = PluginSourceRecipe {
            source: source.as_ref().clone(),
            bootstrap: bootstrap.as_ref().clone(),
        };
        self.require_managed_pair(&recipe)?;
        Ok(recipe)
    }

    fn require_managed_pair(&self, recipe: &PluginSourceRecipe) -> Result<()> {
        let pair = ManagedPluginPair {
            source: recipe.source.plugin.clone(),
            bootstrap: recipe.bootstrap.plugin.clone(),
        };
        if !self.config.managed_plugins.contains(&pair) {
            return Err(Error::new(
                3,
                "plugin descriptor pair has no administrator-reviewed lifecycle contract",
            ));
        }
        Ok(())
    }

    pub(super) async fn validate_plugin_sources(
        &self,
        recipes: Vec<PluginSourceRecipe>,
    ) -> Result<Vec<SourceSchemaVerdict>> {
        if recipes.is_empty() || recipes.len() > 64 {
            return Err(Error::new(
                1,
                "source schema validation requires 1..64 recipes",
            ));
        }
        let precondition = |error: Error| {
            if error.code == 3 {
                Error::new(
                    8,
                    "source schema validation requires supported reviewed loaded plugins",
                )
            } else {
                error
            }
        };
        for recipe in &recipes {
            recipe.source.plugin.validate()?;
            recipe.bootstrap.plugin.validate()?;
            crate::credentials::component(&recipe.source)?;
            crate::credentials::component(&recipe.bootstrap)?;
            self.require_managed_pair(recipe).map_err(precondition)?;
        }
        let plugins = self.plugins()?.lock().await;
        let mut verdicts = Vec::with_capacity(recipes.len());
        for recipe in &recipes {
            verdicts.push(match plugins.managed_factory(recipe) {
                Ok(_) => SourceSchemaVerdict::Valid,
                Err(error) if error.code == 1 => SourceSchemaVerdict::InvalidConfiguration,
                Err(error) => return Err(precondition(error)),
            });
        }
        Ok(verdicts)
    }

    pub(super) async fn check_plugin_source(&self, definition: NodeDefinition) -> Result<bool> {
        let recipe = self.plugin_source_recipe(&definition)?;
        let _topology = self.topology.lock().await;
        self.require_admission_safe().await?;
        // Validate against a detached ledger without reserving or persisting an admission.
        let mut ledger = self.catalog()?.lock().await.clone();
        let id = ledger.insert(definition, &self.config)?;
        let admission = &ledger.nodes[&id];
        if admission.fault.is_some() {
            return Err(Error::new(3, "node has a retained recovery fault"));
        }
        let existing = {
            let graph = self.core()?.component_graph();
            let graph = graph.read().await;
            graph.get_runtime::<Arc<dyn Source>>(&id).cloned()
        };
        if let Some(source) = existing {
            self.require_managed_resident(&id, &source).await?;
            return Ok(false);
        }
        if admission.phase != Phase::Intended && !self.deferred.lock().await.contains(&id) {
            return Err(Error::new(
                3,
                "managed source unexpectedly disappeared; existing-state recovery is required",
            ));
        }
        self.plugins()?.lock().await.managed_factory(&recipe)?;
        Ok(true)
    }

    async fn require_managed_resident(&self, id: &str, source: &Arc<dyn Source>) -> Result<()> {
        if !source
            .as_any()
            .is::<crate::plugins::managed::ManagedSource>()
            || !self.managed_sources.lock().await.contains_key(id)
            || !matches!(
                source.status().await,
                ComponentStatus::Stopped
                    | ComponentStatus::Added
                    | ComponentStatus::Starting
                    | ComponentStatus::Running
            )
        {
            return Err(Error::new(
                3,
                "managed source runtime requires cleanup and recovery",
            ));
        }
        Ok(())
    }

    /// Sensitive admission is called by the host, never decoded from a declarative command.
    pub async fn admit_plugin_source(
        &self,
        definition: NodeDefinition,
        values: AdmissionValues,
    ) -> Result<NodeObservation> {
        let recipe = self.plugin_source_recipe(&definition)?;
        let _topology = self.topology.lock().await;
        self.require_admission_safe().await?;
        let mut ledger = self.catalog()?.lock().await.clone();
        let id = ledger.insert(definition, &self.config)?;
        self.save_catalog(ledger.clone()).await?;
        let admission = ledger.nodes[&id].clone();
        if admission.fault.is_some() {
            return Err(Error::new(3, "node has a retained recovery fault"));
        }
        let core = self.core()?;
        let existing = {
            let graph = core.component_graph();
            let graph = graph.read().await;
            graph.get_runtime::<Arc<dyn Source>>(&id).cloned()
        };
        let source = if let Some(source) = existing {
            self.require_managed_resident(&id, &source).await?;
            None
        } else {
            if admission.phase != Phase::Intended && !self.deferred.lock().await.contains(&id) {
                return Err(Error::new(
                    3,
                    "managed source unexpectedly disappeared; existing-state recovery is required",
                ));
            }
            let factory = self.plugins()?.lock().await.managed_factory(&recipe)?;
            ledger.nodes.get_mut(&id).expect("retained admission").phase = Phase::Provisioned;
            self.save_catalog(ledger.clone()).await?;
            self.deferred.lock().await.remove(&id);
            let instance = factory
                .construct(
                    CredentialScope {
                        graph: self.credential_graph,
                        lifetime: admission.definition.lifetime.clone(),
                        native_id: id.clone(),
                    },
                    values,
                )
                .await;
            match instance {
                Ok(instance) => {
                    let source = instance.source();
                    self.managed_sources
                        .lock()
                        .await
                        .insert(id.clone(), instance);
                    Some(source)
                }
                Err(error) => {
                    self.plugin_fault(&mut ledger, &id, false).await?;
                    return Err(error);
                }
            }
        };
        let registration = match source {
            Some(source) => core.add_source(source).await,
            None => Ok(()),
        };
        let result = match registration {
            Ok(()) => {
                let observation = self.node_status(&id, &ledger.nodes[&id]).await?;
                if matches!(observation.status, NodeStatus::Stopped) {
                    core.start_source(&id).await
                } else {
                    Ok(())
                }
            }
            Err(error) => Err(error),
        };
        if result.is_err() {
            let source = self
                .managed_sources
                .lock()
                .await
                .get(&id)
                .map(ManagedInstance::source);
            let cleanup = match source {
                Some(source) => source.stop().await.is_err(),
                None => true,
            };
            self.plugin_fault(&mut ledger, &id, cleanup).await?;
            return Err(Error::new(3, "managed source admission failed"));
        }
        let observation = self.node_status(&id, &ledger.nodes[&id]).await?;
        if !matches!(
            observation.status,
            NodeStatus::Starting | NodeStatus::Running
        ) {
            self.plugin_fault(&mut ledger, &id, false).await?;
            return Err(Error::new(
                3,
                "managed source did not reach starting or running",
            ));
        }
        if matches!(observation.status, NodeStatus::Running) {
            ledger.nodes.get_mut(&id).expect("retained admission").phase = Phase::Ready;
            self.save_catalog(ledger).await?;
        }
        Ok(observation)
    }

    async fn plugin_fault(
        &self,
        ledger: &mut AdmissionLedger,
        id: &str,
        cleanup: bool,
    ) -> Result<()> {
        let cleanup = cleanup || CredentialLease::quarantined_for_graph(self.credential_graph)?;
        ledger.nodes.get_mut(id).expect("retained admission").fault = Some(if cleanup {
            RecoveryFault::Cleanup
        } else {
            RecoveryFault::Materialization
        });
        if cleanup {
            *self.cleanup_failure.lock().await =
                Some(Error::new(3, "managed source cleanup is unproved"));
        }
        if self.save_catalog(ledger.clone()).await.is_err() {
            let error = Error::new(7, "managed source recovery fault could not be persisted");
            *self.cleanup_failure.lock().await = Some(error.clone());
            return Err(error);
        }
        Ok(())
    }
}
