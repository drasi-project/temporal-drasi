use super::{schema, Plugins};
use crate::{
    credentials::{AdmissionValues, CredentialLease, CredentialScope},
    protocol::*,
};
use async_trait::async_trait;
use drasi_host_sdk::{proxies::source::SourceProxy, LoadedPlugin};
use drasi_lib::{
    bootstrap::{BootstrapContext, BootstrapProvider, BootstrapRequest, BootstrapResult},
    channels::{
        BootstrapEventSender, ChangeReceiver, ComponentStatus, DispatchMode, SourceEventWrapper,
        SubscriptionResponse,
    },
    config::SourceSubscriptionSettings,
    context::SourceRuntimeContext,
    Source,
};
use drasi_plugin_sdk::descriptor::{BootstrapPluginDescriptor, SourcePluginDescriptor};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

fn failed() -> anyhow::Error {
    anyhow::anyhow!("managed plugin operation failed")
}

fn cleanup_failed() -> Error {
    Error::new(
        3,
        "managed plugin cleanup is unproved; process recovery is required",
    )
}

#[derive(Default)]
struct Activity {
    active: usize,
    abandoned: bool,
    closing: bool,
    stopped: bool,
}

#[derive(Clone, Default)]
struct Work(Arc<Mutex<Activity>>);

struct Call {
    work: Work,
    completed: bool,
}

impl Work {
    fn enter(&self) -> anyhow::Result<Call> {
        let mut work = self.0.lock().map_err(|_| failed())?;
        anyhow::ensure!(
            !work.closing && !work.abandoned && work.active < 1024,
            "managed plugin work is unavailable"
        );
        work.active += 1;
        Ok(Call {
            work: self.clone(),
            completed: false,
        })
    }

    fn stopped(&self, stopped: bool) -> anyhow::Result<()> {
        self.0.lock().map_err(|_| failed())?.stopped = stopped;
        Ok(())
    }
}

impl Call {
    fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        if let Ok(mut work) = self.work.0.lock() {
            work.active -= 1;
            work.abandoned |= !self.completed;
        }
    }
}

struct Bootstrap {
    inner: Box<dyn BootstrapProvider>,
    work: Work,
}

#[async_trait]
impl BootstrapProvider for Bootstrap {
    async fn bootstrap(
        &self,
        request: BootstrapRequest,
        context: &BootstrapContext,
        event_tx: BootstrapEventSender,
        settings: Option<&SourceSubscriptionSettings>,
    ) -> anyhow::Result<BootstrapResult> {
        let call = self.work.enter()?;
        let result = self
            .inner
            .bootstrap(request, context, event_tx, settings)
            .await
            .map_err(|_| failed());
        call.complete();
        result
    }
}

struct Receiver {
    inner: Option<Box<dyn ChangeReceiver<SourceEventWrapper>>>,
    call: Option<Call>,
}

#[async_trait]
impl ChangeReceiver<SourceEventWrapper> for Receiver {
    async fn recv(&mut self) -> anyhow::Result<Arc<SourceEventWrapper>> {
        self.inner
            .as_mut()
            .expect("owned receiver")
            .recv()
            .await
            .map_err(|_| failed())
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        drop(self.inner.take());
        if let Some(call) = self.call.take() {
            call.complete();
        }
    }
}

struct State {
    inner: Box<dyn Source>,
    id: String,
    recipe: PluginSourceRecipe,
    work: Work,
}

#[derive(Clone)]
pub(crate) struct ManagedSource(Arc<State>);

#[async_trait]
impl Source for ManagedSource {
    fn id(&self) -> &str {
        &self.0.id
    }

    fn type_name(&self) -> &str {
        &self.0.recipe.source.plugin.component
    }

    fn properties(&self) -> HashMap<String, Value> {
        HashMap::from([("managed_recipe".into(), serde_json::json!(&self.0.recipe))])
    }

    fn dispatch_mode(&self) -> DispatchMode {
        self.0.inner.dispatch_mode()
    }

    fn auto_start(&self) -> bool {
        false
    }

    fn supports_replay(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn initialize(&self, context: SourceRuntimeContext) {
        self.0
            .inner
            .as_any()
            .downcast_ref::<SourceProxy>()
            .expect("verified SDK source proxy")
            .initialize_redacted(context)
            .await;
    }

    async fn start(&self) -> anyhow::Result<()> {
        let call = self.0.work.enter()?;
        self.0.work.stopped(false)?;
        let result = self.0.inner.start().await.map_err(|_| failed());
        call.complete();
        result
    }

    async fn stop(&self) -> anyhow::Result<()> {
        let call = self.0.work.enter()?;
        let result = self.0.inner.stop().await.map_err(|_| failed());
        let result = match result {
            Ok(()) if self.0.inner.status().await == ComponentStatus::Stopped => {
                self.0.work.stopped(true)
            }
            _ => {
                if let Ok(mut work) = self.0.work.0.lock() {
                    work.abandoned = true;
                }
                Err(failed())
            }
        };
        call.complete();
        result
    }

    async fn status(&self) -> ComponentStatus {
        if self
            .0
            .work
            .0
            .lock()
            .map(|work| work.abandoned)
            .unwrap_or(true)
        {
            ComponentStatus::Error
        } else {
            self.0.inner.status().await
        }
    }

    async fn subscribe(
        &self,
        settings: SourceSubscriptionSettings,
    ) -> anyhow::Result<SubscriptionResponse> {
        let call = self.0.work.enter()?;
        let result = self.0.inner.subscribe(settings).await.map_err(|_| failed());
        call.complete();
        let mut response = result?;
        let receiver_call = self.0.work.enter()?;
        response.receiver = Box::new(Receiver {
            inner: Some(response.receiver),
            call: Some(receiver_call),
        });
        if let Some(receiver) = response.bootstrap_result_receiver.take() {
            let call = self.0.work.enter()?;
            let (sender, sanitized) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let result = match receiver.await {
                    Ok(result) => result.map_err(|_| failed()),
                    Err(_) => Err(failed()),
                };
                call.complete();
                // A stopped query may have already dropped its result receiver.
                let _ = sender.send(result);
            });
            response.bootstrap_result_receiver = Some(sanitized);
        }
        Ok(response)
    }

    async fn remove_position_handle(&self, query: &str) {
        self.0.inner.remove_position_handle(query).await;
    }

    async fn on_subscriptions_complete(&self) {
        self.0.inner.on_subscriptions_complete().await;
    }
}

pub(crate) struct ManagedInstance {
    source: Option<ManagedSource>,
    lease: Option<CredentialLease>,
}

impl ManagedInstance {
    pub(crate) fn source(&self) -> ManagedSource {
        self.source.as_ref().expect("owned managed source").clone()
    }

    /// Called only after the SDK graph and its query/subscription owners are released.
    pub(crate) async fn close(mut self) -> Result<()> {
        let source = self.source.as_ref().expect("owned managed source");
        let stopped = {
            let mut work = source.0.work.0.lock().map_err(|_| cleanup_failed())?;
            work.closing = true;
            work.stopped
        };
        if !stopped
            && (source.0.inner.stop().await.is_err()
                || source.0.inner.status().await != ComponentStatus::Stopped)
        {
            return Err(cleanup_failed());
        }
        {
            let work = source.0.work.0.lock().map_err(|_| cleanup_failed())?;
            if work.active != 0 || work.abandoned {
                return Err(cleanup_failed());
            }
        }
        let source = self.source.take().expect("owned managed source");
        match Arc::try_unwrap(source.0) {
            Ok(state) => drop(state),
            Err(state) => {
                self.source = Some(ManagedSource(state));
                return Err(cleanup_failed());
            }
        }
        self.lease
            .take()
            .expect("owned credential lease")
            .release_after_join()
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
    source: Arc<LoadedPlugin>,
    bootstrap: Arc<LoadedPlugin>,
    source_index: usize,
    bootstrap_index: usize,
}

impl Plugins {
    pub(crate) fn managed_factory(&self, recipe: &PluginSourceRecipe) -> Result<ManagedFactory> {
        let select = |component: &ComponentRecipe, category: PluginCategory| {
            let mut selected: Option<(Arc<LoadedPlugin>, &PluginDescriptor)> = None;
            for plugin in self.loaded.values().filter(|plugin| {
                plugin.reference.rsplit_once('@').map(|(_, pin)| pin)
                    == Some(component.plugin.digest.as_str())
            }) {
                for descriptor in plugin.descriptors.iter().filter(|descriptor| {
                    descriptor.category == category
                        && descriptor.component == component.plugin.component
                }) {
                    if let Some((previous, schema)) = &selected {
                        if previous.config_resolver_injection_fn() as usize
                            != plugin.handle.config_resolver_injection_fn() as usize
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
                        selected = Some((plugin.handle.clone(), descriptor));
                    }
                }
            }
            let (handle, descriptor) = selected.ok_or_else(|| {
                Error::new(
                    3,
                    "managed plugin descriptor is not loaded under its exact pin",
                )
            })?;
            schema::validate(descriptor, component)?;
            Ok(handle)
        };
        let source = select(&recipe.source, PluginCategory::Source)?;
        let bootstrap = select(&recipe.bootstrap, PluginCategory::Bootstrap)?;
        let source_index = source
            .source_plugins
            .iter()
            .position(|descriptor| descriptor.kind() == recipe.source.plugin.component)
            .ok_or_else(|| Error::new(3, "managed source descriptor is unavailable"))?;
        let bootstrap_index = bootstrap
            .bootstrap_plugins
            .iter()
            .position(|descriptor| descriptor.kind() == recipe.bootstrap.plugin.component)
            .ok_or_else(|| Error::new(3, "managed bootstrap descriptor is unavailable"))?;
        Ok(ManagedFactory {
            recipe: recipe.clone(),
            source,
            bootstrap,
            source_index,
            bootstrap_index,
        })
    }
}

impl ManagedFactory {
    pub(crate) async fn construct(
        self,
        scope: CredentialScope,
        values: AdmissionValues,
    ) -> Result<ManagedInstance> {
        let id = scope.native_id.clone();
        let lease = values.bind(scope, &self.recipe, &self.source, &self.bootstrap)?;
        let mut owned = ManagedInstance {
            source: None,
            lease: Some(lease),
        };
        let inner = self.source.source_plugins[self.source_index]
            .create_source(
                &id,
                &owned.lease.as_ref().expect("owned lease").inputs().source,
                false,
            )
            .await
            .map_err(|_| Error::new(3, "managed source factory failed; cleanup is unproved"))?;
        owned.source = Some(ManagedSource(Arc::new(State {
            inner,
            id,
            recipe: self.recipe,
            work: Work::default(),
        })));
        let source = owned.source.as_ref().expect("owned source");
        if source.0.inner.id() != source.id()
            || source.0.inner.type_name() != source.type_name()
            || !source.0.inner.as_any().is::<SourceProxy>()
            || source.0.inner.auto_start()
            || !source.0.inner.supports_replay()
        {
            owned.close().await?;
            return Err(Error::new(
                3,
                "managed source does not satisfy its replay and lifecycle contract",
            ));
        }
        let input = owned.lease.as_ref().expect("owned lease").inputs();
        let provider = self.bootstrap.bootstrap_plugins[self.bootstrap_index]
            .create_bootstrap_provider(&input.bootstrap, &input.bootstrap_source)
            .await;
        match provider {
            Ok(inner) => {
                source
                    .0
                    .inner
                    .set_bootstrap_provider(Box::new(Bootstrap {
                        inner,
                        work: source.0.work.clone(),
                    }))
                    .await;
            }
            Err(_) => {
                let result = source.0.inner.stop().await;
                return Err(Error::new(
                    3,
                    if result.is_err() {
                        "managed bootstrap factory and source cleanup failed"
                    } else {
                        "managed bootstrap factory failed; cleanup is unproved"
                    },
                ));
            }
        }
        Ok(owned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unfinished_calls_block_further_work() {
        for completed in [false, true] {
            let work = Work::default();
            let call = work.enter().unwrap();
            assert_eq!(work.0.lock().unwrap().active, 1);
            if completed {
                call.complete();
            } else {
                drop(call);
            }
            let state = work.0.lock().unwrap();
            assert_eq!(state.active, 0);
            assert_eq!(state.abandoned, !completed);
            drop(state);
            assert_eq!(work.enter().is_ok(), completed);
        }
    }

    #[test]
    fn activity_quota_and_closing_reject_new_work() {
        let work = Work::default();
        let calls: Vec<_> = (0..1024).map(|_| work.enter().unwrap()).collect();
        assert!(work.enter().is_err());
        for call in calls {
            call.complete();
        }
        work.enter().unwrap().complete();
        work.0.lock().unwrap().closing = true;
        assert!(work.enter().is_err());
    }
}
