#![allow(unexpected_cfgs)]

use async_trait::async_trait;
use drasi_lib::{
    bootstrap::{BootstrapContext, BootstrapProvider, BootstrapRequest, BootstrapResult},
    channels::{BootstrapEventSender, ComponentStatus, SubscriptionResponse},
    config::SourceSubscriptionSettings,
    context::SourceRuntimeContext,
    sources::{SourceBase, SourceBaseParams, SourceError},
    Source,
};
use drasi_plugin_sdk::descriptor::{BootstrapPluginDescriptor, SourcePluginDescriptor};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

struct TestSource;
struct TestBootstrap;
struct EmptyBootstrap {
    configuration: serde_json::Value,
    source: serde_json::Value,
}
struct EmptySource {
    base: SourceBase,
    configuration: HashMap<String, serde_json::Value>,
    starts: AtomicU64,
}

#[async_trait]
impl Source for EmptySource {
    fn id(&self) -> &str {
        self.base.get_id()
    }

    fn type_name(&self) -> &str {
        "test-source"
    }

    fn properties(&self) -> HashMap<String, serde_json::Value> {
        self.configuration.clone()
    }

    fn auto_start(&self) -> bool {
        self.base.auto_start
            || self
                .configuration
                .get("bad_auto_start")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
    }

    fn supports_replay(&self) -> bool {
        self.configuration
            .get("no_replay")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn initialize(&self, context: SourceRuntimeContext) {
        self.base.initialize(context).await;
    }

    async fn start(&self) -> anyhow::Result<()> {
        let resolved = resolve_connection(&serde_json::to_value(&self.configuration)?).await?;
        if self
            .configuration
            .get("diagnostic_probe")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
            || (self
                .configuration
                .get("fail_first_start")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
                && self.starts.fetch_add(1, Ordering::Relaxed) == 0)
        {
            let value =
                resolved.ok_or_else(|| anyhow::anyhow!("fixture probe requires a credential"))?;
            let diagnostic = format!(
                "fixture private diagnostic: {value}, {}",
                self.configuration["connection"]
            );
            log::error!("{diagnostic}");
            self.base
                .set_status(ComponentStatus::Error, Some(diagnostic.clone()))
                .await;
            return Err(anyhow::anyhow!(diagnostic));
        }
        self.base.set_status(ComponentStatus::Running, None).await;
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        if self
            .configuration
            .get("fail_stop")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            return Err(anyhow::anyhow!(
                "private stop diagnostic: {}",
                resolve_connection(&serde_json::to_value(&self.configuration)?)
                    .await?
                    .unwrap_or_default()
            ));
        }
        self.base.stop_common().await
    }

    async fn status(&self) -> ComponentStatus {
        self.base.status_handle().get_status().await
    }

    async fn subscribe(
        &self,
        settings: SourceSubscriptionSettings,
    ) -> anyhow::Result<SubscriptionResponse> {
        if self
            .configuration
            .get("fail_subscribe")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            return Err(anyhow::anyhow!(
                "private subscribe diagnostic: {:?}",
                resolve_connection(&serde_json::to_value(&self.configuration)?).await?
            ));
        }
        if let Some(position) = &settings.resume_from {
            if position.as_ref() != [0_u8; 8] {
                return Err(SourceError::PositionUnavailable {
                    source_id: self.id().into(),
                    requested: position.clone(),
                    earliest_available: Some(vec![0_u8; 8].into()),
                }
                .into());
            }
        }
        self.base
            .subscribe_with_bootstrap(&settings, self.type_name())
            .await
    }

    async fn set_bootstrap_provider(&self, provider: Box<dyn BootstrapProvider>) {
        self.base.set_bootstrap_provider(provider).await;
    }

    async fn remove_position_handle(&self, query_id: &str) {
        self.base.remove_position_handle(query_id).await;
    }
}

#[async_trait]
impl BootstrapProvider for EmptyBootstrap {
    async fn bootstrap(
        &self,
        _: BootstrapRequest,
        _: &BootstrapContext,
        _: BootstrapEventSender,
        _: Option<&SourceSubscriptionSettings>,
    ) -> anyhow::Result<BootstrapResult> {
        let resolved = resolve_connection(&self.configuration).await?;
        let _ = resolve_connection(&self.source).await?;
        if enabled(&self.configuration, "fail_bootstrap") {
            return Err(anyhow::anyhow!(
                "private bootstrap diagnostic: {resolved:?}, {}",
                self.configuration
            ));
        }
        Ok(BootstrapResult {
            event_count: 0,
            source_position: Some(vec![0_u8; 8].into()),
        })
    }
}

async fn resolve_connection(configuration: &serde_json::Value) -> anyhow::Result<Option<String>> {
    if let Some(value) = configuration.get("connection") {
        let value = serde_json::from_value::<drasi_plugin_sdk::ConfigValue<String>>(value.clone())?;
        let resolved = drasi_plugin_sdk::mapper::DtoMapper::new()
            .resolve_string(&value)
            .await?;
        if let Some(expected) = configuration.get("expected_digest") {
            anyhow::ensure!(
                expected.as_str()
                    == Some(format!("{:x}", Sha256::digest(resolved.as_bytes())).as_str()),
                "fixture credential digest mismatch"
            );
        }
        return Ok(Some(resolved));
    }
    Ok(None)
}

static DESCRIPTOR_CALLS: AtomicU64 = AtomicU64::new(0);
static SOURCE_CALLS: AtomicU64 = AtomicU64::new(0);
static BOOTSTRAP_CALLS: AtomicU64 = AtomicU64::new(0);

#[no_mangle]
pub extern "C" fn temporal_test_descriptor_calls() -> u64 {
    DESCRIPTOR_CALLS.load(Ordering::Relaxed)
}

#[no_mangle]
pub extern "C" fn temporal_test_source_calls() -> u64 {
    SOURCE_CALLS.load(Ordering::Relaxed)
}

#[no_mangle]
pub extern "C" fn temporal_test_bootstrap_calls() -> u64 {
    BOOTSTRAP_CALLS.load(Ordering::Relaxed)
}

fn enabled(configuration: &serde_json::Value, field: &str) -> bool {
    configuration
        .get(field)
        .and_then(serde_json::Value::as_bool)
        == Some(true)
}

fn configuration_schema(name: &str) -> String {
    serde_json::json!({name: {
        "type":"object", "additionalProperties":false,
        "properties":{
            "connection": {
                "oneOf":[{"type":"string"}, {
                    "type":"object", "properties":{"kind":{"const":"Secret"},"name":{"type":"string"}},
                    "required":["kind","name"], "additionalProperties":false
                }], "x-ui":{"secret":true}
            },
            "expected_digest":{"type":"string"},
            "diagnostic_probe":{"type":"boolean"},
            "fail_first_start":{"type":"boolean"},
            "bad_auto_start":{"type":"boolean"},
            "no_replay":{"type":"boolean"},
            "fail_factory":{"type":"boolean"},
            "fail_bootstrap_factory":{"type":"boolean"},
            "fail_bootstrap":{"type":"boolean"},
            "fail_stop":{"type":"boolean"},
            "fail_subscribe":{"type":"boolean"}
        }
    }}).to_string()
}

#[async_trait]
impl SourcePluginDescriptor for TestSource {
    fn kind(&self) -> &str {
        DESCRIPTOR_CALLS.fetch_add(1, Ordering::Relaxed);
        "test-source"
    }

    fn config_version(&self) -> &str {
        "1.0.0"
    }

    fn config_schema_name(&self) -> &str {
        "source.test.Config"
    }

    fn config_schema_json(&self) -> String {
        configuration_schema(self.config_schema_name())
    }

    async fn create_source(
        &self,
        id: &str,
        configuration: &serde_json::Value,
        auto_start: bool,
    ) -> anyhow::Result<Box<dyn drasi_lib::Source>> {
        SOURCE_CALLS.fetch_add(1, Ordering::Relaxed);
        if enabled(configuration, "fail_factory") {
            return Err(anyhow::anyhow!(
                "private factory diagnostic: {:?}, {configuration}",
                resolve_connection(configuration).await?
            ));
        }
        Ok(Box::new(EmptySource {
            base: SourceBase::new(SourceBaseParams::new(id).with_auto_start(auto_start))?,
            configuration: serde_json::from_value(configuration.clone())?,
            starts: AtomicU64::new(0),
        }))
    }
}

#[async_trait]
impl BootstrapPluginDescriptor for TestBootstrap {
    fn kind(&self) -> &str {
        "test-bootstrap"
    }

    fn config_version(&self) -> &str {
        "1.0.0"
    }

    fn config_schema_name(&self) -> &str {
        "bootstrap.test.Config"
    }

    fn config_schema_json(&self) -> String {
        configuration_schema(self.config_schema_name())
    }

    async fn create_bootstrap_provider(
        &self,
        configuration: &serde_json::Value,
        source: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn drasi_lib::bootstrap::BootstrapProvider>> {
        BOOTSTRAP_CALLS.fetch_add(1, Ordering::Relaxed);
        if enabled(configuration, "fail_bootstrap_factory") {
            return Err(anyhow::anyhow!(
                "private bootstrap factory diagnostic: {:?}, {configuration}",
                resolve_connection(configuration).await?
            ));
        }
        Ok(Box::new(EmptyBootstrap {
            configuration: configuration.clone(),
            source: source.clone(),
        }))
    }
}

drasi_plugin_sdk::export_plugin!(
    plugin_id = "temporal-loader-test",
    core_version = "0.5.10",
    lib_version = drasi_lib::VERSION,
    plugin_version = "1.0.0",
    source_descriptors = [TestSource],
    reaction_descriptors = [],
    bootstrap_descriptors = [TestBootstrap],
);
