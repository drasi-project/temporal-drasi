use drasi_bootstrap_postgres::descriptor::{
    PostgresBootstrapConfigDto, PostgresBootstrapDescriptor, SslModeDto,
};
use drasi_lib::{
    bootstrap::{BootstrapContext, BootstrapProvider, BootstrapRequest, BootstrapResult},
    channels::BootstrapEventSender,
    config::SourceSubscriptionSettings,
};
use drasi_plugin_sdk::prelude::{
    BootstrapPluginDescriptor, ConfigValue, DtoMapper, SourcePluginDescriptor,
};
use drasi_source_postgres::{
    connection::ReplicationConnection,
    descriptor::{PostgresSourceConfigDto, PostgresSourceDescriptor},
};
use serde_json::{json, Value};

struct Source(PostgresSourceDescriptor);
struct Bootstrap(PostgresBootstrapDescriptor);

struct RetainedBootstrap {
    provider: Box<dyn BootstrapProvider>,
    source: PostgresSourceConfigDto,
}

#[async_trait::async_trait]
impl BootstrapProvider for RetainedBootstrap {
    async fn bootstrap(
        &self,
        request: BootstrapRequest,
        context: &BootstrapContext,
        events: BootstrapEventSender,
        settings: Option<&SourceSubscriptionSettings>,
    ) -> anyhow::Result<BootstrapResult> {
        let mapper = DtoMapper::new();
        let mut connection = ReplicationConnection::connect(
            &mapper.resolve_string(&self.source.host).await?,
            mapper.resolve_typed(&self.source.port).await?,
            &mapper.resolve_string(&self.source.database).await?,
            &mapper.resolve_string(&self.source.user).await?,
            &mapper.resolve_string(&self.source.password).await?,
        )
        .await?;
        // The released source creates its slot only after receiving the snapshot boundary.
        // Retain WAL before taking that snapshot so the boundary is already replayable.
        connection
            .create_replication_slot(&self.source.slot_name, false)
            .await?;
        connection.close().await?;
        self.provider
            .bootstrap(request, context, events, settings)
            .await
    }
}

fn config_value_schema(literal: Value) -> Value {
    json!({"oneOf": [
        literal,
        {
            "type": "object",
            "properties": {"kind": {"const": "Secret"}, "name": {"type": "string"}},
            "required": ["kind", "name"],
            "additionalProperties": false
        }
    ]})
}

fn configuration_schema(name: &str) -> String {
    json!({(name): {
        "type": "object",
        "required": ["database", "user"],
        "additionalProperties": false,
        "properties": {
            "host": config_value_schema(json!({"type": "string"})),
            "port": config_value_schema(json!({
                "type": "integer", "minimum": 0, "maximum": 65535
            })),
            "database": config_value_schema(json!({"type": "string"})),
            "user": config_value_schema(json!({"type": "string"})),
            "password": {
                "allOf": [config_value_schema(json!({"type": "string"}))],
                "x-ui:widget": "password"
            },
            "sslMode": config_value_schema(json!({
                "type": "string", "enum": ["disable", "prefer", "require"]
            })),
            "tables": {"type": "array", "items": {"type": "string"}},
            "slotName": {"type": "string"},
            "publicationName": {"type": "string"},
            "tableKeys": {"type": "array", "items": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "keyColumns": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["table", "keyColumns"],
                "additionalProperties": false
            }}
        }
    }})
    .to_string()
}

#[async_trait::async_trait]
impl SourcePluginDescriptor for Source {
    fn kind(&self) -> &str {
        self.0.kind()
    }

    fn config_version(&self) -> &str {
        self.0.config_version()
    }

    fn config_schema_name(&self) -> &str {
        self.0.config_schema_name()
    }

    fn config_schema_json(&self) -> String {
        configuration_schema(self.config_schema_name())
    }

    async fn create_source(
        &self,
        id: &str,
        configuration: &Value,
        auto_start: bool,
    ) -> anyhow::Result<Box<dyn drasi_lib::sources::Source>> {
        self.0.create_source(id, configuration, auto_start).await
    }
}

async fn bootstrap_configuration(
    configuration: &Value,
    mapper: &DtoMapper,
) -> anyhow::Result<Value> {
    let dto: PostgresBootstrapConfigDto = serde_json::from_value(configuration.clone())?;
    let mode: ConfigValue<String> = serde_json::from_value(serde_json::to_value(dto.ssl_mode)?)?;
    let mode = mapper.resolve_string(&mode).await?;
    let mode: SslModeDto = serde_json::from_value(Value::String(mode))?;
    let mut resolved = configuration.clone();
    resolved["sslMode"] = serde_json::to_value(ConfigValue::Static(mode))?;
    Ok(resolved)
}

#[async_trait::async_trait]
impl BootstrapPluginDescriptor for Bootstrap {
    fn kind(&self) -> &str {
        self.0.kind()
    }

    fn config_version(&self) -> &str {
        self.0.config_version()
    }

    fn config_schema_name(&self) -> &str {
        self.0.config_schema_name()
    }

    fn config_schema_json(&self) -> String {
        configuration_schema(self.config_schema_name())
    }

    async fn create_bootstrap_provider(
        &self,
        configuration: &Value,
        source: &Value,
    ) -> anyhow::Result<Box<dyn drasi_lib::bootstrap::BootstrapProvider>> {
        let resolved = bootstrap_configuration(configuration, &DtoMapper::new()).await?;
        Ok(Box::new(RetainedBootstrap {
            provider: self.0.create_bootstrap_provider(&resolved, source).await?,
            source: serde_json::from_value(source.clone())?,
        }))
    }
}

drasi_plugin_sdk::export_plugin!(
    plugin_id = "temporal-postgres",
    core_version = env!("TDRASI_CORE_VERSION"),
    lib_version = env!("TDRASI_LIB_VERSION"),
    plugin_version = env!("CARGO_PKG_VERSION"),
    source_descriptors = [Source(PostgresSourceDescriptor)],
    reaction_descriptors = [],
    bootstrap_descriptors = [Bootstrap(PostgresBootstrapDescriptor)],
);

#[cfg(test)]
mod tests {
    use super::*;
    use drasi_plugin_sdk::resolver::{ResolverError, ValueResolver};
    use std::{collections::BTreeSet, sync::Arc};

    #[test]
    fn schemas_cover_the_stock_descriptors_without_unresolved_generic_references() {
        for (name, upstream, adapted) in [
            (
                PostgresSourceDescriptor.config_schema_name(),
                PostgresSourceDescriptor.config_schema_json(),
                Source(PostgresSourceDescriptor).config_schema_json(),
            ),
            (
                PostgresBootstrapDescriptor.config_schema_name(),
                PostgresBootstrapDescriptor.config_schema_json(),
                Bootstrap(PostgresBootstrapDescriptor).config_schema_json(),
            ),
        ] {
            let upstream: Value = serde_json::from_str(&upstream).unwrap();
            let adapted: Value = serde_json::from_str(&adapted).unwrap();
            let fields = |document: &Value| {
                document[name]["properties"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .cloned()
                    .collect::<BTreeSet<_>>()
            };
            assert_eq!(fields(&adapted), fields(&upstream), "{name}");
            assert_eq!(adapted[name]["required"], upstream[name]["required"]);
            assert_eq!(adapted[name]["additionalProperties"], json!(false));
            assert!(!adapted.to_string().contains("\"$ref\""), "{name}");
        }
    }

    #[tokio::test]
    async fn bootstrap_preserves_explicit_tls_modes_and_rejects_unknown_modes() {
        let mapper = DtoMapper::new();
        for mode in ["disable", "prefer", "require"] {
            let configuration = json!({"database": "fixture", "user": "fixture", "sslMode": mode});
            assert_eq!(
                bootstrap_configuration(&configuration, &mapper)
                    .await
                    .unwrap(),
                configuration
            );
        }
        assert!(bootstrap_configuration(
            &json!({"database": "fixture", "user": "fixture", "sslMode": "invalid"}),
            &mapper,
        )
        .await
        .is_err());
    }

    struct TlsResolver(Option<&'static str>);

    #[async_trait::async_trait]
    impl ValueResolver for TlsResolver {
        async fn resolve_to_string(
            &self,
            value: &ConfigValue<String>,
        ) -> Result<String, ResolverError> {
            match value {
                ConfigValue::Secret { name } if name == "tls-mode" => self
                    .0
                    .map(str::to_owned)
                    .ok_or_else(|| ResolverError::SecretResolutionFailed("unavailable".into())),
                _ => Err(ResolverError::WrongResolverType),
            }
        }
    }

    #[tokio::test]
    async fn bootstrap_resolves_tls_references_without_defaulting_on_errors() {
        let configuration = json!({
            "database": "fixture",
            "user": "fixture",
            "sslMode": {"kind": "Secret", "name": "tls-mode"}
        });
        for mode in ["disable", "prefer", "require", "invalid"] {
            let mapper =
                DtoMapper::new().with_resolver("Secret", Arc::new(TlsResolver(Some(mode))));
            let resolved = bootstrap_configuration(&configuration, &mapper).await;
            if mode == "invalid" {
                assert!(resolved.is_err());
            } else {
                assert_eq!(
                    resolved.unwrap(),
                    json!({"database": "fixture", "user": "fixture", "sslMode": mode}),
                    "{mode}"
                );
            }
        }
        let mapper = DtoMapper::new().with_resolver("Secret", Arc::new(TlsResolver(None)));
        assert!(bootstrap_configuration(&configuration, &mapper)
            .await
            .is_err());
    }
}
