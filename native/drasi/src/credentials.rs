//! Ephemeral credentials for already authorized, immutable plugin recipes.
//!
//! This boundary does not select versions, read providers, verify descriptors, or
//! authorize a namespace. A lease is released only after joined plugin cleanup.

use crate::protocol::{
    ComponentRecipe, Error, PluginSourceRecipe, ResourceLifetime, Result, SecretRef,
};
use drasi_host_sdk::LoadedPlugin;
use drasi_plugin_sdk::descriptor::{BootstrapPluginDescriptor, SourcePluginDescriptor};
use drasi_plugin_sdk::ffi::{secret_store::FfiGetSecretResult, FfiStr};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::c_void,
    fmt,
    sync::{Arc, Mutex, OnceLock},
};
use uuid::Uuid;

const VALUE_BYTES: usize = 16 << 10;
const GROUP_BYTES: usize = 256 << 10;
const GRAPH_BYTES: usize = 16 << 20;
const PROCESS_BYTES: usize = 64 << 20;
const GRAPH_GROUPS: usize = 256;
const PROCESS_GROUPS: usize = 4096;
const PROCESS_TOKENS: usize = 65_536;
const CAPABILITY_PREFIX: &str = "tdrasi-credential-";

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentSlot {
    Source,
    Bootstrap,
}

pub struct CredentialScope {
    pub graph: Uuid,
    pub lifetime: ResourceLifetime,
    pub native_id: String,
}

struct SecretBytes(Vec<u8>);

impl Drop for SecretBytes {
    fn drop(&mut self) {
        for byte in &mut self.0 {
            // These stores must remain observable when the allocation is freed.
            unsafe { std::ptr::write_volatile(byte, 0) };
        }
    }
}

pub struct ResolvedField {
    component: ComponentSlot,
    field: String,
    reference: SecretRef,
    value: SecretBytes,
}

impl ResolvedField {
    pub fn new(
        component: ComponentSlot,
        field: String,
        reference: SecretRef,
        value: Vec<u8>,
    ) -> Self {
        Self {
            component,
            field,
            reference,
            value: SecretBytes(value),
        }
    }
}

pub struct AdmissionValues(Vec<ResolvedField>);

impl fmt::Debug for AdmissionValues {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AdmissionValues(<redacted>)")
    }
}

pub struct ComponentInputs {
    pub source: Value,
    pub bootstrap: Value,
    pub bootstrap_source: Value,
}

pub struct CredentialLease {
    group: Uuid,
    inputs: ComponentInputs,
    released: bool,
    retained: Option<Box<dyn Send + Sync>>,
}

impl fmt::Debug for CredentialLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CredentialLease(<redacted>)")
    }
}

#[derive(Default)]
struct Usage {
    bytes: usize,
    groups: usize,
}

#[derive(Default)]
struct State {
    groups: BTreeMap<Uuid, Arc<Group>>,
    quarantined: BTreeSet<Uuid>,
    retained: BTreeMap<Uuid, Box<dyn Send + Sync>>,
    entries: BTreeMap<String, Entry>,
    graphs: BTreeMap<Uuid, Usage>,
    bytes: usize,
    allocated_groups: usize,
    allocated_tokens: usize,
}

impl State {
    fn capacity(&self, graph: Uuid, bytes: usize, tokens: usize) -> Result<()> {
        let usage = self.graphs.get(&graph);
        if self.bytes + bytes > PROCESS_BYTES
            || self.allocated_groups >= PROCESS_GROUPS
            || self.allocated_tokens + tokens > PROCESS_TOKENS
            || usage.is_some_and(|usage| {
                usage.bytes + bytes > GRAPH_BYTES || usage.groups >= GRAPH_GROUPS
            })
        {
            return Err(Error::new(3, "native plugin credential capacity exhausted"));
        }
        Ok(())
    }
}

struct Entry {
    group: Uuid,
    field: usize,
    audience: usize,
}

struct Group {
    scope: CredentialScope,
    fields: Vec<ResolvedField>,
    tokens: Vec<String>,
    bytes: usize,
}

impl Drop for Group {
    fn drop(&mut self) {
        let mut state = router()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.bytes -= self.bytes;
        state.allocated_groups -= 1;
        state.allocated_tokens -= self.tokens.len();
        let usage = state
            .graphs
            .get_mut(&self.scope.graph)
            .expect("reserved graph");
        usage.bytes -= self.bytes;
        usage.groups -= 1;
        if usage.groups == 0 {
            state.graphs.remove(&self.scope.graph);
        }
    }
}

fn router() -> &'static Mutex<State> {
    static ROUTER: OnceLock<Mutex<State>> = OnceLock::new();
    ROUTER.get_or_init(Mutex::default)
}

struct ResolverContext {
    audience: usize,
    binary_sha256: String,
}

type ResolverSlot = Arc<OnceLock<&'static ResolverContext>>;

struct ResidentContext {
    binary_sha256: String,
    artifacts: BTreeSet<String>,
    slot: ResolverSlot,
}

fn contexts() -> &'static Mutex<BTreeMap<usize, ResidentContext>> {
    static CONTEXTS: OnceLock<Mutex<BTreeMap<usize, ResidentContext>>> = OnceLock::new();
    CONTEXTS.get_or_init(Mutex::default)
}

fn unavailable() -> Error {
    Error::new(7, "native credential router is unavailable")
}

pub(crate) fn install(
    plugin: &LoadedPlugin,
    binary_sha256: &str,
    artifact_digest: &str,
) -> Result<()> {
    let audience = plugin.config_resolver_injection_fn() as usize;
    let slot = {
        let mut contexts = contexts().lock().map_err(|_| unavailable())?;
        if contexts.len() >= 256 && !contexts.contains_key(&audience) {
            return Err(Error::new(
                3,
                "resident credential resolver capacity exhausted",
            ));
        }
        let resident = contexts.entry(audience).or_insert_with(|| ResidentContext {
            binary_sha256: binary_sha256.into(),
            artifacts: BTreeSet::new(),
            slot: Arc::new(OnceLock::new()),
        });
        if resident.binary_sha256 != binary_sha256 {
            return Err(Error::new(
                3,
                "resident library aliases have conflicting binary pins",
            ));
        }
        if resident.artifacts.len() >= 256 && !resident.artifacts.contains(artifact_digest) {
            return Err(Error::new(
                3,
                "resident library artifact capacity exhausted",
            ));
        }
        resident.artifacts.insert(artifact_digest.into());
        resident.slot.clone()
    };
    slot.get_or_init(|| {
        let context = Box::leak(Box::new(ResolverContext {
            audience,
            binary_sha256: binary_sha256.into(),
        }));
        plugin.inject_config_resolver(context as *mut ResolverContext as *mut c_void, resolve);
        context
    });
    Ok(())
}

fn audience(plugin: &LoadedPlugin, artifact_digest: &str) -> Result<usize> {
    let key = plugin.config_resolver_injection_fn() as usize;
    let contexts = contexts().lock().map_err(|_| unavailable())?;
    let resident = contexts.get(&key).ok_or_else(unavailable)?;
    if !resident.artifacts.contains(artifact_digest) {
        return Err(Error::new(
            3,
            "resident library does not match the immutable recipe pin",
        ));
    }
    let context = resident.slot.get().ok_or_else(unavailable)?;
    if context.audience != key || context.binary_sha256.len() != 64 {
        return Err(unavailable());
    }
    Ok(key)
}

fn text(value: &str, limit: usize) -> bool {
    !value.trim().is_empty() && value.len() <= limit && !value.chars().any(char::is_control)
}

fn literal(value: &Value, depth: usize) -> Result<()> {
    if depth > 32 {
        return Err(Error::new(
            1,
            "plugin configuration exceeds 32 value levels",
        ));
    }
    match value {
        Value::String(value)
            if (value.starts_with("${") && value.ends_with('}'))
                || value.starts_with(CAPABILITY_PREFIX) =>
        {
            Err(Error::new(
                1,
                "plugin configuration contains a reserved reference",
            ))
        }
        Value::Object(object) => {
            if matches!(
                object.get("kind").and_then(Value::as_str),
                Some("Secret" | "EnvironmentVariable")
            ) {
                return Err(Error::new(
                    1,
                    "plugin configuration contains an SDK reference",
                ));
            }
            for value in object.values() {
                literal(value, depth + 1)?;
            }
            Ok(())
        }
        Value::Array(values) => {
            for value in values {
                literal(value, depth + 1)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(crate) fn component(recipe: &ComponentRecipe) -> Result<()> {
    let object = recipe
        .configuration
        .as_object()
        .ok_or_else(|| Error::new(1, "plugin configuration requires a literal object"))?;
    if recipe.secrets.len() > 64 || serde_json::to_vec(&recipe.configuration)?.len() > 64 << 10 {
        return Err(Error::new(1, "plugin configuration capacity exceeded"));
    }
    for (field, reference) in &recipe.secrets {
        if !text(field, 256)
            || !text(&reference.provider, 128)
            || !text(&reference.key, 1024)
            || !text(&reference.version, 1024)
            || object.contains_key(field)
        {
            return Err(Error::new(
                1,
                "plugin credential reference is invalid or overlaps configuration",
            ));
        }
    }
    for value in object.values() {
        literal(value, 1)?;
    }
    if matches!(
        object.get("kind").and_then(Value::as_str),
        Some("Secret" | "EnvironmentVariable")
    ) {
        return Err(Error::new(
            1,
            "plugin configuration contains an SDK reference",
        ));
    }
    Ok(())
}

impl AdmissionValues {
    pub fn new(fields: Vec<ResolvedField>) -> Result<Self> {
        let values = Self(fields);
        let mut size = 0;
        let mut names = BTreeSet::new();
        if values.0.len() > 128 {
            return Err(Error::new(1, "plugin credential field capacity exceeded"));
        }
        for field in &values.0 {
            size += field.value.0.len();
            if field.value.0.len() > VALUE_BYTES
                || field.value.0.contains(&0)
                || std::str::from_utf8(&field.value.0).is_err()
                || !names.insert((field.component, &field.field))
            {
                return Err(Error::new(1, "plugin credential value or field is invalid"));
            }
        }
        if size > GROUP_BYTES {
            return Err(Error::new(
                1,
                "plugin credential group byte capacity exceeded",
            ));
        }
        Ok(values)
    }

    pub fn bind(
        self,
        scope: CredentialScope,
        recipe: &PluginSourceRecipe,
        source: &LoadedPlugin,
        bootstrap: &LoadedPlugin,
    ) -> Result<CredentialLease> {
        let source_audience = audience(source, &recipe.source.plugin.digest)?;
        let bootstrap_audience = audience(bootstrap, &recipe.bootstrap.plugin.digest)?;
        if !source
            .source_plugins
            .iter()
            .any(|descriptor| descriptor.kind() == recipe.source.plugin.component)
            || !bootstrap
                .bootstrap_plugins
                .iter()
                .any(|descriptor| descriptor.kind() == recipe.bootstrap.plugin.component)
        {
            return Err(Error::new(
                3,
                "resident library does not provide the immutable recipe component",
            ));
        }
        self.bind_audiences(scope, recipe, source_audience, bootstrap_audience)
    }

    fn bind_audiences(
        self,
        scope: CredentialScope,
        recipe: &PluginSourceRecipe,
        source_audience: usize,
        bootstrap_audience: usize,
    ) -> Result<CredentialLease> {
        scope.lifetime.validate()?;
        crate::protocol::identifier(&scope.native_id)?;
        if scope.graph.is_nil() {
            return Err(Error::new(
                1,
                "plugin credential graph identity must be nonzero",
            ));
        }
        component(&recipe.source)?;
        component(&recipe.bootstrap)?;
        if self.0.len() != recipe.source.secrets.len() + recipe.bootstrap.secrets.len() {
            return Err(Error::new(
                1,
                "plugin credential fields do not match the immutable recipe",
            ));
        }
        for field in &self.0 {
            let declaration = match field.component {
                ComponentSlot::Source => &recipe.source,
                ComponentSlot::Bootstrap => &recipe.bootstrap,
            };
            if declaration.secrets.get(&field.field) != Some(&field.reference) {
                return Err(Error::new(
                    1,
                    "plugin credential fields do not match the immutable recipe",
                ));
            }
        }
        let group = Uuid::new_v4();
        let bytes = self
            .0
            .iter()
            .map(|field| field.value.0.len())
            .sum::<usize>();
        let mut inputs = ComponentInputs {
            source: recipe.source.configuration.clone(),
            bootstrap: recipe.bootstrap.configuration.clone(),
            bootstrap_source: recipe.source.configuration.clone(),
        };
        let mut entries = BTreeMap::new();
        for (index, field) in self.0.iter().enumerate() {
            let views = match field.component {
                ComponentSlot::Source => vec![
                    (&mut inputs.source, source_audience),
                    (&mut inputs.bootstrap_source, bootstrap_audience),
                ],
                ComponentSlot::Bootstrap => vec![(&mut inputs.bootstrap, bootstrap_audience)],
            };
            for (view, audience) in views {
                let token = format!("{CAPABILITY_PREFIX}{}", Uuid::new_v4());
                view.as_object_mut()
                    .expect("validated configuration")
                    .insert(
                        field.field.clone(),
                        json!({"kind": "Secret", "name": token}),
                    );
                entries.insert(
                    token,
                    Entry {
                        group,
                        field: index,
                        audience,
                    },
                );
            }
        }
        let mut state = router().lock().map_err(|_| unavailable())?;
        state.capacity(scope.graph, bytes, entries.len())?;
        if state.groups.contains_key(&group)
            || entries
                .keys()
                .any(|token| state.entries.contains_key(token))
        {
            return Err(Error::new(7, "native credential capability collision"));
        }
        let usage = state.graphs.entry(scope.graph).or_default();
        usage.bytes += bytes;
        usage.groups += 1;
        state.bytes += bytes;
        state.allocated_groups += 1;
        state.allocated_tokens += entries.len();
        let allocation = Arc::new(Group {
            scope,
            fields: self.0,
            tokens: entries.keys().cloned().collect(),
            bytes,
        });
        state.entries.extend(entries);
        state.groups.insert(group, allocation);
        Ok(CredentialLease {
            group,
            inputs,
            released: false,
            retained: None,
        })
    }
}

impl CredentialLease {
    pub(crate) fn quarantine_with(mut self, resource: impl Send + Sync + 'static) {
        self.retained = Some(Box::new(resource));
    }

    pub fn inputs(&self) -> &ComponentInputs {
        &self.inputs
    }

    /// Revoke only after source, bootstrap, subscriptions and proxy destruction are joined.
    pub fn release_after_join(mut self) -> Result<()> {
        let allocation = {
            let mut state = router().lock().map_err(|_| unavailable())?;
            let allocation = state.groups.remove(&self.group).ok_or_else(unavailable)?;
            state.quarantined.remove(&self.group);
            for token in &allocation.tokens {
                state.entries.remove(token);
            }
            allocation
        };
        self.released = true;
        drop(allocation);
        Ok(())
    }

    pub fn retained_for_graph(graph: Uuid) -> Result<bool> {
        Ok(router()
            .lock()
            .map_err(|_| unavailable())?
            .graphs
            .contains_key(&graph))
    }

    pub fn quarantined_for_graph(graph: Uuid) -> Result<bool> {
        let state = router().lock().map_err(|_| unavailable())?;
        Ok(state.quarantined.iter().any(|id| {
            state
                .groups
                .get(id)
                .is_some_and(|group| group.scope.graph == graph)
        }))
    }
}

impl Drop for CredentialLease {
    fn drop(&mut self) {
        if !self.released {
            match router().lock() {
                Ok(mut state) => {
                    state.quarantined.insert(self.group);
                    if let Some(resource) = self.retained.take() {
                        state.retained.insert(self.group, resource);
                    }
                }
                Err(_) => {
                    if let Some(resource) = self.retained.take() {
                        std::mem::forget(resource);
                    }
                }
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Lookup<'a> {
    kind: &'a str,
    name: &'a str,
}

fn lookup(context: &ResolverContext, input: &[u8]) -> Result<String> {
    let request: Lookup<'_> = serde_json::from_slice(input)
        .map_err(|_| Error::new(1, "native credential lookup has an invalid shape"))?;
    if request.kind != "Secret" || !request.name.starts_with(CAPABILITY_PREFIX) {
        return Err(Error::new(3, "native credential lookup is not authorized"));
    }
    let (allocation, field) = {
        let state = router().lock().map_err(|_| unavailable())?;
        let entry = state
            .entries
            .get(request.name)
            .filter(|entry| entry.audience == context.audience)
            .ok_or_else(|| Error::new(3, "native credential lookup is not authorized"))?;
        let allocation = state
            .groups
            .get(&entry.group)
            .cloned()
            .ok_or_else(unavailable)?;
        (allocation, entry.field)
    };
    String::from_utf8(allocation.fields[field].value.0.clone()).map_err(|_| unavailable())
}

extern "C" fn resolve(context: *const c_void, input: FfiStr) -> FfiGetSecretResult {
    let result = std::panic::catch_unwind(|| {
        if context.is_null() || input.ptr.is_null() || input.len == 0 || input.len > 512 {
            return Err(Error::new(
                1,
                "native credential lookup has an invalid shape",
            ));
        }
        // Contexts are installed once and live as long as their resident libraries.
        let context = unsafe { &*context.cast::<ResolverContext>() };
        let bytes = unsafe { std::slice::from_raw_parts(input.ptr.cast::<u8>(), input.len) };
        lookup(context, bytes)
    });
    match result {
        Ok(Ok(value)) => FfiGetSecretResult::ok(value),
        Ok(Err(error)) => FfiGetSecretResult::err(error.message),
        Err(_) => FfiGetSecretResult::err("native credential resolution failed".into()),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::protocol::PluginComponentRef;
    use sha2::{Digest, Sha256};

    fn recipe(pin: &str) -> PluginSourceRecipe {
        PluginSourceRecipe {
            source: ComponentRecipe {
                plugin: PluginComponentRef {
                    digest: pin.into(),
                    component: "test-source".into(),
                },
                configuration: json!({}),
                secrets: BTreeMap::from([("connection".into(), reference())]),
            },
            bootstrap: ComponentRecipe {
                plugin: PluginComponentRef {
                    digest: pin.into(),
                    component: "test-bootstrap".into(),
                },
                configuration: json!({}),
                secrets: BTreeMap::new(),
            },
        }
    }

    fn reference() -> SecretRef {
        SecretRef {
            provider: "test-provider".into(),
            key: "test-key".into(),
            version: "version-1".into(),
        }
    }

    fn scope(graph: Uuid) -> CredentialScope {
        CredentialScope {
            graph,
            lifetime: ResourceLifetime {
                namespace_id: Uuid::from_u128(1).to_string(),
                resource_id: "test-source".into(),
                incarnation: "1".into(),
            },
            native_id: "test-native-source".into(),
        }
    }

    fn values(value: &[u8]) -> AdmissionValues {
        AdmissionValues::new(vec![ResolvedField::new(
            ComponentSlot::Source,
            "connection".into(),
            reference(),
            value.to_vec(),
        )])
        .unwrap()
    }

    #[test]
    fn value_limits_and_invalid_bytes() {
        let cases = [
            ("empty", Vec::new(), true),
            ("at limit", vec![b'x'; VALUE_BYTES], true),
            ("over limit", vec![b'x'; VALUE_BYTES + 1], false),
            ("nul", b"private\0value".to_vec(), false),
            ("invalid utf8", vec![0xff], false),
        ];
        for (name, value, valid) in cases {
            let result = AdmissionValues::new(vec![ResolvedField::new(
                ComponentSlot::Source,
                "connection".into(),
                reference(),
                value,
            )]);
            assert_eq!(result.is_ok(), valid, "{name}");
        }
        let fields = |count, size| {
            (0..count)
                .map(|index| {
                    ResolvedField::new(
                        ComponentSlot::Source,
                        format!("field-{index}"),
                        reference(),
                        vec![b'x'; size],
                    )
                })
                .collect()
        };
        assert!(AdmissionValues::new(fields(16, VALUE_BYTES)).is_ok());
        assert!(AdmissionValues::new(fields(17, VALUE_BYTES)).is_err());
        assert!(AdmissionValues::new(fields(128, 0)).is_ok());
        assert!(AdmissionValues::new(fields(129, 0)).is_err());
        let duplicate = || {
            ResolvedField::new(
                ComponentSlot::Source,
                "connection".into(),
                reference(),
                Vec::new(),
            )
        };
        assert!(AdmissionValues::new(vec![duplicate(), duplicate()]).is_err());
        assert_eq!(
            format!("{:?}", values(b"must-not-appear-in-diagnostics")),
            "AdmissionValues(<redacted>)"
        );
    }

    #[test]
    fn literal_configuration_and_exact_bounds() {
        for (configuration, valid) in [
            (json!({}), true),
            (json!({"v": [null, 1, true, "prefix${HOST}suffix"]}), true),
            (json!({"v": "${HOST}"}), false),
            (json!({"v": "${not an identifier}"}), false),
            (json!({"v": [{"kind": "Secret", "name": "private"}]}), false),
            (
                json!({"kind": "EnvironmentVariable", "name": "private"}),
                false,
            ),
            (json!({"v": "tdrasi-credential-forged"}), false),
            (json!({"connection": null}), false),
            (json!([]), false),
        ] {
            let mut candidate = recipe("test");
            candidate.source.configuration = configuration.clone();
            assert_eq!(
                component(&candidate.source).is_ok(),
                valid,
                "{configuration}"
            );
        }
        for levels in [32, 33] {
            let mut nested = json!(0);
            for _ in 1..levels {
                nested = json!([nested]);
            }
            let mut candidate = recipe("test");
            candidate.source.configuration = json!({"value": nested});
            assert_eq!(component(&candidate.source).is_ok(), levels == 32);
        }
        let mut candidate = recipe("test");
        let overhead = json!({"v": ""}).to_string().len();
        candidate.source.configuration = json!({"v": "x".repeat((64 << 10) - overhead)});
        assert!(component(&candidate.source).is_ok());
        candidate.source.configuration["v"] = json!("x".repeat((64 << 10) - overhead + 1));
        assert!(component(&candidate.source).is_err());
    }

    #[test]
    fn fields_and_immutable_references_must_match_exactly() {
        for changed in [
            "provider", "key", "version", "field", "slot", "missing", "extra",
        ] {
            let mut field = ResolvedField::new(
                ComponentSlot::Source,
                "connection".into(),
                reference(),
                b"private-value".to_vec(),
            );
            match changed {
                "provider" => field.reference.provider = "different".into(),
                "key" => field.reference.key = "different".into(),
                "version" => field.reference.version = "different".into(),
                "field" => field.field = "different".into(),
                "slot" => field.component = ComponentSlot::Bootstrap,
                "missing" | "extra" => {}
                _ => unreachable!(),
            }
            let fields = match changed {
                "missing" => Vec::new(),
                "extra" => vec![
                    field,
                    ResolvedField::new(
                        ComponentSlot::Bootstrap,
                        "extra".into(),
                        reference(),
                        Vec::new(),
                    ),
                ],
                _ => vec![field],
            };
            let error = AdmissionValues::new(fields)
                .unwrap()
                .bind_audiences(scope(Uuid::from_u128(101)), &recipe("test"), 1, 2)
                .unwrap_err();
            assert_eq!(error.code, 1, "{changed}");
            assert!(!error.message.contains("private-value"));
        }
        assert!(!CredentialLease::retained_for_graph(Uuid::from_u128(101)).unwrap());
    }

    #[test]
    fn capability_audiences_revocation_and_borrowed_accounting() {
        let graph = Uuid::from_u128(102);
        let lease = values(b"private-source")
            .bind_audiences(scope(graph), &recipe("test"), 1, 2)
            .unwrap();
        let source = serde_json::to_vec(&lease.inputs.source["connection"]).unwrap();
        let bootstrap_source =
            serde_json::to_vec(&lease.inputs.bootstrap_source["connection"]).unwrap();
        let contexts = [1, 2].map(|audience| ResolverContext {
            audience,
            binary_sha256: "a".repeat(64),
        });
        assert_ne!(source, bootstrap_source);
        assert_eq!(lookup(&contexts[0], &source).unwrap(), "private-source");
        assert_eq!(
            lookup(&contexts[1], &bootstrap_source).unwrap(),
            "private-source"
        );
        assert_eq!(lookup(&contexts[1], &source).unwrap_err().code, 3);
        assert_eq!(lookup(&contexts[0], &bootstrap_source).unwrap_err().code, 3);
        assert_eq!(format!("{lease:?}"), "CredentialLease(<redacted>)");
        assert!(!serde_json::to_string(&lease.inputs.source)
            .unwrap()
            .contains("private-source"));
        let borrowed = router().lock().unwrap().groups[&lease.group].clone();
        lease.release_after_join().unwrap();
        assert_eq!(lookup(&contexts[0], &source).unwrap_err().code, 3);
        assert!(CredentialLease::retained_for_graph(graph).unwrap());
        {
            let state = router().lock().unwrap();
            assert!(
                state
                    .capacity(graph, 0, PROCESS_TOKENS - state.entries.len())
                    .is_err(),
                "revocation released token quota while a callback still owned the allocation"
            );
        }
        drop(borrowed);
        assert!(!CredentialLease::retained_for_graph(graph).unwrap());
        assert!(!CredentialLease::quarantined_for_graph(graph).unwrap());
    }

    #[test]
    fn unproved_cleanup_quarantines_credentials_and_reservation() {
        let graph = Uuid::from_u128(103);
        let lease = values(b"private-quarantined")
            .bind_audiences(scope(graph), &recipe("test"), 3, 4)
            .unwrap();
        let input = serde_json::to_vec(&lease.inputs.source["connection"]).unwrap();
        assert!(!CredentialLease::quarantined_for_graph(graph).unwrap());
        drop(lease);
        assert!(CredentialLease::quarantined_for_graph(graph).unwrap());
        assert!(CredentialLease::retained_for_graph(graph).unwrap());
        assert_eq!(
            lookup(
                &ResolverContext {
                    audience: 3,
                    binary_sha256: "a".repeat(64),
                },
                &input,
            )
            .unwrap(),
            "private-quarantined"
        );
    }

    #[test]
    fn all_resident_capacity_boundaries() {
        let graph = Uuid::from_u128(104);
        let mut state = State {
            bytes: PROCESS_BYTES,
            ..State::default()
        };
        assert!(state.capacity(graph, 0, 0).is_ok());
        assert_eq!(state.capacity(graph, 1, 0).unwrap_err().code, 3);
        state.bytes = 0;
        state.allocated_groups = PROCESS_GROUPS - 1;
        assert!(state.capacity(graph, 0, 0).is_ok());
        state.allocated_groups += 1;
        assert_eq!(state.capacity(graph, 0, 0).unwrap_err().code, 3);
        state.allocated_groups = 0;
        state.graphs.insert(
            graph,
            Usage {
                bytes: GRAPH_BYTES,
                groups: 1,
            },
        );
        assert!(state.capacity(graph, 0, 0).is_ok());
        assert_eq!(state.capacity(graph, 1, 0).unwrap_err().code, 3);
        state.graphs.insert(
            graph,
            Usage {
                bytes: 0,
                groups: GRAPH_GROUPS - 1,
            },
        );
        assert!(state.capacity(graph, 0, 0).is_ok());
        state.graphs.get_mut(&graph).unwrap().groups += 1;
        assert_eq!(state.capacity(graph, 0, 0).unwrap_err().code, 3);
        state.graphs.clear();
        state.allocated_tokens = PROCESS_TOKENS;
        assert!(state.capacity(graph, 0, 0).is_ok());
        assert_eq!(state.capacity(graph, 0, 1).unwrap_err().code, 3);
    }

    #[test]
    fn ffi_rejects_malformed_lookups_without_echoing_input() {
        let context = ResolverContext {
            audience: 1,
            binary_sha256: "a".repeat(64),
        };
        let pointer = &context as *const ResolverContext as *const c_void;
        let cases: &[&[u8]] = &[
            b"",
            &[0xff],
            br#"{"kind":"EnvironmentVariable","name":"private-input"}"#,
            br#"{"kind":"Secret","name":"private-input"}"#,
            br#"{"kind":"Secret","name":"tdrasi-credential-private-input","extra":"private-input"}"#,
            br#"{"kind":"Secret","name":"tdrasi-credential-private-input"}"#,
            &[b'x'; 513],
        ];
        for input in cases {
            let result = resolve(
                pointer,
                FfiStr {
                    ptr: input.as_ptr().cast(),
                    len: input.len(),
                },
            );
            let error = unsafe { result.into_result() }.unwrap_err().to_string();
            assert!(!error.contains("private-input"));
            assert!(!error.contains(CAPABILITY_PREFIX));
        }
        let result = resolve(std::ptr::null(), FfiStr::from_str("{}"));
        assert!(unsafe { result.into_result() }.is_err());
    }

    pub(crate) async fn exercise_real_plugin(
        plugin: &LoadedPlugin,
        pin: &str,
        other: &LoadedPlugin,
        other_pin: &str,
        alias: impl FnOnce() -> Arc<LoadedPlugin>,
    ) {
        let mut mismatched = recipe(pin);
        mismatched.source.plugin.digest = format!("sha256:{}", "b".repeat(64));
        let result = values(b"credential-value-must-remain-private").bind(
            scope(Uuid::from_u128(100)),
            &mismatched,
            plugin,
            plugin,
        );
        assert!(
            result.is_err(),
            "credential admission exposed values to a library outside the immutable recipe pin"
        );
        let mut mismatched = recipe(pin);
        mismatched.bootstrap.plugin.component = "missing-component".into();
        assert!(values(b"private")
            .bind(scope(Uuid::from_u128(100)), &mismatched, plugin, plugin)
            .is_err());

        assert_ne!(
            plugin.config_resolver_injection_fn() as usize,
            other.config_resolver_injection_fn() as usize,
            "the proof requires two actually distinct resident library resolvers"
        );
        let source_value = b"${TEMPORAL_DRASI_NOT_AN_ENVIRONMENT_LOOKUP}";
        let bootstrap_value = b"private-bootstrap-value";
        let mut first_recipe = recipe(pin);
        first_recipe.source.configuration =
            json!({"expected_digest": format!("{:x}", Sha256::digest(source_value))});
        first_recipe.bootstrap.plugin.digest = other_pin.into();
        first_recipe.bootstrap.configuration =
            json!({"expected_digest": format!("{:x}", Sha256::digest(bootstrap_value))});
        first_recipe
            .bootstrap
            .secrets
            .insert("connection".into(), reference());
        let first = AdmissionValues::new(vec![
            ResolvedField::new(
                ComponentSlot::Source,
                "connection".into(),
                reference(),
                source_value.to_vec(),
            ),
            ResolvedField::new(
                ComponentSlot::Bootstrap,
                "connection".into(),
                reference(),
                bootstrap_value.to_vec(),
            ),
        ])
        .unwrap()
        .bind(scope(Uuid::from_u128(100)), &first_recipe, plugin, other)
        .unwrap();
        let alias = alias();
        assert_eq!(
            audience(&alias, pin).unwrap(),
            audience(plugin, pin).unwrap()
        );

        let mut second_recipe = recipe(other_pin);
        let second_value = b"namespace-two-private-value";
        second_recipe.source.configuration =
            json!({"expected_digest": format!("{:x}", Sha256::digest(second_value))});
        let mut second_scope = scope(Uuid::from_u128(105));
        second_scope.lifetime.namespace_id = Uuid::from_u128(2).to_string();
        let second = values(second_value)
            .bind(second_scope, &second_recipe, other, other)
            .unwrap();

        for input in [
            &first.inputs.source,
            &first.inputs.bootstrap,
            &first.inputs.bootstrap_source,
            &second.inputs.source,
        ] {
            let encoded = serde_json::to_vec(input).unwrap();
            for value in [
                source_value.as_slice(),
                bootstrap_value.as_slice(),
                second_value.as_slice(),
            ] {
                assert!(!encoded.windows(value.len()).any(|window| window == value));
            }
        }
        let persisted = serde_json::to_string(&first_recipe).unwrap();
        assert!(!persisted.contains(CAPABILITY_PREFIX));
        assert!(!persisted.contains("private-bootstrap-value"));

        let source = plugin.source_plugins[0]
            .create_source("credential-source-one", &first.inputs.source, false)
            .await
            .unwrap();
        let second_source = other.source_plugins[0]
            .create_source("credential-source-two", &second.inputs.source, false)
            .await
            .unwrap();
        let foreign = other.source_plugins[0]
            .create_source("credential-foreign", &first.inputs.source, false)
            .await
            .unwrap();
        assert!(foreign.start().await.is_err());
        for _ in 0..2 {
            source.start().await.unwrap();
            second_source.start().await.unwrap();
            source.stop().await.unwrap();
            second_source.stop().await.unwrap();
        }
        let provider = other.bootstrap_plugins[0]
            .create_bootstrap_provider(&first.inputs.bootstrap, &first.inputs.bootstrap_source)
            .await
            .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let result = provider
            .bootstrap(
                drasi_lib::bootstrap::BootstrapRequest {
                    query_id: "credential-query".into(),
                    node_labels: Vec::new(),
                    relation_labels: Vec::new(),
                    request_id: "credential-bootstrap".into(),
                },
                &drasi_lib::bootstrap::BootstrapContext::new_minimal(
                    "credential-host".into(),
                    "credential-source-one".into(),
                ),
                sender,
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.event_count, 0);
        assert!(receiver.recv().await.is_none());
        drop(provider);
        drop(foreign);
        drop(source);
        drop(second_source);
        first.release_after_join().unwrap();
        second.release_after_join().unwrap();
        assert!(!CredentialLease::retained_for_graph(Uuid::from_u128(100)).unwrap());
        assert!(!CredentialLease::retained_for_graph(Uuid::from_u128(105)).unwrap());

        let lease = values(source_value)
            .bind(scope(Uuid::from_u128(106)), &recipe(pin), plugin, plugin)
            .unwrap();
        let source = plugin.source_plugins[0]
            .create_source(
                "credential-premature-revocation",
                &lease.inputs.source,
                false,
            )
            .await
            .unwrap();
        // This negative control deliberately violates the joined-cleanup precondition.
        lease.release_after_join().unwrap();
        assert!(source.start().await.is_err());
        drop(source);
        exercise_instance_diagnostics(plugin, pin).await;
    }

    async fn exercise_instance_diagnostics(plugin: &LoadedPlugin, pin: &str) {
        let value = b"private-instance-diagnostic-sentinel";
        let mut candidate = recipe(pin);
        candidate.source.configuration = json!({
            "diagnostic_probe": true,
            "expected_digest": format!("{:x}", Sha256::digest(value)),
        });
        let graph = Uuid::from_u128(107);
        let mut scope = scope(graph);
        scope.native_id = "credential-diagnostics-probe".into();
        let lease = values(value)
            .bind(scope, &candidate, plugin, plugin)
            .unwrap();
        let source = plugin.source_plugins[0]
            .create_source("credential-diagnostics-probe", &lease.inputs.source, false)
            .await
            .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(16);
        source
            .as_any()
            .downcast_ref::<drasi_host_sdk::proxies::source::SourceProxy>()
            .unwrap()
            .initialize_redacted(drasi_lib::context::SourceRuntimeContext::new(
                "credential-diagnostics-instance",
                "credential-diagnostics-probe",
                None,
                sender,
                None,
            ))
            .await;
        assert!(source.start().await.is_err());
        let message = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let drasi_lib::component_graph::ComponentUpdate::Status {
                    component_id,
                    status: drasi_lib::channels::ComponentStatus::Error,
                    message,
                } = receiver.recv().await.unwrap()
                {
                    assert_eq!(component_id, "credential-diagnostics-probe");
                    break message.unwrap();
                }
            }
        })
        .await
        .unwrap();
        assert!(
            !message.contains("private-instance-diagnostic-sentinel")
                && !message.contains(CAPABILITY_PREFIX),
            "plugin lifecycle text exposed credential material through the host SDK"
        );
        let registry = drasi_lib::managers::get_or_init_global_registry();
        let logs = registry
            .get_history_by_key(&drasi_lib::managers::ComponentLogKey::new(
                "credential-diagnostics-instance",
                drasi_lib::channels::ComponentType::Source,
                "credential-diagnostics-probe",
            ))
            .await;
        assert!(
            !logs.is_empty(),
            "the proof must exercise the actual per-instance log callback"
        );
        for log in logs {
            assert!(!log.message.contains("private-instance-diagnostic-sentinel"));
            assert!(!log.message.contains(CAPABILITY_PREFIX));
        }
        source.stop().await.unwrap();
        drop(source);
        lease.release_after_join().unwrap();
        assert!(!CredentialLease::retained_for_graph(graph).unwrap());
    }
}
