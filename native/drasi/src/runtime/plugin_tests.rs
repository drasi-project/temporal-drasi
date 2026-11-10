use super::{admission_tests::*, tests::*, *};
use crate::credentials::{ComponentSlot, ResolvedField};
use drasi_core::interface::IndexBackendPlugin;
use drasi_lib::channels::ComponentStatus;
use sha2::{Digest, Sha256};
use std::time::Duration;

const SOURCE_VALUE: &[u8] = b"private-managed-source-sentinel";
const BOOTSTRAP_VALUE: &[u8] = b"private-managed-bootstrap-sentinel";

fn reference() -> SecretRef {
    SecretRef {
        provider: "fixture".into(),
        key: "immutable-key".into(),
        version: "generation-1".into(),
    }
}

fn definition(source: &str, bootstrap: &str, audit: &Path) -> NodeDefinition {
    let component = |pin: &str, kind: &str, value: &[u8]| ComponentRecipe {
        plugin: PluginComponentRef {
            digest: pin.into(),
            component: kind.into(),
        },
        configuration: json!({
            "expected_digest": format!("{:x}", Sha256::digest(value)),
            "factory_audit": audit.to_str().unwrap(),
        }),
        secrets: BTreeMap::from([("connection".into(), reference())]),
    };
    NodeDefinition {
        lifetime: ResourceLifetime {
            namespace_id: "00000000-0000-4000-8000-000000000001".into(),
            resource_id: "external".into(),
            incarnation: "1".into(),
        },
        activation: 1,
        recipe: NodeRecipe::PluginSource {
            source: Box::new(component(source, "test-source", SOURCE_VALUE)),
            bootstrap: Box::new(component(bootstrap, "test-bootstrap", BOOTSTRAP_VALUE)),
        },
    }
}

fn values() -> AdmissionValues {
    AdmissionValues::new(vec![
        ResolvedField::new(
            ComponentSlot::Source,
            "connection".into(),
            reference(),
            SOURCE_VALUE.to_vec(),
        ),
        ResolvedField::new(
            ComponentSlot::Bootstrap,
            "connection".into(),
            reference(),
            BOOTSTRAP_VALUE.to_vec(),
        ),
    ])
    .unwrap()
}

fn configuration(workspace: &Workspace, definition: &NodeDefinition) -> Config {
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    config.start_mode = StartMode::Dormant;
    let NodeRecipe::PluginSource { source, bootstrap } = &definition.recipe else {
        unreachable!()
    };
    config.managed_plugins = vec![ManagedPluginPair {
        source: source.plugin.clone(),
        bootstrap: bootstrap.plugin.clone(),
    }];
    config
}

fn private_absent(value: impl Serialize) {
    let value = serde_json::to_string(&value).unwrap();
    for private in [
        std::str::from_utf8(SOURCE_VALUE).unwrap(),
        std::str::from_utf8(BOOTSTRAP_VALUE).unwrap(),
        "tdrasi-credential-",
    ] {
        assert!(
            !value.contains(private),
            "credential material reached a non-sensitive surface"
        );
    }
}

fn checkpoint_private_absent(path: &Path) {
    if path.is_dir() {
        for entry in std::fs::read_dir(path).unwrap() {
            checkpoint_private_absent(&entry.unwrap().path());
        }
    } else {
        let bytes = std::fs::read(path).unwrap();
        for private in [SOURCE_VALUE, BOOTSTRAP_VALUE, b"tdrasi-credential-"] {
            assert!(
                !bytes.windows(private.len()).any(|window| window == private),
                "credential material reached checkpoint bytes"
            );
        }
    }
}

async fn stop(host: &Host, id: &str) {
    host.stop_node(id).await.expect("joined stop failed");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let nodes = host.observe_nodes().await.unwrap();
            if nodes
                .iter()
                .any(|node| node.native_id == id && matches!(node.status, NodeStatus::Stopped))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("joined stop did not reach Stopped");
}

async fn query_failed(host: &Host, id: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let query = host
                .core()
                .unwrap()
                .query_manager()
                .get_query_instance(id)
                .await
                .unwrap();
            if query.status().await == ComponentStatus::Error {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("asynchronous bootstrap failure was not surfaced");
}

pub(crate) async fn exercise(
    make_plugins: impl Fn(&Path) -> Plugins,
    source_pin: &str,
    bootstrap_pin: &str,
    audit: &Path,
    calls: impl Fn() -> (u64, u64),
) {
    let definition = definition(source_pin, bootstrap_pin, audit);
    let workspace = Workspace::new();
    let mut transport = Host::open(configuration(&workspace, &definition))
        .await
        .unwrap();
    transport.plugins = Some(Mutex::new(make_plugins(Path::new(
        &transport.config.state_dir,
    ))));
    let mut packet = b"TDC1\x02\x00\x00\x00".to_vec();
    for (slot, value) in [(0, SOURCE_VALUE), (1, BOOTSTRAP_VALUE)] {
        packet.push(slot);
        for field in [
            b"connection".as_slice(),
            b"fixture",
            b"immutable-key",
            b"generation-1",
            value,
        ] {
            packet.extend_from_slice(&(field.len() as u32).to_le_bytes());
            packet.extend_from_slice(field);
        }
    }
    let before = calls();
    crate::ffi::tests::exercise_sensitive_admission(transport, definition.clone(), packet).await;
    assert_eq!(calls(), (before.0 + 1, before.1 + 1));

    let workspace = Workspace::new();
    let config = configuration(&workspace, &definition);
    let mut host = Host::open(config.clone()).await.unwrap();
    host.plugins = Some(Mutex::new(make_plugins(Path::new(&host.config.state_dir))));
    let graph = host.credential_graph;
    let before = calls();
    assert!(host
        .apply(Command::AdmitNode {
            definition: definition.clone()
        })
        .await
        .is_err());
    assert!(host.catalog().unwrap().lock().await.nodes.is_empty());
    assert!(host.check_plugin_source(definition.clone()).await.unwrap());
    assert!(host.catalog().unwrap().lock().await.nodes.is_empty());
    assert_eq!(calls(), before);
    let NodeRecipe::PluginSource { source, bootstrap } = &definition.recipe else {
        unreachable!()
    };
    let recipe = PluginSourceRecipe {
        source: source.as_ref().clone(),
        bootstrap: bootstrap.as_ref().clone(),
    };
    let ledger_before = serde_json::to_value(&*host.catalog().unwrap().lock().await).unwrap();
    let stored_before = host
        .store
        .as_ref()
        .unwrap()
        .get(CONTROL, "catalog")
        .await
        .unwrap();
    for (name, recipes, expected) in [
        (
            "valid",
            vec![recipe.clone()],
            vec![SourceSchemaVerdict::Valid],
        ),
        (
            "repeated",
            vec![recipe.clone(), recipe.clone()],
            vec![SourceSchemaVerdict::Valid, SourceSchemaVerdict::Valid],
        ),
        (
            "invalid source",
            {
                let mut invalid = recipe.clone();
                invalid.source.configuration["expected_digest"] = json!(17);
                vec![invalid, recipe.clone()]
            },
            vec![
                SourceSchemaVerdict::InvalidConfiguration,
                SourceSchemaVerdict::Valid,
            ],
        ),
        (
            "invalid bootstrap",
            {
                let mut invalid = recipe.clone();
                invalid.bootstrap.configuration["expected_digest"] = json!(false);
                vec![invalid]
            },
            vec![SourceSchemaVerdict::InvalidConfiguration],
        ),
    ] {
        let command = Command::ValidatePluginSources { recipes };
        assert!(command.is_plugin_management());
        assert_eq!(
            host.apply(command).await.unwrap(),
            json!({"source_schemas": expected}),
            "{name}"
        );
        assert_eq!(calls(), before);
        assert_eq!(
            serde_json::to_value(&*host.catalog().unwrap().lock().await).unwrap(),
            ledger_before
        );
        assert_eq!(
            host.store
                .as_ref()
                .unwrap()
                .get(CONTROL, "catalog")
                .await
                .unwrap(),
            stored_before
        );
    }
    for recipes in [vec![], vec![recipe.clone(); 65]] {
        assert_eq!(
            host.validate_plugin_sources(recipes)
                .await
                .unwrap_err()
                .code,
            1
        );
    }
    let mut unreviewed = recipe.clone();
    unreviewed.source.plugin.component.push_str("-unreviewed");
    let mut invalid = recipe.clone();
    invalid.source.configuration["expected_digest"] = json!(false);
    assert_eq!(
        host.validate_plugin_sources(vec![invalid, unreviewed])
            .await
            .unwrap_err()
            .code,
        8
    );
    assert_eq!(calls(), before);
    assert!(host.observe_nodes().await.unwrap().is_empty());
    assert_eq!(
        host.store
            .as_ref()
            .unwrap()
            .get(CONTROL, "catalog")
            .await
            .unwrap(),
        stored_before
    );
    let observed = host
        .admit_plugin_source(definition.clone(), values())
        .await
        .unwrap();
    assert!(matches!(observed.status, NodeStatus::Running));
    let id = observed.native_id;
    assert!(!host.check_plugin_source(definition.clone()).await.unwrap());
    assert_eq!(calls(), (before.0 + 1, before.1 + 1));
    let source = {
        let graph = host.core().unwrap().component_graph();
        let graph = graph.read().await;
        graph
            .get_runtime::<Arc<dyn drasi_lib::Source>>(&id)
            .unwrap()
            .clone()
    };
    assert!(!source.auto_start());
    private_absent(source.properties());
    assert_eq!(
        source.properties()["managed_recipe"]["source"]["secrets"]["connection"]["version"],
        "generation-1"
    );
    source.stop().await.unwrap();
    assert!(!host.check_plugin_source(definition.clone()).await.unwrap());
    host.admit_plugin_source(
        definition.clone(),
        AdmissionValues::new(Vec::new()).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(calls(), (before.0 + 2, before.1 + 2));
    let restarted = host
        .core()
        .unwrap()
        .component_graph()
        .read()
        .await
        .get_runtime::<Arc<dyn drasi_lib::Source>>(&id)
        .unwrap()
        .clone();
    assert!(Arc::ptr_eq(&source, &restarted));
    drop(source);

    let builtin_recipe = source_definition();
    let builtin = admit(&host, &builtin_recipe).await.unwrap();
    let builtin_id = builtin["native_id"].as_str().unwrap();
    let mut query_recipe = query_definition(&id, "query", "1");
    query_recipe["recipe"]["sources"] = json!([id, builtin_id]);
    let query = admit(&host, &query_recipe).await.unwrap();
    let query_id = query["native_id"].as_str().unwrap();
    feed_source(&host, builtin_id, 1, 10).await;
    let state = query_state(&host, query_id, &[json!({"amount":10})]).await;
    let pending = attach_query(&host, "watch", query_id).await;
    stop(&host, query_id).await;
    stop(&host, &id).await;
    host.admit_plugin_source(
        definition.clone(),
        AdmissionValues::new(Vec::new()).unwrap(),
    )
    .await
    .unwrap();
    admit(&host, &query_recipe).await.unwrap();
    assert_eq!(
        query_state(&host, query_id, &[json!({"amount":10})]).await,
        state
    );
    assert_eq!(calls(), (before.0 + 3, before.1 + 3));
    let pending_bytes = host
        .store()
        .unwrap()
        .get(SUBSCRIPTIONS, "watch")
        .await
        .unwrap();
    let ledger_bytes = host.store().unwrap().get(CONTROL, "catalog").await.unwrap();
    private_absent(host.observe_nodes().await.unwrap());
    private_absent(serde_json::from_slice::<Value>(ledger_bytes.as_ref().unwrap()).unwrap());
    host.close().await.unwrap();
    assert!(!CredentialLease::retained_for_graph(graph).unwrap());
    drop(host);

    let before = calls();
    let mut host = Host::open(config).await.unwrap();
    assert_eq!(
        calls(),
        before,
        "dormant reopen invoked an external factory"
    );
    assert_eq!(
        host.store().unwrap().get(CONTROL, "catalog").await.unwrap(),
        ledger_bytes
    );
    assert_eq!(
        host.store()
            .unwrap()
            .get(SUBSCRIPTIONS, "watch")
            .await
            .unwrap(),
        pending_bytes
    );
    let observations = host.observe_nodes().await.unwrap();
    assert_eq!(observations.len(), 3);
    for node in &observations {
        assert_eq!(
            matches!(node.status, NodeStatus::Deferred),
            node.native_id != builtin_id
        );
    }
    let replayed = super::tests::pending(&host, "watch").await;
    assert_eq!(
        serde_json::to_vec(&replayed).unwrap(),
        serde_json::to_vec(&pending).unwrap()
    );
    accept(&host, &replayed).await;
    let unavailable = host.poll(1).await.unwrap_err();
    assert_eq!(unavailable.code, 3);
    assert!(unavailable.message.contains("deferred"));
    host.plugins = Some(Mutex::new(make_plugins(Path::new(&host.config.state_dir))));
    admit(&host, &builtin_recipe).await.unwrap();
    host.admit_plugin_source(definition.clone(), values())
        .await
        .unwrap();
    admit(&host, &query_recipe).await.unwrap();
    assert_eq!(
        query_state(&host, query_id, &[json!({"amount":10})]).await,
        state
    );
    feed_source(&host, builtin_id, 2, 20).await;
    let next = super::tests::pending(&host, "watch").await;
    assert_eq!(next.sequence, pending.sequence + 1);
    assert_eq!(next.incarnation, pending.incarnation);
    let recovered = query_state(&host, query_id, &[json!({"amount":20})]).await;
    assert_eq!(recovered.generation, state.generation);
    let checkpoint = workspace.0.join("checkpoint");
    host.checkpoint(checkpoint.to_str().unwrap()).await.unwrap();
    checkpoint_private_absent(&checkpoint);
    drop(host);

    for (name, damage) in [
        ("missing hash", 0),
        ("changed hash", 1),
        ("reset marker", 2),
        ("corrupt outbox", 3),
    ] {
        let damaged = Workspace::new();
        let mut config = configuration(&damaged, &definition);
        config.owner_epoch = 2;
        config.restore_from = Some(checkpoint.to_str().unwrap().into());
        let mut host = Host::open(config.clone()).await.unwrap();
        host.close().await.unwrap();
        drop(host);
        config.restore_from = None;
        let provider = RocksDbIndexProvider::new(damaged.0.join("owner/indexes"), true, false)
            .with_memory_budget_bytes(256 << 20)
            .unwrap();
        let indexes = provider.create_indexes(query_id).await.unwrap();
        let checkpoint = indexes.checkpoint_store.as_ref().unwrap();
        match damage {
            0 => checkpoint.clear_checkpoints().await.unwrap(),
            1 => checkpoint
                .write_config_hash(recovered.config_hash.wrapping_add(1))
                .await
                .unwrap(),
            2 => checkpoint.write_config_hash(0).await.unwrap(),
            _ => indexes
                .outbox_writer
                .as_ref()
                .unwrap()
                .append(query_id, recovered.sequence, b"corrupt-outbox")
                .await
                .unwrap(),
        }
        drop(indexes);
        drop(provider);
        let mut host = Host::open(config).await.unwrap();
        host.plugins = Some(Mutex::new(make_plugins(Path::new(&host.config.state_dir))));
        admit(&host, &builtin_recipe).await.unwrap();
        host.admit_plugin_source(definition.clone(), values())
            .await
            .unwrap();
        match admit(&host, &query_recipe).await {
            Ok(node) => assert_eq!(node["native_id"], query_id, "{name}"),
            Err(error) => assert!(!error.message.is_empty(), "{name}"),
        }
        let replayed = super::tests::pending(&host, "watch").await;
        assert_eq!(
            serde_json::to_vec(&replayed).unwrap(),
            serde_json::to_vec(&next).unwrap()
        );
        host.close().await.unwrap();
        drop(host);
    }

    for (name, source_flag, bootstrap_flag, quarantined) in [
        ("bad auto-start", Some("bad_auto_start"), None, false),
        ("no replay", Some("no_replay"), None, false),
        ("factory failure", Some("fail_factory"), None, false),
        (
            "bootstrap factory failure",
            None,
            Some("fail_bootstrap_factory"),
            false,
        ),
        ("failed stop", Some("fail_stop"), None, true),
        ("source diagnostics", Some("diagnostic_probe"), None, false),
    ] {
        let workspace = Workspace::new();
        let mut candidate = definition.clone();
        let NodeRecipe::PluginSource { source, bootstrap } = &mut candidate.recipe else {
            unreachable!()
        };
        if let Some(flag) = source_flag {
            source.configuration[flag] = json!(true);
        }
        if let Some(flag) = bootstrap_flag {
            bootstrap.configuration[flag] = json!(true);
        }
        let config = configuration(&workspace, &candidate);
        let mut host = Host::open(config.clone()).await.unwrap();
        host.plugins = Some(Mutex::new(make_plugins(Path::new(&host.config.state_dir))));
        let graph = host.credential_graph;
        let result = host.admit_plugin_source(candidate.clone(), values()).await;
        if source_flag == Some("fail_stop") {
            result.unwrap();
            assert!(host
                .stop_node(&host.observe_nodes().await.unwrap()[0].native_id)
                .await
                .is_err());
        } else {
            private_absent(result.expect_err(name));
            let observation = host.observe_nodes().await.unwrap();
            assert!(
                matches!(observation[0].status, NodeStatus::RecoveryRequired),
                "{name}"
            );
            let before = calls();
            assert!(host.admit_plugin_source(candidate, values()).await.is_err());
            assert_eq!(calls(), before, "{name}");
        }

        if quarantined {
            assert!(
                host.checkpoint(workspace.0.join("forbidden-checkpoint").to_str().unwrap())
                    .await
                    .is_err(),
                "{name}"
            );
            assert!(!workspace.0.join("forbidden-checkpoint").exists());
            assert!(host
                .apply(Command::AdmitNode {
                    definition: serde_json::from_value(source_definition()).unwrap(),
                })
                .await
                .is_err());
            assert!(
                CredentialLease::retained_for_graph(graph).unwrap(),
                "{name}"
            );
            drop(host);
            assert!(
                Host::open(config).await.is_err(),
                "unproved cleanup released directory ownership"
            );
        } else {
            host.close()
                .await
                .unwrap_or_else(|error| panic!("{name}: {error:?}"));
            assert!(
                !CredentialLease::retained_for_graph(graph).unwrap(),
                "{name}"
            );
        }
    }

    for bootstrap_failure in [false, true] {
        let workspace = Workspace::new();
        let mut candidate = definition.clone();
        let NodeRecipe::PluginSource { source, bootstrap } = &mut candidate.recipe else {
            unreachable!()
        };
        if bootstrap_failure {
            bootstrap.configuration["fail_bootstrap"] = json!(true);
        } else {
            source.configuration["fail_subscribe"] = json!(true);
        }
        let mut host = Host::open(configuration(&workspace, &candidate))
            .await
            .unwrap();
        host.plugins = Some(Mutex::new(make_plugins(Path::new(&host.config.state_dir))));
        let graph = host.credential_graph;
        let source = host.admit_plugin_source(candidate, values()).await.unwrap();
        let query_definition = query_definition(&source.native_id, "failed-query", "1");
        if let Err(error) = admit(&host, &query_definition).await {
            private_absent(error);
        }
        let query = host
            .catalog()
            .unwrap()
            .lock()
            .await
            .nodes
            .iter()
            .find_map(|(id, node)| {
                matches!(node.definition.recipe, NodeRecipe::Query { .. }).then(|| id.clone())
            })
            .unwrap();
        query_failed(&host, &query).await;
        private_absent(host.core().unwrap().get_graph().await);
        host.close().await.unwrap();
        assert!(!CredentialLease::retained_for_graph(graph).unwrap());
    }

    for allow_pair in [false, true] {
        let workspace = Workspace::new();
        let mut candidate = definition.clone();
        let mut config = configuration(&workspace, &candidate);
        if allow_pair {
            let NodeRecipe::PluginSource { source, .. } = &mut candidate.recipe else {
                unreachable!()
            };
            source.configuration["expected_digest"] = json!(42);
        } else {
            config.managed_plugins.clear();
        }
        let mut host = Host::open(config).await.unwrap();
        host.plugins = Some(Mutex::new(make_plugins(Path::new(&host.config.state_dir))));
        let before = calls();
        private_absent(
            host.admit_plugin_source(candidate, values())
                .await
                .unwrap_err(),
        );
        assert_eq!(calls(), before, "invalid admission called a plugin factory");
        if !allow_pair {
            assert!(host.catalog().unwrap().lock().await.nodes.is_empty());
        }
        host.close().await.unwrap();
    }

    let workspace = Workspace::new();
    let mut candidate = definition.clone();
    let NodeRecipe::PluginSource { source, .. } = &mut candidate.recipe else {
        unreachable!()
    };
    source.configuration["fail_first_start"] = json!(true);
    let mut host = Host::open(configuration(&workspace, &candidate))
        .await
        .unwrap();
    host.plugins = Some(Mutex::new(make_plugins(Path::new(&host.config.state_dir))));
    private_absent(
        host.admit_plugin_source(candidate.clone(), values())
            .await
            .unwrap_err(),
    );
    let id = host.observe_nodes().await.unwrap()[0].native_id.clone();
    let resident = host
        .core()
        .unwrap()
        .component_graph()
        .read()
        .await
        .get_runtime::<Arc<dyn drasi_lib::Source>>(&id)
        .unwrap()
        .clone();
    let before = calls();
    stop(&host, &id).await;
    let retried = host
        .admit_plugin_source(candidate, AdmissionValues::new(Vec::new()).unwrap())
        .await
        .unwrap();
    assert!(matches!(retried.status, NodeStatus::Running));
    assert_eq!(retried.native_id, id);
    assert_eq!(
        calls(),
        (before.0 + 1, before.1 + 1),
        "retry must create exactly one replacement child epoch"
    );
    let restarted = host
        .core()
        .unwrap()
        .component_graph()
        .read()
        .await
        .get_runtime::<Arc<dyn drasi_lib::Source>>(&id)
        .unwrap()
        .clone();
    assert!(
        Arc::ptr_eq(&resident, &restarted),
        "retry replaced the graph Source"
    );
    let graph = host.credential_graph;
    host.close().await.unwrap();
    assert!(!CredentialLease::retained_for_graph(graph).unwrap());

    let workspace = Workspace::new();
    let mut host = Host::open(configuration(&workspace, &definition))
        .await
        .unwrap();
    host.plugins = Some(Mutex::new(make_plugins(Path::new(&host.config.state_dir))));
    let observed = host
        .admit_plugin_source(definition.clone(), values())
        .await
        .unwrap();
    host.core()
        .unwrap()
        .remove_source(&observed.native_id, false)
        .await
        .unwrap();
    assert!(matches!(
        host.observe_nodes().await.unwrap()[0].status,
        NodeStatus::RecoveryRequired
    ));
    let before = calls();
    assert!(host
        .admit_plugin_source(definition.clone(), values())
        .await
        .is_err());
    assert_eq!(
        calls(),
        before,
        "unexplained disappearance authorized another factory"
    );
    let graph = host.credential_graph;
    host.close().await.unwrap();
    assert!(!CredentialLease::retained_for_graph(graph).unwrap());

    let workspace = Workspace::new();
    let config = configuration(&workspace, &definition);
    let mut host = Host::open(config.clone()).await.unwrap();
    host.plugins = Some(Mutex::new(make_plugins(Path::new(&host.config.state_dir))));
    let observed = host
        .admit_plugin_source(definition.clone(), values())
        .await
        .unwrap();
    let graph = host.credential_graph;
    stop(&host, &observed.native_id).await;
    let retired = host.retire_node(&observed.native_id).await.unwrap();
    assert!(matches!(retired.status, NodeStatus::Retired));
    assert!(!CredentialLease::retained_for_graph(graph).unwrap());
    assert!(!host
        .managed_sources
        .lock()
        .await
        .contains_key(&observed.native_id));
    private_absent(&retired);
    let before = calls();
    private_absent(
        host.admit_plugin_source(definition.clone(), values())
            .await
            .unwrap_err(),
    );
    assert_eq!(calls(), before);
    host.close().await.unwrap();
    drop(host);
    let mut reopened = Host::open(config).await.unwrap();
    assert!(matches!(
        reopened.observe_nodes().await.unwrap()[0].status,
        NodeStatus::Retired
    ));
    assert_eq!(calls(), before);
    reopened.close().await.unwrap();

    let workspace = Workspace::new();
    let config = configuration(&workspace, &definition);
    let mut host = Host::open(config.clone()).await.unwrap();
    host.plugins = Some(Mutex::new(make_plugins(Path::new(&host.config.state_dir))));
    let observed = host
        .admit_plugin_source(definition.clone(), values())
        .await
        .unwrap();
    let retained = {
        let graph = host.core().unwrap().component_graph();
        let graph = graph.read().await;
        graph
            .get_runtime::<Arc<dyn drasi_lib::Source>>(&observed.native_id)
            .unwrap()
            .clone()
    };
    let graph = host.credential_graph;
    stop(&host, &observed.native_id).await;
    let retired = host.retire_node(&observed.native_id).await.unwrap();
    assert!(matches!(retired.status, NodeStatus::Retired));
    assert!(!CredentialLease::retained_for_graph(graph).unwrap());
    assert!(
        retained.start().await.is_err(),
        "retained Source restarted a retired worker"
    );
    drop(retained);
    assert!(matches!(
        host.retire_node(&observed.native_id).await.unwrap().status,
        NodeStatus::Retired
    ));
    let before = calls();
    private_absent(
        host.admit_plugin_source(definition.clone(), values())
            .await
            .unwrap_err(),
    );
    assert_eq!(calls(), before);
    host.close().await.unwrap();
    drop(host);
    let mut reopened = Host::open(config)
        .await
        .expect("joined retirement retained directory ownership");
    reopened.close().await.unwrap();

    let workspace = Workspace::new();
    let config = configuration(&workspace, &definition);
    let mut host = Host::open(config.clone()).await.unwrap();
    host.plugins = Some(Mutex::new(make_plugins(Path::new(&host.config.state_dir))));
    let observed = host
        .admit_plugin_source(definition, values())
        .await
        .unwrap();
    let retained = {
        let graph = host.core().unwrap().component_graph();
        let graph = graph.read().await;
        graph
            .get_runtime::<Arc<dyn drasi_lib::Source>>(&observed.native_id)
            .unwrap()
            .clone()
    };
    let graph = host.credential_graph;
    let cut = workspace.0.join("retained-reference-checkpoint");
    host.checkpoint(cut.to_str().unwrap()).await.unwrap();
    checkpoint_private_absent(&cut);
    assert!(!CredentialLease::retained_for_graph(graph).unwrap());
    assert!(
        retained.start().await.is_err(),
        "retained Source restarted after checkpoint close"
    );
    drop(retained);
    drop(host);
    assert!(
        Host::open(config.clone()).await.is_err(),
        "checkpoint retirement must still fence ordinary reopen",
    );
    let restored = Workspace::new();
    let mut successor = config;
    successor.state_dir = restored.0.join("owner").to_str().unwrap().into();
    successor.owner_epoch += 1;
    successor.restore_from = Some(cut.to_str().unwrap().into());
    let mut reopened = Host::open(successor)
        .await
        .expect("joined worker checkpoint could not restore in an isolated successor");
    reopened.close().await.unwrap();
}
