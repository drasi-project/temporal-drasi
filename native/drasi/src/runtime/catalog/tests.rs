use super::*;
use crate::runtime::{admission_tests::*, tests::*};

#[test]
fn plugin_recipes_are_validated_before_hashing() {
    let component = ComponentRecipe {
        plugin: PluginComponentRef {
            digest: format!("sha256:{}", "a".repeat(64)),
            component: "fixture".into(),
        },
        configuration: json!({}),
        secrets: BTreeMap::from([(
            "connection".into(),
            SecretRef {
                provider: "fixture".into(),
                key: "key".into(),
                version: "version".into(),
            },
        )]),
    };
    let mut definition: NodeDefinition = serde_json::from_value(source_definition()).unwrap();
    definition.recipe = NodeRecipe::PluginSource {
        source: Box::new(component.clone()),
        bootstrap: Box::new(component),
    };
    definition.native_id().unwrap();
    for bootstrap_slot in [false, true] {
        for invalid in ["pin", "component", "version", "configuration"] {
            let mut candidate = definition.clone();
            let NodeRecipe::PluginSource { source, bootstrap } = &mut candidate.recipe else {
                unreachable!()
            };
            let component = if bootstrap_slot { bootstrap } else { source };
            match invalid {
                "pin" => component.plugin.digest = "invalid".into(),
                "component" => component.plugin.component.clear(),
                "version" => component
                    .secrets
                    .get_mut("connection")
                    .unwrap()
                    .version
                    .clear(),
                _ => {
                    component.configuration = json!({"value":{"kind":"Secret","name":"capability"}})
                }
            }
            assert!(
                candidate.native_id().is_err(),
                "invalid {invalid} reached native identity"
            );
        }
    }
}

#[test]
fn public_resource_ids_do_not_use_native_path_identifier_rules() {
    for id in ["orders.events".into(), "0".into(), "a".repeat(128)] {
        let mut definition: NodeDefinition = serde_json::from_value(source_definition()).unwrap();
        definition.lifetime.resource_id = id;
        let native_id = definition.native_id().unwrap();
        identifier(&native_id).unwrap();
        assert!(native_id.starts_with("s-"));
    }
    for id in [
        "".into(),
        ".source".into(),
        "_source".into(),
        "-source".into(),
        "source/name".into(),
        "source name".into(),
        "sourc\u{e9}".into(),
        "a".repeat(129),
    ] {
        let mut definition: NodeDefinition = serde_json::from_value(source_definition()).unwrap();
        definition.lifetime.resource_id = id;
        assert!(definition.native_id().is_err());
    }
    assert!(identifier("native.query").is_err());
}

#[tokio::test]
async fn admission_crash_cuts_do_not_regrant_consumed_creation() {
    for consumed in [false, true] {
        let workspace = Workspace::new();
        let mut config = workspace.config();
        config.sources.clear();
        config.queries.clear();
        let mut host = Host::open(config.clone()).await.unwrap();
        let source = admit(&host, &source_definition()).await.unwrap();
        let source_id = source["native_id"].as_str().unwrap();
        let healthy_definition =
            query_definition(source_id, "healthy", "00000000-0000-4000-8000-000000000003");
        let healthy = admit(&host, &healthy_definition).await.unwrap();
        let healthy_id = healthy["native_id"].as_str().unwrap();
        let initial = attach_query(&host, "watch", healthy_id).await;
        accept(&host, &initial).await;
        feed_source(&host, source_id, 1, 12).await;
        query_state(&host, healthy_id, &[json!({"amount": 12})]).await;
        let output = pending(&host, "watch").await;
        let bytes = serde_json::to_vec(&output).unwrap();
        let candidate = query_definition(
            source_id,
            "candidate",
            "00000000-0000-4000-8000-000000000004",
        );
        let mut ledger = host.catalog().unwrap().lock().await.clone();
        let candidate_id = ledger
            .insert(serde_json::from_value(candidate.clone()).unwrap(), &config)
            .unwrap();
        if consumed {
            ledger.nodes.get_mut(&candidate_id).unwrap().phase = Phase::Provisioned;
        }
        host.save_catalog(ledger).await.unwrap();
        host.close().await.unwrap();
        drop(host);

        let mut host = Host::open(config).await.unwrap();
        assert_eq!(
            serde_json::to_vec(&pending(&host, "watch").await).unwrap(),
            bytes
        );
        let admitted = admit(&host, &candidate).await;
        if consumed {
            assert!(admitted
                .unwrap_err()
                .message
                .contains("existing-state recovery"));
        } else {
            assert_eq!(admitted.unwrap()["native_id"], candidate_id);
            query_state(&host, &candidate_id, &[json!({"amount": 12})]).await;
        }
        feed_source(&host, source_id, 2, 14).await;
        query_state(&host, healthy_id, &[json!({"amount": 14})]).await;
        assert_eq!(
            serde_json::to_vec(&pending(&host, "watch").await).unwrap(),
            bytes
        );
        accept(&host, &output).await;
        let next = pending(&host, "watch").await;
        assert_eq!(next.sequence, output.sequence + 1);
        assert_eq!(next.incarnation, output.incarnation);
        host.close().await.unwrap();
        drop(host);
        if consumed {
            use drasi_core::interface::IndexBackendPlugin;
            let provider =
                RocksDbIndexProvider::new(workspace.0.join("owner/indexes"), true, false)
                    .with_memory_budget_bytes(256 << 20)
                    .unwrap();
            let indexes = provider.create_indexes(&candidate_id).await.unwrap();
            let checkpoints = indexes.checkpoint_store.as_ref().unwrap();
            assert_eq!(checkpoints.read_config_hash().await.unwrap(), None);
            assert_eq!(
                checkpoints
                    .read_output_generation(&candidate_id)
                    .await
                    .unwrap(),
                None
            );
            assert!(checkpoints.read_all_checkpoints().await.unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn invalid_admissions_do_not_change_catalog_or_graph() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    let mut host = Host::open(config).await.unwrap();
    let source = admit(&host, &source_definition()).await.unwrap();
    let definition = query_definition(
        source["native_id"].as_str().unwrap(),
        "query",
        "00000000-0000-4000-8000-000000000003",
    );
    let catalog = host.store().unwrap().get(CONTROL, "catalog").await.unwrap();
    let graph = host.core().unwrap().get_graph().await;
    for (field, value) in [
        ("incarnation", json!("")),
        (
            "namespace_id",
            json!("00000000-0000-4000-8000-000000000009"),
        ),
    ] {
        let mut invalid = definition.clone();
        invalid["lifetime"][field] = value;
        admit(&host, &invalid).await.unwrap_err();
    }
    for (field, value) in [
        ("sources", json!(["absent"])),
        ("sources", json!([source["native_id"], source["native_id"]])),
        ("text", json!("")),
        ("unexpected", json!("field")),
    ] {
        let mut invalid = definition.clone();
        invalid["recipe"][field] = value;
        admit(&host, &invalid).await.unwrap_err();
    }
    let mut invalid = definition;
    invalid["activation"] = json!(0);
    admit(&host, &invalid).await.unwrap_err();
    assert_eq!(
        host.store().unwrap().get(CONTROL, "catalog").await.unwrap(),
        catalog
    );
    assert_eq!(
        host.core().unwrap().get_graph().await.nodes.len(),
        graph.nodes.len()
    );
    host.close().await.unwrap();
}

#[test]
fn catalog_caps_include_fixed_graph_and_serialized_bytes() {
    let workspace = Workspace::new();
    let config = workspace.config();
    let mut ledger = AdmissionLedger::default();
    let mut source = source_definition();
    for index in 0..63 {
        source["lifetime"]["resource_id"] = json!(format!("source-{index}"));
        let id = ledger
            .insert(serde_json::from_value(source.clone()).unwrap(), &config)
            .unwrap();
        ledger.nodes.get_mut(&id).unwrap().phase = Phase::Provisioned;
    }
    ledger.validate(&config).unwrap();
    source["lifetime"]["resource_id"] = json!("one-source-too-many");
    assert!(ledger
        .insert(serde_json::from_value(source).unwrap(), &config)
        .unwrap_err()
        .message
        .contains("capacity"));
    let mut empty = config;
    empty.sources.clear();
    empty.queries.clear();
    let mut ledger = AdmissionLedger::default();
    let source: NodeDefinition = serde_json::from_value(source_definition()).unwrap();
    let source_id = ledger.insert(source, &empty).unwrap();
    ledger.nodes.get_mut(&source_id).unwrap().phase = Phase::Provisioned;
    let mut bytes = serde_json::to_vec(&ledger).unwrap().len();
    for index in 0..256 {
        let mut query = query_definition(
            &source_id,
            &format!("query-{index}"),
            "00000000-0000-4000-8000-000000000003",
        );
        query["recipe"]["text"] = json!("x".repeat(64 << 10));
        let definition: NodeDefinition = serde_json::from_value(query).unwrap();
        let id = definition.native_id().unwrap();
        let admission = Admission {
            definition,
            phase: Phase::Intended,
            retirement: None,
            fault: None,
        };
        let next_bytes = bytes
            + serde_json::to_vec(&id).unwrap().len()
            + serde_json::to_vec(&admission).unwrap().len()
            + 2;
        if next_bytes > 8 << 20 {
            assert_eq!(serde_json::to_vec(&ledger).unwrap().len(), bytes);
            ledger.validate(&empty).unwrap();
            ledger.nodes.insert(id, admission);
            assert_eq!(serde_json::to_vec(&ledger).unwrap().len(), next_bytes);
            assert!(ledger
                .validate(&empty)
                .unwrap_err()
                .message
                .contains("byte capacity"));
            return;
        }
        ledger.nodes.insert(id, admission);
        bytes = next_bytes;
    }
    panic!("serialized admission bytes were not bounded");
}
