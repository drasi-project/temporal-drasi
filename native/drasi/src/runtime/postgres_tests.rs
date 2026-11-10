use super::{admission_tests::*, tests::*, *};
use crate::credentials::{ComponentSlot, ResolvedField};

fn reference() -> SecretRef {
    SecretRef {
        provider: "postgres-fixture".into(),
        key: "database-password".into(),
        version: "1".into(),
    }
}

fn tls_reference() -> SecretRef {
    SecretRef {
        provider: "postgres-fixture".into(),
        key: "tls-mode".into(),
        version: "1".into(),
    }
}

fn values() -> AdmissionValues {
    AdmissionValues::new(
        [ComponentSlot::Source, ComponentSlot::Bootstrap]
            .into_iter()
            .flat_map(|component| {
                [
                    ResolvedField::new(
                        component,
                        "password".into(),
                        reference(),
                        b"verification-only-password".to_vec(),
                    ),
                    ResolvedField::new(
                        component,
                        "sslMode".into(),
                        tls_reference(),
                        b"disable".to_vec(),
                    ),
                ]
            })
            .collect(),
    )
    .unwrap()
}

fn sql(container: &str, statement: &str) {
    let output = std::process::Command::new("docker")
        .args([
            "exec",
            container,
            "psql",
            "-U",
            "postgres",
            "-d",
            "watchfixture",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            statement,
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "PostgreSQL fixture mutation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn private_absent(path: &Path) {
    if path.is_dir() {
        for entry in std::fs::read_dir(path).unwrap() {
            private_absent(&entry.unwrap().path());
        }
    } else {
        let bytes = std::fs::read(path).unwrap();
        let password = b"verification-only-password";
        assert!(
            !bytes
                .windows(password.len())
                .any(|window| window == password),
            "database credential reached native persisted state"
        );
    }
}

pub(crate) async fn exercise(
    make_plugins: impl Fn(&Path) -> Plugins,
    source_pin: &str,
    bootstrap_pin: &str,
    container: &str,
    port: u16,
) {
    let connection = json!({
        "host": "127.0.0.1",
        "port": port,
        "database": "watchfixture",
        "user": "postgres",
        "tables": ["watch_items"],
        "slotName": "native_fixture_slot",
        "publicationName": "native_fixture_pub"
    });
    let component = |pin: &str| ComponentRecipe {
        plugin: PluginComponentRef {
            digest: pin.into(),
            component: "postgres".into(),
        },
        configuration: connection.clone(),
        secrets: BTreeMap::from([
            ("password".into(), reference()),
            ("sslMode".into(), tls_reference()),
        ]),
    };
    let mut bootstrap = component(bootstrap_pin);
    bootstrap.configuration["slotName"] = json!("unused_bootstrap_slot");
    let definition = NodeDefinition {
        lifetime: ResourceLifetime {
            namespace_id: "00000000-0000-4000-8000-000000000001".into(),
            resource_id: "postgres-fixture".into(),
            incarnation: "source-1".into(),
        },
        activation: 1,
        recipe: NodeRecipe::PluginSource {
            source: Box::new(component(source_pin)),
            bootstrap: Box::new(bootstrap),
        },
    };
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    config.start_mode = StartMode::Dormant;
    config.managed_plugins = vec![ManagedPluginPair {
        source: component(source_pin).plugin,
        bootstrap: component(bootstrap_pin).plugin,
    }];
    let mut host = Host::open(config.clone()).await.unwrap();
    host.plugins = Some(Mutex::new(make_plugins(Path::new(&host.config.state_dir))));
    for (field, value) in [
        ("port", json!(65536)),
        ("port", json!("5432")),
        ("sslMode", json!("invalid")),
    ] {
        let mut invalid = definition.clone();
        let NodeRecipe::PluginSource { source, bootstrap } = &mut invalid.recipe else {
            unreachable!()
        };
        for component in [source, bootstrap] {
            component.secrets.remove(field);
            component.configuration[field] = value.clone();
        }
        assert_eq!(
            host.check_plugin_source(invalid).await.unwrap_err().code,
            1,
            "{field}: {value}"
        );
    }
    assert!(host.check_plugin_source(definition.clone()).await.unwrap());
    let source = host
        .admit_plugin_source(definition.clone(), values())
        .await
        .unwrap();
    let query = NodeDefinition {
        lifetime: ResourceLifetime {
            namespace_id: definition.lifetime.namespace_id.clone(),
            resource_id: "postgres-query".into(),
            incarnation: "query-1".into(),
        },
        activation: 1,
        recipe: NodeRecipe::Query {
            language: Language::Cypher,
            text: "MATCH (n:watch_items) WHERE n.amount >= 10 RETURN n.name AS name, n.amount * 2 AS doubled".into(),
            sources: vec![source.native_id.clone()],
        },
    };
    let observed = admit(&host, &serde_json::to_value(&query).unwrap())
        .await
        .unwrap();
    let query_id = observed["native_id"].as_str().unwrap().to_string();
    let initial = query_state(
        &host,
        &query_id,
        &[json!({"name": "initial", "doubled": 24})],
    )
    .await;
    let snapshot = attach_query(&host, "postgres-watch", &query_id).await;
    assert_eq!(snapshot.kind, EventKind::Snapshot);
    assert_eq!(snapshot.rows.len(), 1);
    assert_eq!(
        snapshot.rows[0].value,
        json!({"name": "initial", "doubled": 24})
    );
    accept(&host, &snapshot).await;

    sql(container, "UPDATE watch_items SET amount = 18 WHERE id = 1");
    let changed = query_state(
        &host,
        &query_id,
        &[json!({"name": "initial", "doubled": 36})],
    )
    .await;
    assert_eq!(changed.generation, initial.generation);
    assert_eq!(changed.sequence, initial.sequence + 1);
    let accepted = pending(&host, "postgres-watch").await;
    assert_eq!(accepted.kind, EventKind::Delta);
    assert_eq!(accepted.sequence, changed.sequence);
    let [OutputDiff::Update { before, after, .. }] = accepted.changes.as_slice() else {
        panic!("PostgreSQL update did not publish exactly one update");
    };
    assert_eq!(
        (before, after),
        (
            &json!({"name": "initial", "doubled": 24}),
            &json!({"name": "initial", "doubled": 36})
        )
    );
    let accepted_bytes = serde_json::to_vec(&accepted).unwrap();
    host.close().await.unwrap();
    drop(host);
    private_absent(&workspace.0);

    sql(container, "UPDATE watch_items SET amount = 22 WHERE id = 1");
    let mut host = Host::open(config).await.unwrap();
    host.plugins = Some(Mutex::new(make_plugins(Path::new(&host.config.state_dir))));
    let restored_source = host
        .admit_plugin_source(definition, values())
        .await
        .unwrap();
    assert_eq!(restored_source.native_id, source.native_id);
    let restored = admit(&host, &serde_json::to_value(&query).unwrap())
        .await
        .unwrap();
    assert_eq!(restored["native_id"], query_id);
    let replayed = pending(&host, "postgres-watch").await;
    assert_eq!(serde_json::to_vec(&replayed).unwrap(), accepted_bytes);
    accept(&host, &replayed).await;
    let resumed = query_state(
        &host,
        &query_id,
        &[json!({"name": "initial", "doubled": 44})],
    )
    .await;
    assert_eq!(resumed.generation, changed.generation);
    assert_eq!(resumed.config_hash, changed.config_hash);
    assert_eq!(resumed.sequence, changed.sequence + 1);
    let offline = pending(&host, "postgres-watch").await;
    assert_eq!(offline.kind, EventKind::Delta);
    assert_eq!(offline.sequence, resumed.sequence);
    let [OutputDiff::Update { before, after, .. }] = offline.changes.as_slice() else {
        panic!("Offline PostgreSQL update did not publish exactly one update");
    };
    assert_eq!(
        (before, after),
        (
            &json!({"name": "initial", "doubled": 36}),
            &json!({"name": "initial", "doubled": 44})
        )
    );
    accept(&host, &offline).await;
    let mut previous = resumed;
    for (statement, expected, kind, data) in [
        (
            "DELETE FROM watch_items WHERE id = 1",
            vec![],
            "DELETE",
            json!({"name": "initial", "doubled": 44}),
        ),
        (
            "INSERT INTO watch_items VALUES (3, 'added', 17)",
            vec![json!({"name": "added", "doubled": 34})],
            "ADD",
            json!({"name": "added", "doubled": 34}),
        ),
        (
            "UPDATE watch_items SET amount = 3 WHERE id = 3",
            vec![],
            "DELETE",
            json!({"name": "added", "doubled": 34}),
        ),
    ] {
        sql(container, statement);
        let current = query_state(&host, &query_id, &expected).await;
        assert_eq!(current.generation, previous.generation);
        assert_eq!(current.sequence, previous.sequence + 1);
        let event = pending(&host, "postgres-watch").await;
        assert_eq!(event.kind, EventKind::Delta);
        assert_eq!(event.sequence, current.sequence);
        let [change] = event.changes.as_slice() else {
            panic!("PostgreSQL mutation did not publish exactly one change");
        };
        let actual = match change {
            OutputDiff::Add { data, .. } => ("ADD", data),
            OutputDiff::Delete { data, .. } => ("DELETE", data),
            OutputDiff::Update { .. } => {
                panic!("PostgreSQL mutation published an unexpected update")
            }
        };
        assert_eq!(actual, (kind, &data));
        accept(&host, &event).await;
        previous = current;
    }
    assert!(host.poll(16).await.unwrap().is_empty());

    let mut observer = query.clone();
    observer.lifetime.resource_id = "postgres-observer".into();
    let observed = admit(&host, &serde_json::to_value(&observer).unwrap())
        .await
        .unwrap();
    let observer_id = observed["native_id"].as_str().unwrap();
    query_state(&host, observer_id, &[]).await;
    stop_node(&host, &query_id).await;
    sql(
        container,
        "INSERT INTO watch_items VALUES (4, 'paused', 23)",
    );
    let expected = [json!({"name": "paused", "doubled": 46})];
    query_state(&host, observer_id, &expected).await;
    let reactivated = admit(&host, &serde_json::to_value(&query).unwrap())
        .await
        .unwrap();
    assert_eq!(reactivated["native_id"], query_id);
    let reactivated = query_state(&host, &query_id, &expected).await;
    assert_eq!(reactivated.config_hash, previous.config_hash);
    assert_eq!(reactivated.generation, previous.generation);
    assert_eq!(reactivated.sequence, previous.sequence + 1);
    let replayed = pending(&host, "postgres-watch").await;
    assert_eq!(replayed.kind, EventKind::Delta);
    assert_eq!(replayed.sequence, reactivated.sequence);
    let [OutputDiff::Add { data, .. }] = replayed.changes.as_slice() else {
        panic!("resumed PostgreSQL query did not replay the paused insert");
    };
    assert_eq!(data, &expected[0]);
    accept(&host, &replayed).await;
    assert!(host.poll(16).await.unwrap().is_empty());
    host.close().await.unwrap();
    drop(host);
    private_absent(&workspace.0);
}
