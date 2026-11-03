use super::{tests::*, *};

fn definition(resource: &str, incarnation: &str, recipe: Value) -> Value {
    json!({
        "lifetime": {
            "namespace_id": "00000000-0000-4000-8000-000000000001",
            "resource_id": resource,
            "incarnation": incarnation
        },
        "activation": 1,
        "recipe": recipe
    })
}

pub(super) fn source_definition() -> Value {
    definition(
        "source",
        "00000000-0000-4000-8000-000000000002",
        json!({"kind": "temporal_source"}),
    )
}

pub(super) fn query_definition(source: &str, id: &str, incarnation: &str) -> Value {
    definition(
        id,
        incarnation,
        json!({
            "kind": "query", "language": "cypher",
            "text": "MATCH (n:Item) RETURN n.amount AS amount", "sources": [source]
        }),
    )
}

pub(super) async fn admit(host: &Host, definition: &Value) -> Result<Value> {
    host.apply(serde_json::from_value(json!({
        "op": "admit_node", "definition": definition
    }))?)
    .await
    .map(|result| result["node"].clone())
}

pub(super) async fn attach_query(host: &Host, id: &str, query: &str) -> Event {
    host.apply(Command::Subscribe {
        subscription: Subscription {
            id: id.into(),
            query_id: query.into(),
            query_revision: 1,
        },
    })
    .await
    .unwrap();
    pending(host, id).await
}

pub(super) async fn feed_source(host: &Host, source: &str, position: u64, amount: i64) {
    host.apply(Command::Feed {
        source_id: source.into(),
        position,
        change: Change {
            kind: ChangeKind::Insert,
            element_id: "item".into(),
            labels: vec!["Item".into()],
            effective_from: position,
            properties: BTreeMap::from([("amount".into(), json!(amount))]),
            relation: None,
        },
    })
    .await
    .unwrap();
}

pub(super) async fn stop_node(host: &Host, id: &str) -> Value {
    let response = host
        .apply(
            serde_json::from_value(json!({"op": "stop_node", "native_id": id}))
                .expect("node stop must be supported"),
        )
        .await
        .unwrap();
    assert_eq!(response["node"]["native_id"], id);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let observations = host.apply(Command::ObserveNodes).await.unwrap();
            let node = observations["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|node| node["native_id"] == id)
                .unwrap()
                .clone();
            if node["status"] == "stopped" {
                let graph = host.core().unwrap().get_graph().await;
                let actual = graph.nodes.iter().find(|node| node.id == id).unwrap();
                assert_eq!(actual.status, drasi_lib::channels::ComponentStatus::Stopped);
                return node;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("node did not stop")
}

#[tokio::test]
async fn catalog_node_stop_and_resume_preserves_identity_and_unrelated_progress() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    config.start_mode = StartMode::Dormant;
    let mut host = Host::open(config).await.unwrap();
    let source_recipe = source_definition();
    let source = admit(&host, &source_recipe).await.unwrap();
    let source_id = source["native_id"].as_str().unwrap();
    let first_recipe = query_definition(source_id, "first", "7");
    let first = admit(&host, &first_recipe).await.unwrap();
    let first_id = first["native_id"].as_str().unwrap();
    let second_recipe = query_definition(source_id, "second", "8");
    let second = admit(&host, &second_recipe).await.unwrap();
    let second_id = second["native_id"].as_str().unwrap();
    feed_source(&host, source_id, 1, 10).await;
    let before = query_state(&host, first_id, &[json!({"amount": 10})]).await;
    let pending = attach_query(&host, "watch", first_id).await;
    let handoff = host
        .store()
        .unwrap()
        .get(SUBSCRIPTIONS, "watch")
        .await
        .unwrap();
    let query = host
        .core()
        .unwrap()
        .query_manager()
        .get_query_instance(first_id)
        .await
        .unwrap();

    let stopped = stop_node(&host, first_id).await;
    assert_eq!(stopped["definition"], first_recipe);
    feed_source(&host, source_id, 2, 20).await;
    query_state(&host, second_id, &[json!({"amount": 20})]).await;
    assert_eq!(
        host.store()
            .unwrap()
            .get(SUBSCRIPTIONS, "watch")
            .await
            .unwrap(),
        handoff
    );
    let resumed = admit(&host, &first_recipe).await.unwrap();
    assert_eq!(resumed["native_id"], first_id);
    let existing = host
        .core()
        .unwrap()
        .query_manager()
        .get_query_instance(first_id)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&query, &existing));
    let after = query_state(&host, first_id, &[json!({"amount": 20})]).await;
    assert_eq!(before.config_hash, after.config_hash);
    assert_eq!(before.generation, after.generation);
    assert_eq!(before.sequence + 1, after.sequence);
    accept(&host, &pending).await;
    let next = super::tests::pending(&host, "watch").await;
    assert_eq!(next.sequence, pending.sequence + 1);
    assert_eq!(next.incarnation, pending.incarnation);
    stop_node(&host, first_id).await;
    stop_node(&host, first_id).await;
    stop_node(&host, second_id).await;
    stop_node(&host, source_id).await;
    stop_node(&host, source_id).await;
    admit(&host, &source_recipe).await.unwrap();
    admit(&host, &first_recipe).await.unwrap();
    assert_eq!(
        query_state(&host, first_id, &[json!({"amount": 20})]).await,
        after
    );
    host.close().await.unwrap();
}

#[tokio::test]
async fn catalog_observations_retain_complete_detached_recipes() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    let mut host = Host::open(config).await.unwrap();
    let expected = source_definition();
    let source = admit(&host, &expected).await.unwrap();
    assert_eq!(source["definition"], expected);
    let mut observations = host.apply(Command::ObserveNodes).await.unwrap();
    assert_eq!(observations["nodes"][0], source);
    observations["nodes"][0]["definition"]["lifetime"]["resource_id"] = json!("changed");
    assert_eq!(
        host.apply(Command::ObserveNodes).await.unwrap()["nodes"][0],
        source
    );
    host.close().await.unwrap();
}

#[tokio::test]
async fn topology_observation_reports_actual_pending_work() {
    let workspace = Workspace::new();
    let mut host = Host::open(workspace.config()).await.unwrap();
    assert_eq!(
        host.apply(Command::ObserveNodes).await.unwrap()["topology_busy"],
        false
    );
    let topology = host.topology.lock().await;
    assert_eq!(
        host.apply(Command::ObserveNodes).await.unwrap()["topology_busy"],
        true
    );
    drop(topology);
    assert_eq!(
        host.apply(Command::ObserveNodes).await.unwrap()["topology_busy"],
        false
    );
    host.close().await.unwrap();
}

#[tokio::test]
async fn duplicate_public_binding_does_not_queue_another_snapshot() {
    use std::{future::Future, task::Context, task::Poll, task::Waker};

    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    let mut host = Host::open(config).await.unwrap();
    let source = admit(&host, &source_definition()).await.unwrap();
    let query = admit(
        &host,
        &query_definition(source["native_id"].as_str().unwrap(), "saved.query", "7"),
    )
    .await
    .unwrap();
    let command = json!({
        "op": "bind_watch", "binding": public_watch_binding(), "query_id": query["native_id"]
    });
    let first = {
        let handoff = host.handoffs.lock().await;
        let mut first =
            std::pin::pin!(host.apply(serde_json::from_value(command.clone()).unwrap()));
        assert!(matches!(
            first.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        let error = host
            .apply(serde_json::from_value(command.clone()).unwrap())
            .await
            .unwrap_err();
        assert_eq!(error.code, 3);
        assert!(error.message.contains("already in progress"));
        drop(handoff);
        first.await.unwrap()
    };
    let retry = host
        .apply(serde_json::from_value(command).unwrap())
        .await
        .unwrap();
    assert_eq!(first, retry);
    let id = first["subscription"]["definition"]["id"].as_str().unwrap();
    let snapshot = pending(&host, id).await;
    assert_eq!(
        serde_json::to_value(pending(&host, id).await).unwrap(),
        serde_json::to_value(snapshot).unwrap()
    );
    host.close().await.unwrap();
}

#[tokio::test]
async fn node_stop_requires_known_identity_and_stopped_dependencies() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    let mut host = Host::open(config).await.unwrap();
    let source = admit(&host, &source_definition()).await.unwrap();
    let source_id = source["native_id"].as_str().unwrap();
    let query = admit(&host, &query_definition(source_id, "query", "7"))
        .await
        .unwrap();
    let query_id = query["native_id"].as_str().unwrap();
    query_state(&host, query_id, &[]).await;
    let catalog = host.store().unwrap().get(CONTROL, "catalog").await.unwrap();
    for (id, code) in [("missing", 2), ("../source", 1), (source_id, 3)] {
        let error = host
            .apply(Command::StopNode {
                native_id: id.into(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, code);
    }
    assert_eq!(
        host.store().unwrap().get(CONTROL, "catalog").await.unwrap(),
        catalog
    );
    feed_source(&host, source_id, 1, 10).await;
    query_state(&host, query_id, &[json!({"amount": 10})]).await;
    host.close().await.unwrap();
}

#[tokio::test]
async fn dormant_catalog_recovery_does_not_activate_retained_nodes() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    config.start_mode = StartMode::Dormant;
    let mut host = Host::open(config.clone()).await.unwrap();
    let source_definition = source_definition();
    let source = admit(&host, &source_definition).await.unwrap();
    let source_id = source["native_id"].as_str().unwrap();
    let query_recipe = query_definition(source_id, "saved.query", "7");
    let query = admit(&host, &query_recipe).await.unwrap();
    let query_id = query["native_id"].as_str().unwrap();
    let mut held_source_definition = source_definition.clone();
    held_source_definition["lifetime"]["resource_id"] = json!("held-source");
    let held_source = admit(&host, &held_source_definition).await.unwrap();
    let held_source_id = held_source["native_id"].as_str().unwrap();
    let held_query = admit(&host, &query_definition(held_source_id, "held-query", "8"))
        .await
        .unwrap();
    let held_query_id = held_query["native_id"].as_str().unwrap();
    feed_source(&host, source_id, 1, 10).await;
    let before = query_state(&host, query_id, &[json!({"amount": 10})]).await;
    let original = attach_query(&host, "watch", query_id).await;
    let handoff = host
        .store()
        .unwrap()
        .get(SUBSCRIPTIONS, "watch")
        .await
        .unwrap();
    let checkpoint = workspace.0.join("checkpoint");
    host.checkpoint(checkpoint.to_str().unwrap()).await.unwrap();
    drop(host);

    config.state_dir = workspace.0.join("successor").to_str().unwrap().into();
    config.restore_from = Some(checkpoint.to_str().unwrap().into());
    config.owner_epoch = 2;
    config.start_mode = StartMode::Dormant;
    let mut host = Host::open(config).await.unwrap();
    let observations = host.observe_nodes().await.unwrap();
    assert_eq!(observations.len(), 4);
    for node in observations {
        assert!(
            matches!(node.status, NodeStatus::Stopped),
            "restored node activated"
        );
    }
    assert_eq!(
        host.store()
            .unwrap()
            .get(SUBSCRIPTIONS, "watch")
            .await
            .unwrap(),
        handoff
    );
    assert_eq!(
        serde_json::to_value(pending(&host, "watch").await).unwrap(),
        serde_json::to_value(&original).unwrap()
    );
    admit(&host, &source_definition).await.unwrap();
    admit(&host, &query_recipe).await.unwrap();
    for node in host.observe_nodes().await.unwrap() {
        if node.native_id == held_source_id || node.native_id == held_query_id {
            assert!(matches!(node.status, NodeStatus::Stopped));
        }
    }
    assert_eq!(
        query_state(&host, query_id, &[json!({"amount": 10})]).await,
        before
    );
    accept(&host, &original).await;
    feed_source(&host, source_id, 2, 20).await;
    query_state(&host, query_id, &[json!({"amount": 20})]).await;
    let next = pending(&host, "watch").await;
    assert_eq!(next.sequence, original.sequence + 1);
    assert_eq!(next.incarnation, original.incarnation);
    host.close().await.unwrap();
}

#[tokio::test]
async fn dormant_start_refuses_fixed_definitions_before_creating_state() {
    let workspace = Workspace::new();
    let mut config = workspace.config();
    config.start_mode = StartMode::Dormant;
    let error = Host::open(config.clone())
        .await
        .err()
        .expect("dormant startup accepted fixed definitions");
    assert_eq!(error.code, 1);
    assert!(error.message.contains("empty source/query envelope"));
    assert!(!Path::new(&config.state_dir).exists());
}

fn public_watch_binding() -> Value {
    json!({
        "watch": {
            "namespace_id": "00000000-0000-4000-8000-000000000001",
            "resource_id": "watch.condition", "incarnation": "42"
        },
        "watch_revision": 43,
        "query": {
            "namespace_id": "00000000-0000-4000-8000-000000000001",
            "resource_id": "saved.query", "incarnation": "7"
        },
        "query_revision": 8
    })
}

#[tokio::test]
async fn bound_watch_polling_isolates_a_query_gap_without_advancing_failed_progress() {
    let workspace = Workspace::new();
    let mut configuration = workspace.config();
    configuration.sources.clear();
    configuration.queries.clear();
    configuration.limits.outbox_entries = 1;
    let mut host = Host::open(configuration).await.unwrap();
    let source = admit(&host, &source_definition()).await.unwrap();
    let source_id = source["native_id"].as_str().unwrap();
    let bad_query = admit(&host, &query_definition(source_id, "saved.query", "7"))
        .await
        .unwrap();
    let healthy_query = admit(&host, &query_definition(source_id, "healthy.query", "8"))
        .await
        .unwrap();
    let registered = host
        .apply(
            serde_json::from_value(json!({
                "op": "bind_watch", "binding": public_watch_binding(),
                "query_id": bad_query["native_id"]
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let bad_id = registered["subscription"]["definition"]["id"]
        .as_str()
        .unwrap();
    let initial = pending(&host, bad_id).await;
    accept(&host, &initial).await;
    for position in 1..=3 {
        feed_source(&host, source_id, position, position as i64).await;
        query_state(
            &host,
            bad_query["native_id"].as_str().unwrap(),
            &[json!({"amount": position})],
        )
        .await;
        query_state(
            &host,
            healthy_query["native_id"].as_str().unwrap(),
            &[json!({"amount": position})],
        )
        .await;
    }
    let mut healthy_binding = public_watch_binding();
    healthy_binding["watch"]["resource_id"] = json!("healthy.watch");
    healthy_binding["query"]["resource_id"] = json!("healthy.query");
    healthy_binding["query"]["incarnation"] = json!("8");
    let healthy = host.apply(serde_json::from_value(json!({
        "op": "bind_watch", "binding": healthy_binding, "query_id": healthy_query["native_id"]
    })).unwrap()).await.unwrap();
    let healthy_id = healthy["subscription"]["definition"]["id"]
        .as_str()
        .unwrap();
    let failed_progress = host
        .store()
        .unwrap()
        .get(SUBSCRIPTIONS, bad_id)
        .await
        .unwrap();
    assert_eq!(host.poll(64).await.unwrap_err().code, 6);
    for (position, kind) in [(3, EventKind::Snapshot), (4, EventKind::Delta)] {
        if position == 4 {
            feed_source(&host, source_id, 4, 4).await;
            query_state(
                &host,
                healthy_query["native_id"].as_str().unwrap(),
                &[json!({"amount": 4})],
            )
            .await;
        }
        let failed = host
            .apply(
                serde_json::from_value(json!({
                    "op": "poll_watch", "subscription_id": bad_id
                }))
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(failed.code, 6);
        assert_eq!(
            host.store()
                .unwrap()
                .get(SUBSCRIPTIONS, bad_id)
                .await
                .unwrap(),
            failed_progress
        );
        let output = host
            .apply(
                serde_json::from_value(json!({
                    "op": "poll_watch", "subscription_id": healthy_id
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        let event: Event = serde_json::from_value(output["event"].clone()).unwrap();
        assert_eq!(event.sequence, position);
        assert_eq!(event.kind, kind);
        accept(&host, &event).await;
    }
    let idle = host
        .apply(
            serde_json::from_value(json!({
                "op": "poll_watch", "subscription_id": healthy_id
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(idle["event"].is_null());
    host.apply(Command::Subscribe {
        subscription: Subscription {
            id: "anonymous".into(),
            query_id: healthy_query["native_id"].as_str().unwrap().into(),
            query_revision: 1,
        },
    })
    .await
    .unwrap();
    for (id, code) in [("anonymous", 3), ("missing", 2), ("invalid/id", 1)] {
        let error = host
            .apply(
                serde_json::from_value(json!({"op": "poll_watch", "subscription_id": id})).unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, code);
    }
    host.apply(Command::Unsubscribe {
        subscription_id: healthy_id.into(),
    })
    .await
    .unwrap();
    assert_eq!(
        host.apply(
            serde_json::from_value(json!({
                "op": "poll_watch", "subscription_id": healthy_id
            }))
            .unwrap()
        )
        .await
        .unwrap_err()
        .code,
        3
    );
    host.close().await.unwrap();
}

#[tokio::test]
async fn public_watch_binding_recovers_exact_pins_and_pending_output() {
    let workspace = Workspace::new();
    let mut configuration = workspace.config();
    configuration.sources.clear();
    configuration.queries.clear();
    let mut host = Host::open(configuration.clone()).await.unwrap();
    let source = admit(&host, &source_definition()).await.unwrap();
    let source_id = source["native_id"].as_str().unwrap();
    let query = query_definition(source_id, "saved.query", "7");
    let query = admit(&host, &query).await.unwrap();
    let query_id = query["native_id"].as_str().unwrap();
    let binding = public_watch_binding();
    let request = json!({"op": "bind_watch", "binding": binding, "query_id": query_id});
    let registered = host
        .apply(serde_json::from_value(request.clone()).unwrap())
        .await
        .unwrap()["subscription"]
        .clone();
    assert_eq!(registered["binding"], binding);
    let id = registered["definition"]["id"].as_str().unwrap();
    assert!(id.starts_with("w-"));
    assert_eq!(id.len(), 66);
    assert_eq!(registered["definition"]["query_revision"], 1);
    let initial = pending(&host, id).await;
    let initial_bytes = serde_json::to_vec(&initial).unwrap();
    feed_source(&host, source_id, 1, 12).await;
    query_state(&host, query_id, &[json!({"amount": 12})]).await;
    assert_eq!(
        host.apply(serde_json::from_value(request.clone()).unwrap())
            .await
            .unwrap()["subscription"],
        registered
    );
    assert_eq!(
        serde_json::to_vec(&pending(&host, id).await).unwrap(),
        initial_bytes
    );
    host.close().await.unwrap();
    drop(host);

    let mut host = Host::open(configuration.clone()).await.unwrap();
    assert_eq!(
        host.apply(serde_json::from_value(request.clone()).unwrap())
            .await
            .unwrap()["subscription"],
        registered
    );
    let checkpoint = workspace.0.join("bindings-checkpoint");
    host.checkpoint(checkpoint.to_str().unwrap()).await.unwrap();
    drop(host);
    configuration.state_dir = workspace
        .0
        .join("restored-bindings")
        .to_string_lossy()
        .into();
    configuration.owner_epoch = 2;
    configuration.restore_from = Some(checkpoint.to_string_lossy().into());
    let mut host = Host::open(configuration).await.unwrap();
    let listed = host
        .apply(serde_json::from_value(json!({"op": "list_subscriptions", "limit": 1})).unwrap())
        .await
        .unwrap();
    assert_eq!(listed["subscriptions"], json!([registered]));
    assert_eq!(listed["next_page_token"], "");
    assert_eq!(
        serde_json::to_vec(&pending(&host, id).await).unwrap(),
        initial_bytes
    );
    accept(&host, &initial).await;
    let next = pending(&host, id).await;
    assert_eq!(next.sequence, initial.sequence + 1);
    assert_eq!(next.incarnation, initial.incarnation);
    host.apply(Command::Unsubscribe {
        subscription_id: id.into(),
    })
    .await
    .unwrap();
    assert!(host
        .apply(serde_json::from_value(request).unwrap())
        .await
        .unwrap_err()
        .message
        .contains("tombstone"));
    let listed = host
        .apply(serde_json::from_value(json!({"op": "list_subscriptions", "limit": 1})).unwrap())
        .await
        .unwrap();
    assert_eq!(listed["subscriptions"][0]["binding"], binding);
    assert_eq!(listed["subscriptions"][0]["deleted"], true);
    host.close().await.unwrap();
}

#[tokio::test]
async fn public_watch_binding_refuses_rebinding_and_validates_recovery_metadata() {
    let workspace = Workspace::new();
    let configuration = workspace.config();
    let mut host = Host::open(configuration.clone()).await.unwrap();
    let source = admit(&host, &source_definition()).await.unwrap();
    let source_id = source["native_id"].as_str().unwrap();
    let query = admit(&host, &query_definition(source_id, "saved.query", "7"))
        .await
        .unwrap();
    let request = json!({
        "op": "bind_watch", "binding": public_watch_binding(), "query_id": query["native_id"]
    });
    let registered = host
        .apply(serde_json::from_value(request.clone()).unwrap())
        .await
        .unwrap()["subscription"]
        .clone();
    let id = registered["definition"]["id"].as_str().unwrap();
    let before = host.store().unwrap().get(SUBSCRIPTIONS, id).await.unwrap();
    for (path, value) in [
        ("/binding/watch/namespace_id", json!(uuid::Uuid::nil())),
        ("/binding/watch/resource_id", json!(".invalid")),
        ("/binding/watch/incarnation", json!("")),
        (
            "/binding/query/namespace_id",
            json!("00000000-0000-4000-8000-000000000002"),
        ),
        ("/binding/query/resource_id", json!("other")),
        ("/binding/query/incarnation", json!("8")),
        ("/binding/watch_revision", json!(0)),
        ("/binding/watch_revision", json!(44)),
        ("/binding/query_revision", json!(0)),
        ("/binding/query_revision", json!(9)),
        ("/query_id", json!("missing")),
        ("/query_id", json!(source_id)),
    ] {
        let mut changed = request.clone();
        *changed.pointer_mut(path).unwrap() = value;
        assert!(
            host.apply(serde_json::from_value(changed).unwrap())
                .await
                .is_err(),
            "accepted changed binding at {path}"
        );
        assert_eq!(
            host.store().unwrap().get(SUBSCRIPTIONS, id).await.unwrap(),
            before,
            "changed pending output or binding at {path}"
        );
    }
    let mut anonymous = request.clone();
    anonymous["query_id"] = json!("q1");
    assert!(host
        .apply(serde_json::from_value(anonymous).unwrap())
        .await
        .is_err());
    assert!(host
        .apply(Command::Subscribe {
            subscription: serde_json::from_value(registered["definition"].clone()).unwrap()
        })
        .await
        .unwrap_err()
        .message
        .contains("retained binding"));
    let mut replacement = request.clone();
    replacement["binding"]["watch"]["incarnation"] = json!("44");
    replacement["binding"]["watch_revision"] = json!(44);
    let replacement = host
        .apply(serde_json::from_value(replacement).unwrap())
        .await
        .unwrap()["subscription"]
        .clone();
    assert_ne!(
        replacement["definition"]["id"],
        registered["definition"]["id"]
    );
    let first = host
        .apply(
            serde_json::from_value(json!({
                "op": "list_subscriptions", "limit": 1
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first["subscriptions"].as_array().unwrap().len(), 1);
    let next = first["next_page_token"].as_str().unwrap();
    assert!(!next.is_empty());
    let second = host
        .apply(
            serde_json::from_value(json!({
                "op": "list_subscriptions", "limit": 1, "after": next
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second["subscriptions"].as_array().unwrap().len(), 1);
    assert_ne!(first["subscriptions"], second["subscriptions"]);
    assert_eq!(second["next_page_token"], "");
    let all = host
        .apply(serde_json::from_value(json!({"op": "list_subscriptions", "limit": 64})).unwrap())
        .await
        .unwrap();
    assert_eq!(all["subscriptions"].as_array().unwrap().len(), 2);
    assert_eq!(all["next_page_token"], "");
    for limit in [0, 65] {
        assert!(host
            .apply(
                serde_json::from_value(json!({
                    "op": "list_subscriptions", "limit": limit
                }))
                .unwrap()
            )
            .await
            .is_err());
    }
    let mut damaged: Value = serde_json::from_slice(before.as_ref().unwrap()).unwrap();
    damaged.as_object_mut().unwrap().remove("binding");
    host.store()
        .unwrap()
        .set(SUBSCRIPTIONS, id, serde_json::to_vec(&damaged).unwrap())
        .await
        .unwrap();
    host.close().await.unwrap();
    drop(host);
    let error = match Host::open(configuration.clone()).await {
        Ok(_) => panic!("missing binding metadata was admitted"),
        Err(error) => error,
    };
    assert_eq!(error.code, 7);
    assert!(error.message.contains("binding is missing"));
    let store = RedbStateStoreProvider::new(Path::new(&configuration.state_dir).join("state.redb"))
        .unwrap();
    assert_eq!(
        store.get(SUBSCRIPTIONS, id).await.unwrap(),
        Some(serde_json::to_vec(&damaged).unwrap())
    );
    assert_eq!(
        store.get(CONTROL, "owner_epoch").await.unwrap(),
        Some(1_u64.to_be_bytes().to_vec())
    );
}

#[tokio::test]
async fn consumed_query_warm_restart_refuses_gap_and_preserves_pending_output() {
    let workspace = Workspace::new();
    let mut configuration = workspace.config();
    configuration.sources.clear();
    configuration.queries.clear();
    let mut host = Host::open(configuration).await.unwrap();
    let source = admit(&host, &source_definition()).await.unwrap();
    let source_id = source["native_id"].as_str().unwrap();
    let definition = query_definition(source_id, "saved.query", "7");
    let query = admit(&host, &definition).await.unwrap();
    let query_id = query["native_id"].as_str().unwrap();
    let initial = attach_query(&host, "watch", query_id).await;
    accept(&host, &initial).await;
    feed_source(&host, source_id, 1, 12).await;
    query_state(&host, query_id, &[json!({"amount": 12})]).await;
    let first = pending(&host, "watch").await;
    let handoff = host
        .store()
        .unwrap()
        .get(SUBSCRIPTIONS, "watch")
        .await
        .unwrap();
    stop_query(&host, query_id).await;
    feed_source(&host, source_id, 2, 14).await;
    let partition = format!("temporal-feed:{source_id}");
    let key = format!("{:020}", 2);
    let event = host
        .store()
        .unwrap()
        .get(&partition, &key)
        .await
        .unwrap()
        .unwrap();
    host.store()
        .unwrap()
        .delete(&partition, &key)
        .await
        .unwrap();
    let error = admit(&host, &definition).await.unwrap_err();
    assert!(
        error
            .message
            .contains(&format!("Position 2 unavailable for source '{source_id}'")),
        "{error:?}"
    );
    assert_eq!(
        host.store()
            .unwrap()
            .get(SUBSCRIPTIONS, "watch")
            .await
            .unwrap(),
        handoff
    );
    stop_query(&host, query_id).await;
    host.store()
        .unwrap()
        .set(&partition, &key, event)
        .await
        .unwrap();
    admit(&host, &definition).await.unwrap();
    query_state(&host, query_id, &[json!({"amount": 14})]).await;
    assert_eq!(
        serde_json::to_vec(&pending(&host, "watch").await).unwrap(),
        serde_json::to_vec(&first).unwrap()
    );
    accept(&host, &first).await;
    let next = pending(&host, "watch").await;
    assert_eq!(next.sequence, first.sequence + 1);
    assert_eq!(next.incarnation, first.incarnation);
    host.close().await.unwrap();
}

#[tokio::test]
async fn incremental_catalog_preserves_shared_graph_and_pending_output_on_reopen() {
    let workspace = Workspace::new();
    let mut configuration = workspace.config();
    configuration.sources.clear();
    configuration.queries.clear();
    let mut host = Host::open(configuration.clone()).await.unwrap();
    let source = admit(&host, &source_definition()).await.unwrap();
    let source_id = source["native_id"].as_str().unwrap();
    let first_definition =
        query_definition(source_id, "first", "00000000-0000-4000-8000-000000000003");
    let first_node = admit(&host, &first_definition).await.unwrap();
    let first_id = first_node["native_id"].as_str().unwrap();
    assert_eq!(first_id.len(), 66);
    let initial = attach_query(&host, "watch-1", first_id).await;
    accept(&host, &initial).await;
    feed_source(&host, source_id, 1, 12).await;
    let first_state = query_state(&host, first_id, &[json!({"amount": 12})]).await;
    let first = pending(&host, "watch-1").await;
    let first_bytes = serde_json::to_vec(&first).unwrap();

    let second_definition =
        query_definition(source_id, "second", "00000000-0000-4000-8000-000000000004");
    let second_node = admit(&host, &second_definition).await.unwrap();
    let second_id = second_node["native_id"].as_str().unwrap();
    assert_ne!(first_id, second_id);
    query_state(&host, second_id, &[json!({"amount": 12})]).await;
    assert_eq!(
        query_state(&host, first_id, &[json!({"amount": 12})]).await,
        first_state
    );
    assert_eq!(
        serde_json::to_vec(&pending(&host, "watch-1").await).unwrap(),
        first_bytes
    );
    let late = attach_query(&host, "watch-2", first_id).await;
    assert_eq!(late.sequence, first.sequence);
    assert_eq!(late.incarnation, first.incarnation);
    accept(&host, &late).await;
    let replayed = admit(&host, &first_definition).await.unwrap();
    assert_eq!(replayed["native_id"], first_node["native_id"]);
    assert_eq!(replayed["query_revision"], 1);
    let mut changed = first_definition.clone();
    changed["recipe"]["text"] = json!("MATCH (n:Other) RETURN n");
    assert!(admit(&host, &changed)
        .await
        .unwrap_err()
        .message
        .contains("lifetime"));
    let mut invalid =
        query_definition(source_id, "invalid", "00000000-0000-4000-8000-000000000005");
    invalid["recipe"]["text"] = json!("THIS IS NOT CYPHER");
    admit(&host, &invalid).await.unwrap_err();
    feed_source(&host, source_id, 2, 14).await;
    query_state(&host, first_id, &[json!({"amount": 14})]).await;
    query_state(&host, second_id, &[json!({"amount": 14})]).await;
    assert_eq!(
        serde_json::to_vec(&pending(&host, "watch-1").await).unwrap(),
        first_bytes
    );
    let before_restart = query_state(&host, first_id, &[json!({"amount": 14})]).await;
    let runtime = host
        .core()
        .unwrap()
        .query_manager()
        .get_query_instance(first_id)
        .await
        .unwrap();
    assert_eq!(
        runtime.get_config().recovery_policy,
        Some(drasi_lib::RecoveryPolicy::Strict)
    );
    stop_query(&host, first_id).await;
    admit(&host, &first_definition).await.unwrap();
    assert!(Arc::ptr_eq(
        &runtime,
        &host
            .core()
            .unwrap()
            .query_manager()
            .get_query_instance(first_id)
            .await
            .unwrap()
    ));
    assert_eq!(
        runtime.get_config().recovery_policy,
        Some(drasi_lib::RecoveryPolicy::Strict)
    );
    drop(runtime);
    assert_eq!(
        query_state(&host, first_id, &[json!({"amount": 14})]).await,
        before_restart
    );
    assert_eq!(
        serde_json::to_vec(&pending(&host, "watch-1").await).unwrap(),
        first_bytes
    );
    let observations = host.apply(Command::ObserveNodes).await.unwrap();
    assert_eq!(
        observations["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|node| node["status"] == "failed")
            .count(),
        1
    );
    let checkpoint = workspace.0.join("catalog-checkpoint");
    host.checkpoint(checkpoint.to_str().unwrap()).await.unwrap();
    drop(host);

    configuration.state_dir = workspace.0.join("restored").to_string_lossy().into();
    configuration.owner_epoch = 2;
    configuration.restore_from = Some(checkpoint.to_string_lossy().into());
    let mut host = Host::open(configuration).await.unwrap();
    assert_eq!(
        serde_json::to_vec(&pending(&host, "watch-1").await).unwrap(),
        first_bytes
    );
    let reopened = admit(&host, &first_definition).await.unwrap();
    assert_eq!(reopened["native_id"], first_node["native_id"]);
    query_state(&host, second_id, &[json!({"amount": 14})]).await;
    let before_warm_restart = query_state(&host, first_id, &[json!({"amount": 14})]).await;
    stop_query(&host, first_id).await;
    admit(&host, &first_definition).await.unwrap();
    assert_eq!(
        query_state(&host, first_id, &[json!({"amount": 14})]).await,
        before_warm_restart
    );
    admit(&host, &invalid).await.unwrap_err();
    accept(&host, &first).await;
    let next = pending(&host, "watch-1").await;
    assert_eq!(next.sequence, first.sequence + 1);
    assert_eq!(next.incarnation, first.incarnation);
    host.close().await.unwrap();
}
