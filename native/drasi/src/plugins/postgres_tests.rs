use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Library {
    path: PathBuf,
    version: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    source: Library,
    bootstrap: Library,
    container: String,
    port: u16,
}

async fn library(fixture: &Library, id: &str, category: &str) -> (PluginConfig, WorkerDescriptor) {
    let hash = format!(
        "{:x}",
        Sha256::digest(std::fs::read(&fixture.path).unwrap())
    );
    let artifact = PluginConfig {
        id: id.into(),
        reference: format!("registry.example/trusted/{id}@sha256:{hash}"),
        filename: fixture.path.file_name().unwrap().to_str().unwrap().into(),
        binary_sha256: hash,
        local_catalog_path: None,
    };
    let metadata = PluginMetadataJson {
        name: "postgres".into(),
        kind: "postgres".into(),
        plugin_type: category.into(),
        version: fixture.version.clone(),
        sdk_version: SDK_VERSION.into(),
        core_version: CORE_VERSION.into(),
        lib_version: LIB_VERSION.into(),
        target_triple: TARGET_TRIPLE.into(),
        description: None,
        license: None,
    };
    let handle = resident(&artifact, &fixture.path, &metadata).await.unwrap();
    (artifact, handle)
}

#[tokio::test]
#[ignore = "run test-sdk-recovery.py --native-postgres with real PostgreSQL libraries"]
async fn native_postgres_bootstrap_cdc_and_recovery() {
    let path = std::env::var("TEMPORAL_DRASI_POSTGRES_LIBRARY_FIXTURE")
        .expect("the real PostgreSQL library fixture must be configured");
    let fixture: Fixture = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let (source, source_handle) =
        library(&fixture.source, "native-postgres-source-fixture", "source").await;
    assert_eq!(fixture.source.path, fixture.bootstrap.path);
    assert_eq!(fixture.source.version, fixture.bootstrap.version);
    let bootstrap = source.clone();
    // Test-only descriptor injection does not exercise installation or signing.
    crate::runtime::postgres_tests::exercise(
        |state| Plugins {
            loaded: BTreeMap::from([(
                source.id.clone(),
                ActivePlugin {
                    reference: source.reference.clone(),
                    artifact: source_handle.artifact.clone(),
                    descriptors: source_handle.descriptors.clone(),
                },
            )]),
            catalogs: Vec::new(),
            directory: state.join("plugins"),
            host_id: "native-postgres-library-fixture".into(),
            observations: BTreeMap::new(),
        },
        &digest(&source),
        &digest(&bootstrap),
        &fixture.container,
        fixture.port,
    )
    .await;
}
