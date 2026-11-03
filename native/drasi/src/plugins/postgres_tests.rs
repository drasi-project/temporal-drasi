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

fn library(fixture: &Library, id: &str, category: &str) -> (PluginConfig, Arc<LoadedPlugin>) {
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
        core_version: drasi_core::VERSION.into(),
        lib_version: drasi_lib::VERSION.into(),
        target_triple: TARGET_TRIPLE.into(),
        description: None,
        license: None,
    };
    let handle = resident(&artifact, &fixture.path, &metadata).unwrap();
    (artifact, handle)
}

#[tokio::test]
#[ignore = "run test-sdk-recovery.py --native-postgres; unsigned libraries, not signature acceptance"]
async fn native_postgres_bootstrap_cdc_and_recovery() {
    let path = std::env::var("TEMPORAL_DRASI_POSTGRES_LIBRARY_FIXTURE")
        .expect("the real PostgreSQL library fixture must be configured");
    let fixture: Fixture = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let (source, source_handle) =
        library(&fixture.source, "native-postgres-source-fixture", "source");
    let (bootstrap, bootstrap_handle) = library(
        &fixture.bootstrap,
        "native-postgres-bootstrap-fixture",
        "bootstrap",
    );
    // Direct test-only handles do not exercise installation or signing.
    crate::runtime::postgres_tests::exercise(
        || Plugins {
            loaded: BTreeMap::from([
                (
                    source.id.clone(),
                    ActivePlugin {
                        reference: source.reference.clone(),
                        handle: source_handle.clone(),
                        descriptors: descriptors(&source.id, &source_handle).unwrap(),
                    },
                ),
                (
                    bootstrap.id.clone(),
                    ActivePlugin {
                        reference: bootstrap.reference.clone(),
                        handle: bootstrap_handle.clone(),
                        descriptors: descriptors(&bootstrap.id, &bootstrap_handle).unwrap(),
                    },
                ),
            ]),
            catalogs: Vec::new(),
            trust: Trust::default(),
            directory: PathBuf::new(),
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
