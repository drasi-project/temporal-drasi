use super::*;
use crate::runtime::tests::Workspace;

fn configuration(workspace: &Workspace) -> Config {
    let mut config = workspace.config();
    config.sources.clear();
    config.queries.clear();
    config.start_mode = StartMode::Dormant;
    config
}

async fn retained(workspace: &Workspace) -> Config {
    let config = configuration(workspace);
    let mut host = Host::open(config.clone()).await.unwrap();
    host.close().await.unwrap();
    drop(host);
    config
}

#[tokio::test]
async fn recovery_fences_live_owners_and_permanently_retires_stale_openers() {
    let workspace = Workspace::new();
    let config = configuration(&workspace);
    let mut host = Host::open(config.clone()).await.unwrap();
    let mut recovery_config = config.clone();
    recovery_config.start_mode = StartMode::CheckpointRecovery;
    assert!(Host::open(recovery_config.clone())
        .await
        .err()
        .unwrap()
        .message
        .contains("already owned"));
    host.close().await.unwrap();
    assert!(Host::open(recovery_config.clone()).await.is_err());
    drop(host);
    let mut recovery = Host::open(recovery_config).await.unwrap();
    assert!(recovery.core.is_none());
    assert!(recovery.plugins.is_none());
    assert!(recovery.store.is_none());
    assert!(recovery.apply(Command::ObserveNodes).await.is_err());
    let destination = workspace.0.join("recovered");
    let first = recovery
        .checkpoint(destination.to_str().unwrap())
        .await
        .unwrap();
    let manifest = std::fs::read(destination.join("checkpoint-v1.json")).unwrap();
    let repeated = recovery
        .checkpoint(destination.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(first, repeated);
    assert_eq!(
        manifest,
        std::fs::read(destination.join("checkpoint-v1.json")).unwrap()
    );
    drop(recovery);
    assert!(Host::open(config.clone())
        .await
        .err()
        .unwrap()
        .message
        .contains("definition changed"));
    let mut successor = config;
    successor.owner_epoch = 2;
    successor.state_dir = workspace.0.join("successor").to_str().unwrap().into();
    successor.restore_from = Some(destination.to_str().unwrap().into());
    let mut host = Host::open(successor.clone()).await.unwrap();
    host.close().await.unwrap();
    drop(host);
    let mut retried = Host::open(successor).await.unwrap();
    retried.close().await.unwrap();
}

#[tokio::test]
async fn recovery_rebuilds_only_its_staging_slot_and_never_replaces_a_final_cut() {
    let workspace = Workspace::new();
    let mut config = retained(&workspace).await;
    config.start_mode = StartMode::CheckpointRecovery;
    let mut recovery = Host::open(config.clone()).await.unwrap();
    let retirement = retire(&config).await.unwrap();
    let stage = workspace.0.join(format!(".recovery-{retirement}.stage"));
    std::fs::create_dir(&stage).unwrap();
    std::fs::write(stage.join("partial"), b"incomplete copy").unwrap();
    let old_cut = workspace.0.join("legacy-cut");
    std::fs::create_dir(&old_cut).unwrap();
    std::fs::write(old_cut.join("partial"), b"may be named by a delayed Seal").unwrap();
    let destination = workspace.0.join("recovered");
    recovery
        .checkpoint(destination.to_str().unwrap())
        .await
        .unwrap();
    assert!(!stage.exists());
    assert_eq!(
        std::fs::read(old_cut.join("partial")).unwrap(),
        b"may be named by a delayed Seal"
    );
    let manifest_path = destination.join("checkpoint-v1.json");
    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    manifest["retirement_id"] = json!(uuid::Uuid::new_v4().to_string());
    let changed = serde_json::to_vec(&manifest).unwrap();
    std::fs::write(&manifest_path, &changed).unwrap();
    assert!(recovery
        .checkpoint(destination.to_str().unwrap())
        .await
        .is_err());
    assert_eq!(std::fs::read(&manifest_path).unwrap(), changed);
}

#[tokio::test]
async fn recovery_never_creates_missing_state_or_repairs_missing_admission_metadata() {
    for missing in [
        "owner.lock",
        "state.redb",
        "definition",
        "owner_epoch",
        "identity",
        "catalog",
    ] {
        let workspace = Workspace::new();
        let mut config = retained(&workspace).await;
        if missing.ends_with(".redb") || missing.ends_with(".lock") {
            std::fs::remove_file(Path::new(&config.state_dir).join(missing)).unwrap();
        } else {
            let store =
                RedbStateStoreProvider::new(Path::new(&config.state_dir).join("state.redb"))
                    .unwrap();
            store.delete(CONTROL, missing).await.unwrap();
            store.sync().await.unwrap();
        }
        config.start_mode = StartMode::CheckpointRecovery;
        assert!(Host::open(config.clone()).await.is_err(), "{missing}");
        if missing.ends_with(".redb") || missing.ends_with(".lock") {
            assert!(
                !Path::new(&config.state_dir).join(missing).exists(),
                "{missing}"
            );
        }
    }
    let workspace = Workspace::new();
    let mut config = configuration(&workspace);
    config.start_mode = StartMode::CheckpointRecovery;
    assert!(Host::open(config.clone()).await.is_err());
    assert!(!Path::new(&config.state_dir).exists());
}

#[tokio::test]
async fn isolated_restore_rebuilds_its_interrupted_stage_but_refuses_unknown_final_state() {
    let workspace = Workspace::new();
    let mut config = retained(&workspace).await;
    config.start_mode = StartMode::CheckpointRecovery;
    let mut recovery = Host::open(config.clone()).await.unwrap();
    let destination = workspace.0.join("cut");
    recovery
        .checkpoint(destination.to_str().unwrap())
        .await
        .unwrap();
    drop(recovery);
    config.start_mode = StartMode::Dormant;
    config.owner_epoch = 2;
    config.restore_from = Some(destination.to_str().unwrap().into());
    config.state_dir = destination.join("forbidden-child").to_str().unwrap().into();
    assert!(Host::open(config.clone()).await.is_err());
    assert!(!destination.join(".forbidden-child.restore.lock").exists());
    assert!(!Path::new(&config.state_dir).exists());
    config.state_dir = workspace.0.join("successor").to_str().unwrap().into();
    use sha2::{Digest, Sha256};
    let hash = format!(
        "{:x}",
        Sha256::digest(std::fs::read(destination.join("checkpoint-v1.json")).unwrap())
    );
    let stage = workspace.0.join(format!(".successor.restore-{hash}.stage"));
    std::fs::create_dir(&stage).unwrap();
    std::fs::write(stage.join("partial"), b"incomplete restore").unwrap();
    let mut successor = Host::open(config.clone()).await.unwrap();
    assert!(!stage.exists());
    successor.close().await.unwrap();
    drop(successor);
    config.state_dir = workspace.0.join("unknown-final").to_str().unwrap().into();
    std::fs::create_dir(&config.state_dir).unwrap();
    std::fs::write(Path::new(&config.state_dir).join("unknown"), b"preserve").unwrap();
    assert!(Host::open(config.clone()).await.is_err());
    assert_eq!(
        std::fs::read(Path::new(&config.state_dir).join("unknown")).unwrap(),
        b"preserve"
    );
}
