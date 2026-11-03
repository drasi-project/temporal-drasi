use crate::protocol::{Checkpoint, Config, Error, Result, MAX_OUTPUT, UPSTREAM, VERSION};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::Read,
    path::{Component, Path},
};

const MANIFEST: &str = "checkpoint-v1.json";
const RESTORE_RECEIPT: &str = "restore-v1.json";

fn io(error: impl std::fmt::Display) -> Error {
    Error::new(7, error.to_string())
}

fn digest(path: &Path) -> Result<String> {
    let mut file = File::open(path).map_err(io)?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let length = file.read(&mut buffer).map_err(io)?;
        if length == 0 {
            break;
        }
        digest.update(&buffer[..length]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn copy(
    source: &Path,
    destination: &Path,
    relative: &Path,
    files: &mut BTreeMap<String, String>,
) -> Result<()> {
    for entry in std::fs::read_dir(source.join(relative)).map_err(io)? {
        let entry = entry.map_err(io)?;
        let path = relative.join(entry.file_name());
        if relative.as_os_str().is_empty()
            && (path == Path::new("owner.lock")
                || path == Path::new(MANIFEST)
                || path == Path::new(RESTORE_RECEIPT))
        {
            continue;
        }
        let kind = entry.file_type().map_err(io)?;
        if kind.is_dir() {
            std::fs::create_dir(destination.join(&path)).map_err(io)?;
            copy(source, destination, &path, files)?;
        } else if kind.is_file() {
            std::fs::copy(source.join(&path), destination.join(&path)).map_err(io)?;
            File::open(destination.join(&path))
                .map_err(io)?
                .sync_all()
                .map_err(io)?;
            files.insert(
                path.to_str()
                    .ok_or_else(|| Error::new(1, "checkpoint paths must be UTF-8"))?
                    .to_string(),
                digest(&destination.join(&path))?,
            );
        } else {
            return Err(Error::new(
                1,
                "checkpoint directories cannot contain symlinks or special files",
            ));
        }
    }
    File::open(destination.join(relative))
        .map_err(io)?
        .sync_all()
        .map_err(io)?;
    Ok(())
}

fn validate_destination(source: &Path, destination: &Path) -> Result<()> {
    if !destination.is_absolute() || destination.file_name().is_none() {
        return Err(Error::new(
            1,
            "checkpoint/restore destination must be an absolute non-root directory",
        ));
    }
    let parent = destination
        .parent()
        .ok_or_else(|| Error::new(1, "invalid destination"))?
        .canonicalize()
        .map_err(io)?;
    let source = source.canonicalize().map_err(io)?;
    if parent.starts_with(source) {
        return Err(Error::new(
            1,
            "checkpoint destination cannot be inside its source",
        ));
    }
    Ok(())
}

fn create_destination(source: &Path, destination: &Path) -> Result<()> {
    if !destination.is_absolute() || destination.exists() {
        return Err(Error::new(
            1,
            "checkpoint/restore destination must be a new absolute directory",
        ));
    }
    validate_destination(source, destination)?;
    std::fs::create_dir(destination).map_err(io)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o700))
            .map_err(io)?;
    }
    Ok(())
}

pub fn publish(
    source: &Path,
    destination: &Path,
    config: &Config,
    retirement_id: Option<String>,
) -> Result<Checkpoint> {
    create_destination(source, destination)?;
    let mut files = BTreeMap::new();
    copy(source, destination, Path::new(""), &mut files)?;
    let checkpoint = Checkpoint {
        version: VERSION,
        upstream: UPSTREAM.into(),
        instance_id: config.instance_id.clone(),
        owner_epoch: config.owner_epoch,
        retirement_id,
        files,
    };
    std::fs::write(destination.join(MANIFEST), serde_json::to_vec(&checkpoint)?).map_err(io)?;
    File::open(destination.join(MANIFEST))
        .map_err(io)?
        .sync_all()
        .map_err(io)?;
    File::open(destination)
        .map_err(io)?
        .sync_all()
        .map_err(io)?;
    File::open(
        destination
            .parent()
            .ok_or_else(|| Error::new(1, "invalid destination"))?,
    )
    .map_err(io)?
    .sync_all()
    .map_err(io)?;
    Ok(checkpoint)
}

pub(super) fn read_manifest(source: &Path) -> Result<Checkpoint> {
    let path = source.join(MANIFEST);
    let metadata = std::fs::symlink_metadata(&path).map_err(io)?;
    if !metadata.is_file() || metadata.len() > MAX_OUTPUT as u64 {
        return Err(Error::new(
            7,
            "checkpoint manifest must be a bounded regular file",
        ));
    }
    Ok(serde_json::from_slice(&std::fs::read(path).map_err(io)?)?)
}

pub(super) fn validate_tree(source: &Path) -> Result<()> {
    if !std::fs::symlink_metadata(source).map_err(io)?.is_dir() {
        return Err(Error::new(
            7,
            "native storage must be an existing regular directory",
        ));
    }
    for entry in std::fs::read_dir(source).map_err(io)? {
        let entry = entry.map_err(io)?;
        let kind = entry.file_type().map_err(io)?;
        if kind.is_dir() {
            validate_tree(&entry.path())?;
        } else if !kind.is_file() {
            return Err(Error::new(
                7,
                "native storage cannot contain symlinks or special files",
            ));
        }
    }
    Ok(())
}

fn collect(source: &Path, relative: &Path, files: &mut BTreeMap<String, String>) -> Result<()> {
    for entry in std::fs::read_dir(source.join(relative)).map_err(io)? {
        let entry = entry.map_err(io)?;
        let path = relative.join(entry.file_name());
        if relative.as_os_str().is_empty()
            && (path == Path::new("owner.lock")
                || path == Path::new(MANIFEST)
                || path == Path::new(RESTORE_RECEIPT))
        {
            continue;
        }
        if entry.file_type().map_err(io)?.is_dir() {
            collect(source, &path, files)?;
        } else {
            files.insert(
                path.to_str()
                    .ok_or_else(|| Error::new(1, "checkpoint paths must be UTF-8"))?
                    .to_string(),
                digest(&source.join(path))?,
            );
        }
    }
    Ok(())
}

fn verify(source: &Path, config: &Config) -> Result<Checkpoint> {
    validate_tree(source)?;
    let checkpoint = read_manifest(source)?;
    if checkpoint.version != VERSION
        || checkpoint.upstream != UPSTREAM
        || checkpoint.instance_id != config.instance_id
        || checkpoint.owner_epoch == 0
        || !checkpoint.files.contains_key("state.redb")
    {
        return Err(Error::new(
            3,
            "checkpoint format, upstream build identity or instance identity is incompatible",
        ));
    }
    for (path, expected) in &checkpoint.files {
        if path.is_empty()
            || !Path::new(path)
                .components()
                .all(|part| matches!(part, Component::Normal(_)))
        {
            return Err(Error::new(1, "checkpoint manifest contains an unsafe path"));
        }
        if digest(&source.join(path))? != *expected {
            return Err(Error::new(3, "checkpoint file integrity check failed"));
        }
    }
    let mut actual = BTreeMap::new();
    collect(source, Path::new(""), &mut actual)?;
    if actual != checkpoint.files {
        return Err(Error::new(
            3,
            "checkpoint content differs from its manifest",
        ));
    }
    Ok(checkpoint)
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path).map_err(io)?.sync_all().map_err(io)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn publish_directory(source: &Path, destination: &Path) -> Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let source = CString::new(source.as_os_str().as_bytes()).map_err(io)?;
    let destination = CString::new(destination.as_os_str().as_bytes()).map_err(io)?;
    #[cfg(target_os = "macos")]
    let result = {
        unsafe extern "C" {
            fn renamex_np(
                from: *const std::ffi::c_char,
                to: *const std::ffi::c_char,
                flags: u32,
            ) -> i32;
        }
        unsafe { renamex_np(source.as_ptr(), destination.as_ptr(), 4) }
    };
    #[cfg(target_os = "linux")]
    let result = {
        unsafe extern "C" {
            fn renameat2(
                from_fd: i32,
                from: *const std::ffi::c_char,
                to_fd: i32,
                to: *const std::ffi::c_char,
                flags: u32,
            ) -> i32;
        }
        unsafe { renameat2(-100, source.as_ptr(), -100, destination.as_ptr(), 1) }
    };
    if result != 0 {
        return Err(io(std::io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn publish_directory(_: &Path, _: &Path) -> Result<()> {
    Err(Error::new(
        8,
        "atomic native directory publication is unsupported on this platform",
    ))
}

fn remove_stage(stage: &Path) -> Result<()> {
    match std::fs::symlink_metadata(stage) {
        Ok(_) => {
            validate_tree(stage)?;
            std::fs::remove_dir_all(stage).map_err(io)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io(error)),
    }
}

pub(super) fn publish_recovered(
    source: &Path,
    destination: &Path,
    config: &Config,
    retirement_id: &str,
) -> Result<Checkpoint> {
    validate_destination(source, destination)?;
    if destination.exists() {
        let checkpoint = verify(destination, config)?;
        if checkpoint.owner_epoch != config.owner_epoch
            || checkpoint.retirement_id.as_deref() != Some(retirement_id)
        {
            return Err(Error::new(
                7,
                "recovery checkpoint has a different retired owner",
            ));
        }
        sync_directory(destination)?;
        sync_directory(
            destination
                .parent()
                .ok_or_else(|| Error::new(1, "invalid destination"))?,
        )?;
        return Ok(checkpoint);
    }
    let stage = destination.with_file_name(format!(".recovery-{retirement_id}.stage"));
    remove_stage(&stage)?;
    let checkpoint = publish(source, &stage, config, Some(retirement_id.into()))?;
    verify(&stage, config)?;
    publish_directory(&stage, destination)?;
    sync_directory(
        destination
            .parent()
            .ok_or_else(|| Error::new(1, "invalid destination"))?,
    )?;
    Ok(checkpoint)
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RestoreReceipt {
    version: u32,
    manifest_sha256: String,
    instance_id: String,
    owner_epoch: u64,
}

pub fn restore(source: &Path, destination: &Path, config: &Config) -> Result<()> {
    validate_destination(source, destination)?;
    let checkpoint = verify(source, config)?;
    if checkpoint.owner_epoch.checked_add(1) != Some(config.owner_epoch) {
        return Err(Error::new(
            3,
            "checkpoint restore requires the immediate successor epoch",
        ));
    }
    let name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::new(1, "invalid restore destination"))?;
    let gate = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(destination.with_file_name(format!(".{name}.restore.lock")))
        .map_err(io)?;
    gate.try_lock_exclusive()
        .map_err(|_| Error::new(3, "native restore is already in progress"))?;
    let receipt = RestoreReceipt {
        version: VERSION,
        manifest_sha256: digest(&source.join(MANIFEST))?,
        instance_id: config.instance_id.clone(),
        owner_epoch: config.owner_epoch,
    };
    if destination.exists() {
        if !std::fs::symlink_metadata(destination).map_err(io)?.is_dir() {
            return Err(Error::new(
                7,
                "restore destination is not a regular directory",
            ));
        }
        let path = destination.join(RESTORE_RECEIPT);
        let metadata = std::fs::symlink_metadata(&path).map_err(io)?;
        if !metadata.is_file() || metadata.len() > 4096 {
            return Err(Error::new(
                7,
                "restore receipt must be a bounded regular file",
            ));
        }
        let previous: RestoreReceipt = serde_json::from_slice(&std::fs::read(path).map_err(io)?)?;
        if previous != receipt {
            return Err(Error::new(
                7,
                "existing native directory has a different restore origin",
            ));
        }
        return Ok(());
    }
    let stage =
        destination.with_file_name(format!(".{name}.restore-{}.stage", receipt.manifest_sha256));
    remove_stage(&stage)?;
    create_destination(source, &stage)?;
    let mut actual = BTreeMap::new();
    copy(source, &stage, Path::new(""), &mut actual)?;
    if actual != checkpoint.files {
        return Err(Error::new(
            3,
            "restored checkpoint differs from its manifest",
        ));
    }
    let path = stage.join(RESTORE_RECEIPT);
    std::fs::write(&path, serde_json::to_vec(&receipt)?).map_err(io)?;
    File::open(path).map_err(io)?.sync_all().map_err(io)?;
    sync_directory(&stage)?;
    publish_directory(&stage, destination)?;
    sync_directory(
        destination
            .parent()
            .ok_or_else(|| Error::new(1, "invalid destination"))?,
    )?;
    Ok(())
}
