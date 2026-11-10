use sha2::{Digest, Sha256};

fn main() {
    let manifest = std::path::PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR").expect("native manifest directory is required"),
    );
    let lock_path = manifest
        .ancestors()
        .map(|directory| directory.join("Cargo.lock"))
        .find(|path| path.is_file())
        .expect("native workspace dependency lock is required");
    println!("cargo:rerun-if-changed={}", lock_path.display());
    let lock = std::fs::read_to_string(lock_path).expect("native dependency lock is required");
    let mut dependencies = Vec::new();
    for entry in lock.split("[[package]]") {
        let value = |key| {
            entry.lines().find_map(|line| {
                line.strip_prefix(&format!("{key} = \""))
                    .and_then(|value| value.strip_suffix('"'))
            })
        };
        let Some(name) = value("name").filter(|name| name.starts_with("drasi-")) else {
            continue;
        };
        assert_eq!(
            value("source"),
            Some("registry+https://github.com/rust-lang/crates.io-index"),
            "Drasi dependencies must come from crates.io"
        );
        let version = value("version").expect("Drasi package version is required");
        let checksum = value("checksum").expect("Drasi registry checksum is required");
        dependencies.push(format!("{name}@{version}#{checksum}"));
    }
    assert!(!dependencies.is_empty(), "Drasi dependencies are required");
    dependencies.sort();
    let identity = Sha256::digest(dependencies.join("\n").as_bytes());
    println!("cargo:rustc-env=TDRASI_UPSTREAM=crates.io:{identity:x}");
    for (package, key) in [
        ("drasi-core", "TDRASI_CORE_VERSION"),
        ("drasi-lib", "TDRASI_LIB_VERSION"),
    ] {
        let name = format!("name = \"{package}\"");
        let mut versions = lock.split("[[package]]").filter_map(|entry| {
            if entry.lines().any(|line| line == name) {
                entry.lines().find_map(|line| {
                    line.strip_prefix("version = \"")
                        .and_then(|value| value.strip_suffix('"'))
                })
            } else {
                None
            }
        });
        let version = versions
            .next()
            .expect("Drasi dependency version is missing");
        assert!(
            versions.next().is_none(),
            "Drasi dependency version is ambiguous"
        );
        println!("cargo:rustc-env={key}={version}");
    }
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        let name = std::env::var("CARGO_PKG_NAME").expect("native package name is required");
        println!(
            "cargo:rustc-cdylib-link-arg=-Wl,-install_name,@rpath/lib{}.dylib",
            name.replace('-', "_")
        );
    }
}
