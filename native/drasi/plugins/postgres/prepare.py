#!/usr/bin/env python3
"""Prepare an unsigned PostgreSQL plugin bundle from its actual exported ABI metadata."""

import argparse
import ctypes
import json
import os
from pathlib import Path
import platform
import re
import runpy
import shutil
import subprocess
import sys
import tempfile
import tomllib


PLUGIN = Path(__file__).resolve().parent
NATIVE = PLUGIN.parents[1]
ROOT = NATIVE.parent.parent
sys.path.insert(0, str(NATIVE))
from package import digest


class FfiStr(ctypes.Structure):
    _fields_ = [("ptr", ctypes.c_void_p), ("len", ctypes.c_size_t)]

    def text(self):
        if not self.ptr or not 0 < self.len <= 4096:
            raise ValueError("plugin ABI metadata contains an invalid string")
        return ctypes.string_at(self.ptr, self.len).decode("utf-8")


class PluginMetadata(ctypes.Structure):
    _fields_ = [
        (name, FfiStr)
        for name in (
            "sdk_version", "core_version", "lib_version", "plugin_version",
            "target_triple", "git_commit", "build_timestamp",
        )
    ]


def binary_metadata(path):
    library = ctypes.CDLL(str(path))
    entry = library.drasi_plugin_metadata
    entry.argtypes = []
    entry.restype = ctypes.POINTER(PluginMetadata)
    pointer = entry()
    if not pointer:
        raise ValueError("plugin returned null ABI metadata")
    metadata = pointer.contents
    return {name: getattr(metadata, name).text() for name, _ in metadata._fields_}


def dependency_version(lock, name):
    packages = [package for package in lock["package"] if package["name"] == name]
    expected = "registry+https://github.com/rust-lang/crates.io-index"
    if len(packages) != 1 or packages[0].get("source") != expected or len(packages[0].get("checksum", "")) != 64:
        raise ValueError(f"{name} must resolve uniquely to a checksummed crates.io package")
    return packages[0]["version"]


def ffi_abi_version(source):
    metadata = source / "src/ffi/metadata.rs"
    versions = re.findall(
        r'^pub const FFI_SDK_VERSION: &str = "([^"]+)";$', metadata.read_text(), re.MULTILINE,
    )
    if len(versions) != 1:
        raise ValueError("official SDK must declare exactly one public FFI ABI version")
    return versions[0]


def registry_metadata(exported, lock, version, expected_target, expected_abi):
    core = dependency_version(lock, "drasi-core")
    lib = dependency_version(lock, "drasi-lib")
    sdk = dependency_version(lock, "drasi-plugin-sdk")
    if (exported["core_version"], exported["lib_version"], exported["plugin_version"]) != (
        core, lib, version,
    ):
        raise ValueError("plugin binary versions differ from the locked package versions")
    if exported["target_triple"] != expected_target:
        raise ValueError("plugin binary target differs from the requested native target")
    if exported["sdk_version"] != expected_abi:
        raise ValueError("plugin binary FFI ABI differs from the official SDK contract")
    return {
        "name": "temporal-postgres",
        "kind": "postgres",
        "type": "source",
        "version": version,
        "sdk_version": sdk,
        "core_version": core,
        "lib_version": lib,
        "target_triple": exported["target_triple"],
        "description": "Temporal PostgreSQL source and bootstrap adapter",
    }


def rust_target():
    return next(
        line.removeprefix("host: ")
        for line in subprocess.check_output(["rustc", "-vV"], text=True).splitlines()
        if line.startswith("host: ")
    )


def prepared_library(directory):
    directory = Path(directory)
    for name in ("metadata.json", "provenance.json"):
        path = directory / name
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
            raise ValueError("prepared plugin metadata must be a bounded regular file")
    record = json.loads((directory / "provenance.json").read_text())
    metadata = json.loads((directory / "metadata.json").read_text())
    if record["format"] != 2 or record["profile"] not in ("debug", "release"):
        raise ValueError("the library fixture requires a prepared plugin bundle")
    filename = record["binary_filename"]
    if Path(filename).name != filename or "\\" in filename:
        raise ValueError("prepared plugin filename must be a basename")
    library = directory / filename
    if library.is_symlink() or not library.is_file() or library.stat().st_size > 256 << 20:
        raise ValueError("prepared plugin library must be a bounded regular file")
    if digest(library) != record["binary_sha256"] or digest(directory / "metadata.json") != record["metadata_sha256"]:
        raise ValueError("prepared plugin bytes differ from their recorded hashes")
    verifier = runpy.run_path(str(NATIVE / "verify-dependencies.py"))
    dependencies = verifier["dependencies"]()
    if record["drasi_dependencies"] != verifier["provenance"](dependencies):
        raise ValueError("prepared plugin does not use the locked registry packages")
    exported = binary_metadata(library.absolute())
    lock = tomllib.loads((NATIVE / "Cargo.lock").read_text())
    version = tomllib.loads((PLUGIN / "Cargo.toml").read_text())["package"]["version"]
    expected = registry_metadata(
        exported, lock, version, rust_target(), ffi_abi_version(dependencies["drasi-plugin-sdk"]["path"]),
    )
    if metadata != expected or record["exported_abi_metadata"] != exported or record["ffi_abi_version"] != exported["sdk_version"]:
        raise ValueError("prepared plugin metadata differs from its actual exported ABI")
    return library.absolute(), metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new bundle directory in an existing parent")
    parser.add_argument("--profile", choices=("debug", "release"), default="release")
    parser.add_argument("--expected-target", help="require this exact native Rust target")
    arguments = parser.parse_args()
    output = arguments.output.absolute()
    if output.exists() or output.is_symlink():
        parser.error("output already exists; prepared plugin bundles are never replaced")
    output = output.parent.resolve(strict=True) / output.name
    if os.environ.get("CARGO_BUILD_TARGET") or os.environ.get("CARGO_TARGET_DIR"):
        parser.error("cross-target or redirected Cargo builds are unsupported by this package command")
    suffix = {"Darwin": ".dylib", "Linux": ".so"}.get(platform.system())
    if suffix is None:
        parser.error("plugin packaging requires a native Linux or macOS build")
    native_target = rust_target()
    expected_target = arguments.expected_target or native_target
    if expected_target != native_target:
        parser.error("requested plugin target is not the native Rust host target")
    verifier = runpy.run_path(str(NATIVE / "verify-dependencies.py"))
    dependencies = verifier["dependencies"]()
    command = [
        "cargo", "build", "--manifest-path", str(NATIVE / "Cargo.toml"), "--locked",
        "-p", "temporal-drasi-postgres-plugin", "--lib", "--message-format=json-render-diagnostics",
    ]
    if arguments.profile == "release":
        command.append("--release")
    build = subprocess.run(command, cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE)
    library = NATIVE / "target" / arguments.profile / f"libtemporal_drasi_postgres_plugin{suffix}"
    artifacts = []
    for line in build.stdout.splitlines():
        event = json.loads(line)
        if event["reason"] == "compiler-artifact" and event["target"]["name"] == "temporal_drasi_postgres_plugin":
            artifacts.extend(
                Path(filename).resolve(strict=True)
                for filename in event["filenames"] if filename.endswith(suffix)
            )
    if artifacts != [library.resolve(strict=True)]:
        raise ValueError("Cargo did not emit the expected native plugin; redirected builds are unsupported")
    exported = binary_metadata(library)
    lock = tomllib.loads((NATIVE / "Cargo.lock").read_text())
    manifest = tomllib.loads((PLUGIN / "Cargo.toml").read_text())
    metadata = registry_metadata(
        exported, lock, manifest["package"]["version"], expected_target,
        ffi_abi_version(dependencies["drasi-plugin-sdk"]["path"]),
    )
    if verifier["provenance"](verifier["dependencies"]()) != verifier["provenance"](dependencies):
        raise ValueError("Drasi registry dependencies changed during packaging")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT))
    with tempfile.TemporaryDirectory(prefix=".postgres-package-", dir=output.parent) as temporary:
        staged = Path(temporary) / "bundle"
        staged.mkdir()
        shutil.copyfile(library, staged / library.name)
        (staged / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        record = {
            "format": 2,
            "signing_status": "unsigned",
            "profile": arguments.profile,
            "source_revision": revision,
            "uncommitted_source": dirty,
            "drasi_dependencies": verifier["provenance"](dependencies),
            "ffi_abi_version": exported["sdk_version"],
            "binary_filename": library.name,
            "binary_sha256": digest(staged / library.name),
            "metadata_sha256": digest(staged / "metadata.json"),
            "exported_abi_metadata": exported,
        }
        (staged / "provenance.json").write_text(json.dumps(record, indent=2) + "\n")
        component = {"digest": "sha256:" + record["binary_sha256"], "component": "postgres"}
        catalog = {
            "catalogs": [{
                "id": "local-postgres", "display_name": "Local PostgreSQL plugin",
                "plugins": [{
                    "version": metadata["version"],
                    "artifact": {
                        "id": "local-postgres",
                        "reference": "local@" + component["digest"],
                        "filename": library.name,
                        "binary_sha256": record["binary_sha256"],
                        "local_catalog_path": str(output / library.name),
                    },
                }],
            }],
            "managed_plugins": [{"source": component, "bootstrap": component}],
        }
        (staged / "catalog.json").write_text(json.dumps(catalog, indent=2) + "\n")
        output.mkdir()
        for name in (library.name, "metadata.json", "provenance.json", "catalog.json"):
            os.link(staged / name, output / name)
    prepared_library(output)
    print(output)


if __name__ == "__main__":
    main()
