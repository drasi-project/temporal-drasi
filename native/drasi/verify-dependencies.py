#!/usr/bin/env python3
"""Verify that every Drasi dependency is an unmodified, locked crates.io package."""

import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import tomllib


ROOT = Path(__file__).resolve().parent
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"


def verify(source, expected):
    source = Path(source)
    archive = source.parents[2] / "cache" / source.parent.name / (source.name + ".crate")
    if hashlib.sha256(archive.read_bytes()).hexdigest() != expected:
        raise ValueError("Drasi package checksum differs from Cargo.lock")
    paths = list(source.rglob("*"))
    if source.is_symlink() or any(path.is_symlink() for path in paths):
        raise ValueError("Drasi package contains an invalid source path")
    expected_files = set()
    with tarfile.open(archive, "r:gz") as package:
        for member in package.getmembers():
            relative = Path(member.name).relative_to(source.name)
            path = source / relative
            if ".." in relative.parts or path.is_symlink():
                raise ValueError("Drasi package contains an invalid source path")
            if member.isdir():
                continue
            if not member.isfile():
                raise ValueError("Drasi package contains a non-regular source file")
            expected_files.add(relative)
            if path.read_bytes() != package.extractfile(member).read():
                raise ValueError(f"Drasi package source was modified: {relative}")
    actual_files = {
        path.relative_to(source)
        for path in paths if path.is_file() and path.relative_to(source) != Path(".cargo-ok")
    }
    if actual_files != expected_files:
        raise ValueError("Drasi package contains additional or missing source files")


def dependencies():
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
    locked = {
        package["name"]: package
        for package in lock["package"]
        if package["name"].startswith("drasi-")
    }
    if not locked:
        raise ValueError("No locked Drasi registry packages were found")
    if sum(package["name"].startswith("drasi-") for package in lock["package"]) != len(locked):
        raise ValueError("Drasi dependencies must resolve to one version per package")
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--locked", "--format-version=1",
         "--manifest-path", str(ROOT / "Cargo.toml")],
        text=True,
    ))
    packages = {}
    for package in metadata["packages"]:
        name = package["name"]
        if not name.startswith("drasi-"):
            continue
        selected = locked[name]
        if package["source"] != REGISTRY or selected.get("source") != REGISTRY:
            raise ValueError(f"{name} must come from crates.io, not Git or a path override")
        checksum = selected.get("checksum", "")
        if len(checksum) != 64 or package["version"] != selected["version"]:
            raise ValueError(f"{name} has no exact checksummed registry selection")
        source = Path(package["manifest_path"]).parent
        verify(source, checksum)
        packages[name] = {
            "version": package["version"],
            "source": REGISTRY,
            "checksum": checksum,
            "license": package["license"],
            "path": source,
        }
    if packages.keys() != locked.keys():
        raise ValueError("Resolved Drasi packages differ from Cargo.lock")
    return packages


def provenance(packages):
    return {
        name: {key: value for key, value in package.items() if key != "path"}
        for name, package in sorted(packages.items())
    }


def main():
    print(json.dumps(provenance(dependencies()), indent=2))


if __name__ == "__main__":
    main()
