#!/usr/bin/env python3
"""Prove packaged native execution after relocation, without checkout fallback."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import tarfile
import tempfile


NATIVE = Path(__file__).resolve().parent.parent
ROOT = NATIVE.parent.parent
sys.path.insert(0, str(NATIVE))
from package import link_flags


def main():
    if not __debug__:
        raise RuntimeError("package verification requires Python assertions to be enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("debug", "release"), default="debug")
    parser.add_argument("archive", nargs="?", type=Path, help="verify an existing package instead of building one")
    arguments = parser.parse_args()
    scratch = NATIVE / ".verification"
    scratch.mkdir(exist_ok=True)
    environment = dict(os.environ, CGO_ENABLED="1")
    environment.setdefault("GOMAXPROCS", "2")
    environment.setdefault("CARGO_BUILD_JOBS", "2")
    environment.setdefault("RUST_LOG", "error")
    for name in ("LD_LIBRARY_PATH", "LD_PRELOAD", "DYLD_LIBRARY_PATH",
                 "DYLD_FALLBACK_LIBRARY_PATH", "DYLD_INSERT_LIBRARIES"):
        environment.pop(name, None)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    with tempfile.TemporaryDirectory(prefix="package-proof-", dir=scratch) as temporary:
        temporary = Path(temporary)
        archive = temporary / "server.tar.gz"
        command = [sys.executable, str(NATIVE / "package.py"), "--profile", arguments.profile, str(archive)]
        if arguments.archive is None:
            subprocess.run(command, cwd=ROOT, env=environment, check=True)
        else:
            shutil.copyfile(arguments.archive, archive)
        previous = hashlib.sha256(archive.read_bytes()).digest()
        refused = subprocess.run(
            command, cwd=ROOT, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        assert refused.returncode != 0, "packager replaced an existing artifact"
        assert hashlib.sha256(archive.read_bytes()).digest() == previous
        for name, changes, diagnostic in (
            ("cross-go", {"GOOS": "windows"}, b"require native Linux/macOS"),
            ("cross-cargo", {"CARGO_BUILD_TARGET": "unsupported-package-target"}, b"cross-target Cargo"),
        ):
            rejected = temporary / f"{name}.tar.gz"
            result = subprocess.run(
                [sys.executable, str(NATIVE / "package.py"), "--profile", arguments.profile, str(rejected)],
                cwd=ROOT, env=dict(environment, **changes), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            assert result.returncode != 0 and diagnostic in result.stderr, f"{name}: {result.stderr!r}"
            assert not rejected.exists(), f"{name}: rejected build published an archive"
        relocated = temporary / "relocated bundle"
        relocated.mkdir()
        with tarfile.open(archive) as package:
            assert all(member.isfile() or member.isdir() for member in package.getmembers())
            package.extractall(relocated, filter="data")
        manifest = json.loads((relocated / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["format"] == 1 and manifest["native_abi"] == 1
        assert manifest["profile"] == arguments.profile
        assert manifest["provenance"] == "unpublished-development-source-build"
        actual = {
            str(path.relative_to(relocated)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in relocated.rglob("*") if path.is_file() and path.name != "manifest.json"
        }
        assert actual == manifest["files"], "packaged files do not match their manifest"
        server = relocated / "bin" / "temporal-server"
        version = subprocess.check_output([str(server), "--version"], cwd=relocated, env=environment)
        assert b"temporal version" in version, version
        proof = relocated / "bin" / "native-proof"
        subprocess.run(
            ["go", "test", "-c", "-trimpath", link_flags(), "-tags",
             "test_dep,temporal_with_drasi,temporal_drasi_packaged",
             "-o", str(proof), "./service/watch/native"],
            cwd=ROOT, env=environment, check=True,
        )
        invocation = [str(proof), "-test.run", "^TestNativeEvaluationAndPersistentHandoff$",
                      "-test.count=1", "-test.timeout=45s"]
        subprocess.run(invocation, cwd=relocated, env=environment, check=True)
        library = next((relocated / "lib").iterdir())
        library.rename(temporary / library.name)
        missing = subprocess.run(
            invocation, cwd=relocated, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        assert missing.returncode != 0, "packaged binary silently loaded a library outside its bundle"
        assert b"PASS" not in missing.stdout, missing.stdout
    print(f"Package ({arguments.profile}): manifest, relocation, real native evaluation/replay "
          "and missing-library refusal passed")


if __name__ == "__main__":
    main()
