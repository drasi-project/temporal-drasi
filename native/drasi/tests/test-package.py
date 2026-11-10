#!/usr/bin/env python3
"""Prove packaged native execution after relocation, without checkout fallback."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import runpy
import selectors
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid


NATIVE = Path(__file__).resolve().parent.parent
ROOT = NATIVE.parent.parent
sys.path.insert(0, str(NATIVE))
from package import link_flags


def source_worker(worker, bundle, manifest, environment):
    subprocess.run(
        ["cargo", "build", "--quiet", "--manifest-path", str(NATIVE / "Cargo.toml"),
         "--locked", "--target-dir", str(NATIVE / "target"), "-p", "temporal-drasi-test-plugin"],
        cwd=ROOT, env=environment, check=True,
    )
    suffix = ".dylib" if manifest["platform"]["GOOS"] == "darwin" else ".so"
    fixture = bundle / "fixture-plugins" / f"libtemporal_drasi_test_plugin{suffix}"
    fixture.parent.mkdir()
    shutil.copyfile(NATIVE / "target/debug" / fixture.name, fixture)
    dependencies = manifest["drasi_dependencies"]
    artifact = {
        "path": str(fixture),
        "binary_sha256": hashlib.sha256(fixture.read_bytes()).hexdigest(),
        "sdk_version": dependencies["drasi-plugin-sdk"]["version"],
        "core_version": dependencies["drasi-core"]["version"],
        "lib_version": dependencies["drasi-lib"]["version"],
        "target_triple": next(
            line.removeprefix("host: ") for line in manifest["rustc_version"].splitlines()
            if line.startswith("host: ")
        ),
        "plugin_version": "1.0.0",
        "abi_version": "0.14.0",
    }
    binding = {"nonce": str(uuid.uuid4()), "identity": "relocated-package-source-proof"}
    def component(kind):
        return {
            "plugin": {"digest": f"sha256:{artifact['binary_sha256']}", "component": kind},
            "configuration": {}, "secrets": {},
        }
    specification = {
        "identity": binding["identity"], "source_id": "package-source",
        "recipe": {"source": component("test-source"), "bootstrap": component("test-bootstrap")},
        "source_artifact": artifact, "bootstrap_artifact": artifact,
        "source_config": {}, "bootstrap_config": {}, "bootstrap_source_config": {},
        "state_directory": str(bundle / "source-worker-state"),
    }
    process = subprocess.Popen(
        [str(worker)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, cwd=bundle, env={},
    )
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)

            def receive(count, deadline):
                result = bytearray()
                while len(result) < count:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not selector.select(remaining):
                        raise RuntimeError("relocated source worker did not respond")
                    chunk = os.read(process.stdout.fileno(), count - len(result))
                    if not chunk:
                        raise RuntimeError("relocated source worker exited before completion")
                    result.extend(chunk)
                return result

            def exchange(command):
                phases = (
                    "SourceLibrary", "ResolverInjection", "SourceFactory",
                    "SourceContract", "BootstrapFactory", "AttachBootstrap",
                    "StateDirectory", "StateStore", "StateBridge", "Initialize", "Ready",
                ) if isinstance(command, dict) and "Construct" in command["Request"]["request"] else ()
                observed = []
                deadline = time.monotonic() + 15
                body = json.dumps({"version": 1, "binding": binding, "command": command}).encode()
                process.stdin.write(b"TDW1" + struct.pack(">I", len(body)) + body)
                process.stdin.flush()
                while True:
                    header = receive(8, deadline)
                    assert header[:4] == b"TDW1"
                    length = struct.unpack(">I", header[4:])[0]
                    assert 0 < length <= 8 << 20
                    response = json.loads(receive(length, deadline))
                    assert response["version"] == 1 and response["binding"] == binding
                    result = response["command"]
                    if isinstance(result, dict) and "ConstructionPhase" in result:
                        assert len(observed) < len(phases)
                        assert result == {"ConstructionPhase": phases[len(observed)]}
                        observed.append(result["ConstructionPhase"])
                        continue
                    assert tuple(observed) == phases
                    return result

            assert exchange({"Request": {"id": 1, "request": {"Construct": specification}}}) == {
                "Response": {"id": 1, "response": "Constructed"}
            }
            assert exchange({"Request": {"id": 2, "request": "Start"}}) == {
                "Response": {"id": 2, "response": "Ok"}
            }
            assert exchange("Stop") == "Stopped"
            assert process.wait(timeout=5) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stdin.close()
        process.stdout.close()


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
            ("redirected-cargo", {"CARGO_TARGET_DIR": str(temporary / "other-target")}, b"redirected Cargo"),
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
        assert manifest["format"] == 2 and manifest["native_abi"] == 1
        assert manifest["profile"] == arguments.profile
        assert manifest["provenance"] == "unpublished-development-source-build"
        verifier = runpy.run_path(str(NATIVE / "verify-dependencies.py"))
        assert manifest["drasi_source"] == "crates.io"
        assert manifest["drasi_dependencies"] == verifier["provenance"](verifier["dependencies"]())
        assert "patches" not in manifest
        actual = {
            str(path.relative_to(relocated)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in relocated.rglob("*") if path.is_file() and path.name != "manifest.json"
        }
        assert actual == manifest["files"], "packaged files do not match their manifest"
        worker = relocated / "lib" / "temporal-drasi-plugin-worker"
        assert worker.is_file() and os.access(worker, os.X_OK), "packaged source worker is missing"
        source_worker(worker, relocated, manifest, environment)
        worker.rename(temporary / worker.name)
        try:
            subprocess.run([str(worker)], cwd=relocated, env={}, check=True, timeout=5)
        except FileNotFoundError:
            pass
        else:
            raise AssertionError("missing packaged source worker resolved outside its bundle")
        (temporary / worker.name).rename(worker)
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
        library_name = (
            "libtemporal_drasi.dylib" if manifest["platform"]["GOOS"] == "darwin"
            else "libtemporal_drasi.so"
        )
        library = relocated / "lib" / library_name
        library.rename(temporary / library.name)
        missing = subprocess.run(
            invocation, cwd=relocated, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        assert missing.returncode != 0, "packaged binary silently loaded a library outside its bundle"
        assert b"PASS" not in missing.stdout, missing.stdout
    print(f"Package ({arguments.profile}): manifest, relocation, real source-worker execution, "
          "native evaluation/replay and missing-worker/library refusal passed")


if __name__ == "__main__":
    main()
