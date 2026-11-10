#!/usr/bin/env python3
"""Build a relocatable, unsigned development server and native-library bundle."""

import argparse
import ctypes
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import runpy
import shutil
import subprocess
import tarfile
import tempfile


NATIVE = Path(__file__).resolve().parent
ROOT = NATIVE.parent.parent


def digest(path):
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(65536), b""):
            checksum.update(block)
    return checksum.hexdigest()


def link_flags():
    rpath = {
        "Darwin": "-Wl,-rpath,@executable_path/../lib",
        "Linux": "-Wl,-rpath,$ORIGIN/../lib",
    }[platform.system()]
    return "-ldflags=-extldflags=" + rpath


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new .tar.gz file in an existing directory")
    parser.add_argument("--profile", choices=("debug", "release"), default="release")
    arguments = parser.parse_args()
    output = arguments.output.absolute()
    if output.exists() or output.is_symlink():
        parser.error("output already exists; existing packages are never replaced")
    output = output.parent.resolve(strict=True) / output.name
    environment = dict(os.environ, CGO_ENABLED="1")
    environment.setdefault("GOMAXPROCS", "2")
    environment.setdefault("CARGO_BUILD_JOBS", "2")
    if environment.get("CARGO_BUILD_TARGET"):
        parser.error("cross-target Cargo builds are unsupported by this package command")
    if environment.get("CARGO_TARGET_DIR"):
        parser.error("redirected Cargo target directories are unsupported by this package command")
    target = subprocess.check_output(
        ["go", "env", "-json", "GOOS", "GOARCH"], cwd=ROOT, env=environment, text=True
    )
    target = json.loads(target)
    system = {"Darwin": "darwin", "Linux": "linux"}.get(platform.system())
    architecture = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}.get(platform.machine())
    if system is None or architecture is None or target != {"GOOS": system, "GOARCH": architecture}:
        parser.error("package builds require native Linux/macOS amd64 or arm64 Go targets")
    library_name = "libtemporal_drasi.dylib" if system == "darwin" else "libtemporal_drasi.so"
    subprocess.run(
        ["bash", str(NATIVE / "build.sh"), arguments.profile],
        cwd=ROOT, env=environment, check=True,
    )
    with tempfile.TemporaryDirectory(prefix=".drasi-package-", dir=output.parent) as temporary:
        temporary = Path(temporary)
        bundle = temporary / "bundle"
        (bundle / "bin").mkdir(parents=True)
        (bundle / "lib").mkdir()
        library = bundle / "lib" / library_name
        shutil.copy2(NATIVE / "target" / arguments.profile / library_name, library)
        worker = bundle / "lib" / "temporal-drasi-plugin-worker"
        shutil.copy2(
            NATIVE / "target" / arguments.profile / "temporal-drasi-plugin-worker",
            worker,
        )
        server = bundle / "bin" / "temporal-server"
        subprocess.run(
            ["go", "build", "-trimpath", link_flags(), "-tags",
             "disable_grpc_modules,temporal_with_drasi,temporal_drasi_packaged",
             "-o", str(server), "./cmd/server"],
            cwd=ROOT, env=environment, check=True,
        )
        if digest(library) != digest(NATIVE / "lib" / library_name):
            raise RuntimeError("native library publication changed during packaging")
        if digest(worker) != digest(NATIVE / "lib" / worker.name):
            raise RuntimeError("native worker publication changed during packaging")
        probe = ctypes.CDLL(str(library))
        probe.temporal_drasi_v1_abi_version.restype = ctypes.c_uint32
        shutil.copyfile(ROOT / "LICENSE", bundle / "LICENSE")
        shutil.copyfile(NATIVE / "README.md", bundle / "README.md")
        verifier = runpy.run_path(str(NATIVE / "verify-dependencies.py"))
        dependencies = verifier["dependencies"]()
        if any(package["license"] != "Apache-2.0" for package in dependencies.values()):
            raise ValueError("Drasi package licensing differs from the bundled Apache license")
        shutil.copyfile(NATIVE / "LICENSE.drasi-core", bundle / "LICENSE.drasi-core")
        manifest = {
            "format": 2,
            "provenance": "unpublished-development-source-build",
            "profile": arguments.profile,
            "platform": target,
            "native_abi": probe.temporal_drasi_v1_abi_version(),
            "drasi_source": "crates.io",
            "drasi_dependencies": verifier["provenance"](dependencies),
            "cargo_manifest_sha256": digest(NATIVE / "Cargo.toml"),
            "cargo_lock_sha256": digest(NATIVE / "Cargo.lock"),
            "rustc_version": subprocess.check_output(
                [environment.get("RUSTC", "rustc"), "--version", "--verbose"],
                cwd=NATIVE, env=environment, text=True,
            ),
            "server_revision": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            "server_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)),
            "go_build": subprocess.check_output(
                ["go", "version", "-m", str(server)], text=True
            ).replace(str(server), "bin/temporal-server", 1),
            "files": {
                str(path.relative_to(bundle)): digest(path)
                for path in sorted(bundle.rglob("*")) if path.is_file()
            },
        }
        (bundle / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        archive = temporary / "package.tar.gz"
        with archive.open("xb") as destination:
            with gzip.GzipFile(filename="", mode="wb", fileobj=destination, mtime=0) as compressed:
                with tarfile.open(mode="w", fileobj=compressed) as package:
                    for path in sorted(bundle.rglob("*")):
                        info = package.gettarinfo(str(path), arcname=str(path.relative_to(bundle)))
                        info.uid = info.gid = info.mtime = 0
                        info.uname = info.gname = ""
                        info.mode = 0o755 if path.is_dir() or path.parent.name in ("bin", "lib") else 0o644
                        if path.is_file():
                            with path.open("rb") as source:
                                package.addfile(info, source)
                        else:
                            package.addfile(info)
            destination.flush()
            os.fsync(destination.fileno())
        os.link(archive, output)
        directory = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    print(output)


if __name__ == "__main__":
    main()
