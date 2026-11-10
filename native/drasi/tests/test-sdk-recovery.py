#!/usr/bin/env python3
"""Verify published SDK recovery and PostgreSQL without changing registry sources."""

import argparse
import json
import os
import runpy
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TESTS = (
    "queries::e2e_checkpoint_tests",
    "queries::checkpoint_tests",
    "queries::tests",
    "queries::base::tests",
    "queries::manager::tests",
    "recovery::tests",
    "queries::config_hash::tests",
    "managers::lifecycle_helpers::tests",
    "lib_core_ops::query_ops::tests",
)
POSTGRES_TESTS = (
    "test_schema_discovery_reports_columns_end_to_end",
    "test_full_crud_cycle",
    "test_bootstrap_cdc_handover_has_no_overlap_or_gap",
    "test_events_carry_source_position_bytes",
    "test_resume_subscription_replays_from_lsn",
    "test_postgres_multi_query_no_duplicate_on_restart",
)
NATIVE_POSTGRES_TEST = "plugins::postgres_tests::native_postgres_bootstrap_cdc_and_recovery"


def native_postgres(env, temp, bundle=None):
    if sys.platform not in ("darwin", "linux"):
        raise RuntimeError("The native PostgreSQL library fixture requires macOS or Linux")
    env = dict(env, CARGO_PROFILE_DEV_STRIP="none")
    subprocess.run(
        ["cargo", "build", "--quiet", "--manifest-path", str(ROOT / "Cargo.toml"),
         "--locked", "--bin", "temporal-drasi-plugin-worker"],
        env=env,
        check=True,
    )
    subprocess.run(
        ["cargo", "clippy", "--quiet", "--manifest-path", str(ROOT / "Cargo.toml"),
         "--locked", "-p", "temporal-drasi-postgres-plugin", "--all-targets",
         "--", "-D", "warnings"],
        env=env,
        check=True,
    )
    subprocess.run(
        ["cargo", "test", "--quiet", "--manifest-path", str(ROOT / "Cargo.toml"),
         "--locked", "-p", "temporal-drasi-postgres-plugin", "--lib"],
        env=env,
        check=True,
    )
    if bundle is None:
        subprocess.run(
            ["cargo", "build", "--quiet", "--manifest-path", str(ROOT / "Cargo.toml"),
             "--locked", "-p", "temporal-drasi-postgres-plugin", "--lib"],
            env=env,
            check=True,
        )
        suffix = ".dylib" if sys.platform == "darwin" else ".so"
        path = ROOT / "target" / "debug" / f"libtemporal_drasi_postgres_plugin{suffix}"
        version = tomllib.loads((ROOT / "plugins" / "postgres" / "Cargo.toml").read_text())["package"]["version"]
    else:
        package = runpy.run_path(str(ROOT / "plugins" / "postgres" / "prepare.py"))
        path, metadata = package["prepared_library"](bundle)
        version = metadata["version"]
    libraries = {}
    for kind in ("source", "bootstrap"):
        if not path.is_file():
            raise RuntimeError(f"PostgreSQL {kind} library was not built")
        libraries[kind] = {"path": str(path), "version": version}
    command = [
        "cargo", "test", "--quiet", "--manifest-path", str(ROOT / "Cargo.toml"),
        "--locked", "--lib", "--", "--ignored", "--exact", "--test-threads=1",
        NATIVE_POSTGRES_TEST,
    ]
    listing = subprocess.check_output([*command, "--list"], env=env, text=True)
    selected = {line.removesuffix(": test") for line in listing.splitlines() if line.endswith(": test")}
    if selected != {NATIVE_POSTGRES_TEST}:
        raise RuntimeError(f"Native PostgreSQL test selection is incomplete: {sorted(selected)}")
    container = subprocess.check_output(
        ["docker", "create", "--name", f"temporal-drasi-native-postgres-{uuid.uuid4().hex}",
         "--label", "temporal.drasi.verification=native-postgres-library",
         "--publish", "127.0.0.1::5432", "--cpus", "2", "--memory", "512m",
         "--env", "POSTGRES_DB=watchfixture", "--env", "POSTGRES_PASSWORD=verification-only-password",
         "postgres:17-alpine@sha256:266d3c257594e195b309ae9d1805e54c788f1d7b375f56bf6c367aac6c917e7b",
         "-c", "wal_level=logical", "-c", "max_replication_slots=8", "-c", "max_wal_senders=8"],
        text=True,
    ).strip()
    try:
        subprocess.run(["docker", "start", container], check=True, stdout=subprocess.DEVNULL)
        deadline = time.monotonic() + 60
        while True:
            ready = subprocess.run(
                ["docker", "exec", container, "pg_isready", "-h", "127.0.0.1",
                 "-U", "postgres", "-d", "watchfixture"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
                check=False,
            )
            if ready.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("The owned PostgreSQL library fixture did not become ready")
            time.sleep(0.25)
        subprocess.run(
            ["docker", "exec", "-i", container, "psql", "-U", "postgres", "-d", "watchfixture",
             "-v", "ON_ERROR_STOP=1"],
            input="""CREATE TABLE watch_items (id integer PRIMARY KEY, name text NOT NULL, amount integer NOT NULL);
ALTER TABLE watch_items REPLICA IDENTITY FULL;
INSERT INTO watch_items VALUES (1, 'initial', 12), (2, 'excluded', 3);
CREATE PUBLICATION native_fixture_pub FOR TABLE watch_items;
""",
            text=True,
            check=True,
        )
        address = subprocess.check_output(["docker", "port", container, "5432/tcp"], text=True).strip()
        if not address.startswith("127.0.0.1:") or "\n" in address:
            raise RuntimeError("PostgreSQL fixture did not bind one loopback address")
        fixture = Path(temp) / "native-postgres.json"
        fixture.write_text(json.dumps({**libraries, "container": container, "port": int(address.rsplit(":", 1)[1])}))
        subprocess.run(
            command,
            env=dict(env, TEMPORAL_DRASI_POSTGRES_LIBRARY_FIXTURE=str(fixture)),
            check=True,
            timeout=180,
        )
    finally:
        subprocess.run(["docker", "rm", "--force", container], check=True, stdout=subprocess.DEVNULL)


def published_source(packages, name, temporary):
    source = Path(temporary) / "source"
    shutil.copytree(packages[name]["path"], source)
    workspace = Path(temporary)
    manifest = (source / "Cargo.toml").read_text()
    manifest = manifest.replace('[package]\n', '[package]\npublish = false\n', 1)
    manifest = manifest.replace('path = "', 'path = "source/')
    manifest += '\n[workspace]\nresolver = "2"\n'
    for dependency in ("drasi-index-rocksdb",) if name == "drasi-lib" else ():
        if f"[dev-dependencies.{dependency}]" in manifest:
            raise RuntimeError("Published SDK now declares the retained test dependency; review the test harness")
        manifest += f'\n[dev-dependencies.{dependency}]\nversion = "={packages[dependency]["version"]}"\n'
    (workspace / "Cargo.toml").write_text(manifest)
    return str(workspace / "Cargo.toml")


def install_test_lock(name, temporary):
    workspace = Path(temporary)
    shutil.copyfile(ROOT / "tests/fixtures" / f"{name}.registry-tests.Cargo.lock", workspace / "Cargo.lock")
    lock = tomllib.loads((workspace / "Cargo.lock").read_text())
    for package in lock["package"]:
        if package["name"].startswith("drasi-") and (
            package.get("source") not in (None, "registry+https://github.com/rust-lang/crates.io-index")
            or (package.get("source") is None and package["name"] != name)
        ):
            raise RuntimeError("Published SDK test lock contains a non-registry Drasi dependency")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--postgres",
        action="store_true",
        help="Run real PostgreSQL SDK container tests, not configured-plugin or UI acceptance",
    )
    group.add_argument(
        "--native-postgres",
        action="store_true",
        help="Test native bootstrap, CDC and cold recovery with unsigned test-only PostgreSQL libraries",
    )
    parser.add_argument(
        "--postgres-bundle", type=Path,
        help="Use an actual prepared unsigned debug/release bundle for --native-postgres",
    )
    args = parser.parse_args()
    if args.postgres_bundle is not None and not args.native_postgres:
        parser.error("--postgres-bundle requires --native-postgres")
    verifier = runpy.run_path(str(ROOT / "verify-dependencies.py"))
    packages = verifier["dependencies"]()
    (ROOT / ".verification").mkdir(exist_ok=True)
    env = dict(os.environ, CARGO_TARGET_DIR=str(ROOT / "target"))
    with tempfile.TemporaryDirectory(prefix="sdk-recovery-", dir=ROOT / ".verification") as temp:
        if args.native_postgres:
            native_postgres(env, temp, args.postgres_bundle)
        elif args.postgres:
            manifest = published_source(packages, "drasi-source-postgres", temp)
            install_test_lock("drasi-source-postgres", temp)
            command = [
                "cargo", "test", "--quiet", "--manifest-path", manifest, "--locked",
                "--test", "integration_tests",
                "--", "--ignored", "--exact", "--test-threads=1", *POSTGRES_TESTS,
            ]
            listing = subprocess.check_output([*command, "--list"], env=env, text=True)
            selected = {line.removesuffix(": test") for line in listing.splitlines() if line.endswith(": test")}
            if selected != set(POSTGRES_TESTS):
                raise RuntimeError(f"PostgreSQL SDK test selection is incomplete: {sorted(selected)}")
            subprocess.run(
                command,
                env=env,
                check=True,
            )
        else:
            manifest = published_source(packages, "drasi-lib", temp)
            install_test_lock("drasi-lib", temp)
            command = (
                ["cargo", "test", "--quiet", "--manifest-path", manifest, "--locked",
                 "--lib", "--", *TESTS]
            )
            listing = subprocess.check_output([*command, "--list"], env=env, text=True)
            for prefix in TESTS:
                if not any(line.startswith(prefix + "::") for line in listing.splitlines()):
                    raise RuntimeError(f"Published SDK test selection is incomplete: {prefix}")
            subprocess.run(command, env=env, check=True)
    verifier["dependencies"]()
    if args.native_postgres:
        print("Native PostgreSQL library bootstrap, CDC and cold recovery passed; not configured-plugin/UI acceptance")
    elif args.postgres:
        print("Real PostgreSQL SDK bootstrap, CDC and recovery checks passed; not configured-plugin/UI acceptance")
    else:
        print("Published SDK query, bootstrap and recovery checks passed; no existing-only or preserving-removal guarantee")


if __name__ == "__main__":
    main()
