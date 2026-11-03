#!/usr/bin/env python3
"""Verify SDK recovery and PostgreSQL libraries without changing the source cache."""

import argparse
import hashlib
import json
import os
import runpy
import subprocess
import sys
import tempfile
import time
import tomllib
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOCK = ROOT / "tests" / "fixtures" / "drasi-core.Cargo.lock"
LOCK_SHA256 = "fae0da19cfd0c227d9312beb2d668f93aaaf854472018fa28b334ba8af0699d1"
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


def native_postgres(checkout, env, temp):
    if sys.platform not in ("darwin", "linux"):
        raise RuntimeError("The native PostgreSQL library fixture requires macOS or Linux")
    env = dict(env, CARGO_PROFILE_DEV_STRIP="none")
    regressions = {
        "config_value::tests::canonical_schemas_include_typed_values_and_complete_reference_shapes",
        "descriptor::tests::ssl_mode_references_preserve_the_resolved_policy_and_reject_invalid_values",
    }
    sdk_tests = [
        "cargo", "test", "--quiet", "--manifest-path", str(checkout / "Cargo.toml"),
        "--locked", "-p", "drasi-plugin-sdk", "-p", "drasi-bootstrap-postgres",
        "--lib", "--", "--exact", *sorted(regressions),
    ]
    listing = subprocess.check_output([*sdk_tests, "--list"], env=env, text=True)
    selected = {line.removesuffix(": test") for line in listing.splitlines() if line.endswith(": test")}
    if selected != regressions:
        raise RuntimeError(f"PostgreSQL schema/reference regressions are missing: {sorted(selected)}")
    subprocess.run(sdk_tests, env=env, check=True)
    subprocess.run(
        ["cargo", "clippy", "--quiet", "--manifest-path", str(checkout / "Cargo.toml"),
         "--locked", "-p", "drasi-plugin-sdk", "-p", "drasi-source-postgres",
         "-p", "drasi-bootstrap-postgres", "--all-targets", "--features",
         "dynamic-plugin", "--", "-D", "warnings"],
        env=env,
        check=True,
    )
    subprocess.run(
        ["cargo", "build", "--quiet", "--manifest-path", str(checkout / "Cargo.toml"),
         "--locked", "-p", "drasi-source-postgres", "-p", "drasi-bootstrap-postgres",
         "--lib", "--features", "dynamic-plugin"],
        env=env,
        check=True,
    )
    suffix = ".dylib" if sys.platform == "darwin" else ".so"
    libraries = {}
    for kind, directory in (("source", "sources"), ("bootstrap", "bootstrappers")):
        manifest = tomllib.loads((checkout / "components" / directory / "postgres" / "Cargo.toml").read_text())
        path = ROOT / "target" / "debug" / f"libdrasi_{kind}_postgres{suffix}"
        if not path.is_file():
            raise RuntimeError(f"PostgreSQL {kind} library was not built")
        libraries[kind] = {"path": str(path), "version": manifest["package"]["version"]}
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
SELECT pg_create_logical_replication_slot('native_fixture_slot', 'pgoutput');
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--postgres",
        action="store_true",
        help="Run real PostgreSQL SDK container tests, not signed-plugin or UI acceptance",
    )
    group.add_argument(
        "--native-postgres",
        action="store_true",
        help="Test native bootstrap, CDC and cold recovery with unsigned test-only PostgreSQL libraries",
    )
    args = parser.parse_args()
    lock = LOCK.read_bytes()
    if hashlib.sha256(lock).hexdigest() != LOCK_SHA256:
        raise RuntimeError("SDK validation lock checksum does not match the pin")
    subprocess.run([sys.executable, str(ROOT / "prepare-core.py")], check=True)
    preparer = runpy.run_path(str(ROOT / "prepare-core.py"))
    (ROOT / ".verification").mkdir(exist_ok=True)
    env = dict(os.environ, CARGO_TARGET_DIR=str(ROOT / "target"))
    with tempfile.TemporaryDirectory(prefix="sdk-recovery-", dir=ROOT / ".verification") as temp:
        checkout = Path(temp) / "source"
        subprocess.run(
            ["git", "-c", "advice.detachedHead=false", "clone", "--quiet", "--shared",
             str(preparer["SOURCE"]), str(checkout)],
            check=True,
        )
        for name, _ in preparer["PATCHES"]:
            subprocess.run(
                ["git", "-C", str(checkout), "apply", "--index", str(ROOT / "patches" / name)],
                check=True,
            )
        preparer["verify"](checkout)
        (checkout / "Cargo.lock").write_bytes(lock)
        manifest = str(checkout / "lib" / "Cargo.toml")
        if args.native_postgres:
            native_postgres(checkout, env, temp)
        elif args.postgres:
            command = [
                "cargo", "test", "--quiet", "--manifest-path", manifest, "--locked",
                "-p", "drasi-source-postgres", "--test", "integration_tests",
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
            subprocess.run(
                ["cargo", "clippy", "--quiet", "--manifest-path", manifest, "--locked",
                 "-p", "drasi-source-postgres", "-p", "drasi-bootstrap-postgres",
                 "--all-targets", "--features", "dynamic-plugin", "--", "-D", "warnings"],
                env=env,
                check=True,
            )
        else:
            subprocess.run(
                ["cargo", "test", "--quiet", "--manifest-path", manifest, "--locked",
                 "-p", "drasi-lib", "--lib", "--", *TESTS],
                env=env,
                check=True,
            )
            subprocess.run(
                ["cargo", "clippy", "--quiet", "--manifest-path", manifest, "--locked",
                 "-p", "drasi-lib", "--all-targets", "--", "-D", "warnings"],
                env=env,
                check=True,
            )
    preparer["verify"](preparer["SOURCE"])
    if args.native_postgres:
        print("Native PostgreSQL library bootstrap, CDC and cold recovery passed; not signed-plugin/UI acceptance")
    elif args.postgres:
        print("Real PostgreSQL SDK bootstrap, CDC and recovery checks passed; not signed-plugin/UI acceptance")
    else:
        print("Prepared SDK recovery, preserving removal and joined-stop checks passed")


if __name__ == "__main__":
    main()
