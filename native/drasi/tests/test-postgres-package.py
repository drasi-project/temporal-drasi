#!/usr/bin/env python3
"""Verify real PostgreSQL plugin packaging without signing or publication."""

import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import textwrap
import unittest


NATIVE = Path(__file__).resolve().parents[1]
PREPARE = NATIVE / "plugins" / "postgres" / "prepare.py"
MODULE = runpy.run_path(str(PREPARE))


class PostgresPackageTests(unittest.TestCase):
    def test_invalid_abi_strings_are_rejected_before_dereference(self):
        for pointer, length in [(None, 1), (1, 0), (1, 4097)]:
            with self.subTest(pointer=pointer, length=length):
                with self.assertRaisesRegex(ValueError, "invalid string"):
                    MODULE["FfiStr"](pointer, length).text()

    def test_locked_versions_and_target_must_match_the_binary(self):
        source = "registry+https://github.com/rust-lang/crates.io-index"
        lock = {"package": [
            {"name": "drasi-core", "version": "1.2.3", "source": source, "checksum": "a" * 64},
            {"name": "drasi-lib", "version": "2.3.4", "source": source, "checksum": "b" * 64},
            {"name": "drasi-plugin-sdk", "version": "3.4.5", "source": source, "checksum": "c" * 64},
        ]}
        exported = {
            "core_version": "1.2.3", "lib_version": "2.3.4",
            "plugin_version": "4.5.6", "target_triple": "fixture-target", "sdk_version": "5.6.7",
        }
        result = MODULE["registry_metadata"](exported, lock, "4.5.6", "fixture-target", "5.6.7")
        self.assertEqual(result["sdk_version"], "3.4.5")
        for field in exported:
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "differ"):
                    MODULE["registry_metadata"](
                        {**exported, field: "wrong"}, lock, "4.5.6", "fixture-target", "5.6.7",
                    )
        with self.assertRaisesRegex(ValueError, "checksummed crates.io"):
            MODULE["dependency_version"](
                {"package": [{"name": "drasi-core", "version": "1.2.3"}]}, "drasi-core",
            )

    def test_actual_bundle_and_refusal_to_replace_it(self):
        scratch = NATIVE / ".verification"
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="postgres-package-proof-", dir=scratch) as temporary:
            output = Path(temporary) / "bundle"
            command = [sys.executable, str(PREPARE), "--profile", "debug", str(output)]
            environment = dict(os.environ, CARGO_BUILD_JOBS="2")
            subprocess.run(command, env=environment, check=True)
            record = json.loads((output / "provenance.json").read_text())
            metadata = json.loads((output / "metadata.json").read_text())
            exported = MODULE["binary_metadata"](output / record["binary_filename"])
            actual_library, actual_metadata = MODULE["prepared_library"](output)
            self.assertEqual(actual_library, output / record["binary_filename"])
            self.assertEqual(actual_metadata, metadata)
            self.assertEqual(record["ffi_abi_version"], exported["sdk_version"])
            verifier = runpy.run_path(str(NATIVE / "verify-dependencies.py"))
            packages = verifier["dependencies"]()
            self.assertEqual(record["drasi_dependencies"], verifier["provenance"](packages))
            self.assertEqual(record["ffi_abi_version"], MODULE["ffi_abi_version"](packages["drasi-plugin-sdk"]["path"]))
            self.assertEqual(metadata["version"], exported["plugin_version"])
            self.assertEqual(metadata["core_version"], exported["core_version"])
            self.assertEqual(metadata["lib_version"], exported["lib_version"])
            self.assertEqual(metadata["target_triple"], exported["target_triple"])
            self.assertNotEqual(metadata["sdk_version"], exported["sdk_version"])
            self.assertEqual(record["binary_sha256"], MODULE["digest"](actual_library))
            self.assertEqual(record["metadata_sha256"], MODULE["digest"](output / "metadata.json"))
            component = {"digest": "sha256:" + record["binary_sha256"], "component": "postgres"}
            self.assertEqual(json.loads((output / "catalog.json").read_text()), {
                "catalogs": [{
                    "id": "local-postgres", "display_name": "Local PostgreSQL plugin",
                    "plugins": [{
                        "version": metadata["version"],
                        "artifact": {
                            "id": "local-postgres", "reference": "local@" + component["digest"],
                            "filename": record["binary_filename"], "binary_sha256": record["binary_sha256"],
                            "local_catalog_path": str(actual_library),
                        },
                    }],
                }],
                "managed_plugins": [{"source": component, "bootstrap": component}],
            })
            before = {path.name: MODULE["digest"](path) for path in output.iterdir()}
            refused = subprocess.run(command, env=environment, capture_output=True)
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn(b"never replaced", refused.stderr)
            self.assertEqual(before, {path.name: MODULE["digest"](path) for path in output.iterdir()})
            for name, changes, arguments in [
                ("cross-target", {"CARGO_BUILD_TARGET": "fixture-target"}, []),
                ("redirect", {"CARGO_TARGET_DIR": temporary}, []),
                ("wrong-native-target", {}, ["--expected-target", "fixture-target"]),
            ]:
                with self.subTest(name=name):
                    rejected = Path(temporary) / name
                    result = subprocess.run(
                        [sys.executable, str(PREPARE), "--profile", "debug", *arguments, str(rejected)],
                        env=dict(environment, **changes), capture_output=True,
                    )
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse(rejected.exists())
            for name, changes in [
                ("wrong-origin", {"drasi_dependencies": {}}),
                ("wrong-abi", {"ffi_abi_version": "wrong"}),
                ("binary-mismatch", {"binary_sha256": "b" * 64}),
                ("metadata-mismatch", {"metadata_sha256": "b" * 64}),
                ("path-traversal", {"binary_filename": "../outside"}),
            ]:
                with self.subTest(prepared_library_refusal=name):
                    (output / "provenance.json").write_text(json.dumps({**record, **changes}))
                    with self.assertRaises(ValueError):
                        MODULE["prepared_library"](output)
            (output / "provenance.json").write_text(json.dumps({
                key: value for key, value in record.items() if key != "signing_status"
            }))
            self.assertEqual(MODULE["prepared_library"](output), (actual_library, metadata))

    def test_postgres_database_starts_only_after_plugin_compatibility_gate(self):
        with tempfile.TemporaryDirectory(prefix="postgres-gate-order-", dir=NATIVE / ".verification") as temporary:
            directory = Path(temporary)
            commands = {
                "python3": '#!/bin/sh\nprintf "inspect\\n" >> "$GATE_LOG"\n',
                "bash": textwrap.dedent("""\
                    #!/bin/sh
                    case "$1" in
                        */build.sh) printf "build\\n" >> "$GATE_LOG" ;;
                        */verify-plugins.sh) exec /bin/bash "$@" ;;
                        *) exit 98 ;;
                    esac
                """),
                "go": '#!/bin/sh\nprintf "plugin-gate\\n" >> "$GATE_LOG"\nexit "$GATE_RESULT"\n',
                "docker": '#!/bin/sh\nprintf "docker\\n" >> "$GATE_LOG"\nexit 29\n',
            }
            for name, script in commands.items():
                command = directory / name
                command.write_text(script)
                command.chmod(0o700)
            config = directory / "policy.json"
            config.write_text("{}")
            for name, gate_result, expected_code, expected_stages in [
                ("refused-plugin", 17, 17, ["inspect", "build", "plugin-gate"]),
                ("admitted-plugin", 0, 29, ["inspect", "build", "plugin-gate", "docker"]),
            ]:
                with self.subTest(name=name):
                    log = directory / f"{name}.log"
                    result = subprocess.run(
                        ["/bin/bash", str(NATIVE / "verify-postgres.sh")],
                        cwd=NATIVE.parents[1],
                        env=dict(
                            os.environ, PATH=str(directory) + os.pathsep + os.environ["PATH"],
                            TEMPORAL_DRASI_PLUGIN_CONFIG=str(config), GATE_LOG=str(log),
                            GATE_RESULT=str(gate_result),
                        ),
                        text=True, capture_output=True,
                    )
                    self.assertEqual(result.returncode, expected_code, result.stderr)
                    self.assertEqual(log.read_text().splitlines(), expected_stages)

    def test_catalog_inspection_uses_pins_without_requesting_signatures(self):
        inspector = runpy.run_path(str(NATIVE / "tests/inspect-postgres-catalog.py"))
        with tempfile.TemporaryDirectory(prefix="postgres-catalog-proof-", dir=NATIVE / ".verification") as temporary:
            directory = Path(temporary)
            metadata = {"name": "postgres", "version": "0.1.0", "target_triple": "aarch64-apple-darwin"}
            metadata_bytes = json.dumps(metadata).encode()
            binary = {"mediaType": "application/vnd.drasi.plugin.v1+binary", "digest": "sha256:" + "a" * 64}
            metadata_layer = {
                "mediaType": "application/vnd.drasi.plugin.v1+metadata",
                "digest": "sha256:" + hashlib.sha256(metadata_bytes).hexdigest(),
            }
            manifest_bytes = json.dumps({"layers": [binary, metadata_layer]}).encode()
            digest = "sha256:" + hashlib.sha256(manifest_bytes).hexdigest()
            location = "ghcr.io/drasi-project/source/postgres"
            artifact = {
                "id": "postgres", "reference": location + "@" + digest,
                "binary_sha256": "a" * 64, "filename": "libpostgres.dylib",
            }
            config = {"catalogs": [{"plugins": [{"version": "0.1.0", "artifact": artifact}]}]}
            config_path = directory / "config.json"
            config_path.write_text(json.dumps(config))
            base = "https://ghcr.io/v2/drasi-project/source/postgres/"
            responses = {
                base + "manifests/" + digest: manifest_bytes,
                base + "blobs/" + metadata_layer["digest"]: metadata_bytes,
                base + "manifests/0.1.0-darwin-arm64": manifest_bytes,
            }
            for verify_tags in [False, True]:
                with self.subTest(verify_release_tags=verify_tags):
                    requests = []

                    def fetch_bytes(url, token=None):
                        requests.append(url)
                        if url.startswith("https://ghcr.io/token?"):
                            return b'{"token":"fixture-public-pull-token"}'
                        self.assertEqual(token, "fixture-public-pull-token")
                        return responses[url]

                    output = directory / f"report-{verify_tags}.json"
                    arguments = [str(config_path), str(output)]
                    if verify_tags:
                        arguments.append("--verify-release-tags")
                    inspector["main"](arguments, fetch_bytes)
                    report = json.loads(output.read_text())
                    expected = {"reference": artifact["reference"], "binary": binary, "metadata": metadata}
                    if verify_tags:
                        expected["release_tag"] = "0.1.0-darwin-arm64"
                    self.assertEqual(report["artifacts"], [expected])
                    self.assertEqual(base + "manifests/0.1.0-darwin-arm64" in requests, verify_tags)
                    self.assertEqual(report["signatures"], "not required by this proof of concept")
            responses[base + "manifests/0.1.0-darwin-arm64"] = b'{"layers":[]}'
            with self.assertRaisesRegex(ValueError, "release tag no longer matches"):
                inspector["main"](
                    [str(config_path), str(directory / "refused.json"), "--verify-release-tags"], fetch_bytes,
                )
            self.assertFalse((directory / "refused.json").exists())

    def test_local_catalog_inspection_checks_content_and_release_without_network(self):
        inspector = runpy.run_path(str(NATIVE / "tests/inspect-postgres-catalog.py"))
        with tempfile.TemporaryDirectory(prefix="local-catalog-proof-", dir=NATIVE / ".verification") as temporary:
            directory = Path(temporary)
            library = directory / "fixture.dylib"
            library.write_bytes(b"local catalog content pin fixture")
            binary_hash = MODULE["digest"](library)
            metadata = {"name": "fixture", "version": "0.1.0"}
            metadata_path = directory / "metadata.json"
            metadata_path.write_text(json.dumps(metadata))
            artifact = {
                "reference": "local@sha256:" + binary_hash, "binary_sha256": binary_hash,
                "local_catalog_path": str(library),
            }
            config = directory / "catalog.json"
            config.write_text(json.dumps({"catalogs": [{"plugins": [{"version": "0.1.0", "artifact": artifact}]}]}))

            def no_network(url, token=None):
                self.fail(f"local inspection made a network request: {url}")

            output = directory / "report.json"
            inspector["main"]([str(config), str(output)], no_network)
            self.assertEqual(json.loads(output.read_text())["artifacts"], [{
                "reference": artifact["reference"], "metadata": metadata,
                "binary": {"digest": "sha256:" + binary_hash}, "local_catalog_path": str(library),
            }])
            with self.assertRaisesRegex(ValueError, "do not have upstream release tags"):
                inspector["main"]([str(config), str(output), "--verify-release-tags"], no_network)
            metadata_path.write_text(json.dumps({**metadata, "version": "different"}))
            with self.assertRaisesRegex(ValueError, "Release version mismatch"):
                inspector["main"]([str(config), str(output)], no_network)
            library.write_bytes(b"modified bytes")
            with self.assertRaisesRegex(ValueError, "Pinned binary digest mismatch"):
                inspector["main"]([str(config), str(output)], no_network)


if __name__ == "__main__":
    unittest.main()
