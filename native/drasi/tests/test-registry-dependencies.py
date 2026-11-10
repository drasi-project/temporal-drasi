#!/usr/bin/env python3
"""Verify exact registry resolution and reject modified dependency source caches."""

import hashlib
import io
from pathlib import Path
import runpy
import tarfile
import tempfile
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[1]
MODULE = runpy.run_path(str(ROOT / "verify-dependencies.py"))


class RegistryDependencyTests(unittest.TestCase):
    def test_all_workspace_drasi_dependencies_use_exact_registry_versions(self):
        for manifest in [
            ROOT / "Cargo.toml",
            ROOT / "plugins/postgres/Cargo.toml",
            ROOT / "tests/plugin-fixture/Cargo.toml",
            ROOT / "tests/other-plugin-fixture/Cargo.toml",
        ]:
            with self.subTest(manifest=manifest):
                data = tomllib.loads(manifest.read_text())
                self.assertFalse(data.get("patch"))
                self.assertFalse(data.get("replace"))
                for name, dependency in data["dependencies"].items():
                    if not name.startswith("drasi-"):
                        continue
                    if isinstance(dependency, dict):
                        self.assertNotIn("git", dependency)
                        self.assertNotIn("path", dependency)
                        dependency = dependency["version"]
                    self.assertTrue(dependency.startswith("="), name)

    def test_actual_dependency_versions_and_archives(self):
        packages = MODULE["dependencies"]()
        self.assertEqual(
            {name: package["version"] for name, package in packages.items()},
            {
                "drasi-bootstrap-postgres": "0.2.15",
                "drasi-core": "0.5.10",
                "drasi-ffi-primitives": "0.11.3",
                "drasi-functions-cypher": "0.5.10",
                "drasi-functions-gql": "0.5.10",
                "drasi-host-sdk": "0.11.3",
                "drasi-index-rocksdb": "0.6.4",
                "drasi-lib": "0.9.3",
                "drasi-plugin-sdk": "0.11.3",
                "drasi-postgres-common": "0.1.2",
                "drasi-query-ast": "0.3.5",
                "drasi-query-cypher": "0.3.6",
                "drasi-query-gql": "0.3.6",
                "drasi-source-postgres": "0.2.12",
                "drasi-state-store-redb": "0.2.8",
                "drasi-wal-redb": "0.2.10",
            },
        )
        provenance = MODULE["provenance"](packages)
        for package in provenance.values():
            self.assertEqual(package["source"], MODULE["REGISTRY"])
            self.assertEqual(package["license"], "Apache-2.0")
            self.assertRegex(package["checksum"], "^[0-9a-f]{64}$")
            self.assertNotIn("path", package)
        protocol = (ROOT / "src/protocol.rs").read_text()
        self.assertIn('env!("TDRASI_UPSTREAM")', protocol)
        self.assertIn('"+temporal-native-"', protocol)
        self.assertIn('env!("CARGO_PKG_VERSION")', protocol)

    def test_modified_archives_sources_and_extra_files_are_rejected(self):
        with tempfile.TemporaryDirectory(prefix="registry-integrity-") as directory:
            registry = Path(directory) / "registry"
            source = registry / "src" / "fixture-index" / "drasi-fixture-1.0.0"
            archive = registry / "cache" / "fixture-index" / (source.name + ".crate")
            source.mkdir(parents=True)
            archive.parent.mkdir(parents=True)
            original = b"pub fn fixture() {}\n"
            (source / "lib.rs").write_bytes(original)
            (source / ".cargo-ok").write_text('{"v":1}')
            with tarfile.open(archive, "w:gz") as package:
                member = tarfile.TarInfo(source.name + "/lib.rs")
                member.size = len(original)
                package.addfile(member, io.BytesIO(original))
            checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
            MODULE["verify"](source, checksum)
            MODULE["verify"](source, checksum)
            with self.assertRaisesRegex(ValueError, "checksum differs"):
                MODULE["verify"](source, "a" * 64)
            (source / "lib.rs").write_bytes(original + b"// Modified.\n")
            with self.assertRaisesRegex(ValueError, "source was modified"):
                MODULE["verify"](source, checksum)
            (source / "lib.rs").write_bytes(original)
            extra = source / "extra.rs"
            extra.write_text("untracked source")
            with self.assertRaisesRegex(ValueError, "additional or missing"):
                MODULE["verify"](source, checksum)
            extra.unlink()
            (source / "lib.rs").unlink()
            (source / "lib.rs").symlink_to(archive)
            with self.assertRaisesRegex(ValueError, "invalid source path"):
                MODULE["verify"](source, checksum)

    def test_sdk_test_manifest_does_not_modify_published_sources(self):
        driver = runpy.run_path(str(ROOT / "tests/test-sdk-recovery.py"))
        with tempfile.TemporaryDirectory(prefix="registry-test-manifest-") as directory:
            root = Path(directory)
            source = root / "published"
            source.mkdir()
            manifest = (
                '[package]\nname = "drasi-lib"\nversion = "0.9.3"\n'
                '[lib]\nname = "drasi_lib"\npath = "src/lib.rs"\n'
            )
            (source / "Cargo.toml").write_text(manifest)
            (source / "src").mkdir()
            (source / "src/lib.rs").write_text("pub fn published() {}\n")
            workspace = root / "verification"
            workspace.mkdir()
            generated = Path(driver["published_source"]({
                "drasi-lib": {"path": source},
                "drasi-index-rocksdb": {"version": "0.6.4"},
            }, "drasi-lib", workspace))
            self.assertEqual((source / "Cargo.toml").read_text(), manifest)
            self.assertEqual((workspace / "source/Cargo.toml").read_text(), manifest)
            self.assertEqual(
                (workspace / "source/src/lib.rs").read_bytes(),
                (source / "src/lib.rs").read_bytes(),
            )
            harness = tomllib.loads(generated.read_text())
            self.assertFalse(harness["package"]["publish"])
            self.assertEqual(harness["lib"]["path"], "source/src/lib.rs")
            self.assertEqual(harness["dev-dependencies"], {
                "drasi-index-rocksdb": {"version": "=0.6.4"},
            })

    def test_sdk_verification_locks_preserve_the_production_release_family(self):
        packages = MODULE["dependencies"]()
        for name in ("drasi-lib", "drasi-source-postgres"):
            with self.subTest(name=name):
                lock = tomllib.loads(
                    (ROOT / "tests/fixtures" / f"{name}.registry-tests.Cargo.lock").read_text()
                )
                local = [package for package in lock["package"] if "source" not in package]
                self.assertEqual(len(local), 1)
                self.assertEqual((local[0]["name"], local[0]["version"]), (name, packages[name]["version"]))
                for package in lock["package"]:
                    if "source" not in package:
                        continue
                    self.assertEqual(package["source"], MODULE["REGISTRY"])
                    self.assertRegex(package["checksum"], "^[0-9a-f]{64}$")
                    if package["name"] in packages:
                        selected = packages[package["name"]]
                        self.assertEqual(
                            (package["version"], package["checksum"]),
                            (selected["version"], selected["checksum"]),
                        )


if __name__ == "__main__":
    unittest.main()
