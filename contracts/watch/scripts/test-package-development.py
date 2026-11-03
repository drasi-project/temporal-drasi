#!/usr/bin/env python3

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest


MODULE = Path(__file__).resolve().parents[1]
PACKAGER = MODULE / "scripts" / "package-development.py"


def build(output):
    result = subprocess.run(
        [sys.executable, str(PACKAGER), "--output", str(output)],
        check=True, stdout=subprocess.PIPE, text=True,
    )
    return json.loads(result.stdout)


class DevelopmentPackageTests(unittest.TestCase):
    def test_reproducible_archive_is_an_independent_go_and_typescript_consumer(self):
        with tempfile.TemporaryDirectory(prefix="watch-contract-consumer-") as directory:
            root = Path(directory)
            output = root / "artifacts"
            first = build(output)
            self.assertEqual(first, build(output))
            self.assertEqual(False, first["signed"])
            self.assertEqual(True, first["development"])
            archive = output / first["artifact"]
            self.assertEqual(first["sha256"], hashlib.sha256(archive.read_bytes()).hexdigest())
            manifest = json.loads(archive.with_suffix(".json").read_text())
            self.assertEqual(first["sha256"], manifest["sha256"])
            expected = {"package/" + entry["path"] for entry in manifest["npm"]["files"]}
            with tarfile.open(archive, "r:gz") as packed:
                members = packed.getmembers()
                self.assertEqual(expected, {member.name for member in members})
                for member in members:
                    self.assertTrue(member.isfile())
                    self.assertTrue(member.name.startswith("package/"))
                    self.assertNotIn("..", Path(member.name).parts)
                    self.assertNotIn(".tools", Path(member.name).parts)
                    self.assertNotIn("node_modules", Path(member.name).parts)
                    destination = root / member.name
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    with packed.extractfile(member) as source:
                        destination.write_bytes(source.read())
                    destination.chmod(member.mode & 0o777)
            consumer = root / "package"
            build_directory = root / "go-build"
            build_directory.mkdir()
            env = dict(os.environ, GOWORK="off", CGO_ENABLED="0", GOTMPDIR=str(build_directory))
            subprocess.run(
                ["go", "test", "-mod=readonly", "-count=1", "-tags", "test_dep", "./..."],
                cwd=consumer, env=env, check=True,
            )
            compiler = MODULE / "node_modules" / ".bin" / "tsc"
            subprocess.run(
                [str(compiler), "--outDir", ".tools/ts-test", "--noEmit", "false",
                 "--module", "CommonJS", "--moduleResolution", "Node"],
                cwd=consumer, check=True,
            )
            subprocess.run(["node", "scripts/test-json.cjs"], cwd=consumer, check=True)
            env["PATH"] = str(compiler.parent) + os.pathsep + env["PATH"]
            rebuilt = json.loads(subprocess.check_output(
                [sys.executable, "scripts/package-development.py", "--output", str(root / "rebuilt")],
                cwd=consumer, env=env, text=True,
            ))
            self.assertEqual(first, rebuilt)

    def test_existing_tampered_files_and_symlinks_are_not_overwritten(self):
        for target in ("archive", "manifest", "symlink"):
            with self.subTest(target=target), tempfile.TemporaryDirectory(prefix="watch-contract-tamper-") as directory:
                output = Path(directory)
                pin = build(output)
                archive = output / pin["artifact"]
                protected = output / "protected"
                protected.write_bytes(b"untouched")
                if target == "manifest":
                    archive.with_suffix(".json").write_bytes(b"tampered")
                elif target == "archive":
                    archive.write_bytes(b"tampered")
                else:
                    archive.unlink()
                    archive.symlink_to(protected)
                result = subprocess.run(
                    [sys.executable, str(PACKAGER), "--output", str(output)],
                    capture_output=True, text=True,
                )
                self.assertNotEqual(0, result.returncode)
                self.assertIn("artifact", result.stderr)
                self.assertEqual(b"untouched", protected.read_bytes())
                if target == "manifest":
                    self.assertEqual(b"tampered", archive.with_suffix(".json").read_bytes())
                elif target == "archive":
                    self.assertEqual(b"tampered", archive.read_bytes())
                else:
                    self.assertTrue(archive.is_symlink())


if __name__ == "__main__":
    unittest.main()
