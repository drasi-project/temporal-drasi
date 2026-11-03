#!/usr/bin/env python3
"""Verify development source pins against real isolated Git checkouts."""

import re
import runpy
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PREPARER = runpy.run_path(str(ROOT / "prepare-core.py"))
VERIFY = PREPARER["verify"]


class OverlayTests(unittest.TestCase):
    def test_cargo_paths_match_the_versioned_cache(self):
        manifest = (ROOT / "Cargo.toml").read_text()
        source = PREPARER["SOURCE"]
        self.assertIn(PREPARER["TREE"], source.name)
        self.assertIn(str(source.relative_to(ROOT)), tomllib.loads(manifest)["workspace"]["exclude"])
        overrides = manifest.split('[patch."https://github.com/drasi-project/drasi-core"]', 1)[1].split("\n[", 1)[0]
        paths = re.findall(r'path\s*=\s*"([^"]+)"', overrides)
        self.assertTrue(paths)
        for value in paths:
            path = ROOT / value
            self.assertTrue(path.is_relative_to(source))
            self.assertTrue((path / "Cargo.toml").is_file())
        self.assertIn(
            f'{PREPARER["BASE"]}+development.{PREPARER["TREE"]}',
            (ROOT / "src" / "protocol.rs").read_text(),
        )

    def test_patched_checkout_and_cache_damage(self):
        with tempfile.TemporaryDirectory(prefix="overlay-test-", dir=ROOT / ".verification") as temp:
            source = Path(temp) / "source"
            subprocess.run(
                ["git", "-c", "advice.detachedHead=false", "clone", "--quiet", "--shared",
                 str(PREPARER["SOURCE"]), str(source)],
                check=True,
            )
            for name, _ in PREPARER["PATCHES"]:
                subprocess.run(
                    ["git", "-C", str(source), "apply", "--index",
                     str(ROOT / "patches" / name)],
                    check=True,
                )
            VERIFY(source)
            VERIFY(source)
            extra = source / "untracked-source"
            extra.write_text("untrusted\n")
            with self.assertRaisesRegex(RuntimeError, "untracked files"):
                VERIFY(source)
            extra.unlink()

            changed = source / "core" / "src" / "lib.rs"
            changed.write_bytes(changed.read_bytes() + b"\n// Changed cache.\n")
            with self.assertRaises(subprocess.CalledProcessError):
                VERIFY(source)
            subprocess.run(
                ["git", "-C", str(source), "add", "core/src/lib.rs"], check=True
            )
            with self.assertRaisesRegex(RuntimeError, "patched tree"):
                VERIFY(source)

    def test_patch_checksum_is_checked_before_network_or_cache_creation(self):
        for damaged, _ in PREPARER["PATCHES"]:
            with self.subTest(patch=damaged), tempfile.TemporaryDirectory(
                prefix="overlay-test-", dir=ROOT / ".verification"
            ) as temp:
                directory = Path(temp)
                shutil.copyfile(ROOT / "prepare-core.py", directory / "prepare-core.py")
                (directory / "patches").mkdir()
                for name, _ in PREPARER["PATCHES"]:
                    target = directory / "patches" / name
                    if name == damaged:
                        target.write_text("changed")
                    else:
                        shutil.copyfile(ROOT / "patches" / name, target)
                result = subprocess.run(
                    [sys.executable, str(directory / "prepare-core.py")],
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("patch checksum", result.stderr)
                self.assertFalse((directory / ".upstream").exists())


if __name__ == "__main__":
    unittest.main()
