#!/usr/bin/env python3
"""Run SDK-owned diagnostics checks without modifying verified source caches."""

import runpy
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main():
    preparer = runpy.run_path(str(ROOT / "prepare-core.py"))
    preparer["main"]()
    with tempfile.TemporaryDirectory(
        prefix="sdk-diagnostics-", dir=ROOT / ".verification"
    ) as temporary:
        source = Path(temporary) / "source"
        subprocess.run(
            ["git", "-c", "advice.detachedHead=false", "clone", "--quiet",
             "--shared", str(preparer["SOURCE"]), str(source)],
            check=True,
        )
        for name, _ in preparer["PATCHES"]:
            subprocess.run(
                ["git", "-C", str(source), "apply", "--index",
                 str(ROOT / "patches" / name)],
                check=True,
            )
        preparer["verify"](source)
        common = [
            "--manifest-path", str(source / "Cargo.toml"),
            "--quiet", "--target-dir", str(ROOT / "target"),
            "-p", "drasi-host-sdk",
        ]
        subprocess.run(
            ["cargo", "test", *common, "--lib", "redacted_callback_tests"],
            check=True,
        )
        subprocess.run(
            ["cargo", "clippy", *common, "--all-targets", "--", "-D", "warnings"],
            check=True,
        )
    preparer["verify"](preparer["SOURCE"])


if __name__ == "__main__":
    main()
