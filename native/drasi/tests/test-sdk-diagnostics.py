#!/usr/bin/env python3
"""Verify Temporal's native diagnostic boundary against the unmodified SDK."""

import runpy
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEST = "plugins::tests::sdk_loader_is_idempotent_resident_and_returns_real_source_and_bootstrap_schemas"


def main():
    verifier = runpy.run_path(str(ROOT / "verify-dependencies.py"))
    verifier["dependencies"]()
    common = ["--manifest-path", str(ROOT / "Cargo.toml"), "--quiet", "--locked"]
    subprocess.run(
        ["cargo", "build", *common, "-p", "temporal-drasi-test-plugin",
         "-p", "temporal-drasi-other-test-plugin"],
        check=True,
    )
    command = ["cargo", "test", *common, "--lib", "--", "--exact", TEST, "--test-threads=1"]
    listing = subprocess.check_output([*command, "--list"], text=True)
    selected = {line.removesuffix(": test") for line in listing.splitlines() if line.endswith(": test")}
    if selected != {TEST}:
        raise RuntimeError(f"Native diagnostics test selection is incomplete: {sorted(selected)}")
    subprocess.run(command, check=True)
    verifier["dependencies"]()


if __name__ == "__main__":
    main()
