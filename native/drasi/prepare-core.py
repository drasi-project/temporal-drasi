#!/usr/bin/env python3
"""Prepare and verify the pinned, unpublished Drasi development overlay."""

import fcntl
import hashlib
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent
BASE = "9469f3e01ff22c4f0092c9900dfbd5051ceae545"
PATCHES = (
    ("drasi-core-current-main-prerequisites.patch", "42d598f0ea404d5de81b605651968953a6d6d3e2bc3ae45fcdb73031e9cf2813"),
)
TREE = "198a2fbe0e9eb5f819d4ac5cce5622252d6ffe72"
SOURCE = ROOT / ".upstream" / f"drasi-core-{TREE}"


def verify(source):
    head = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
    ).strip()
    tree = subprocess.check_output(
        ["git", "-C", str(source), "write-tree"], text=True
    ).strip()
    if head != BASE or tree != TREE:
        raise RuntimeError("Drasi overlay revision or patched tree does not match the pin")
    subprocess.run(["git", "-C", str(source), "diff", "--quiet"], check=True)
    if subprocess.check_output(["git", "-C", str(source), "ls-files", "--others"]):
        raise RuntimeError("Drasi overlay contains untracked files")


def main():
    patches = []
    for name, digest in PATCHES:
        patch = ROOT / "patches" / name
        if hashlib.sha256(patch.read_bytes()).hexdigest() != digest:
            raise RuntimeError(f"Drasi development patch checksum does not match the pin: {name}")
        patches.append(patch)
    source = SOURCE
    cache = source.parent
    if cache.is_symlink() or source.is_symlink():
        raise RuntimeError("Drasi overlay cache must not be a symlink")
    cache.mkdir(exist_ok=True)
    with (cache / "prepare.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not source.exists():
            with tempfile.TemporaryDirectory(prefix="prepare-", dir=cache) as staging:
                checkout = Path(staging) / "source"
                subprocess.run(["git", "init", "--quiet", str(checkout)], check=True)
                subprocess.run(
                    ["git", "-C", str(checkout), "fetch", "--quiet", "--depth=1",
                     "https://github.com/drasi-project/drasi-core", BASE],
                    check=True,
                )
                subprocess.run(
                    ["git", "-c", "core.autocrlf=false", "-C", str(checkout),
                     "checkout", "--quiet", "--detach", BASE],
                    check=True,
                )
                for patch in patches:
                    subprocess.run(
                        ["git", "-C", str(checkout), "apply", "--index", str(patch)],
                        check=True,
                    )
                verify(checkout)
                checkout.rename(source)
        verify(source)
    print(f"Drasi development overlay verified: base {BASE}, tree {TREE}")


if __name__ == "__main__":
    main()
