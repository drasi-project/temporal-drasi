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
    ("drasi-core-current-main-prerequisites.patch", "b16241b0d77acb9a2c3922c7ff16ed96d55279e66574ac8593174f1bd2ebce47"),
)
TREE = "029e8390bfc0a6e426ace96012593e743b068f65"
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
