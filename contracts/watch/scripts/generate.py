#!/usr/bin/env python3
"""Regenerate artifacts, or compare them byte-for-byte without leaving edits."""

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
check = sys.argv[1:] == ["--check"]
if sys.argv[1:] not in ([], ["--check"]):
    sys.exit("Usage: scripts/generate.sh [--check]")


def files():
    return sorted(
        list((ROOT / "v1").glob("*.pb.go"))
        + list((ROOT / "v1").glob("*.pb.gw.go"))
        + list((ROOT / "openapi").rglob("*.json"))
        + list((ROOT / "typescript").rglob("*.ts"))
        + list((ROOT / "validation").glob("adapters.gen.go"))
    )


before = {path: path.read_bytes() for path in files()} if check else {}
try:
    subprocess.run(["node_modules/.bin/buf", "lint", "--path", "proto/drasi/watch/v1/service.proto"], cwd=ROOT, check=True)
    subprocess.run(["node_modules/.bin/buf", "generate"], cwd=ROOT, check=True)
    subprocess.run(["go", "run", "./internal/generate"], cwd=ROOT, check=True)
    if check:
        after = {path: path.read_bytes() for path in files()}
        changed = sorted(str(path.relative_to(ROOT)) for path in before.keys() | after.keys()
                         if before.get(path) != after.get(path))
        if changed:
            sys.exit("Generated artifacts differ:\n" + "\n".join(changed))
        print("Generated artifacts are current.")
finally:
    if check:
        for path in set(files()) - before.keys():
            path.unlink()
        for path, content in before.items():
            path.write_bytes(content)
