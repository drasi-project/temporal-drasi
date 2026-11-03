#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile


MODULE = Path(__file__).resolve().parents[1]


def publish(source, destination):
    try:
        os.link(source, destination)
    except FileExistsError:
        if destination.is_symlink() or not destination.is_file():
            raise RuntimeError(f"refusing non-file artifact: {destination}")
        if source.read_bytes() != destination.read_bytes():
            raise RuntimeError(f"existing artifact does not match its pin: {destination}")


def package(output):
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".watch-contract-", dir=output) as temporary:
        staging = Path(temporary)
        result = subprocess.run(
            ["npm", "pack", "--ignore-scripts", "--json", "--pack-destination", str(staging)],
            cwd=MODULE, check=True, stdout=subprocess.PIPE, text=True,
        )
        metadata, = json.loads(result.stdout)
        filename = metadata["filename"]
        if Path(filename).name != filename:
            raise RuntimeError("npm returned an invalid package filename")
        artifact = staging / filename
        sha256 = hashlib.sha256(artifact.read_bytes()).hexdigest()
        basename = f"watch-contract-dev-{sha256}"
        manifest = {
            "development": True,
            "signed": False,
            "artifact": f"{basename}.tgz",
            "sha256": sha256,
            "npm": metadata,
        }
        manifest_path = staging / f"{basename}.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        publish(artifact, output / manifest["artifact"])
        publish(manifest_path, output / manifest_path.name)
    return manifest


def main():
    parser = argparse.ArgumentParser(description="Package a local, unpublished Watch API development pin.")
    parser.add_argument("--output", type=Path, default=MODULE / ".tools" / "distribution")
    args = parser.parse_args()
    manifest = package(args.output)
    print(json.dumps({
        key: manifest[key] for key in ("development", "signed", "artifact", "sha256")
    }, indent=2))


if __name__ == "__main__":
    main()
