#!/usr/bin/env python3
"""Inspect immutable local or public artifact pins without signature requirements."""

import argparse
import hashlib
import json
from pathlib import Path
import runpy
import urllib.parse
import urllib.request


def fetch(url, token=None):
    headers = {
        "Accept": "application/vnd.oci.image.manifest.v1+json",
        "User-Agent": "temporal-drasi-postgres-verification",
    }
    if token:
        headers["Authorization"] = "Bearer " + token
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30) as response:
        return response.read()


def main(arguments=None, fetch_bytes=fetch):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--verify-release-tags", action="store_true",
        help="also verify the upstream version-platform release-tag convention",
    )
    args = parser.parse_args(arguments)
    config = json.loads(args.config.read_text())
    verifier = runpy.run_path(str(Path(__file__).resolve().parents[1] / "verify-dependencies.py"))
    report = {
        "signatures": "not required by this proof of concept",
        "host_sdk_source": "crates.io",
        "host_dependencies": verifier["provenance"](verifier["dependencies"]()),
        "artifacts": [],
    }
    for catalog in config["catalogs"]:
        for release in catalog["plugins"]:
            artifact = release["artifact"]
            location, digest = artifact["reference"].split("@")
            if location == "local":
                if args.verify_release_tags:
                    raise ValueError("local artifacts do not have upstream release tags")
                library = Path(artifact["local_catalog_path"])
                if not library.is_absolute() or library.is_symlink() or not library.is_file() or library.stat().st_size > 256 << 20:
                    raise ValueError("local plugin must be an absolute bounded regular file")
                with library.open("rb") as source:
                    actual = hashlib.file_digest(source, "sha256").hexdigest()
                if digest != "sha256:" + actual or artifact["binary_sha256"] != actual:
                    raise ValueError("Pinned binary digest mismatch")
                path = library.parent / "metadata.json"
                if path.is_symlink() or not path.is_file() or path.stat().st_size > 1 << 20:
                    raise ValueError("local metadata must be a bounded regular file")
                metadata = json.loads(path.read_text())
                if metadata["version"] != release["version"]:
                    raise ValueError("Release version mismatch")
                report["artifacts"].append({
                    "reference": artifact["reference"], "metadata": metadata,
                    "binary": {"digest": digest}, "local_catalog_path": str(library),
                })
                continue
            registry, repository = location.split("/", 1)
            if registry != "ghcr.io":
                raise ValueError("public inspection requires a GHCR repository")
            token = json.loads(fetch_bytes("https://ghcr.io/token?" + urllib.parse.urlencode({
                "service": "ghcr.io", "scope": f"repository:{repository}:pull",
            })))["token"]
            base = f"https://ghcr.io/v2/{repository}/"
            raw = fetch_bytes(base + "manifests/" + digest, token)
            if "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
                raise ValueError("Pinned manifest digest mismatch")
            manifest = json.loads(raw)
            binary = next(layer for layer in manifest["layers"] if layer["mediaType"].endswith("+binary"))
            if binary["digest"] != "sha256:" + artifact["binary_sha256"]:
                raise ValueError("Pinned binary layer digest mismatch")
            metadata_layer = next(layer for layer in manifest["layers"] if layer["mediaType"].endswith("+metadata"))
            metadata_bytes = fetch_bytes(base + "blobs/" + metadata_layer["digest"], token)
            if "sha256:" + hashlib.sha256(metadata_bytes).hexdigest() != metadata_layer["digest"]:
                raise ValueError("Metadata layer digest mismatch")
            metadata = json.loads(metadata_bytes)
            if metadata["version"] != release["version"]:
                raise ValueError("Release version mismatch")
            platform_tag = None
            if args.verify_release_tags:
                platform = {
                    "aarch64-apple-darwin": "darwin-arm64",
                    "x86_64-apple-darwin": "darwin-amd64",
                    "aarch64-unknown-linux-gnu": "linux-arm64",
                    "x86_64-unknown-linux-gnu": "linux-amd64",
                }[metadata["target_triple"]]
                platform_tag = release["version"] + "-" + platform
                tagged = fetch_bytes(base + "manifests/" + platform_tag, token)
                if "sha256:" + hashlib.sha256(tagged).hexdigest() != digest:
                    raise ValueError("Public release tag no longer matches the reviewed immutable pin")
            item = {
                "reference": artifact["reference"],
                "binary": binary,
                "metadata": metadata,
            }
            if platform_tag is not None:
                item["release_tag"] = platform_tag
            report["artifacts"].append(item)
            print(json.dumps(item, sort_keys=True))
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
