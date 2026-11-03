#!/usr/bin/env python3
"""Reproduce the public release pins and advertised certificate provenance."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import runpy
import subprocess
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    preparer = runpy.run_path(str(Path(__file__).resolve().parents[1] / "prepare-core.py"))
    report = {
        "cryptographic_verification": "Performed separately by the native drasi-host-sdk verifier, not by this metadata inspector",
        "host_revision": preparer["BASE"],
        "host_tree": preparer["TREE"],
        "workflow": f"https://github.com/drasi-project/drasi-core/blob/{preparer['BASE']}/.github/workflows/publish-plugins.yml",
        "artifacts": [],
    }
    for catalog in config["catalogs"]:
        for release in catalog["plugins"]:
            artifact = release["artifact"]
            location, digest = artifact["reference"].split("@")
            registry, repository = location.split("/", 1)
            if registry != "ghcr.io" or not repository.startswith("drasi-project/"):
                raise ValueError("This public verification inspector only accepts upstream GHCR artifacts")
            token = json.loads(fetch("https://ghcr.io/token?" + urllib.parse.urlencode({
                "service": "ghcr.io", "scope": f"repository:{repository}:pull",
            })))["token"]
            base = f"https://ghcr.io/v2/{repository}/"
            raw = fetch(base + "manifests/" + digest, token)
            if "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
                raise ValueError("Pinned manifest digest mismatch")
            manifest = json.loads(raw)
            binary = next(layer for layer in manifest["layers"] if layer["mediaType"].endswith("+binary"))
            if binary["digest"] != "sha256:" + artifact["binary_sha256"]:
                raise ValueError("Pinned binary layer digest mismatch")
            metadata_layer = next(layer for layer in manifest["layers"] if layer["mediaType"].endswith("+metadata"))
            metadata_bytes = fetch(base + "blobs/" + metadata_layer["digest"], token)
            if "sha256:" + hashlib.sha256(metadata_bytes).hexdigest() != metadata_layer["digest"]:
                raise ValueError("Metadata layer digest mismatch")
            metadata = json.loads(metadata_bytes)
            if metadata["version"] != release["version"]:
                raise ValueError("Release version mismatch")
            platform = {
                "aarch64-apple-darwin": "darwin-arm64",
                "x86_64-apple-darwin": "darwin-amd64",
                "aarch64-unknown-linux-gnu": "linux-arm64",
                "x86_64-unknown-linux-gnu": "linux-amd64",
            }[metadata["target_triple"]]
            platform_tag = release["version"] + "-" + platform
            tagged = fetch(base + "manifests/" + platform_tag, token)
            if "sha256:" + hashlib.sha256(tagged).hexdigest() != digest:
                raise ValueError("Public release tag no longer matches the reviewed immutable pin")
            signature_tag = "sha256-" + digest.removeprefix("sha256:") + ".sig"
            signature = json.loads(fetch(base + "manifests/" + signature_tag, token))
            certificates = []
            for layer in signature["layers"]:
                certificate = layer.get("annotations", {}).get("dev.sigstore.cosign/certificate")
                if certificate:
                    text = subprocess.run(
                        ["openssl", "x509", "-text", "-noout"],
                        input=certificate, text=True, capture_output=True, check=True,
                    ).stdout
                    for identity in config["trust"]["identities"]:
                        if identity["subject"] in re.findall(r"URI:([^,\s]+)", text) and identity["issuer"] in text:
                            certificates.append(identity)
                            break
            if not certificates:
                raise ValueError("No advertised signature certificate matches the exact configured identity")
            item = {
                "reference": artifact["reference"],
                "release": f"https://github.com/drasi-project/drasi-core/releases/tag/{metadata['name']}-v{release['version']}",
                "release_tag": platform_tag,
                "binary": binary,
                "metadata": metadata,
                "advertised_signers": certificates,
            }
            report["artifacts"].append(item)
            print(json.dumps(item, sort_keys=True))
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
