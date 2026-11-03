#!/usr/bin/env python3
"""Fetch only locked public proto inputs into the ignored module-local cache."""

import hashlib
import json
from pathlib import Path
import re
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
REPOSITORIES = {
    "google/api/": ("googleapis/googleapis", "6efda153a78866d02076d7fd9e463089d4ccf3fd"),
    "temporal/api/": ("temporalio/api", "9156239c8272003b6f228951602ca2b130a55876"),
}
lock_path = ROOT / "proto-dependencies.lock.json"
update = sys.argv[1:] == ["--update-lock"]
lock = {} if update else json.loads(lock_path.read_text())
pending = [ROOT / "proto/drasi/watch/v1/service.proto"]
seen = set()
while pending:
    source = pending.pop()
    for name in re.findall(r'^\s*import\s+"([^"]+)";', source.read_text(), re.MULTILINE):
        if name.startswith("google/protobuf/") or name in seen:
            continue
        seen.add(name)
        repository, revision = next(
            value for prefix, value in REPOSITORIES.items() if name.startswith(prefix)
        )
        url = f"https://raw.githubusercontent.com/{repository}/{revision}/{name}"
        destination = ROOT / ".tools/proto" / name
        expected = lock.get(name, {})
        if not update and expected.get("url") != url:
            sys.exit(f"Unpinned proto input: {name}")
        data = destination.read_bytes() if destination.exists() else urllib.request.urlopen(url, timeout=60).read()
        digest = hashlib.sha256(data).hexdigest()
        if not update and expected.get("sha256") != digest:
            sys.exit(f"Proto input digest mismatch: {name}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        lock[name] = {"url": url, "sha256": digest}
        pending.append(destination)
if update:
    lock_path.write_text(json.dumps(lock, indent=2, sort_keys=True) + "\n")
elif seen != set(lock):
    sys.exit("Proto dependency lock contains unused inputs")
