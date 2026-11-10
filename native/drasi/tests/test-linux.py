#!/usr/bin/env python3
"""Run the full native verifier in an isolated, resource-limited Linux container."""

import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


NATIVE = Path(__file__).resolve().parents[1]
ROOT = NATIVE.parent.parent


def main():
    scratch = NATIVE / ".verification"
    scratch.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="linux-proof-", dir=scratch) as temporary:
        temporary = Path(temporary)
        context = temporary / "image"
        context.mkdir()
        shutil.copyfile(NATIVE / "tests" / "linux.Dockerfile", context / "Dockerfile")
        image = subprocess.check_output(
            ["docker", "build", "--quiet", str(context)], text=True
        ).strip()
        checkout = temporary / "source"
        subprocess.run(
            ["git", "clone", "--quiet", "--no-local", "--depth=1", str(ROOT), str(checkout)],
            check=True,
        )
        paths = subprocess.check_output(
            ["git", "-C", str(ROOT), "ls-files", "--cached", "--others", "--exclude-standard", "-z"]
        )
        index = subprocess.check_output(["git", "-C", str(ROOT), "ls-files", "--stage", "-z"])
        submodules = {
            record.split(b"\t", 1)[1]
            for record in index.split(b"\0") if record.startswith(b"160000 ")
        }
        for name in paths.split(b"\0"):
            if not name or name in submodules:
                continue
            relative = Path(os.fsdecode(name))
            if relative.is_absolute() or ".." in relative.parts:
                raise RuntimeError(f"Invalid worktree snapshot path: {relative}")
            source = ROOT / relative
            target = checkout / relative
            if target.is_symlink():
                target.unlink()
            if source.is_symlink():
                target.parent.mkdir(parents=True, exist_ok=True)
                target.unlink(missing_ok=True)
                target.symlink_to(os.readlink(source))
            elif source.is_file():
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
            elif not source.exists():
                target.unlink(missing_ok=True)
            else:
                raise RuntimeError(f"Worktree snapshot path is not a file: {relative}")
        if subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"]) != (
            subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"])
        ):
            raise RuntimeError("Linux snapshot changed the server revision")
        cache = "temporal-drasi-linux-" + hashlib.sha256(os.fsencode(ROOT)).hexdigest()[:12]
        container = subprocess.check_output(
            ["docker", "create", "--cpus=2", "--memory=6g",
             "--mount", f"type=bind,source={checkout},target=/input,readonly",
             "--mount", f"type=volume,source={cache},target=/cache",
             "--env", "CARGO_HOME=/cache/cargo",
             "--env", "GOCACHE=/cache/go-build",
             "--env", "GOMODCACHE=/cache/go-mod",
             image, "bash", "-c",
             "cp -R /input/. /workspace && mkdir -p /cache/target && "
             "ln -s /cache/target native/drasi/target && "
             "flock /cache/verification.lock bash native/drasi/verify.sh"],
            text=True,
        ).strip()
        try:
            subprocess.run(["docker", "start", "--attach", container], check=True)
            outcome = subprocess.check_output(
                ["docker", "inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", container],
                text=True,
            ).strip()
            if outcome != "exited 0":
                raise RuntimeError(f"Linux verifier did not complete successfully: {outcome}")
        finally:
            subprocess.run(["docker", "rm", "--force", container], check=True)
    print("Linux native verifier passed; configured PostgreSQL and browser acceptance remain separate")


if __name__ == "__main__":
    main()
