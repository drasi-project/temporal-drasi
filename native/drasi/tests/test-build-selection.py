#!/usr/bin/env python3

import os
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[3]


def run_make(recipe: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["make", "--no-print-directory", "-s", "-f", "Makefile", "-f", "-", *arguments, "verify-selection"],
        cwd=ROOT,
        input="verify-selection:\n" + recipe,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


class BuildSelectionTests(unittest.TestCase):
    def test_native_publication_refuses_redirected_builds(self) -> None:
        for variable in ("CARGO_BUILD_TARGET", "CARGO_TARGET_DIR"):
            with self.subTest(variable=variable):
                result = subprocess.run(
                    ["bash", "native/drasi/build.sh"],
                    cwd=ROOT,
                    env=dict(os.environ, **{variable: "unsupported-native-output"}),
                    text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("requires the native default Cargo target directory", result.stderr)

    def test_source_inventory_excludes_ignored_build_artifacts(self) -> None:
        cache = ROOT / "native/drasi/.verification"
        cache.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="build-selection-", dir=cache) as directory:
            paths = [pathlib.Path(directory) / "ignored_test.go", pathlib.Path(directory) / "ignored.sh"]
            for path in paths:
                path.write_text("")
            relative = pathlib.Path(directory).relative_to(ROOT).as_posix()
            result = run_make(
                f"\t@printf '%s\\n' '$(filter ./{relative}/%,$(ALL_SRC) $(ALL_SCRIPTS))'\n"
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.split(), [])

    def test_root_tests_respect_module_boundaries(self) -> None:
        result = run_make("\t@printf '%s\\n' '$(TEST_DIRS)'\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        directories = result.stdout.split()
        for directory in ("./service/watch/", "./service/watch/native/"):
            with self.subTest(root_package=directory):
                self.assertIn(directory, directories)
        manifests = subprocess.check_output(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", "*/go.mod"],
            cwd=ROOT,
            text=True,
        ).splitlines()
        self.assertTrue(manifests)
        for manifest in manifests:
            prefix = "./" + str(pathlib.PurePosixPath(manifest).parent) + "/"
            with self.subTest(module=manifest):
                self.assertEqual([directory for directory in directories if directory.startswith(prefix)], [])

    def test_native_opt_in_requires_tag_and_cgo(self) -> None:
        result = run_make("\t$(call check-build-tag,temporal_with_drasi)\n")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_native_opt_in_rejects_a_stub_only_package(self) -> None:
        result = run_make(
            "\t$(call check-build-tag,temporal_with_drasi)\n",
            "OPT_IN_CGO_PACKAGE_temporal_with_drasi=go.temporal.io/server/client/watch",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must select native files", result.stderr)


if __name__ == "__main__":
    unittest.main()
