#!/usr/bin/env python3

import pathlib
import subprocess
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
