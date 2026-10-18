"""Verify eventd's complete package family, runtime boundaries and security metadata."""

import json
import tomllib
import unittest
from pathlib import Path
from typing import Any, ClassVar

EXPECTED_PACKAGES = {
    "eventd",
    "evctl",
    "eventd-debuginfo",
    "evctl-debuginfo",
    "eventd-debugsource",
    "eventd-source",
}
STATE_ROOT = "var/state/eventd"
STATE_LEAVES = ("events", "logs", "metrics")
SERVICE_DESCRIPTOR = (
    "O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)"
    "(A;OICI;GA;;;S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124)"
)


class FamilyTests(unittest.TestCase):
    """Exercise the assembled manifests and provenance, rather than recipe text alone."""

    root: ClassVar[Path]
    index: ClassVar[dict[str, Any]]
    packages: ClassVar[dict[str, Any]]
    manifests: ClassVar[dict[str, Any]]

    @classmethod
    def setUpClass(cls) -> None:
        """Load the declared Bazel store and its canonical manifest file maps."""
        cls.root = Path("packages.store")
        cls.index = json.loads((cls.root / "index.json").read_text(encoding="utf-8"))
        cls.packages = {
            package["name"].removeprefix("dev.peios."): package for package in cls.index["packages"]
        }
        cls.manifests = {
            name: tomllib.loads((cls.root / info["manifest"]).read_text(encoding="utf-8"))["files"]
            for name, info in cls.packages.items()
        }

    def test_complete_unsigned_family(self) -> None:
        """Every boundary has one package identity and development provenance stays explicit."""
        self.assertEqual(set(self.packages), EXPECTED_PACKAGES)
        self.assertEqual(self.index["package_count"], len(EXPECTED_PACKAGES))
        self.assertEqual(len({package["version"] for package in self.packages.values()}), 1)
        self.assertFalse(self.index["release_ready"])
        self.assertEqual(self.index["trust"], "development")
        for name, info in self.packages.items():
            self.assertFalse(info["release_ready"], name)
            expected_arch = "noarch" if name.endswith(("-source", "-debugsource")) else "x86_64"
            self.assertEqual(info["arch"], expected_arch, name)
            if "build" in info:
                self.assertFalse(info["build"]["signed"], name)
                self.assertFalse(info["build"]["release_ready"], name)
                self.assertIn(
                    info["build"]["userspace_abi"], {"peios-native", "debian-development"}
                )

    def test_runtime_packages_are_separate(self) -> None:
        """The standalone client contains no daemon state or service registration."""
        binaries = {"eventd": "usr/sbin/eventd", "evctl": "usr/bin/evctl"}
        for component, binary in binaries.items():
            for name, files in self.manifests.items():
                self.assertEqual(binary in files, name == component, (name, binary))
            self.assertTrue(self.manifests[component][binary]["exec"])
        for name, files in self.manifests.items():
            for path in files:
                if path.startswith((STATE_ROOT, "usr/share/regim/", "usr/share/regman/")):
                    self.assertEqual(name, "eventd", path)
        client = self.packages["evctl"]["package_metadata"]
        self.assertNotIn("dev.peios.eventd", client.get("dependencies", {}))
        self.assertNotIn("signing", client)
        self.assertNotIn("sd_overrides", client)

    def test_only_runtime_binaries_are_executable(self) -> None:
        """Bazel's read-only artifact modes must not mark manuals or debug sources executable."""
        executables = {"eventd": {"usr/sbin/eventd"}, "evctl": {"usr/bin/evctl"}}
        for name, files in self.manifests.items():
            if name == "eventd-source":
                # The rebuildable source package retains authored executable scripts.
                continue
            actual = {path for path, entry in files.items() if entry.get("exec")}
            self.assertEqual(actual, executables.get(name, set()), name)

    def test_input_transport_symlinks_do_not_enter_payloads(self) -> None:
        """This family contains regular files/directories; runfiles links are not package links."""
        for name, files in self.manifests.items():
            for path, entry in files.items():
                self.assertNotIn("symlink", entry, (name, path, entry))

    def test_daemon_authority_and_empty_state(self) -> None:
        """Preserve the TCB signing request and exact protected service-account directories."""
        metadata = self.packages["eventd"]["package_metadata"]
        self.assertEqual(metadata["signing"], {"usr/sbin/eventd": "tcb.priv"})
        expected = {STATE_ROOT, *(f"{STATE_ROOT}/{leaf}" for leaf in STATE_LEAVES)}
        self.assertEqual(metadata["sd_overrides"], dict.fromkeys(expected, SERVICE_DESCRIPTOR))
        files = self.manifests["eventd"]
        for leaf in STATE_LEAVES:
            directory = f"{STATE_ROOT}/{leaf}"
            self.assertEqual(files[directory], {"dir": True})
            self.assertFalse(any(path.startswith(directory + "/") for path in files), directory)
        self.assertEqual(
            metadata["dependencies"]["dev.peios.evctl"], f"= {self.packages['evctl']['version']}"
        )
        self.assertEqual(metadata["dependencies"]["dev.peios.authd"], ">= 0.0.14")

    def test_documentation_and_inert_seeds(self) -> None:
        """Runtime packages deliver their manuals and the daemon's complete reference files."""
        daemon = self.manifests["eventd"]
        for path in (
            "usr/share/man/man8/eventd.8.gz",
            "usr/share/regim/eventd-config.reg",
            "usr/share/regim/eventd-service.reg",
            "usr/share/regman/eventd.regman",
            "usr/share/evman/eventd.evman",
        ):
            self.assertIn(path, daemon)
        self.assertIn("usr/share/man/man1/evctl.1.gz", self.manifests["evctl"])
        for name in ("eventd", "evctl"):
            self.assertEqual(self.packages[name]["package_metadata"]["side_effects"], ["man-db"])

    def test_debug_payloads_match_runtime_versions(self) -> None:
        """Symbols remain independent packages with exact runtime dependency versions."""
        for component in ("eventd", "evctl"):
            name = component + "-debuginfo"
            files = self.manifests[name]
            debug_files = [path for path in files if path.endswith(".debug")]
            self.assertEqual(len(debug_files), 1, name)
            self.assertRegex(
                debug_files[0], r"^usr/lib/debug/\.build-id/[0-9a-f]{2}/[0-9a-f]+\.debug$"
            )
            self.assertEqual(
                self.packages[name]["package_metadata"]["dependencies"],
                {f"dev.peios.{component}": f"= {self.packages[component]['version']}"},
            )
            notices = f"usr/share/licenses/dev.peios.{name}/third-party/crate-licenses.tsv"
            self.assertIn(notices, files)
        self.assertTrue(
            any(
                path.startswith("usr/src/debug/dev.peios.eventd/")
                for path in self.manifests["eventd-debugsource"]
            )
        )

    def test_source_payload_is_rebuildable_and_clean(self) -> None:
        """Source packages include build machinery while excluding retired/generated trees."""
        files = self.manifests["eventd-source"]
        for relative in (
            "Cargo.lock",
            "Cargo.toml",
            "MODULE.bazel",
            "bazel",
            "packages/stage.py",
            "packages/validate.py",
            "eventd-core/BUILD.bazel",
            "vendor/ppk/src/lib.rs",
            "registry.d/eventd-service.reg",
            "docs/eventd.regman",
        ):
            self.assertIn("usr/src/eventd/" + relative, files)
        forbidden = {".git", ".trail-cache", ".ruff_cache", "__pycache__", "target", "bazel-out"}
        for path in files:
            self.assertFalse(forbidden & set(Path(path).parts), path)
            self.assertFalse("pekit" in path.lower(), path)


if __name__ == "__main__":
    unittest.main()
