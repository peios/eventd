#!/usr/bin/env python3
"""Exercise portable repository snapshots without a live Bazel cache."""

import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "vendor", Path(__file__).absolute().with_name("bazel-vendor.py")
)
assert spec is not None and spec.loader is not None
vendor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vendor)


class VendorTests(unittest.TestCase):
    def test_git_subdirectory_survives_removing_the_acquisition_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "snapshot"
            source = root / "repo/.tmp_git_root/crate"
            source.mkdir(parents=True)
            (source / "Cargo.toml").write_text("[package]\n", encoding="utf-8")
            (root / "bazel-external").symlink_to("/absent/acquisition/cache")
            manifest = root / "repo/Cargo.toml"
            manifest.symlink_to("../bazel-external/repo/.tmp_git_root/crate/Cargo.toml")
            vendor.freeze(root)
            self.assertEqual(manifest.read_text(encoding="utf-8"), "[package]\n")
            self.assertFalse((root / "bazel-external").is_symlink())
            view = Path(temporary) / "view"
            vendor.view(root, view)
            vendor.view(root, view)
            (view / "bazel-external").symlink_to("/new/build/cache")
            self.assertEqual((view / "repo/Cargo.toml").read_text(encoding="utf-8"), "[package]\n")
            self.assertFalse((root / "bazel-external").exists())

    def test_missing_target_is_an_acquisition_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "repo").mkdir()
            (root / "repo/source").symlink_to("../bazel-external/repo/missing")
            with self.assertRaises(ValueError):
                vendor.freeze(root)

    def test_snapshot_escape_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "snapshot"
            (root / "repo").mkdir(parents=True)
            (root / "repo/source").symlink_to("../bazel-external/../../outside")
            with self.assertRaises(ValueError):
                vendor.freeze(root)

    def test_view_does_not_replace_existing_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "snapshot"
            root.mkdir()
            (root / "repo").mkdir()
            view = Path(temporary) / "view"
            view.mkdir()
            existing = view / "repo"
            existing.write_text("keep", encoding="utf-8")
            with self.assertRaises(ValueError):
                vendor.view(root, view)
            self.assertEqual(existing.read_text(encoding="utf-8"), "keep")


if __name__ == "__main__":
    unittest.main()
