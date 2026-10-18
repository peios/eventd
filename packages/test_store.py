import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import package


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.writer = Path(os.environ["PPK_WRITER"])
        self.checker = Path(os.environ["PPK_CHECKER"])

    def store(self, name: str, value: bytes = b"shared content") -> Path:
        payload = self.root / (name + "-payload")
        payload.mkdir()
        (payload / "program").write_bytes(value)
        (payload / "program").chmod(0o755)
        (payload / "link").symlink_to("program")
        (payload / "empty").mkdir()
        output = self.root / name
        identity = subprocess.check_output(
            [
                str(self.writer),
                "commit",
                name,
                "0.0.0-dev",
                "x86_64",
                "--content",
                str(payload),
                "--output",
                str(output),
            ],
            text=True,
        ).strip()
        manifest = f"manifests/{name}/0.0.0-dev/x86_64/manifest.toml"
        package.write_json(
            output / "provenance.json",
            dict(
                name=name,
                version="0.0.0-dev",
                arch="x86_64",
                manifest=manifest,
                manifest_hash=identity,
                release_ready=False,
            ),
        )
        return output

    def test_current_canonical_format_and_deduplicated_store(self) -> None:
        first, second = self.store("first"), self.store("second")
        merged = self.root / "merged"
        package.merge([first, second], merged, self.checker)
        index = json.loads((merged / "index.json").read_text(encoding="utf-8"))
        self.assertEqual(index["package_count"], 2)
        self.assertEqual(index["blob_count"], 1)
        manifest = merged / index["packages"][0]["manifest"]
        expected = (
            'name = "first"\nversion = "0.0.0-dev"\narch = "x86_64"\n\n[files]\n'
            '"empty" = { dir = true }\n"link" = { symlink = "program" }\n'
            '"program" = { blob = "sha256:'
            + hashlib.sha256(b"shared content").hexdigest()
            + '", exec = true }\n'
        )
        self.assertEqual(manifest.read_text(encoding="utf-8"), expected)
        again = self.root / "again"
        package.merge([second, first], again, self.checker)
        left = {
            str(p.relative_to(merged)): package.digest(p) for p in merged.rglob("*") if p.is_file()
        }
        right = {
            str(p.relative_to(again)): package.digest(p) for p in again.rglob("*") if p.is_file()
        }
        self.assertEqual(left, right)

    def test_manifest_tampering_fails(self) -> None:
        store = self.store("one")
        next(store.rglob("manifest.toml")).write_text("tampered", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "manifest hash"):
            package.merge([store], self.root / "merged", self.checker)

    def test_missing_blob_fails(self) -> None:
        store = self.store("one")
        next(store.rglob("*.zst")).unlink()
        with self.assertRaisesRegex(ValueError, "missing or unreferenced"):
            package.merge([store], self.root / "merged", self.checker)

    def test_unreferenced_blob_fails(self) -> None:
        store = self.store("one")
        (store / "blobs/sha256/extra.zst").write_bytes(b"extra")
        with self.assertRaisesRegex(ValueError, "missing or unreferenced"):
            package.merge([store], self.root / "merged", self.checker)

    def test_corrupt_blob_fails(self) -> None:
        store = self.store("one")
        next(store.rglob("*.zst")).write_bytes(b"corrupt")
        with self.assertRaises(subprocess.CalledProcessError):
            package.merge([store], self.root / "merged", self.checker)

    def test_valid_compression_with_wrong_identity_fails(self) -> None:
        store = self.store("one")
        other = self.store("two", b"different content")
        next(store.rglob("*.zst")).write_bytes(next(other.rglob("*.zst")).read_bytes())
        with self.assertRaises(subprocess.CalledProcessError):
            package.merge([store], self.root / "merged", self.checker)

    def test_duplicate_identity_fails(self) -> None:
        store = self.store("one")
        with self.assertRaisesRegex(ValueError, "duplicate package"):
            package.merge([store, store], self.root / "merged", self.checker)

    def test_source_contains_only_declared_files_and_modes(self) -> None:
        source = self.root / "input"
        source.mkdir()
        (source / "declared").write_text("declared", encoding="utf-8")
        (source / "declared").chmod(0o755)
        (source / "undeclared").write_text("must not be packaged", encoding="utf-8")
        manifest = self.root / "inputs.json"
        package.write_json(
            manifest, [{"relative": "scripts/declared", "path": str(source / "declared")}]
        )
        output = self.root / "source"
        result = package.materialise(manifest, output / "usr/src/eventd")
        self.assertEqual(result["source_files"], 1)
        self.assertEqual((output / "usr/src/eventd/scripts/declared").stat().st_mode & 0o777, 0o755)
        self.assertEqual(len([p for p in output.rglob("*") if p.is_file()]), 1)

    def test_cancelled_writer_cleans_its_large_payload_scratch(self) -> None:
        source = self.root / "source"
        source.write_bytes(b"payload")
        manifest = self.root / "inputs.json"
        package.write_json(manifest, [{"relative": "source", "path": str(source)}])
        marker = self.root / "writer-started"
        writer = self.root / "blocking-writer"
        writer.write_text(
            "#!/usr/bin/python3\nfrom pathlib import Path\nimport time\n"
            f'Path({str(marker)!r}).write_text("started")\ntime.sleep(30)\n',
            encoding="utf-8",
        )
        writer.chmod(0o755)
        output = self.root / "cancelled"
        process = subprocess.Popen(
            [
                sys.executable,
                package.__file__,
                "commit",
                "--output",
                str(output),
                "--writer",
                str(writer),
                "--source-manifest",
                str(manifest),
                "--version",
                "0.0.0-dev",
            ],
            start_new_session=True,
        )
        try:
            deadline = time.monotonic() + 5
            while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(marker.exists(), "writer did not start")
            process.send_signal(signal.SIGTERM)
            self.assertEqual(process.wait(timeout=5), 128 + signal.SIGTERM)
            self.assertEqual(list(output.glob(".peios-ppk-*")), [])
            self.assertFalse((output / "provenance.json").exists())
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                # The process group has already exited, so there is nothing to clean up.
                pass
            process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
