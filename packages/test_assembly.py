import importlib.util
import tempfile
import unittest
from collections.abc import Mapping, Sequence
from pathlib import Path

spec = importlib.util.spec_from_file_location("assembly", Path(__file__).with_name("assembly.py"))
assert spec is not None and spec.loader is not None
assembly = importlib.util.module_from_spec(spec)
spec.loader.exec_module(assembly)


class AssemblyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.output = self.root / "output"

    def write(self, name: str, content: str = "content") -> Path:
        path = self.source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def assemble(self, mappings: Mapping[str, str], excludes: Sequence[str] = ()) -> int:
        return assembly.assemble(
            {"files": mappings, "excludes": excludes},
            {"tools": self.source},
            self.output,
        )

    def test_recursive_and_wildcard_parent_mappings(self) -> None:
        self.write("lib/python3.13/site-packages/perf.so")
        self.write("lib/python3.13/site-packages/ignored.a")
        self.assemble({"tools:lib/python3*/site-packages/**": "usr/lib"}, ["tools:**/*.a"])
        self.assertTrue((self.output / "usr/lib/python3.13/site-packages/perf.so").is_file())
        self.assertFalse((self.output / "usr/lib/python3.13/site-packages/ignored.a").exists())

    def test_symlinks_and_executable_modes(self) -> None:
        path = self.write("bin/program")
        path.chmod(0o755)
        (path.parent / "alias").symlink_to("program")
        self.assemble({"tools:bin/**": "usr/bin"})
        self.assertEqual((self.output / "usr/bin/alias").readlink(), Path("program"))
        self.assertEqual((self.output / "usr/bin/program").stat().st_mode & 0o777, 0o755)

    def test_symlink_directories_are_not_followed(self) -> None:
        outside = self.root / "outside"
        outside.mkdir()
        (outside / "secret").write_text("outside", encoding="utf-8")
        (self.source / "link").symlink_to(outside)
        self.assemble({"tools:**": "usr/share"})
        self.assertTrue((self.output / "usr/share/link").is_symlink())
        self.assertEqual(
            list((self.output / "usr/share").iterdir()), [self.output / "usr/share/link"]
        )

    def test_conflicting_files_fail(self) -> None:
        self.write("one", "one")
        self.write("two", "two")
        with self.assertRaisesRegex(ValueError, "conflicting"):
            self.assemble({"tools:one": "bin/x", "tools:two": "bin/x"})

    def test_missing_input_and_path_traversal_fail(self) -> None:
        with self.assertRaisesRegex(ValueError, "matched nothing"):
            self.assemble({"tools:missing*": "bin"})
        self.write("one")
        with self.assertRaisesRegex(ValueError, "unsafe"):
            self.assemble({"tools:one": "../escape"})
        with self.assertRaisesRegex(ValueError, "unsafe"):
            self.assemble({"tools:../outside": "bin"})

    def test_destination_symlink_cannot_redirect_later_mapping(self) -> None:
        (self.source / "link").symlink_to("../outside")
        self.write("one")
        with self.assertRaisesRegex(ValueError, "traverses a symlink"):
            self.assemble({"tools:link": "bin", "tools:one": "bin/one"})
        self.assertFalse((self.root / "outside/one").exists())


if __name__ == "__main__":
    unittest.main()
