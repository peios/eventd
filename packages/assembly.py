"""Strict file mapping for the declared eventd package payloads."""

import fnmatch
import glob
import os
import shutil
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import NotRequired, TypedDict


class AssemblyDefinition(TypedDict):
    """Recipe fields used when mapping files into a package."""

    files: Mapping[str, str]
    excludes: NotRequired[Sequence[str]]


def relative(value: str | Path) -> Path:
    """Return a nonempty relative path without parent-directory components."""
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ValueError(f"unsafe package path: {value!r}")
    return path


def leaves(path: Path) -> Iterator[Path]:
    """Yield files, symlinks, and empty directories without following links."""
    if path.is_symlink() or not path.is_dir():
        yield path
        return
    entries = sorted(path.iterdir())
    if not entries:
        yield path
    for child in entries:
        yield from leaves(child)


def same(left: Path, right: Path) -> bool:
    """Compare entries by the payload properties preserved in a package."""
    if left.is_symlink() or right.is_symlink():
        return left.is_symlink() and right.is_symlink() and os.readlink(left) == os.readlink(right)
    if left.is_dir() or right.is_dir():
        return left.is_dir() and right.is_dir()
    return (
        bool(left.stat().st_mode & 0o111) == bool(right.stat().st_mode & 0o111)
        and left.read_bytes() == right.read_bytes()
    )


def install(source: Path, destination: Path, output: Path) -> None:
    """Install one payload entry, rejecting conflicts and symlink traversal."""
    # Never let a previously installed symlink turn a later mapping into an
    # out-of-tree write, even when its target would currently be nonexistent.
    for parent in destination.parents:
        if parent == output:
            break
        if parent.is_symlink():
            raise ValueError(f"package destination traverses a symlink: {destination}")
    if destination.exists() or destination.is_symlink():
        if not same(source, destination):
            raise ValueError(f"conflicting package mappings: {destination}")
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.is_symlink():
        destination.symlink_to(os.readlink(source))
    elif source.is_dir():
        destination.mkdir()
    elif source.is_file():
        if source.stat().st_mode & 0o6000:
            raise ValueError(f"unsupported setuid/setgid file: {source}")
        shutil.copyfile(source, destination)
        destination.chmod(0o755 if source.stat().st_mode & 0o111 else 0o644)
    else:
        raise ValueError(f"unsupported package entry: {source}")


def _mapping_leaves(matches: list[Path], root: Path) -> set[Path]:
    """Collect payload leaves without following symlinks in matched ancestors."""
    candidates: set[Path] = set()
    for match in matches:
        if any(
            parent.is_symlink()
            for parent in match.parents
            if parent != root and root in parent.parents
        ):
            continue
        candidates.update(leaves(match))
    return candidates


def assemble(definition: AssemblyDefinition, roots: Mapping[str, Path], output: Path) -> int:
    """Map declared inputs into output and return the nonzero number of entries."""
    output.mkdir(parents=True, exist_ok=True)
    count = 0
    excluded = definition.get("excludes", [])
    for mapping, target in definition["files"].items():
        stage, pattern = mapping.split(":", 1)
        if stage not in roots:
            raise ValueError(f"unknown package input stage: {stage}")
        root = Path(roots[stage])
        pattern_path = relative(pattern)
        destination = relative(target)
        wildcard = glob.has_magic(pattern)
        if wildcard:
            parts = pattern_path.parts
            stop = next(i for i, part in enumerate(parts) if glob.has_magic(part))
            base = root.joinpath(*parts[:stop])
            matches = [
                Path(p) for p in glob.glob(str(root / pattern), recursive=True, include_hidden=True)
            ]
        else:
            matches = [root / pattern]
            base = root / pattern
        if not matches or not any(p.exists() or p.is_symlink() for p in matches):
            raise ValueError(f"package mapping matched nothing: {mapping}")
        candidates = _mapping_leaves(matches, root)
        for path in sorted(candidates):
            name = str(path.relative_to(root))
            if any(fnmatch.fnmatchcase(stage + ":" + name, rule) for rule in excluded):
                continue
            if wildcard or (base.is_dir() and not base.is_symlink()):
                dest = output / destination / path.relative_to(base)
            else:
                dest = output / destination
            install(path, dest, output)
            count += 1
    if count == 0:
        raise ValueError("package contains no mapped entries")
    return count
