#!/usr/bin/env python3
"""Make Bazel acquisition output portable and usable as a read-only input."""

import os
import sys
from pathlib import Path


def freeze(root: Path) -> None:
    """Replace acquisition-cache symlinks with paths inside the snapshot."""
    prefix = "../bazel-external/"
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = Path(directory) / name
            if not path.is_symlink():
                continue
            target = os.readlink(path)
            if not target.startswith(prefix):
                continue
            destination = root / target.removeprefix(prefix)
            destination.resolve().relative_to(root.resolve())
            if not destination.exists():
                raise ValueError(f"vendor target is not present: {destination}")
            path.unlink()
            path.symlink_to(os.path.relpath(destination, path.parent))
    (root / "bazel-external").unlink(missing_ok=True)


def view(root: Path, destination: Path) -> None:
    """Give Bazel a writable top level without copying immutable repositories."""
    destination.mkdir(parents=True, exist_ok=True)
    for entry in root.iterdir():
        if entry.name == "bazel-external":
            continue
        target = destination / entry.name
        if target.is_symlink() and os.readlink(target) == str(entry):
            continue
        if target.exists() or target.is_symlink():
            raise ValueError(f"refusing to replace vendor view entry: {target}")
        target.symlink_to(entry)


if __name__ == "__main__":
    mode = sys.argv[1]
    root = Path(sys.argv[2]).absolute()
    if mode == "freeze":
        freeze(root)
    elif mode == "view":
        view(root, Path(sys.argv[3]))
    else:
        raise ValueError(f"unknown mode: {mode}")
