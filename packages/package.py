#!/usr/bin/python3
"""Assemble declared payloads and invoke the unchanged canonical ppk writer."""

import argparse
import hashlib
import json
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

sys.dont_write_bytecode = True
# Disable bytecode before importing local code from immutable Bazel inputs.
from assembly import AssemblyDefinition, assemble, relative  # noqa: E402

WRITER_REVISION = "9d586e59b6560a2e95eb41564fdcc759ea4f0733"


def digest(path: Path) -> str:
    """Return the SHA-256 identity of a file without loading it all into memory."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path: Path, value: object) -> None:
    """Write deterministic UTF-8 JSON with a final newline."""
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def materialise(manifest: Path, sources: Path) -> dict[str, str | int]:
    """Materialize only the declared source files and return their provenance."""
    # Package the declared source view. Bazel's input symlinks
    # are execution plumbing, not package symlinks. No undeclared tree walk.
    fingerprints = {}
    for entry in json.loads(manifest.read_text(encoding="utf-8")):
        name = relative(entry["relative"])
        original = Path(entry["path"])
        target = sources / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(original, target)
        mode = 0o755 if original.stat().st_mode & 0o111 else 0o644
        target.chmod(mode)
        fingerprints[str(name)] = digest(target) + ":" + oct(mode)
    if not fingerprints:
        raise ValueError("empty source package")
    encoded = json.dumps(fingerprints, sort_keys=True).encode("utf-8")
    return {
        "input_digest": hashlib.sha256(encoded).hexdigest(),
        "source_files": len(fingerprints),
        "source_links": "materialised",
    }


def commit(args: argparse.Namespace) -> None:
    """Assemble one payload, verify the canonical writer result, and record provenance."""
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    # Provenance combines heterogeneous decoded build metadata at the JSON boundary.
    provenance: dict[str, Any] = {
        "release_ready": False,
        "trust": "development",
        "writer_revision": WRITER_REVISION,
        "version": args.version,
    }
    # Source and debug payloads can be several GiB. Keep scratch beside the
    # declared output instead of assuming the host's /tmp tmpfs is large.
    # Keep scratch inside the declared output too, so Bazel can reclaim it
    # after a hard cancellation; it is removed before the action succeeds.
    with tempfile.TemporaryDirectory(prefix=".peios-ppk-", dir=output) as temporary:
        payload = Path(temporary) / "payload"
        payload.mkdir()
        if args.source_manifest:
            name, arch = "dev.peios.eventd-source", "noarch"
            provenance["source"] = materialise(args.source_manifest, payload / "usr/src/eventd")
        else:
            definition = json.loads(args.definition.read_text(encoding="utf-8"))
            metadata = definition["package"]
            name, arch = metadata["name"], metadata["architecture"]
            if metadata["version"] != args.version:
                raise ValueError("package version differs from the source family")
            data = Path(temporary) / "data"
            materialise(args.data_manifest, data)
            roots = {"source": data, "stage": args.stage.resolve()}
            assemble(cast(AssemblyDefinition, definition), roots, payload)
            # Bazel makes tree-output files executable while making its cache immutable.
            # Installed modes are recipe policy, not transport/cache permissions.
            executables = set(definition.get("executables", []))
            for entry in payload.rglob("*"):
                if entry.is_file() and not entry.is_symlink():
                    entry.chmod(0o755 if str(entry.relative_to(payload)) in executables else 0o644)
            for entry in executables:
                if not (payload / relative(entry)).is_file():
                    raise ValueError(f"missing declared executable: {entry}")
            provenance["recipe_sha256"] = digest(args.definition)
            provenance["build"] = json.loads(
                (args.stage / "build.json").read_text(encoding="utf-8")
            )
            # The canonical manifest is deliberately unchanged. Policy remains explicit
            # alongside it until an installer supports these fields in its own schema.
            provenance["package_metadata"] = {
                key: value for key, value in definition.items() if key != "files"
            }
        result = subprocess.check_output(
            [
                str(args.writer.resolve()),
                "commit",
                name,
                args.version,
                arch,
                "--content",
                str(payload),
                "--output",
                str(output),
            ],
            text=True,
        ).strip()
    manifest = Path("manifests") / name / args.version / arch / "manifest.toml"
    if result != "sha256:" + digest(output / manifest):
        raise ValueError("writer returned an incorrect manifest identity")
    provenance.update(name=name, arch=arch, manifest=str(manifest), manifest_hash=result)
    write_json(output / "provenance.json", provenance)


def manifest_blobs(store: Path, info: Mapping[str, Any]) -> set[Path]:
    """Validate canonical manifest identity and entries, returning referenced blob paths."""
    # This JSON boundary validates identity and manifest shape before consuming it.
    path = relative(info["manifest"])
    manifest = store / path
    if "sha256:" + digest(manifest) != info["manifest_hash"]:
        raise ValueError(f"manifest hash mismatch: {manifest}")
    doc = tomllib.loads(manifest.read_text(encoding="utf-8"))
    if set(doc) != {"name", "version", "arch", "files"}:
        raise ValueError("unexpected canonical manifest fields")
    for key in ("name", "version", "arch"):
        if doc[key] != info[key] or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+~-]*", doc[key]):
            raise ValueError(f"package identity mismatch: {key}")
    expected = Path("manifests") / doc["name"] / doc["version"] / doc["arch"] / "manifest.toml"
    if path != expected:
        raise ValueError("manifest is not at its canonical path")
    blobs = set()
    for name, entry in doc["files"].items():
        relative(name)
        keys = set(entry)
        if keys in ({"blob"}, {"blob", "exec"}):
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", entry["blob"]):
                raise ValueError("invalid blob identity")
            if "exec" in entry and entry["exec"] is not True:
                raise ValueError("invalid executable marker")
            blobs.add(Path("blobs/sha256") / (entry["blob"][7:] + ".zst"))
        elif keys == {"symlink"} and isinstance(entry["symlink"], str):
            pass
        elif keys == {"dir"} and entry["dir"] is True:
            pass
        else:
            raise ValueError(f"invalid file entry: {name}")
    return blobs


def merge(stores: Sequence[Path], output: Path, checker: Path) -> None:
    """Merge declared stores, rejecting identity conflicts and invalid blob contents."""
    output.mkdir(parents=True, exist_ok=True)
    packages, seen = [], set()
    blob_names = set()
    for store in stores:
        info = json.loads((store / "provenance.json").read_text(encoding="utf-8"))
        identity = (info["name"], info["version"], info["arch"])
        if identity in seen:
            raise ValueError(f"duplicate package identity: {identity}")
        seen.add(identity)
        blobs = manifest_blobs(store, info)
        actual = {p.relative_to(store) for p in (store / "blobs/sha256").glob("*.zst")}
        if actual != blobs:
            raise ValueError(f"missing or unreferenced blobs: {identity}")
        for path in sorted(blobs | {Path(info["manifest"])}):
            source, target = store / path, output / path
            if target.exists():
                if digest(source) != digest(target):
                    raise ValueError(f"conflicting store content: {path}")
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
        blob_names.update(blobs)
        packages.append(info)
    if not packages:
        raise ValueError("empty package store")
    # Decode every distinct blob and verify its uncompressed content identity.
    subprocess.run([str(checker.resolve()), str(output / "blobs/sha256")], check=True)
    write_json(
        output / "index.json",
        {
            "release_ready": False,
            "trust": "development",
            "blob_count": len(blob_names),
            "package_count": len(packages),
            "packages": sorted(packages, key=lambda p: (p["name"], p["version"], p["arch"])),
        },
    )


def main() -> None:
    """Dispatch the package-commit or store-merge command."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("commit")
    for name in ("output", "writer"):
        build.add_argument("--" + name, type=Path, required=True)
    for name in ("definition", "stage", "source-manifest", "data-manifest"):
        build.add_argument("--" + name, type=Path)
    build.add_argument("--version", required=True)
    join = commands.add_parser("merge")
    join.add_argument("--store", type=Path, action="append", required=True)
    join.add_argument("--output", type=Path, required=True)
    join.add_argument("--checker", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "commit":
        commit(args)
    else:
        merge(args.store, args.output, args.checker)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(128 + signal.SIGTERM))
    main()
