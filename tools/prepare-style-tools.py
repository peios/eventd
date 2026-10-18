#!/usr/bin/env python3
"""Install the eventd subset of hash-pinned PSC style tools outside the repository."""

import argparse
import hashlib
import io
import json
import os
import platform
import subprocess
import sys
import tarfile
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parent.parent


def digest(data: bytes) -> str:
    """Return a downloaded artifact's SHA-256 digest."""
    return hashlib.sha256(data).hexdigest()


def install_binary(directory: Path, name: str, entry: dict[str, str | None]) -> None:
    """Verify both the archive and executable before publishing the selected tool."""
    destination = directory / name
    if destination.is_file() and digest(destination.read_bytes()) == entry["binary_sha256"]:
        return
    url = entry["url"]
    if not isinstance(url, str) or not url.startswith("https://"):
        raise ValueError(f"{name}: download URL must use HTTPS")
    with urlopen(url, timeout=60) as response:
        archive = response.read()
    if digest(archive) != entry["sha256"]:
        raise ValueError(f"{name}: archive checksum mismatch")
    member = entry["member"]
    if member is None:
        executable = archive
    else:
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as bundle:
            selected = bundle.extractfile(member)
            if selected is None:
                raise ValueError(f"{name}: executable missing from archive")
            with selected:
                executable = selected.read()
    if digest(executable) != entry["binary_sha256"]:
        raise ValueError(f"{name}: executable checksum mismatch")
    temporary = destination.with_suffix(".new")
    temporary.write_bytes(executable)
    temporary.chmod(0o755)
    temporary.replace(destination)


def main() -> None:
    """Prepare one explicit Linux x86_64 tool directory for local checks or CI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    output = parser.parse_args().output.resolve()
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        parser.error("the pinned artifacts require Linux x86_64")
    if output == ROOT or ROOT in output.parents:
        parser.error("put style tool installations outside the repository")
    binary = output / "bin"
    binary.mkdir(parents=True, exist_ok=True)
    lock = json.loads((ROOT / "tools/style-tools.lock.json").read_text(encoding="utf-8"))
    for name, entry in lock.items():
        install_binary(binary, name, entry)
    environment = output / "venv"
    subprocess.run([sys.executable, "-m", "venv", str(environment)], check=True)
    subprocess.run(
        [
            str(environment / "bin/python"),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--require-hashes",
            "--only-binary=:all:",
            "-r",
            str(ROOT / "tools/style-requirements.lock"),
        ],
        check=True,
    )
    ruff = binary / "ruff"
    if os.path.lexists(ruff):
        ruff.unlink()
    ruff.symlink_to(environment / "bin/ruff")
    print(f"Run checks with PEIOS_STYLE_BIN={binary}")


if __name__ == "__main__":
    main()
