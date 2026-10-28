#!/usr/bin/env python3
"""Check owned Rust, Python and shell source with the pinned PSC configurations."""

import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUST_ROOTS = ("eventd-core", "eventd-client", "eventd", "evctl")
SHELL_SOURCES = ("tools/check",)


def tool(name: str, version: str) -> str:
    """Locate a tool and refuse a version that differs from the shared PSC pin."""
    directory = os.environ.get("PEIOS_STYLE_BIN")
    executable = str(Path(directory) / name) if directory else shutil.which(name)
    if executable is None:
        raise SystemExit(f"{name} is missing; run tools/prepare-style-tools.py OUTSIDE_REPO")
    actual = subprocess.check_output([executable, "--version"], text=True)
    expected = {
        "ruff": f"ruff {version}",
        "shfmt": f"v{version}",
        "shellcheck": f"version: {version}",
    }[name]
    if expected not in actual.splitlines():
        raise SystemExit(f"{name} must be version {version}, found {actual.strip()}")
    return executable


def run(arguments: list[str]) -> None:
    """Run a checker from the source root and preserve its failure."""
    subprocess.run(arguments, cwd=ROOT, check=True)


def main() -> None:
    """Check declared owned source roots; generated and imported vendor files stay excluded."""
    rust = sorted(
        str(path.relative_to(ROOT)) for name in RUST_ROOTS for path in (ROOT / name).rglob("*.rs")
    )
    python = sorted(str(path.relative_to(ROOT)) for path in (ROOT / "tools").rglob("*.py"))
    run(
        [
            "rustup",
            "run",
            "1.98.1",
            "rustfmt",
            "--check",
            "--config-path",
            "tools/rustfmt.toml",
            *rust,
        ]
    )
    ruff = tool("ruff", "0.17.0")
    run([ruff, "check", "--no-cache", "--config", "tools/ruff.toml", *python])
    run([ruff, "format", "--check", "--no-cache", "--config", "tools/ruff.toml", *python])
    run([tool("shellcheck", "0.11.0"), *SHELL_SOURCES])
    run([tool("shfmt", "3.14.1"), "-d", "-i", "4", *SHELL_SOURCES])


if __name__ == "__main__":
    main()
