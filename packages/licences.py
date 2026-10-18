"""Collect notices from the actual declared Bazel crate closure."""

import shutil
import tomllib
from pathlib import Path

NOTICE_PREFIXES = ("LICENSE", "LICENCE", "COPYING", "NOTICE", "UNLICENSE")


def normalise(expression: str) -> str:
    """Normalize whitespace and alternate-license spelling for package metadata."""
    expression = " ".join(expression.replace("/", " OR ").split())
    if "(" not in expression and " AND " not in expression and " WITH " not in expression:
        expression = " OR ".join(sorted(set(expression.split(" OR "))))
    return expression


def combined(expressions: set[str]) -> str:
    """Conjoin distinct crate license expressions without changing their grouping."""
    return " AND ".join(
        f"({value})"
        if len(expressions) > 1
        and any(operator in value for operator in (" OR ", " AND ", " WITH "))
        and not (value.startswith("(") and value.endswith(")"))
        else value
        for value in sorted(expressions)
    )


def collect(inputs: list[Path], output: Path) -> str:
    """Fail on missing metadata/notices; no Cargo invocation or ambient crate cache."""
    files = {path for path in inputs if path.is_file()}
    manifests = sorted(path for path in files if path.name == "Cargo.toml")
    if not manifests:
        raise ValueError("empty crate metadata closure")
    output.mkdir(parents=True, exist_ok=True)
    expressions = {"MIT"}  # Every first-party crate is covered by the repository licence.
    rows = ["crate\tversion\tdeclared-license\tsource\n"]
    seen = set()
    lock = tomllib.loads(Path("Cargo.lock").read_text(encoding="utf-8"))["package"]
    sources = {(p["name"], p["version"]): p.get("source", "workspace/path") for p in lock}
    for manifest in manifests:
        package = tomllib.loads(manifest.read_text(encoding="utf-8")).get("package")
        if not package:
            continue
        # Only repository members may inherit metadata from the declared root manifest.
        if "external" not in manifest.parts:
            workspace = tomllib.loads(Path("Cargo.toml").read_text(encoding="utf-8"))["workspace"][
                "package"
            ]
            for key in ("version", "license"):
                if package.get(key) == {"workspace": True}:
                    package[key] = workspace[key]
        identity = (package["name"], package["version"])
        if identity in seen:
            continue
        seen.add(identity)
        if identity not in sources:
            raise ValueError(f"crate absent from Cargo.lock: {identity}")
        source = sources[identity]
        declared = package.get("license")
        if not isinstance(declared, str) or not declared:
            raise ValueError(f"missing licence expression: {manifest}")
        expression = normalise(declared)
        expressions.add(expression)
        rows.append("\t".join((*identity, expression, source)) + "\n")
        if source == "workspace/path":
            continue
        notices = sorted(
            path
            for path in files
            if path.parent == manifest.parent and path.name.upper().startswith(NOTICE_PREFIXES)
        )
        if not notices and source.startswith("git+https://github.com/peios/peios-rs.git"):
            notices = [Path("third-party/peios-rs-LICENSE")]
        if not notices:
            raise ValueError(f"missing declared licence notices: {manifest}")
        destination = output / "-".join(identity)
        destination.mkdir()
        for notice in notices:
            shutil.copyfile(notice, destination / notice.name)
    (output / "crate-licenses.tsv").write_text("".join(rows), encoding="utf-8")
    expression = combined(expressions)
    (output / "crate-expression.txt").write_text(expression + "\n", encoding="utf-8")
    return expression
