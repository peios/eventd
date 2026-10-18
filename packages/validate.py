"""Validate the staged eventd payload without starting services or applying registry seeds."""

import gzip
import json
import os
import re
import subprocess
import tomllib
from pathlib import Path
from typing import Any

BINARIES = {"eventd": "usr/sbin/eventd", "evctl": "usr/bin/evctl"}
STATE_LEAVES = ("events", "logs", "metrics")
PRIVILEGES = ["SeChangeNotifyPrivilege", "SeSecurityPrivilege", "SeAuditPrivilege"]
SERVICE_SID = "S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124"

# Existing protected, inheritable descriptor: SYSTEM, Administrators and eventd's service SID.
STATE_DESCRIPTOR = bytes.fromhex(
    "010004901400000020000000000000002c000000010100000000000512000000"
    "01010000000000051200000002005c0003000000000314000000001001010000"
    "0000000512000000000318000000001001020000000000052000000020020000"
    "0003280000000010010600000000000550000000d2840e756d1f666dfc67a263"
    "8a84d887a4a3e576"
)


def require(condition: bool, message: str) -> None:
    """Fail a packaging gate even when Python runs with assertions disabled."""
    if not condition:
        raise ValueError(message)


def readelf(path: Path, option: str) -> str:
    """Read one ELF view using stable English binutils output."""
    return subprocess.check_output(
        ["readelf", option, str(path)],
        text=True,
        errors="replace",
        env=os.environ | {"LC_ALL": "C"},
    )


def build_id(notes: str, path: Path) -> str:
    """Extract the single GNU build identity required by split debugging."""
    matches = re.findall(r"Build ID: ([0-9a-f]+)", notes)
    require(len(matches) == 1 and len(matches[0]) >= 3, f"{path}: missing or ambiguous build ID")
    return matches[0]


def validate_elf(stage: Path, component: str, relative: str, *, native: bool) -> None:
    """Check linked hardening and corresponding split debug data for one executable."""
    elf = stage / relative
    require(elf.is_file() and os.access(elf, os.X_OK), f"{elf}: missing executable")
    header, program, dynamic = (readelf(elf, option) for option in ("-h", "-lW", "-dW"))
    notes, sections = readelf(elf, "-n"), readelf(elf, "-SW")
    require(bool(re.search(r"Type:\s+DYN\b", header)), f"{elf}: not position independent")
    require("GNU_RELRO" in program and "BIND_NOW" in dynamic, f"{elf}: missing full RELRO")
    stack = [line for line in program.splitlines() if "GNU_STACK" in line]
    require(len(stack) == 1 and bool(re.search(r"\sRW\s", stack[0])), f"{elf}: executable stack")
    require(bool(re.search(r"FLAGS_1.*\bPIE\b", dynamic)), f"{elf}: missing PIE flag")
    require("Shared library: [libpeios.so.0]" in dynamic, f"{elf}: missing libpeios ABI linkage")
    require(not re.search(r"\((RPATH|RUNPATH|TEXTREL)\)", dynamic), f"{elf}: unsafe dynamic tag")
    require(".debug_info" not in sections, f"{elf}: unsplit debug information")
    if native:
        require("IBT" in notes and "SHSTK" in notes, f"{elf}: missing native CET properties")
        require(bool(re.search(r"\(RELR\)", dynamic)), f"{elf}: missing native packed relocations")
    identity = build_id(notes, elf)
    debug = stage / f"debug/{component}/usr/lib/debug/.build-id/{identity[:2]}/{identity[2:]}.debug"
    require(debug.is_file() and debug.stat().st_size > 0, f"{elf}: missing split debug file")
    require(".debug_info" in readelf(debug, "-SW"), f"{debug}: missing debug information")
    require(build_id(readelf(debug, "-n"), debug) == identity, f"{debug}: build ID mismatch")
    require(debug.name in readelf(elf, "--string-dump=.gnu_debuglink"), f"{elf}: wrong debug link")
    forbidden = {str(Path.cwd().resolve()), str(Path.cwd().resolve()).split("/execroot/")[0]}
    for path in (elf, debug):
        contents = path.read_bytes()
        for prefix in forbidden:
            require(len(prefix) <= 1 or prefix.encode() not in contents, f"{path}: leaked {prefix}")


def registry_values(document: dict[str, Any], key_path: str) -> dict[str, Any]:
    """Find one registry key, rejecting ambiguous keys or duplicate value names."""
    keys = [key for key in document["keys"] if key["path"] == key_path]
    require(len(keys) == 1, f"missing or duplicate registry key: {key_path}")
    entries = keys[0].get("values", [])
    values = {entry["name"]: entry["data"] for entry in entries}
    require(len(values) == len(entries), f"duplicate registry value in {key_path}")
    return values


def validate_registry(stage: Path) -> None:
    """Check the shipped service identity, privileges, paths and inert provisioning seeds."""
    directory = stage / "usr/share/regim"
    require(
        {path.name for path in directory.glob("*.reg")}
        == {"eventd-config.reg", "eventd-service.reg"},
        "unexpected registry seed family",
    )
    config = json.loads((directory / "eventd-config.reg").read_text(encoding="utf-8"))
    service = json.loads((directory / "eventd-service.reg").read_text(encoding="utf-8"))
    values = registry_values(service, r"Machine\System\Services\eventd")
    expected = {
        "ImagePath": "/usr/sbin/eventd",
        "Identity": "Service",
        "HookIdentity": "SYSTEM",
        "ExecStartPre": ["/usr/sbin/eventd --prepare-security"],
        "RequiredPrivileges": PRIVILEGES,
        "Requires": ["authd"],
        "ErrorControl": 1,
        "Readiness": 0,
        "RuntimeDirectories": ["eventd"],
    }
    for name, value in expected.items():
        require(values.get(name) == value, f"service policy changed: {name}")
    paths = registry_values(config, r"Machine\System\eventd")
    require(
        paths
        == {
            "EventStorePath": "/var/state/eventd/events/",
            "LogStorePath": "/var/state/eventd/logs/",
            "MetricStorePath": "/var/state/eventd/metrics/",
            "QuerySocketPath": "/run/eventd/query.sock",
            "LogSocketPath": "/run/eventd/log.sock",
            "MetricSocketPath": "/run/eventd/metric.sock",
        },
        "configured deployment paths changed",
    )
    identity = registry_values(config, rf"Machine\Generic\Authn\Policy\{SERVICE_SID}")
    require(identity == {"Privileges": PRIVILEGES, "Integrity": "System"}, "service token changed")
    for leaf in STATE_LEAVES:
        provision = registry_values(config, rf"Machine\System\Init\ProvisionedPaths\eventd-{leaf}")
        require(provision["Path"] == f"/var/state/eventd/{leaf}", "incorrect provisioned path")
        require(provision["Required"] == 1 and provision["Kind"] == "directory", "optional state")
        descriptor = bytes.fromhex(provision["Security"])
        require(descriptor == STATE_DESCRIPTOR, "state security descriptor changed")


def validate_data(stage: Path) -> None:
    """Check manual delivery, registry reference and the unpopulated state skeleton."""
    for leaf in STATE_LEAVES:
        directory = stage / "var/state/eventd" / leaf
        require(
            directory.is_dir() and not any(directory.iterdir()), f"{directory}: populated state"
        )
    for name, section in (("evctl", 1), ("eventd", 8)):
        manual = stage / f"usr/share/man/man{section}/{name}.{section}.gz"
        with gzip.open(manual, "rt", encoding="utf-8") as stream:
            text = stream.read()
        require(bool(re.search(r"^\.TH ", text, re.MULTILINE)), f"{manual}: missing manual heading")
        if name == "evctl":
            for option in ("format", "socket", "file", "help", "version"):
                require(f"-{option}" in text, f"{manual}: missing --{option}")
    reference = (stage / "usr/share/regman/eventd.regman").read_text(encoding="utf-8")
    require(r"Machine\System\eventd" in reference, "missing eventd registry reference")
    for key in ("EventStorePath", "LogStorePath", "MetricStorePath"):
        require(key in reference, f"missing registry reference: {key}")


def validate(stage: Path, *, native: bool, sdk: Path) -> None:
    """Validate the exact staged payload; propagate failures before any package commit."""
    for component, relative in BINARIES.items():
        validate_elf(stage, component, relative, native=native)
    environment = os.environ | {"LD_LIBRARY_PATH": str(sdk.resolve() / "lib"), "LC_ALL": "C"}
    version = tomllib.loads(Path("Cargo.toml").read_text(encoding="utf-8"))["workspace"]["package"][
        "version"
    ]
    client = stage / "usr/bin/evctl"
    reported = subprocess.check_output([str(client), "version"], env=environment, text=True).strip()
    require(reported == f"evctl {version}", "staged CLI version differs from source")
    help_text = subprocess.check_output([str(client), "help"], env=environment, text=True)
    require("Usage:" in help_text, "staged CLI help is incomplete")
    validate_registry(stage)
    validate_data(stage)
