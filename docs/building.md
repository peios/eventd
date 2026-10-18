# Building and Checking eventd

Build on x86-64 Linux with Python 3.11 or newer, a C compiler, binutils,
libclang, pkg-config, gzip, curl, and Rust 1.98.1 with rustfmt and Clippy.
The `bazel` launcher verifies and caches Bazel 8.4.2. Bazel builds each
Rust crate directly and resolves dependencies from `Cargo.lock`.

## Prepare a development SDK

Select exact libpeios and kernel revisions, then prepare a new SDK directory:

```sh
tools/prepare-dev-sdk /path/to/libpeios /path/to/kernel/include/uapi /tmp/eventd-sdk
export PEIOS_SDK=/tmp/eventd-sdk
```

The helper builds libpeios with Cargo because that component owns its build
interface. It copies its headers, the selected kernel UAPI, and
`libpeios.so.0` into the SDK. It does not install anything on the host.
CI pins both SDK source revisions in `.github/workflows/check.yml`.
A development SDK uses the host ABI; it does not qualify native Peios
security or kernel integration.

## Build and test

```sh
./bazel build //:artifacts
./bazel test //...
./bazel run //evctl -- help
```

The test set includes the store, daemon, client, and command-line interface
(CLI) suites, four Loom
queue models, package assembly and blob verification, and package-family
policy checks. The portable store and queue tests run on a Linux host.
Loom explores bounded publication, capacity-release, and closure schedules;
it supplements stress tests and ownership checks.

For the full contributor check, install the pinned PSC tools into a directory
outside the repository, then run the shared entry point:

```sh
python3 tools/prepare-style-tools.py /tmp/eventd-style
export PEIOS_STYLE_BIN=/tmp/eventd-style/bin
tools/check
```

`tools/check` checks owned source formatting and lint, Starlark formatting,
and the Bazel test set. It then uses Cargo for strict Clippy diagnostics and
rustdoc tests, including compile-fail checks on queue receiver ownership.
These supplemental Cargo checks do not produce the packaged programs.
The tool versions and hashes are recorded in `tools/style-tools.lock.json`.

## Build packages

```sh
./bazel build //:packages
```

The verified content-addressed store is `bazel-bin/packages.store`. It
contains canonical manifests, compressed SHA-256 blobs, and an `index.json`
record describing the six packages:

- `dev.peios.eventd`: daemon, registry seeds, event and registry references,
  manual, and empty state directories.
- `dev.peios.evctl`: query client and manual.
- `dev.peios.eventd-debuginfo` and `dev.peios.evctl-debuginfo`: separate
  symbols matching each runtime package version.
- `dev.peios.eventd-debugsource`: declared source inputs for debugging.
- `dev.peios.eventd-source`: source and build machinery.

Packaged programs retain the workspace release profile: full link-time
optimization, one code-generation unit, and aborting panics.

Individual outputs are available under `//packages:<name>`, with the
`dev.peios.` prefix omitted. Staging checks ELF hardening, split debug
information, versions, licenses, manuals, and registry policy before writing
packages. Store assembly verifies every manifest and decompresses every
unique blob to check its content identity.

The canonical manifest schema is unchanged. Dependency constraints,
security descriptors, manual-cache effects, and the daemon's `tcb.priv`
signing requirement remain explicit in the accompanying package metadata.
Those records express installation and signing requirements; building the
store does not apply them. Outputs remain unsigned and marked
`release_ready: false`. Publication and native runtime qualification are
separate release steps.

## Build in native Peios userspace

Native packaging requires an isolated Peios userspace with its CET-enabled
Rust compiler and standard library, C toolchain, libpeios development SDK
(at least 0.5.8, ABI 0), kernel headers, Python, and binutils. The caller owns
that environment and its package lock. Do not run a native build against an
unrecorded mix of host and Peios libraries.

Acquire an immutable dependency snapshot on a networked development host:

```sh
tools/offline-build acquire /absolute/path/eventd-inputs
```

Copy the exact source, including `MODULE.bazel.lock`, into the isolated
environment. Mount the acquired snapshot read-only and provide a private
writable work directory. Inside that environment:

```sh
tools/prepare-native-sdk /tmp/eventd-native-sdk
export PEIOS_SDK=/tmp/eventd-native-sdk
export PEIOS_RUSTC=/usr/bin/rustc
tools/offline-build test /eventd-inputs /eventd-work
```

The native configuration enables CET, full RELRO, immediate binding, and
packed relocations. Package checks require the corresponding ELF properties.
The offline wrapper refuses repository downloads and a mismatched module
lock. It produces the package family and runs Bazel tests; it does not start
services, install registry seeds, or sign artifacts.

Native userspace tests still run on the caller's kernel. Full qualification
requires a compatible Peios kernel and tests of KMES ingestion, KACS access
checks, service identity, provisioning, restart recovery, and installed-client
queries. Keep those results separate from host tests and package checks.

## Measure performance

```sh
tools/bench
```

Run benchmarks on deployment-class hardware before changing queue, batch,
index, or SQLite parameters. Record the build profile and workload with
the result. A host microbenchmark does not measure native access checks or
the complete ingestion path.

## Check documentation

The Learn sources are under `docs/learn/`. Use a Trail version supporting
merges at every tree level:

```sh
trail lint docs/learn
trail build docs/learn --strict --out /tmp/eventd-learn
```

Panorama merges this component's `peios.antho` contribution into the shared
Peios documentation. Validate that combined tree when changing shared links
or citation targets. Keep generated output out of source control.
