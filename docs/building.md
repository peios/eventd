# Building and Checking eventd

Use pekit to build the event daemon and its command-line client, and to run
their tests. Pekit builds each crate on its own, so a change rebuilds only
the crates it reaches. The supported development host is x86-64 Linux.

## Before you start

You need pekit and Git. Everything else is pinned and fetched by pekit on
the first build: the Bootlin C toolchain, Rust 1.98.1 with Clippy, Python
3.14, libclang, and the crates `Cargo.lock` selects. The first build needs
network access; later builds find what they need in pekit's store, and every
step runs without network access in a sandbox that holds only its declared
inputs.

The build files use the gcslib libraries named in `workspace.gcsl` and the
build files, and the libraries of peios-rs and the Peios SDK. `pekit lock`
pins their commits in `pekit.lock`.

## Build

```sh
pekit build eventd evctl
```

| Target | Output |
|---|---|
| `eventd` | Event daemon |
| `evctl` | Command-line client of eventd's query channel |

Pekit prints where each output is, under `pekit-out/<target>/`. The
programs load `libpeios.so.0` by its shared object name (SONAME) and carry
no runtime search path, so running them needs libpeios installed. Do not run
the daemon on the development host as part of a build check.

Programs are built with the workspace's release profile: full link-time
optimization, one code-generation unit, and aborting panics.

## Test

```sh
pekit test
```

| Test | Check |
|---|---|
| `eventd_core`, `eventd_client`, `eventd`, `evctl` | Each member's unit tests |
| `loom` | Loom's models of the queue's publication, capacity-release, and closure schedules |
| `doctests` | Each member's documentation tests, including compile-fail checks on queue receiver ownership |
| `clippy` | Clippy's checks of every target of every member, with every warning denied |

The `loom` test compiles eventd-core with `--cfg loom` and runs only the
queue's models, one at a time. Loom explores bounded schedules; it
supplements stress tests and ownership checks.

A test whose inputs did not change is found in the store and not run
again. The portable store and queue tests run on a Linux host.

For the full contributor check, install the pinned PSC tools into a
directory outside the repository, then run the shared entry point:

```sh
python3 tools/prepare-style-tools.py "$HOME/.cache/eventd-style"
export PEIOS_STYLE_BIN="$HOME/.cache/eventd-style/bin"
tools/check
```

`tools/check` checks owned source formatting and lint with rustfmt, Ruff,
ShellCheck, and shfmt, then runs `pekit test`. The style checks use the
host's Rust 1.98.1 formatter through rustup. The tool versions and hashes
are recorded in `tools/style-tools.lock.json`.

## How the build is planned

`build.gcsl` plans the Cargo workspace with the gcslib rust library's
`workspace`: Cargo works out each member's build, unit by unit, and pekit
compiles each unit as a step. Planning reads the manifests, `Cargo.lock`,
and which files exist, never the sources, so editing a source plans nothing
again. Each member's crates are compiled with the lints in the workspace's
`[lints]` table, as Cargo compiles them.

Keep Rust dependency versions in the Cargo manifests and `Cargo.lock`, and
update the lockfile deliberately with Cargo. `peios-sys` generates its
bindings from the SDK's headers with bindgen and links libpeios, so it, and
every crate that depends on it, is given what peios-rs's library says it
needs, `crate_settings()`: libpeios and its headers.

libpeios and its headers are built from the Peios SDK's own files, through
the SDK's GCSL library, which peios-rs's library imports. The commits
`pekit.lock` pins say which SDK and which peios-rs; the rust library fails a
build whose `Cargo.lock` takes peios-rs's crates at another commit than
`pekit.lock` pins its library at, so the crates and the settings they are
built with always agree. authd takes the same peios-rs commit, so that a
program linking both eventd-client and authd's crates holds one peios-sys.

## Not built with pekit yet

Packages, the hot-path benchmark, and builds with the native Peios
toolchain, which hardens programs with control-flow enforcement technology
(CET), are not yet built with pekit. The package definitions and native
build settings from the Bazel build are kept outside this repository until
they are.

## Check documentation

The Learn sources are under `docs/learn/`. Use a Trail version supporting
merges at every tree level:

```sh
trail lint docs/learn
trail build docs/learn --strict --out "$HOME/.cache/eventd-learn"
```

Panorama merges this component's `peios.product` contribution into the shared
Peios documentation. Validate that combined tree when changing shared links
or citation targets. Keep generated output out of source control.
