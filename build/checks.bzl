"""Host checks with declared runfiles and the selected Bazel Rust compiler.

These checks intentionally use the host C compiler, Python and binutils. They
are development-host checks, not a hermetic cross-compilation toolchain.
"""

def _host_test_impl(ctx):
    rust = ctx.toolchains["@rules_rust//rust:toolchain_type"]
    launcher = ctx.actions.declare_file(ctx.label.name + ".sh")
    script = """#!/bin/bash
set -euo pipefail
cd "${RUNFILES_DIR:-$0.runfiles}/%s"
export RUSTC="$PWD/%s"
export PATH=/usr/bin:/bin
%s
""" % (ctx.workspace_name, rust.rustc.short_path, ctx.attr.command)
    ctx.actions.write(launcher, script, is_executable = True)
    return [DefaultInfo(
        executable = launcher,
        runfiles = ctx.runfiles(
            files = ctx.files.data,
            transitive_files = rust.all_files,
        ),
    )]

host_test = rule(
    implementation = _host_test_impl,
    attrs = {
        "command": attr.string(mandatory = True, doc = "Bash command run from input runfiles."),
        "data": attr.label_list(allow_files = True, doc = "Declared check inputs."),
    },
    test = True,
    toolchains = ["@rules_rust//rust:toolchain_type"],
    doc = "Run a host check with pinned rustc; tag external and no-remote.",
)
