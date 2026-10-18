"""Canonical PPK commits and a verified, deduplicated package family."""

load("//build:version.bzl", "VERSION")

def _package_impl(ctx):
    output = ctx.actions.declare_directory(ctx.label.name + ".ppk")
    args = [
        ctx.file._driver.path,
        "commit",
        "--output",
        output.path,
        "--writer",
        ctx.executable._writer.path,
        "--version",
        VERSION + "-1",
    ]
    inputs = ctx.files.srcs + [ctx.file._driver, ctx.file._assembly]
    manifest = ctx.actions.declare_file(ctx.label.name + ".inputs.json")
    ctx.actions.write(manifest, json.encode([
        {"path": f.path, "relative": f.short_path}
        for f in ctx.files.srcs
    ]))
    inputs.append(manifest)
    if ctx.attr.source:
        args += ["--source-manifest", manifest.path]
    else:
        inputs += ctx.files.stage + [ctx.file.definition]
        args += [
            "--data-manifest",
            manifest.path,
            "--stage",
            ctx.files.stage[0].path,
            "--definition",
            ctx.file.definition.path,
        ]
    ctx.actions.run(
        executable = "/usr/bin/python3",
        arguments = args,
        inputs = depset(inputs),
        tools = [ctx.executable._writer],
        outputs = [output],
        env = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
        execution_requirements = {"no-sandbox": "1", "no-remote-exec": "1"},
        mnemonic = "PpkCommit",
    )
    return [DefaultInfo(files = depset([output]))]

event_package = rule(
    implementation = _package_impl,
    doc = "Assemble one declared payload and emit canonical PPK blobs, manifest and provenance.",
    attrs = {
        "definition": attr.label(
            allow_single_file = True,
            doc = "JSON payload recipe for binary packages.",
        ),
        "stage": attr.label(doc = "Validated staging output for binary packages."),
        "source": attr.bool(doc = "Package the declared source view instead of a binary recipe."),
        "srcs": attr.label_list(
            allow_files = True,
            doc = "Declared files available to the action.",
        ),
        "_driver": attr.label(default = "//packages:package.py", allow_single_file = True),
        "_assembly": attr.label(default = "//packages:assembly.py", allow_single_file = True),
        "_writer": attr.label(default = "//vendor/ppk:ppk", executable = True, cfg = "exec"),
    },
)

def _store_impl(ctx):
    output = ctx.actions.declare_directory(ctx.label.name + ".store")
    args = [
        ctx.file._driver.path,
        "merge",
        "--output",
        output.path,
        "--checker",
        ctx.executable._checker.path,
    ]
    for f in ctx.files.packages:
        args += ["--store", f.path]
    ctx.actions.run(
        executable = "/usr/bin/python3",
        arguments = args,
        inputs = depset(ctx.files.packages + [ctx.file._driver, ctx.file._assembly]),
        tools = [ctx.executable._checker],
        outputs = [output],
        env = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
        execution_requirements = {"no-remote-exec": "1"},
        mnemonic = "PpkStore",
        progress_message = "Assembling and verifying the eventd package store",
    )
    return [DefaultInfo(files = depset([output]))]

package_store = rule(
    implementation = _store_impl,
    doc = "Merge package outputs into a verified content-addressed store and family index.",
    attrs = {
        "packages": attr.label_list(
            mandatory = True,
            doc = "Canonical package output directories.",
        ),
        "_driver": attr.label(default = "//packages:package.py", allow_single_file = True),
        "_assembly": attr.label(default = "//packages:assembly.py", allow_single_file = True),
        "_checker": attr.label(default = "//packages:check-blobs", executable = True, cfg = "exec"),
    },
)

def _store_test_impl(ctx):
    executable = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(executable, """#!/bin/bash
set -euo pipefail
root="${RUNFILES_DIR:-$0.runfiles}/%s"
export PPK_WRITER="$root/%s"
export PPK_CHECKER="$root/%s"
exec /usr/bin/python3 "$root/%s"
""" % (
        ctx.workspace_name,
        ctx.executable._writer.short_path,
        ctx.executable._checker.short_path,
        ctx.file.main.short_path,
    ), is_executable = True)
    return [DefaultInfo(executable = executable, runfiles = ctx.runfiles(
        files = ctx.files.srcs + [ctx.file.main, ctx.executable._writer, ctx.executable._checker],
    ))]

package_store_test = rule(
    implementation = _store_test_impl,
    test = True,
    doc = "Run Python package-store checks with the declared writer and blob checker binaries.",
    attrs = {
        "main": attr.label(
            allow_single_file = True,
            mandatory = True,
            doc = "Python test entry point.",
        ),
        "srcs": attr.label_list(
            allow_files = True,
            doc = "Declared files available to the action.",
        ),
        "_writer": attr.label(default = "//vendor/ppk:ppk", executable = True, cfg = "exec"),
        "_checker": attr.label(default = "//packages:check-blobs", executable = True, cfg = "exec"),
    },
)
