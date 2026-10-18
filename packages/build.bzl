"""Stage eventd's declared Rust closures for package assembly and validation."""

load("@rules_rust//rust:rust_common.bzl", "BuildInfo")
load("//build:version.bzl", "VERSION")

_SourceInfo = provider(
    "Declared packaging inputs from a Rust dependency closure.",
    fields = {"files": "Declared source and compile inputs."},
)

def _sources_impl(target, ctx):
    direct = []
    transitive = []
    for name in ["srcs", "compile_data", "data"]:
        direct += getattr(ctx.rule.files, name, [])
    if BuildInfo in target and target[BuildInfo].out_dir:
        direct.append(target[BuildInfo].out_dir)
    for name in ["deps", "proc_macro_deps", "script", "crate"]:
        dependencies = getattr(ctx.rule.attr, name, [])
        if type(dependencies) != "list":
            dependencies = [dependencies] if dependencies else []
        for dependency in dependencies:
            if _SourceInfo in dependency:
                transitive.append(dependency[_SourceInfo].files)
    return [_SourceInfo(files = depset(direct, transitive = transitive))]

_sources = aspect(
    implementation = _sources_impl,
    attr_aspects = ["deps", "proc_macro_deps", "script", "crate"],
)

def _stage_impl(ctx):
    output = ctx.actions.declare_directory(ctx.label.name)
    manifest = ctx.actions.declare_file(ctx.label.name + ".inputs.json")
    rust = ctx.toolchains["@rules_rust//rust:toolchain_type"]
    inputs = ctx.files.data + ctx.files.drivers + ctx.files.sdk + rust.all_files.to_list()
    components = {}
    for target, name in ctx.attr.components.items():
        files = target[_SourceInfo].files.to_list()
        binary = target[DefaultInfo].files.to_list()
        if len(binary) != 1:
            fail("Expected one binary for " + name)
        components[name] = {"binary": binary[0].path, "inputs": [f.path for f in files]}
        inputs += binary + files
    ctx.actions.write(manifest, json.encode({
        "components": components,
        "rustc": rust.rustc.path,
        "sdk": ctx.file.sdk_root.dirname,
        "version": VERSION,
        "native": ctx.attr.native,
        "sources": [f.path for f in ctx.files.data],
    }))
    ctx.actions.run(
        executable = "/usr/bin/python3",
        arguments = [ctx.file.driver.path, manifest.path, output.path],
        inputs = depset(inputs + [manifest]),
        outputs = [output],
        env = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
        # Preserve real symlinks inside declared tree outputs, not sandbox transport links.
        execution_requirements = {"no-sandbox": "1", "no-remote-exec": "1"},
        mnemonic = "EventPackageStage",
        progress_message = "Staging and checking eventd package payloads",
    )
    return [DefaultInfo(files = depset([output]))]

def _package_configuration(settings, _attr):
    # Preserve the workspace's release profile for every packaged Rust closure.
    return {
        "//build:package-mode": True,
        "//command_line_option:compilation_mode": "opt",
        "@rules_rust//rust/settings:lto": "fat",
        "@rules_rust//rust/settings:codegen_units": 1,
        "@rules_rust//rust/settings:extra_rustc_flags": settings["@rules_rust//rust/settings:extra_rustc_flags"] + ["-Cpanic=abort"],
    }

_package_transition = transition(
    implementation = _package_configuration,
    inputs = ["@rules_rust//rust/settings:extra_rustc_flags"],
    outputs = [
        "//build:package-mode",
        "//command_line_option:compilation_mode",
        "@rules_rust//rust/settings:lto",
        "@rules_rust//rust/settings:codegen_units",
        "@rules_rust//rust/settings:extra_rustc_flags",
    ],
)

event_stage = rule(
    doc = "Stage and validate declared payloads before recording their build provenance.",
    implementation = _stage_impl,
    toolchains = ["@rules_rust//rust:toolchain_type"],
    attrs = {
        "components": attr.label_keyed_string_dict(
            aspects = [_sources],
            cfg = _package_transition,
            doc = "Binary targets mapped to the installed component names.",
        ),
        "data": attr.label_list(allow_files = True, doc = "Declared packaging and source inputs."),
        "drivers": attr.label_list(allow_files = True, doc = "Staging implementation and helpers."),
        "driver": attr.label(allow_single_file = True, doc = "Python staging entry point."),
        "native": attr.bool(doc = "Require native Peios hardening checks for the staged binaries."),
        "sdk": attr.label(default = "@peios_sdk//:files", doc = "Declared native SDK inputs."),
        "sdk_root": attr.label(
            default = "@peios_sdk//:root",
            allow_single_file = True,
            doc = "SDK root marker used to locate SDK libraries.",
        ),
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
)
