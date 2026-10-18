"""Import an explicit, installed Peios development SDK into the build graph."""

def _sdk_repository_impl(ctx):
    sdk = ctx.os.environ.get("PEIOS_SDK")
    if not sdk:
        # Loading shared build definitions must not require an SDK for client-only
        # or source-package targets. Fail when an SDK target is actually analysed.
        ctx.file("missing.bzl", """
def _missing_impl(_ctx):
    fail(
        "Set PEIOS_SDK to an absolute SDK directory containing include/peios.h, " +
        "include/pkm and lib/libpeios.so.0. See docs/building.md for SDK setup.",
    )
missing_sdk = rule(implementation = _missing_impl)
""")
        ctx.file("BUILD.bazel", """
load(":missing.bzl", "missing_sdk")
package(default_visibility = ["//visibility:public"])
missing_sdk(name = "files")
missing_sdk(name = "root")
missing_sdk(name = "peios")
""")
        return
    if not sdk.startswith("/"):
        fail("PEIOS_SDK must be an absolute directory.")
    for name in ["include/peios.h", "include/pkm", "lib/libpeios.so.0"]:
        if not ctx.path(sdk + "/" + name).exists:
            fail("Missing Peios SDK input: " + sdk + "/" + name)
    ctx.symlink(sdk + "/include", "include")
    ctx.symlink(sdk + "/lib/libpeios.so.0", "lib/libpeios.so.0")
    ctx.symlink("lib/libpeios.so.0", "lib/libpeios.so")
    ctx.file("root", "")
    ctx.file("BUILD.bazel", """
load("@rules_cc//cc:defs.bzl", "cc_import")
package(default_visibility = ["//visibility:public"])
exports_files(["root"])
filegroup(name = "files", srcs = glob(["include/**", "lib/**"]))
cc_import(
    name = "peios",
    shared_library = select({"%s": None, "%s": None, "//conditions:default": "lib/libpeios.so.0"}),
    interface_library = "lib/libpeios.so",
    system_provided = select({"%s": True, "%s": True, "//conditions:default": False}),
    hdrs = glob(["include/**"]),
    includes = ["include"],
)
""" % (
        ctx.attr.native_config,
        ctx.attr.package_config,
        ctx.attr.native_config,
        ctx.attr.package_config,
    ))

sdk_repository = repository_rule(
    doc = "Import PEIOS_SDK headers and libpeios; defer missing-SDK failure until target analysis.",
    implementation = _sdk_repository_impl,
    environ = ["PEIOS_SDK"],
    attrs = {
        "native_config": attr.label(
            default = Label("//build:native"),
            doc = "Native runtime selection.",
        ),
        "package_config": attr.label(
            default = Label("//build:packaged"),
            doc = "Installed package selection.",
        ),
    },
    local = True,
    configure = True,
)
