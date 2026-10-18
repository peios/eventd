"""Use the installed Peios Rust compiler and hardened standard library."""

def _run(ctx, args):
    result = ctx.execute(args)
    if result.return_code:
        fail(result.stderr)
    return result.stdout.strip()

def _native_rust_impl(ctx):
    compiler = ctx.os.environ.get("PEIOS_RUSTC", "/usr/bin/rustc")
    version = _run(ctx, [compiler, "--version"]).split(" ")[1]
    parts = [int(part) for part in version.split(".")]
    if parts < [1, 98, 1]:
        fail("Native release builds require Peios Rust >= 1.98.1, found " + version)
    triple = "x86_64-unknown-linux-gnu"
    lib = ctx.path(_run(ctx, [compiler, "--print", "target-libdir", "--target", triple]))
    driver_lib = lib.dirname.dirname.dirname
    ctx.symlink(compiler, "bin/rustc")
    ctx.symlink(ctx.path(compiler).dirname.get_child("rustdoc"), "bin/rustdoc")
    ctx.symlink(lib, "lib/rustlib/" + triple + "/lib")
    ctx.symlink(driver_lib, "compiler-libs")
    ctx.file("BUILD.bazel", """
load("@rules_rust//rust:toolchain.bzl", "rust_stdlib_filegroup", "rust_toolchain")
package(default_visibility = ["//visibility:public"])
filegroup(
    name = "driver",
    srcs = glob(
        ["compiler-libs/librustc_driver*.so", "compiler-libs/libLLVM*.so*"],
        allow_empty = True,
    ),
)
rust_stdlib_filegroup(name = "std", srcs = glob(["lib/rustlib/x86_64-unknown-linux-gnu/lib/**"]))
rust_toolchain(
    name = "implementation",
    rustc = "bin/rustc",
    rust_doc = "bin/rustdoc",
    rustc_lib = ":driver",
    rust_std = ":std",
    binary_ext = "",
    dylib_ext = ".so",
    staticlib_ext = ".a",
    exec_triple = "x86_64-unknown-linux-gnu",
    target_triple = "x86_64-unknown-linux-gnu",
    stdlib_linkflags = ["-ldl", "-lpthread"],
    default_edition = "2024",
    version = "%s",
    debug_info = {"dbg": "2", "fastbuild": "0", "opt": "2"},
    strip_level = {"dbg": "none", "fastbuild": "none", "opt": "none"},
    env = {"RUSTC_BOOTSTRAP": "1"},
    extra_rustc_flags = [
        "-Zcf-protection=full",
        "-Clink-arg=-Wl,-z,relro",
        "-Clink-arg=-Wl,-z,now",
        "-Clink-arg=-Wl,--as-needed",
        "-Clink-arg=-Wl,-z,pack-relative-relocs",
        "-Clink-arg=-Wl,--build-id=sha1",
        "--remap-path-prefix=$${output_base}=/usr/src/debug/dev.peios.eventd/.build",
        "--remap-path-prefix=$${pwd}=/usr/src/debug/dev.peios.eventd",
        "--remap-path-prefix=$${exec_root}=/usr/src/debug/dev.peios.eventd",
        "--remap-path-prefix=external=/usr/src/debug/dev.peios.eventd/external",
        "--remap-path-prefix=.=/usr/src/debug/dev.peios.eventd",
    ],
    extra_exec_rustc_flags = ["-Zcf-protection=full"],
)
toolchain(
    name = "toolchain",
    toolchain = ":implementation",
    toolchain_type = "@rules_rust//rust:toolchain_type",
    exec_compatible_with = ["@platforms//cpu:x86_64", "@platforms//os:linux"],
    target_compatible_with = ["@platforms//cpu:x86_64", "@platforms//os:linux"],
)
""" % version)

native_rust = repository_rule(
    doc = "Expose the installed Peios compiler and hardened standard library as a Rust toolchain.",
    implementation = _native_rust_impl,
    local = True,
    configure = True,
    environ = ["PEIOS_RUSTC"],
)
