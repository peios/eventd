"""Expose an explicit qualification command without booting VMs during bazel test."""

def _integration_check_impl(ctx):
    executable = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.symlink(output = executable, target_file = ctx.file.script, is_executable = True)
    return [DefaultInfo(executable = executable)]

integration_check = rule(
    implementation = _integration_check_impl,
    attrs = {"script": attr.label(allow_single_file = True, mandatory = True)},
    executable = True,
)
