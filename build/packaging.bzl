"""Internal setting selected by package artifact transitions."""

def _mode_impl(_ctx):
    return []

package_mode = rule(
    implementation = _mode_impl,
    build_setting = config.bool(flag = True),
    doc = "Select installed-library linkage for package artifacts via the package transition.",
)
