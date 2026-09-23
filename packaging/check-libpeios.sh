#!/bin/sh
# Require the packaged libpeios SDK from the dependency root. A newer
# API-compatible SDK is valid; a different SONAME ABI is not.

set -eu

pkg-config --atleast-version=0.5.0 peios || {
  echo "eventd: dev.peios.libpeios-devel >= 0.5.0 (ABI 0) is required" >&2
  exit 1
}
readelf -dW "$(pkg-config --variable=libdir peios)/libpeios.so" |
  grep -Eq '\(SONAME\).*\[libpeios\.so\.0\]'
