#!/usr/bin/env bash
# Build the repository's FILEBENCH-aware fork out of tree.  The old parser
# uses patterns that trigger current glibc fortify checks, so this compatibility
# build disables fortify and stack-protector instrumentation for Filebench only.

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/../.." && pwd)

if (( $# > 1 )); then
  echo "usage: $0 [empty-build-directory]" >&2
  exit 2
fi

if (( $# == 1 )); then
  build_dir=$1
  mkdir -p -- "$build_dir"
  if [[ -n $(find "$build_dir" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
    echo "build directory is not empty: $build_dir" >&2
    exit 2
  fi
  build_dir=$(cd -- "$build_dir" && pwd)
else
  build_dir=$(mktemp -d /tmp/orchfs-filebench-XXXXXX)
fi

cp -a -- "$repo_root/filebench/." "$build_dir/"
(
  cd -- "$build_dir"
  autoreconf -ivf
  CFLAGS='-g -O2 -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=0 -fno-stack-protector' \
    ./configure
  make -j"$(nproc)"
) >&2

printf '%s\n' "$build_dir/filebench"
