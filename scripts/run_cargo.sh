#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

if [[ "${OSTYPE:-}" == linux* && -z "${BINDGEN_EXTRA_CLANG_ARGS:-}" ]]; then
  clang_resource_include=""
  if command -v clang >/dev/null 2>&1; then
    clang_resource_candidate="$(clang -print-resource-dir 2>/dev/null || true)/include"
    if [[ -f "$clang_resource_candidate/stddef.h" ]]; then
      clang_resource_include="$clang_resource_candidate"
    fi
  fi
  if [[ -z "$clang_resource_include" ]]; then
    for clang_resource_candidate in /usr/lib/llvm-*/lib/clang/*/include; do
      if [[ -f "$clang_resource_candidate/stddef.h" ]]; then
        clang_resource_include="$clang_resource_candidate"
      fi
    done
  fi
  if [[ -n "$clang_resource_include" ]]; then
    export BINDGEN_EXTRA_CLANG_ARGS="-isystem${clang_resource_include}"
  fi
fi

if [[ -z "${PROTOC:-}" ]]; then
  if ! command -v mise >/dev/null 2>&1; then
    echo "Missing mise. Install the version required by mise.toml, then run 'mise install --locked protoc'." >&2
    exit 1
  fi
  if ! PROTOC="$(cd "$repo_root" && mise which protoc)"; then
    echo "Pinned protoc is unavailable. Run 'mise install --locked protoc' from the repository root." >&2
    exit 1
  fi
  export PROTOC
fi

stamp_shared_build_directory() {
  # Cargo expands build-dir templates after resolving the workspace manifest.
  # Querying it here keeps the stamp tied to the exact shared generation
  # instead of duplicating Cargo's path-hash algorithm in this launcher.
  local -a metadata_context=()
  local metadata_target_directory=""
  local metadata_target_directory_set=false
  local argument
  while (($# > 0)); do
    argument="$1"
    case "$argument" in
      --)
        break
        ;;
      --target-dir)
        if (($# < 2)); then
          echo "Cargo $argument is missing its value while resolving the build-directory access stamp." >&2
          exit 1
        fi
        if [[ -z "$2" ]]; then
          echo "Cargo $argument has an empty value while resolving the build-directory access stamp." >&2
          exit 1
        fi
        metadata_target_directory="$2"
        metadata_target_directory_set=true
        shift 2
        ;;
      --target-dir=*)
        metadata_target_directory="${argument#--target-dir=}"
        if [[ -z "$metadata_target_directory" ]]; then
          echo "Cargo $argument has an empty value while resolving the build-directory access stamp." >&2
          exit 1
        fi
        metadata_target_directory_set=true
        shift
        ;;
      --manifest-path|--config)
        if (($# < 2)); then
          echo "Cargo $argument is missing its value while resolving the build-directory access stamp." >&2
          exit 1
        fi
        metadata_context+=("$argument" "$2")
        shift 2
        ;;
      --manifest-path=*|--config=*)
        metadata_context+=("$argument")
        shift
        ;;
      --locked|--offline|--frozen)
        metadata_context+=("$argument")
        shift
        ;;
      *)
        shift
        ;;
      esac
  done
  if [[ "$metadata_target_directory_set" == true ]]; then
    case "$metadata_target_directory" in
      *$'\n'*|*$'\r'*)
        echo "Cargo --target-dir contains a newline while resolving the build-directory access stamp." >&2
        exit 1
        ;;
    esac
    local metadata_target_directory_toml="$metadata_target_directory"
    metadata_target_directory_toml="${metadata_target_directory_toml//\\/\\\\}"
    metadata_target_directory_toml="${metadata_target_directory_toml//\"/\\\"}"
    # cargo metadata has no --target-dir option. A command-line config
    # override preserves the CLI path's current-working-directory semantics,
    # and appending it gives it precedence over any forwarded --config override.
    metadata_context+=(--config "build.target-dir=\"$metadata_target_directory_toml\"")
  fi
  local metadata
  if ! metadata="$(cargo metadata "${metadata_context[@]}" --no-deps --format-version=1)"; then
    echo "Cargo metadata failed while resolving the build-directory access stamp." >&2
    exit 1
  fi
  local build_directory
  build_directory="$(printf '%s' "$metadata" | sed -n 's/.*"build_directory":"\([^"]*\)".*/\1/p')"
  if [[ -z "$build_directory" || "$build_directory" != /* ]]; then
    echo "Cargo metadata returned an invalid build directory for the access stamp." >&2
    exit 1
  fi
  if [[ ! -e "$build_directory" ]]; then
    return
  fi
  if [[ ! -d "$build_directory" || -L "$build_directory" ]]; then
    echo "Cargo build directory must be a non-symlink directory: $build_directory" >&2
    exit 1
  fi
  touch "$build_directory/.cargo-build-last-access"
}

cargo "$@"
stamp_shared_build_directory "$@"
