#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$root/config.sh"

commit_snapshot=true
push_remote=""

usage() {
  echo "usage: $0 [--no-commit] [--push <remote>] <clean-source-worktree>" >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-commit)
      commit_snapshot=false
      shift
      ;;
    --push)
      [[ $# -ge 2 ]] || usage
      push_remote="$2"
      shift 2
      ;;
    --help|-h)
      usage
      ;;
    --*)
      usage
      ;;
    *)
      break
      ;;
  esac
done

[[ $# -eq 1 ]] || usage
source_worktree="$(cd "$1" && pwd)"

[[ "$(git -C "$root" symbolic-ref --short HEAD)" == "patch-history" ]] || {
  echo "FATAL: history worktree must have patch-history checked out" >&2
  exit 1
}
[[ -z "$(git -C "$root" status --porcelain)" ]] || {
  echo "FATAL: patch-history worktree is not clean" >&2
  exit 1
}
[[ -z "$(git -C "$source_worktree" status --porcelain)" ]] || {
  echo "FATAL: source worktree is not clean" >&2
  exit 1
}

history_common_dir="$(git -C "$root" rev-parse --path-format=absolute --git-common-dir)"
source_common_dir="$(git -C "$source_worktree" rev-parse --path-format=absolute --git-common-dir)"
[[ "$history_common_dir" == "$source_common_dir" ]] || {
  echo "FATAL: source worktree belongs to a different repository" >&2
  exit 1
}

tip="$(git -C "$source_worktree" rev-parse HEAD)"
case "$UPSTREAM_MODE" in
  merge-base)
    base="$(git -C "$source_worktree" merge-base "$UPSTREAM_REF" "$tip")"
    ;;
  marker)
    marker_path="${UPSTREAM_MARKER:?UPSTREAM_MARKER is required for marker mode}"
    base="$(git -C "$source_worktree" show "$tip:$marker_path" | tr -d '\n')"
    ;;
  *)
    echo "FATAL: unsupported UPSTREAM_MODE: $UPSTREAM_MODE" >&2
    exit 1
    ;;
esac

[[ "$base" =~ ^[0-9a-f]{40}$ ]] || {
  echo "FATAL: derived upstream base is not a full commit ID: $base" >&2
  exit 1
}
git -C "$source_worktree" merge-base --is-ancestor "$base" "$tip" || {
  echo "FATAL: upstream base is not an ancestor of source HEAD" >&2
  exit 1
}

pathspecs=(.)
for excluded in "${EXCLUDED_PATHS[@]}"; do
  pathspecs+=(":(exclude,glob)$excluded")
done

mapfile -t commits < <(
  git -C "$source_worktree" rev-list --reverse "$base..$tip" -- "${pathspecs[@]}"
)
mapfile -t non_merge_commits < <(
  git -C "$source_worktree" rev-list --reverse --no-merges "$base..$tip" -- "${pathspecs[@]}"
)
[[ ${#commits[@]} -gt 0 ]] || {
  echo "FATAL: source contains no exported commits after the upstream base" >&2
  exit 1
}
[[ ${#commits[@]} -eq ${#non_merge_commits[@]} ]] || {
  echo "FATAL: exported source history contains a merge commit" >&2
  exit 1
}

temp="$(mktemp -d)"
cleanup() {
  rm -rf "$temp"
}
trap cleanup EXIT
mkdir -p "$temp/patches"

git -C "$source_worktree" format-patch \
  --zero-commit \
  --full-index \
  --binary \
  --no-signature \
  --output-directory "$temp/patches" \
  "$base..$tip" \
  -- "${pathspecs[@]}" >/dev/null

shopt -s nullglob
generated=("$temp"/patches/*.patch)
shopt -u nullglob
[[ ${#generated[@]} -eq ${#commits[@]} ]] || {
  echo "FATAL: generated ${#generated[@]} patches for ${#commits[@]} commits" >&2
  exit 1
}

printf '%s\n' "$base" >"$temp/UPSTREAM"
printf '%s\n' "${commits[-1]}" >"$temp/SOURCE"
printf '# patch-file source-commit\n' >"$temp/SERIES"
for i in "${!generated[@]}"; do
  printf '%s %s\n' "$(basename "${generated[$i]}")" "${commits[$i]}" >>"$temp/SERIES"
done

index="$temp/index"
GIT_INDEX_FILE="$index" git -C "$source_worktree" read-tree "$base"
for patch in "${generated[@]}"; do
  GIT_INDEX_FILE="$index" git -C "$source_worktree" apply \
    --cached --binary --whitespace=nowarn "$patch"
done
result_tree="$(GIT_INDEX_FILE="$index" git -C "$source_worktree" write-tree)"
git -C "$source_worktree" diff --quiet "$result_tree" "$tip" -- "${pathspecs[@]}" || {
  echo "FATAL: generated series does not reproduce the exported source tree" >&2
  exit 1
}
printf '%s\n' "$result_tree" >"$temp/RESULT_TREE"

rm -rf "$root/patches"
mv "$temp/patches" "$root/patches"
for generated_file in UPSTREAM SOURCE SERIES RESULT_TREE; do
  mv "$temp/$generated_file" "$root/$generated_file"
done

git -C "$root" add -- UPSTREAM SOURCE SERIES RESULT_TREE patches
git -C "$root" diff --cached --check -- . ':(exclude)patches/**'

if git -C "$root" diff --cached --quiet; then
  echo "patch history is already current"
else
  echo "prepared ${#commits[@]} patches from $base through ${commits[-1]}"
  if $commit_snapshot; then
    short_source="${commits[-1]:0:12}"
    short_base="${base:0:12}"
    git -C "$root" commit \
      -m "patch-history: record source train at $short_source" \
      -m "Record ${#commits[@]} ordered downstream patches based on upstream $short_base. The snapshot was generated and tree-verified by scripts/update.sh."
  fi
fi

if [[ -n "$push_remote" ]]; then
  $commit_snapshot || {
    echo "FATAL: --push cannot be combined with --no-commit" >&2
    exit 1
  }
  [[ -z "$(git -C "$root" status --porcelain)" ]] || {
    echo "FATAL: refusing to push a dirty patch-history worktree" >&2
    exit 1
  }
  git -C "$root" push --set-upstream "$push_remote" patch-history
fi
