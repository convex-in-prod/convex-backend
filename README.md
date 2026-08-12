# Maintained patch history

This orphan branch records the maintained downstream commit train in two forms:
the normal source commits remain on the source branch, while this branch retains
an ordered, applyable patch snapshot for every recorded revision of that train.
Updating this branch after an upstream rebase preserves the previous patch
forms in ordinary Git history instead of replacing them.

The generated snapshot contains:

- `UPSTREAM`: the exact upstream commit on which the series applies;
- `SOURCE`: the last source commit that contributes to the exported series;
- `SERIES`: patch filenames paired with their source commit IDs;
- `RESULT_TREE`: the exact tree produced by applying the series; and
- `patches/`: stable `git format-patch` files with commit messages and authors.

`scripts/update.sh` is the only supported way to refresh generated files. It
requires a clean source worktree from this repository, derives the configured
upstream base, regenerates the complete series, applies every patch to a
temporary index, compares the result with the source tree, and creates the
history commit. For example:

```sh
./scripts/update.sh ../convex-backend
./scripts/update.sh --push convex-in-prod ../convex-backend
```

The first command records locally. The second also publishes the resulting
fast-forward update to the remote `patch-history` branch. Use `--no-commit` only
to inspect generated changes before recording them.

To reconstruct the source commits on a clean checkout at `UPSTREAM`:

```sh
./scripts/apply.sh /path/to/upstream-checkout
```

The apply command uses `git am`, retaining commit authors and messages, and
then requires the resulting tree to equal `RESULT_TREE`.

The generated patches deliberately use zeroed mail-header commit IDs. Source
commit IDs remain in `SERIES`, while unchanged patch content stays stable across
a conflict-free rebase. A rebase still records the new upstream and source IDs,
and any actual patch change remains visible in this branch's diff.
