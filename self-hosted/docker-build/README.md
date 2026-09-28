# Building docker images

The contents of this directory are used to build the docker images for the
self-hosted backend and dashboard. If you're looking for ways to run self-hosted
Convex, see the [these instructions](../README.md). You may build the images
locally from here, but we recommend using the images we provide on GHCR.

Build the backend from scratch by running:

```sh
docker build -t convex-backend -f self-hosted/docker-build/Dockerfile.backend .
```

The backend Dockerfile requires BuildKit. It keeps Cargo target, Git, registry,
and sccache data in persistent, architecture-scoped cache mounts, so an
unchanged source and dependency graph reuses the existing native build output.
The caches remain build-host state and are not copied into the runtime image.

Installed JavaScript dependencies have a separate image layer before Cargo.
Rust-only edits and cancelled native builds therefore reuse `node_modules`, not
just pnpm's download store. Cargo's build scripts still build the required
JavaScript outputs normally; this does not assert that those outputs are prebuilt.
The `build-dependencies` target completes cargo-chef dependency cooking before
first-party source is copied. It runs Cargo for dependencies, but does not build
the final backend binary.

The build-stage base is pinned to a multi-platform image digest. Update that
digest deliberately when upgrading the system build environment; following the
floating tag can otherwise invalidate every setup layer between retries. The
repository's `rust-toolchain` file still selects the Rust compiler separately.

To set the `sccache` local-cache retention budget without baking a machine
policy into the Dockerfile, pass its normal size setting for a local build:

```sh
docker build \
  --build-arg SCCACHE_CACHE_SIZE=20G \
  -t convex-backend \
  -f self-hosted/docker-build/Dockerfile.backend \
  .
```

If the BuildKit garbage collector removes the `sccache` mount, the next build
is cold; cache mounts never change the resulting image.

The backend build uses Cargo's `release` profile by default. Select another
built-in or custom workspace profile with `CARGO_BUILD_PROFILE`:

```sh
docker build \
  -t convex-backend \
  -f self-hosted/docker-build/Dockerfile.backend \
  --build-arg CARGO_BUILD_PROFILE=slim-release \
  .
```

Set a bounded Cargo job count with `CARGO_BUILD_JOBS`. Experimental local
backend features can be selected explicitly with `LOCAL_BACKEND_FEATURES`;
normal images leave this argument empty:

```sh
docker build \
  --build-arg CARGO_BUILD_JOBS=8 \
  --build-arg LOCAL_BACKEND_FEATURES=feature-name \
  -t convex-backend \
  -f self-hosted/docker-build/Dockerfile.backend \
  .
```

On a constrained host, native dependency packages can be compiled in a separate
first phase with a wider Cargo jobserver while the final Rust graph keeps a
lower `CARGO_BUILD_JOBS`. Supply whitespace-separated Cargo package specs and a
positive prebuild job count. Both phases use the selected Cargo profile and the
same target cache:

```sh
docker build \
  --build-arg CARGO_BUILD_JOBS=1 \
  --build-arg NATIVE_PREBUILD_JOBS=4 \
  --build-arg 'NATIVE_PREBUILD_PACKAGES=native-package-a native-package-b' \
  -t convex-backend \
  -f self-hosted/docker-build/Dockerfile.backend \
  .
```

To include the source-package authority helper, set
`BUILD_SOURCE_PACKAGE_PREACTIVATION_AUTHORITY=true` together with
`LOCAL_BACKEND_FEATURES=static-hermes-wasmtime-gate`. The default is `false`.
The helper is built from the same source and Cargo profile into the existing
artifact directory, then included as `/convex/source_package_preactivation_authority`
in the final image. Its application gate feature is explicit, including when
parallel frontend flags require a separate backend-binary invocation. Existing
Cargo and native dependency cache mounts are reused.

Extract it from a stopped container without starting the backend:

```sh
docker create --name source-authority-export convex-backend
docker cp source-authority-export:/convex/source_package_preactivation_authority ./source_package_preactivation_authority
docker rm source-authority-export
```

Use a new container name and output path, retain the image and binary digests,
and run the extracted binary only on a compatible Linux runtime. The additional
binary needs compilation/linking and artifact/image-export space; cache reuse
does not make that cost zero. An image without this option does not supply the
helper, and adding it does not change the backend entrypoint.

For a release profiling build, request Cargo debuginfo and disable both Cargo's
strip setting and the Dockerfile's final strip pass:

```sh
docker build \
  -t convex-backend \
  -f self-hosted/docker-build/Dockerfile.backend \
  --build-arg CARGO_PROFILE_RELEASE_DEBUG=1 \
  --build-arg CARGO_PROFILE_RELEASE_STRIP=none \
  .
```

Build the dashboard from scratch by running:

```sh
docker build -t convex-dashboard -f self-hosted/docker-build/Dockerfile.dashboard .
```
