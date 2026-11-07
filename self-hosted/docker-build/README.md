# Building docker images

The contents of this directory are used to build the docker images for the
self-hosted backend and dashboard. If you're looking for ways to run self-hosted
Convex, see the [these instructions](../README.md). You may build the images
locally from here, but we recommend using the images we provide on GHCR.

Build the backend from scratch by running:

```sh
docker build -t convex-backend -f self-hosted/docker-build/Dockerfile.backend .
```

## Hosted CI builds

The manual
[Build patched backend image](../../.github/workflows/build_patched_backend.yml)
workflow builds an optimized Linux amd64 image on `ubuntu-24.04`, checks its
capabilities and SQLite startup, and publishes
`ghcr.io/<owner>/<repository>:sha-<commit>`. It includes the Wasm runtime gate
and source-package authority helper. The job summary supplies the image digest
for downstream Dockerfiles. No registry account beyond GitHub is required; the
workflow uses `GITHUB_TOKEN` with package write permission.

The builder has a 12 GiB memory limit with no additional swap, two Cargo jobs,
and serialized backend LLVM optimization. It removes unused host SDKs and
reports available disk before and during compilation. This uses reclaimed runner
space; fitting within a strict 14 GB disk quota is not guaranteed. A cold build
must also finish within GitHub's six-hour job limit.

Build layers are cached in GHCR at `buildcache-amd64`. A separate Actions cache
preserves up to 2 GiB of sccache output across runners. Cargo target and
download cache mounts remain local to each runner, so source changes still
require dependency downloads, build-script execution and linking. Exact cached
build layers can skip those steps. The cache extraction temporarily needs
additional disk space, and old Actions cache entries are subject to GitHub's
eviction.

The workflow sets `CARGO_CHEF_COOK=false`: a fresh runner cannot recover the
target mount from registry layers, and cooking dependencies without the final
feature set adds work before the sccache-enabled build. The default remains
`true` for persistent local builders. Consumers pull only the runtime image;
they do not download build caches.

## Local build options

The backend build uses Cargo's `release` profile by default. Select another
built-in or custom workspace profile with `CARGO_BUILD_PROFILE`:

```sh
docker build \
  -t convex-backend \
  -f self-hosted/docker-build/Dockerfile.backend \
  --build-arg CARGO_BUILD_PROFILE=slim-release \
  .
```

Ordinary optimized images retain available function symbols in
`/convex/convex-local-backend.debug` before packaging strips the executable.
This companion is optional for runtime use and required only when requesting
native profile attribution from a stripped executable. For source lines and
inline-call information as well, request Cargo debuginfo and disable both
Cargo's strip setting and the Dockerfile's final strip pass:

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
