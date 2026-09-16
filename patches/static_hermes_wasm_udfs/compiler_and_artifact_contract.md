# Static Hermes Compiler and Artifact Contract

This document describes the maintained producer and publication contract for
the [Static Hermes Wasm UDF patch](README.md). The producer is external to this
backend repository.

## Complete source authority

The producer must start from one complete generated deployment inventory using
the same module-resolution and generated-source rules as the existing
deployment toolchain. The source envelope must bind:

- every source byte and normalized path;
- runtime imports, re-exports, registrations, visibility, and UDF kinds;
- emitted runtime module paths;
- bundler, condition, target, format, splitting, and plugin identities;
- dependency and initialization ordering; and
- the active runtime module and source-package runtime-content identities.

The source-package identity is the digest of runtime-relevant content, not the
source archive digest. It covers normalized runtime modules, external
dependency material, and the selected Node version without allowing archive
metadata or compression to change runtime authority.

Analysis is fail closed. Missing inventory, unresolved ownership, unsupported
registration, source mismatch, incomplete context-reuse evidence, or an
incomplete graph closure cannot produce an executable route. A diagnostic
result is not executable authorization.

### Exact external dependency selection

A producer constructing runtime authority before activation selects dependency
material on the target first. `POST /api/deploy2/prepare_external_deps` accepts
`adminKey` and nonempty `nodeDependencies` with unique package names. It returns
`{kind: "convex-external-deps-package-v1", id, sha256, storageKey, size,
dependencies}`. Dependencies are sorted `{package, version}` declarations;
`size` is the ZIP byte count. Preparation uses the existing dependency build
cache, but does not upload functions, analyze modules, prepare schemas, or
activate code. Packages must satisfy the existing package-size limits.

`POST /api/deploy2/download_external_deps` accepts `{adminKey, id, sha256}` and
streams that package's ZIP, with a five-minute deadline spanning storage
lookup and streaming. Both operations require deployment authorization.
The response's `Content-Length` is checked against the selected package size,
and streaming rejects early EOF or bytes beyond that length. Storage errors
propagate, and cancellation drops the storage stream. Clients
must verify the descriptor's size and SHA-256 before publishing a reusable
archive cache entry.

The frozen `start_push` request carries `externalDepsPackage: {id, sha256}`.
The backend resolves storage metadata from that exact target document and
checks its SHA-256 and complete declarations before uploading any source
package. Newer declaration-equivalent packages cannot replace this selection,
and an invalid selection never permits a dependency rebuild. Ordinary clients
intentionally omit the field and retain the existing cache/build behavior;
an explicit `null` is rejected, not treated as omission.

The preactivation helper checks the supplied ZIP bytes against the selected
SHA-256 before writing a source package. It and the backend use the same
runtime-content hash implementation; document IDs, storage keys, and ZIP
representation of the source package do not enter that digest. The external
dependency ZIP digest and declarations do enter it.

## Deployment-v8 route closure

The maintained deployment is `convex-wasm-deployment-v8`. Every selected query
or mutation route names:

- a route digest;
- a cohort-contract digest;
- an entry digest and 64-bit selector identity;
- the exact export name, UDF kind, and visibility;
- a bound active module digest and source-package runtime-content digest; and
- the deployment's module-graph binding.

The `convex-wasm-deployment-module-graph-binding-v2` record binds the source
envelope, schedule, ordered cohorts, graph-manifest digests, cohort-contract
digests, complete sorted route set, and application context-reuse analysis.
Every selected route must appear exactly once. A graph, cohort contract,
context-reuse result, or generation that differs from the deployment binding
is rejected.

The embedded `convex-wasm-module-graph-cohort-contract-v2` is executable route
authority. It binds the producer implementation, compiler, precompiler,
engine, execution policy, entries, selectors, routes, and one cohort analysis
derived from the deployment-wide context-reuse authority. Cohort analyses must
share the application policy and shared-analysis digest. Route lookup alone is
not authority.

## Module graph

The maintained graph manifest, graph package, and graph provenance are version
`5`:

- `convex-wasm-module-graph-manifest-v5`;
- `convex-wasm-module-graph-package-v5`; and
- `convex-wasm-module-graph-provenance-v5`.

A graph contains:

1. one base module;
2. zero or more shared modules in authenticated order; and
3. one leaf module for the cohort.

For every module the graph authenticates:

- the mandatory Core Wasm artifact and the serialized AOT artifact identity;
- exact ordered imports and exports with canonical Core Wasm types;
- the provider for every import;
- weak-symbol and dynamic-linking metadata;
- shared memory, table, stack, and relocation layout;
- data-relocation, constructor, and initialization order; and
- the context-reuse analysis that authorized the graph composition.

The serialized AOT record is mandatory, but its target-specific `.cwasm`
payload may be omitted when the destination can reconstruct it from the
authenticated Core Wasm with the production engine configuration. The AOT
record continues to bind the expected size and SHA-256. The destination must
compile, verify that exact identity, deserialize, and validate every selected
module before generation readiness. Any supplied AOT payload remains supported
and must be authenticated; a corrupt or mismatched payload is not treated as
omission.

The base owns the shared memory, function table, and stack pointer. Loader
relocation globals, direct providers, GOT providers, and weak-zero providers
must agree with the authenticated contract. Missing, extra, reordered, or
type-incompatible imports and exports are invalid even when the AOT digest is
otherwise correct.

## Runtime surface and language boundary

The current graph path uses:

- opaque value ABI version `3`;
- guest-native JSON value encoding;
- the typed capability request-envelope ABI;
- invocation-scoped capability identities; and
- guest Promise event-loop execution.

The producer may emit only host imports named by the authenticated graph and
supported by the runtime with the exact signature. Capability dispatch, query
streams, synchronous capability calls, async completion, cancellation, crypto,
randomness, console messages, and invocation time remain explicit imports.
Partial async-completion or query-stream import sets are invalid.

The admitted language remains a statically proved subset. Mutable module state,
dynamic evaluation, arbitrary thenables, unresolved capability ownership,
unsupported data shapes, or unproved async control flow remain on V8. The
producer must reject these cases before artifact publication; runtime failure
is not an eligibility mechanism.

Older execution-manifest schemas, compiler-assigned operation descriptors,
monolithic capability-entry packages, direct batch lowering, and deployment
and graph contract versions before the maintained versions remain
compatibility surfaces in the backend. They do not define the deployment-v8
module-graph contract.

## Active source proof

Before publication, the producer must collect the active runtime module digest
and latest source-package runtime-content digest from one consistent target
read. These values are embedded in each selected route's deployed-runtime
identity.

The backend checks both digests inside the invocation transaction before
selector discovery can access a package or AOT artifact. Execution checks them
again before using the routed module. A later source deployment invalidates the
older generation even if all route and artifact names are unchanged.

## Compiler and producer identity

The graph manifest, provenance, cohort contract, and deployment must agree on
the producer implementation identity. The producer identity must cover the
source tree, lockfiles, pinned toolchains, compiler binary, Static Hermes,
Emscripten and LLVM materials, generated headers, runtime libraries, linker,
target SDK, deterministic environment, and every compile and link flag.

The deployment's precompiler material identity must exactly equal the identity
in each cohort contract and AOT provenance record. Target triple, baseline CPU,
Wasmtime revision, engine configuration, and precompile-compatibility digest
must match the backend's pinned runtime.

Changing any material input, contract, route schedule, source envelope,
context-reuse result, engine setting, target, or toolchain produces a new
content identity. Published material is never edited in place.

## Artifact identity

The maintained immutable graph cache is version `v6`. Each artifact cache key
authenticates the artifact-pipeline kind, stage, and complete semantic identity.
The generation record, graph manifest, provenance, package entry, cache entry,
and physical file must all agree on cache key, stage, role, kind, size, and
SHA-256.

Current identities include:

| Contract | Value |
| --- | --- |
| Graph manifest | `convex-wasm-module-graph-manifest-v5` |
| Graph package | `convex-wasm-module-graph-package-v5` |
| Graph provenance | `convex-wasm-module-graph-provenance-v5` |
| Cache entry | `convex-wasm-artifact-cache-entry-v5` |
| Artifact pipeline | `convex-wasm-artifact-pipeline-v9` |
| Native artifact identity schema | `2` |
| Wasmtime engine identity | `convex-wasm-wasmtime-engine-identity` |

Core Wasm artifacts are bounded at `320 MiB`. Serialized AOT records and any
supplied payloads are bounded at `640 MiB`. Artifact records with a zero size,
malformed digest, unsupported kind, wrong stage, inconsistent engine metadata,
or inconsistent Core Wasm contract are rejected. Omitting Core Wasm, metadata,
or completion markers is invalid; only the reconstructible `.cwasm` payload may
be absent.

The runtime uses no production profiler. Producer AOT metadata continues to
identify `perf-map`; Wasmtime profiler choice is not part of the AOT
compatibility digest and does not require artifact rotation.

## Maintained generation kinds

The publisher may produce:

- `convex-wasm-runtime-registry-generation-v9`, which is primary-admitted and
  authorizes one exact complete deployment-v8 graph closure; or
- `convex-wasm-runtime-registry-generation-v10-shadow-only`, which is
  structurally shadow-only and identifies the same complete deployment-v8
  closure through its shadow-routing boundary.

Generation-v10 requires admission value `shadow-only`, an artifact-complete
shadow state, and the exact sorted route set. It can never be promoted to
Wasm-primary merely by changing process configuration.

Both kinds embed the deployment record, v2 graph binding, v2 cohort contracts,
and v5 graph records. The runtime requires the generation copies to equal the
authenticated deployment-v8 material.

## Source catalog and exact selector

The maintained source catalog is
`convex-wasm-runtime-registry-source-catalog-v2`. Each descriptor contains:

- `sourcePackageRuntimeContentSha256`;
- `deploymentSha256`;
- `generation.sha256` and `generation.size`; and
- `generationSha256`.

Those four digests form the exact source/generation selector used by the
database source-package record, readiness, active-pair reporting, and
invocation routing. The manifest size is also authenticated descriptor
identity. The catalog is canonical, sorted by the exact selector, duplicate
free, nonempty, and bounded to 4,096 entries.

Publishing a descriptor grants no readiness or execution authority. The
backend opens the selected immutable generation later and authenticates its
deployment, graph, artifact closure, admission lane, source binding, and every
selected routed module before publishing readiness.

Catalog successors may add or remove selectors but must remain nonempty. A
selector retained across reload keeps its exact authenticated descriptor: a
new catalog identity may change the descriptor's catalog pointer identity,
but not its generation-manifest digest or size, deployment digest, generation
digest, or runtime-content digest. Removing a selector does not remove its
immutable files.

## Publication

The maintained layout is:

```text
runtime-registry/
  current
  source-catalog.json
  generations/
    <deployment-sha256>/
      <generation-sha256>/
        COMPLETE
        deployment.json
        generation.json
  module-graph-cache/
    immutable/
      v6/
        packages/<graph-manifest-sha256>/...
        artifacts/<stage>/<cache-key>/...
  packages/
```

All directories use exact mode `0700`; all files use exact mode `0600`.
Canonical JSON has exactly one trailing newline. Package, artifact, and
generation completion markers are written last and contain the authenticated
content identity followed by one newline.

Publish every package, artifact, and generation through a private staging
directory. Write and sync the complete contents, write and sync `COMPLETE`,
sync the parent, then atomically rename into the content-addressed final name.
Never replace or modify a published content-addressed directory.

After all referenced immutable content is durable, construct an additive v2
source catalog containing every previously published descriptor plus the new
descriptor. Write and sync it as a private sibling, then atomically replace
`source-catalog.json` and sync the registry root. Source-keyed activation does
not use `current`. After activation commits, a separate retention step may
publish a smaller catalog and remove files no longer referenced by its active
pair. The compatibility `current` pointer must name a retained complete
generation before its prior target is deleted. A later rollback must restore
a descriptor and its immutable files if retention removed them.

The compatibility `current` pointer remains an exact root entry for non-source
mode. A non-source deployment atomically replaces `current` to activate its
generation; source-keyed retention may replace it only to keep the
compatibility layout valid. Current module-graph generations must use the
nested generation-digest directory; flat generation layouts are
compatibility-only.

A source-keyed rollback selects an already published descriptor through the
paired database activation contract. Compiler caches, staging directories,
generated source caches, and compiler packages are not runtime-registry
inputs.
