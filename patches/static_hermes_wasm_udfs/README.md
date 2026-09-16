# Static Hermes Wasm UDF Execution

## Status

This patch is feature-gated and disabled by default. It adds a runtime consumer
for Static Hermes module
graphs compiled to Core Wasm and precompiled for the backend's pinned Wasmtime
engine. The maintained validation mode keeps V8 authoritative and runs selected
root-component queries and mutations as bounded Wasm shadows. After promotion,
the same policy can make Wasm authoritative for selected routes, with either a
sampled V8 verifier or no verifier. Unselected and quarantined routes remain
V8-only.

An external producer must analyze the complete deployment, produce the module
graphs and AOT artifacts, bind them to the active deployed source, and publish
an immutable runtime-registry generation. That producer is not part of this
repository.

See also:

- [Compiler and artifact contract](compiler_and_artifact_contract.md)
- [Operations and validation](operations.md)

## Current architecture

The maintained publication contract is a deployment-v8 module-graph closure.
It binds each selected route to one cohort contract and one graph manifest. A
graph contains an ordered base module, zero or more ordered shared modules, and
one leaf module. The graph contract authenticates every Core Wasm artifact,
every AOT artifact identity and supplied payload, every ordered import and
export, provider resolution, shared memory and table layout, initialization
order, engine identity, route identity, and the context-reuse analysis that
governs each selected cohort.

```text
active deployment-v8 source inventory
                 |
                 v
module-graph binding + cohort contracts
                 |
                 v
immutable registry generation
        |                         |
        v                         v
generation-v9               generation-v10
primary-admitted             shadow-only
        |                         |
        +------------+------------+
                     v
authenticated graph closure
  base -> ordered shared modules -> route leaf
```

Generation-v9 is structurally primary-admitted. Generation-v10 is structurally
shadow-only and cannot be loaded or activated while normal Wasm-primary routing
is enabled. The runtime also retains older monolithic package formats for
compatibility tests and migration work; they are not the maintained
publication target.

Only root-component queries and mutations are eligible. Actions, HTTP actions,
system functions, non-root component functions, unselected routes, and routes
without an authenticated graph remain on the existing runtime.

A selected Wasm route is strict. A missing or invalid artifact, source
identity mismatch, engine incompatibility, contract mismatch, memory rejection,
timeout, cancellation, or runtime error never changes that invocation into V8
fallback.

## Primary and verifier modes

Query and mutation authority and sampling are independent. The process-wide
gate permits Wasm-primary routing, while optional per-UDF switches select the
direction for queries and mutations separately:

- for a UDF type configured with V8 primary, an admitted sample may run a Wasm
  shadow;
- for a UDF type configured with Wasm primary, selected routes may run an
  admitted V8 verifier; and
- for a UDF type configured with Wasm primary and an inverse sample rate of
  zero or a sample miss, selected routes use Wasm without a verifier.

In every mode, unselected routes, V8-only source packages, and routes forced to
V8 by quarantine use V8 only. A query verifier runs only for an admitted
query-cache-miss leader or a direct no-cache query invoked by an administrator
or the system. A mutation verifier runs in an independent transaction and its
writes are always discarded. Only the selected primary publishes the response
or query-cache entry and only the selected primary may commit.

Each admitted verifier receives an immutable copy of the invocation timestamp,
caller identity, arguments, journal, RNG seed, and relevant environment input.
The V8 and Wasm lanes use separate transactions and invocation capabilities.
Verifier admission has a shared immediate concurrency limit and does not queue.
V8 verifier execution starts only after the successful Wasm result passes
retention validation, so it cannot consume primary capacity while that Wasm
invocation is still running.
In V8-primary mode, a missing route, overload, cancellation, or Wasm failure
leaves V8 behavior unchanged. In Wasm-primary mode, V8 verifier overload,
failure, timeout, cancellation, divergence, or late completion never becomes
fallback and never delays, fails, or replaces a successful Wasm result.

Shadow reports contain only authenticated generation and route digests,
bounded classifications, timings, counters, generated-runtime controller
memory accounting, and bounded data-free trap capsules. A trap capsule can
include authenticated module-role frame offsets, resource and fuel state,
fixed host-operation outcomes, hashed stderr, and an opaque execution
correlation. Controller accounting is not process RSS or direct
heap measurement. Reports do not contain paths, arguments, values, application
identities, journals, environment values, logs, raw stderr, raw backtraces, or
raw errors. Route evidence is bounded by capacity and retention limits.
Traffic sampling cannot prove unobserved routes or replace deterministic parity
tests.

## Source-keyed activation boundary

In source-keyed mode, startup authenticates only the bounded
`source-catalog.json` and retains immutable generation descriptors. It does not
open or authenticate the historical generations named by those descriptors.
Catalog reload accepts additions and removals while preserving the exact
identity of every descriptor that remains. It installs new descriptors without
generation I/O; a nonempty catalog is required.

The active source-package record in the database names one exact descriptor by
runtime-content, deployment, generation-manifest, and generation digests.
Readiness, active-pair reporting, and invocation selection share the same lazy
load path. Descriptor presence grants no readiness or execution authority. The
selected generation must pass complete generation, deployment, graph,
artifact, lane, and selected-route preload validation before it becomes ready.
An absent descriptor is `NotStaged`; a present generation that cannot be
loaded is an operational failure.

The non-source compatibility mode retains `current`-pointer activation. At
startup it preloads the authenticated current generation and re-confirms the
pointer before publishing the runtime configuration. On reload it authenticates
and preloads the complete candidate, confirms the pointer, and only then
atomically replaces the active generation.

Every resolved route retains an `Arc` to the exact deployment generation. The
routed-module cache identity includes the deployment digest, generation digest,
package or cohort identity, and entry identity. Graph instance-pool identity
also includes the generation digest, graph digest, engine compatibility digest,
and every ordered AOT digest. A same-deployment generation replacement cannot
reuse the retired generation's route cache or instance pool.

Source-keyed full-generation residency is a capacity-two LRU. Eviction marks
the exact generation incarnation retired, removes only its routed-module cache
entries and idle instances, and does not invalidate active `Arc` owners.
Retirement releases the generation's readiness-only compiled-module owners;
active routes and instances retain the compiled modules they still use.
Later loads cannot reuse a retired incarnation's pooled instance even when all
authenticated content digests are equal. Compact authenticated query-shadow
route selections may outlive full-generation residency. A retirement event is
not an on-disk garbage-collection lease.

## Active source binding

Every maintained selected route binds two identities from the active V8
deployment:

- the active runtime module digest; and
- the latest active source-package runtime-content digest.

Before selector discovery can load a package or deserialize AOT, the
function-runner authenticates both identities inside the invocation
transaction. Execution checks them again before using the routed module. A
later V8 deployment therefore invalidates the older Wasm route even when its
route name or generated bytes are unchanged.

Missing, mismatched, or unprovable source authority fails closed. A previous
matching shadow result is not source authority.

## Maintained registry contract

The maintained versions are:

| Contract | Value |
| --- | --- |
| Current pointer | `convex-wasm-runtime-registry-current-v1` |
| Source catalog | `convex-wasm-runtime-registry-source-catalog-v2` |
| Primary-admitted generation | `convex-wasm-runtime-registry-generation-v9` |
| Shadow-only generation | `convex-wasm-runtime-registry-generation-v10-shadow-only` |
| Deployment | `convex-wasm-deployment-v8` |
| Deployment graph binding | `convex-wasm-deployment-module-graph-binding-v2` |
| Cohort contract | `convex-wasm-module-graph-cohort-contract-v2` |
| Graph manifest/package/provenance | version `5` |
| Immutable graph cache | `v6` |
| Artifact cache entry | `convex-wasm-artifact-cache-entry-v5` |
| Artifact pipeline | `convex-wasm-artifact-pipeline-v9` |
| Opaque value ABI | version `3` |
| Engine identity | `convex-wasm-wasmtime-engine-identity` |

The registry layout is:

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
        packages/
          <graph-manifest-sha256>/
            COMPLETE
            build-provenance.json
            graph-manifest.json
            package-entry.json
        artifacts/
          <stage>/
            <cache-key>/
              COMPLETE
              entry.json
              artifact.wasm | artifact.cwasm
  packages/
```

The empty `packages/` directory and compatibility `current` pointer remain
part of the exact graph-only root layout. Source-keyed selection uses
`source-catalog.json`, not `current`. Current module-graph generations must use
the nested immutable generation directory; the legacy flat generation layout
is compatibility-only for older generation kinds.

Registry directories require exact mode `0700`; files require exact mode
`0600`. The loader rejects symlinks at opened boundaries, non-regular files,
missing or extra entries, non-canonical JSON, invalid completion markers,
unsorted or duplicate records, size or digest drift, deployment/binding/cohort
disagreement, and incomplete graph closure. The registry is an
operator-controlled integrity boundary, not a signature system for an
untrusted producer.

## AOT and module contract

Generated routes share one process-wide Wasmtime engine configured with fuel,
epoch interruption, Wasm exceptions, the baseline
`x86_64-unknown-linux-gnu` target, and no runtime profiler. Builds that select
the feature for another target are rejected. The Wasmtime revision is pinned in
the isolate dependency.
Producer profiling metadata remains `perf-map`; profiler selection is not part
of Wasmtime AOT compatibility and does not rotate authenticated artifacts.

Every graph AOT record binds the expected size, SHA-256, engine identity, Core
Wasm input, and module contract. Its serialized AOT payload may be supplied
directly or omitted for destination reconstruction; the Core Wasm payload and
all metadata and completion markers remain mandatory. Generation loading
authenticates the complete graph and artifact closure and preloads every
selected routed module before publishing readiness or activating a non-source
candidate. Each graph-module cache miss:

1. reopens the authenticated Core Wasm and any supplied AOT without following
   its final symlink and rechecks the payload size and SHA-256;
2. verifies that supplied AOT bytes identify a precompiled Wasmtime core
   module, or uses the production engine to compile an omitted AOT payload;
3. verifies that reconstructed bytes exactly match the authenticated AOT size
   and SHA-256 and identify a precompiled core module;
4. deserializes an immutable supplied or reconstructed AOT snapshot; and
5. validates exact ordered imports, exports, host ABI signatures, providers,
   graph layout, selector export, and execution surface.

Invalid AOT or a contract mismatch is a load failure and is never cached as an
executable route. Compiled base, shared, and leaf modules are cached by engine
compatibility digest and AOT digest; routed records remain generation-scoped.
Ready generations retain their compiled graph members so cache pressure cannot
turn a later request into destination compilation. Request-time resolution can
reuse a retained or already cached reconstruction, but it rejects an uncached
omitted AOT payload.

Legacy monolithic packages are copied into an authenticated unlinked snapshot
before `deserialize_open_file`. That compatibility path requires a canonical,
owner-private mode-`0700`, executable, disk-backed snapshot directory. The
current maximum authenticated AOT artifact size is `640 MiB`; Core Wasm is
bounded at `320 MiB`.

## Runtime surface

The current module-graph path uses the guest-native value codec, typed request
envelopes, invocation capabilities, and the guest Promise event loop. Host
imports are accepted only when the authenticated graph contract names the exact
import and the active runtime provides the exact signature. Capability dispatch
preserves the ordinary transaction owner, query journal, accounting,
validation, timeout suspension, cancellation, and OCC behavior.

Invocation state owns opaque values, outstanding async operations, host-secret
buffers, fuel, timeout state, and the invocation capability. Reuse is allowed
only after authority is revoked, operations and handles are cleared, and the
initialization read set validates in the new transaction.

The older compiler-assigned operation-descriptor and monolithic Promise
contracts remain compatibility surfaces. They do not describe the maintained
deployment-v8 module-graph architecture.

## Memory, caching, and failure behavior

The memory controller accounts for cached modules, active and idle Stores,
guest growth, retained host values, forecast liability, and a safety reserve.
Module admission charges at least the authenticated Core Wasm and AOT sizes,
then resizes to include Wasmtime's mapped image range. Cache-only least-recently
used entries may be evicted, but active or pooled instances retain module
ownership. In-memory graph loading reserves the authenticated Core Wasm
allowance and both the expected AOT image and Wasmtime code mapping until a
supplied input buffer or reconstructed output buffer is released.

Exact generation loads use per-selector single-flight work and one global cold
generation permit. Caller cancellation does not abandon an in-progress shared
load. Catalog and residency locks are never held across filesystem access,
hashing, destination compilation, deserialization, or route preload. Exact
source-keyed loads and non-source reloads run on blocking workers; initial
non-source loading completes during startup before request workers are exposed.

Active guest CPU has a separate concurrency limit. Async host waits release
that CPU permit but retain the Store and memory permit. Shadow work uses
immediate admission; primary execution may wait only for its configured bounded
admission interval.

The controller also retains cumulative per-authenticated-route statistics for
every started Wasm execution. Primary and shadow populations, fresh and reused
counts, logarithmic quantile bounds, and exact maxima are available through the
authenticated `/api/app_metrics/generated_wasm_memory_statistics` endpoint.
The route store is process-local and bounded to 4,096 least-recently-used
records; restart and record eviction lose history, and the endpoint reports the
eviction count. Query-shadow evidence retains only bounded raw memory-anomaly
examples and is not the percentile authority. The operations guide defines the
response and retention semantics.

Impossible route drift, invalid artifacts, platform failures, and internal
errors propagate. The runtime does not log and continue, silently substitute
defaults, or retry on V8.

## Configuration

The image must enable Cargo feature `static-hermes-wasmtime-gate`. The principal
runtime settings are:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `CONVEX_STATIC_HERMES_WASM_GATE_ENABLED` | `0` | Normal Wasm-primary routing switch. |
| `APPLICATION_STATIC_HERMES_QUERY_WASM_PRIMARY_ENABLED` | follows process-wide gate | Optional query-specific Wasm-primary switch. |
| `APPLICATION_STATIC_HERMES_MUTATION_WASM_PRIMARY_ENABLED` | follows process-wide gate | Optional mutation-specific Wasm-primary switch. |
| `CONVEX_STATIC_HERMES_WASM_GATE_RUNTIME_REGISTRY_ROOT` | unset | Authenticated registry root. |
| `CONVEX_STATIC_HERMES_WASM_GATE_SOURCE_KEYED_DEPLOYMENT` | `0` | Select exact generations from the database source record and source catalog. |
| `CONVEX_STATIC_HERMES_WASM_GATE_SERIALIZED_MODULE_SNAPSHOT_DIRECTORY` | unset | Required private executable snapshot directory when a registry or compatibility package is configured. |
| `CONVEX_STATIC_HERMES_WASM_GATE_REUSE_INSTANCES` | `1` | Retain clean generated instances; set to `0` only for diagnostic fresh-instance execution. |
| `APPLICATION_STATIC_HERMES_QUERY_SHADOW_BPS` | `0` | Query shadow sampling. |
| `APPLICATION_STATIC_HERMES_MUTATION_SHADOW_BPS` | `0` | Mutation shadow sampling. |
| `APPLICATION_STATIC_HERMES_QUERY_WASM_PRIMARY_V8_SHADOW_BPS` | `0` | Query V8-verifier sampling behind a Wasm primary. |
| `APPLICATION_STATIC_HERMES_MUTATION_WASM_PRIMARY_V8_SHADOW_BPS` | `0` | Mutation V8-verifier sampling behind a Wasm primary. |

Verifier sampling requires an authenticated registry. When
`CONVEX_STATIC_HERMES_WASM_GATE_ENABLED=0`, both Wasm-primary/V8-verifier rates
must be zero and neither per-UDF switch may enable Wasm primary. For each UDF
type, only the sampling rate behind its selected primary may be nonzero. Query
and mutation may use opposite directions. Contradictory settings fail startup
instead of being ignored. Zero inverse rates for a Wasm-primary UDF type select
execution without a V8 verifier. A shadow-only generation is rejected if either
UDF type has normal routing enabled. Boolean settings accept only `0` or `1`.
Configuration is loaded at process start. Source-keyed mode polls the source
catalog for authenticated descriptor changes; non-source mode continues to poll
`current` for authenticated generation replacement.

Compatibility-only direct-package and legacy deployment settings remain in the
code for focused tests. New publication and operational procedures should use
the maintained registry contract above.

## Validation limits

Focused tests cover descriptor-only startup and reload, exact-selector lazy
loading, bounded generation residency, exact-incarnation retirement, graph
authentication, artifact tampering, exact module contracts, source binding,
shadow isolation, capability execution, memory admission, timeouts,
cancellation, supplied-AOT deserialization, and destination AOT reconstruction.

These tests do not establish complete V8/Wasm parity or capacity for a
particular deployment. Promotion requires deterministic route coverage,
representative memory and latency testing, OCC and cancellation evidence, and
an explicit operator decision. See the operations guide for the maintained
validation and rollback sequence.
