# Static Hermes Wasm UDF Operations and Validation

This is a generic self-hosted adoption guide for the
[Static Hermes Wasm UDF patch](README.md). It does not claim that any deployment
has completed the required parity, safety, or capacity gates.

## Feature build

Build the backend with Cargo feature `static-hermes-wasmtime-gate`. The
self-hosted Dockerfile accepts features through `LOCAL_BACKEND_FEATURES`. The
authenticated AOT contract currently requires Docker target architecture
`amd64` (`x86_64-unknown-linux-gnu`):

```sh
docker build \
  --build-arg LOCAL_BACKEND_FEATURES=static-hermes-wasmtime-gate \
  --build-arg CARGO_BUILD_JOBS=8 \
  -t convex-backend:static-hermes-local \
  -f self-hosted/docker-build/Dockerfile.backend \
  .
```

Normal images omit the runtime unless the feature is selected.

## Maintained operating mode

The maintained validation path uses an authenticated deployment-v8
module-graph registry, source-keyed selection, and V8-primary query and
mutation shadows. Use
`convex-wasm-runtime-registry-generation-v10-shadow-only` for this phase. The
runtime rejects that generation kind if normal Wasm routing is enabled.

`convex-wasm-runtime-registry-generation-v9` is structurally primary-admitted.
Publishing and pairing it is a separate promotion decision after shadow
evidence, deterministic parity, capacity, OCC, timeout, and cancellation gates
have passed. Do not use generation-v9 merely to collect shadow evidence.

After promotion, normal routing makes Wasm primary only for selected routes.
Those routes may run a sampled V8 verifier or may run without a verifier.
Unselected routes, V8-only source packages, and quarantined routes remain
V8-only. Only the selected primary publishes or commits. A V8 verifier timeout,
failure, overload rejection, cancellation, divergence, or late completion is
discarded; it never becomes fallback and never delays or replaces a successful
Wasm result.

Compatibility-only direct packages, legacy deployment manifests, older
generation kinds, and non-source `current` activation remain available for
focused tests and migration. They are not the maintained operational
procedure.

## Moving frozen deployments between targets

External dependency package IDs and storage keys belong to the destination.
To reuse a frozen deployment on a different backend, send its retained archive
to `POST /api/deploy2/prepare_external_deps` with the usual administrator key
and `nodeDependencies`, plus `archive: { sha256, bytes }`. `sha256` is the
archive's hexadecimal SHA-256; `bytes` is its unpadded base64url encoding.
The backend checks the digest, ZIP metadata and package size, stores the exact
bytes, and returns the normal prepared-package descriptor. This operation
does not resolve dependencies, install packages, or activate application code.

Bind the returned ID and unchanged archive digest in `start_push`. Keep the
original module bytes, dependency declarations and Node version unchanged.
Runtime content identity excludes destination-owned IDs and storage keys, so
this rebinding does not require recompiling compatible Wasm artifacts.
Ordinary clients omit `archive` and retain normal dependency preparation.
An explicit `archive: null` is rejected rather than starting a dependency build.

## Configuration

Boolean gate variables accept only `0` or `1`. Invalid or inconsistent values
fail startup.

| Variable | Default | Contract |
| --- | ---: | --- |
| `CONVEX_STATIC_HERMES_WASM_GATE_ENABLED` | `0` | Normal Wasm-primary routing switch. |
| `APPLICATION_STATIC_HERMES_QUERY_WASM_PRIMARY_ENABLED` | follows process-wide gate | Optional query-specific Wasm-primary switch; accepts only `0` or `1`. |
| `APPLICATION_STATIC_HERMES_MUTATION_WASM_PRIMARY_ENABLED` | follows process-wide gate | Optional mutation-specific Wasm-primary switch; accepts only `0` or `1`. |
| `CONVEX_STATIC_HERMES_WASM_GATE_RUNTIME_REGISTRY_ROOT` | unset | Exact authenticated registry root. |
| `CONVEX_STATIC_HERMES_WASM_GATE_SOURCE_KEYED_DEPLOYMENT` | `0` | Select the exact source/generation pair stored in the database source record. |
| `CONVEX_STATIC_HERMES_WASM_GATE_SERIALIZED_MODULE_SNAPSHOT_DIRECTORY` | unset | Required with a configured registry; canonical owner-private executable directory. |
| `CONVEX_STATIC_HERMES_WASM_GATE_REUSE_INSTANCES` | `1` | Retain clean generated instances; set to `0` only for diagnostic fresh-instance execution. |
| `CONVEX_STATIC_HERMES_WASM_GATE_HOST_SECRET_SELECTORS` | unset | Unique comma-separated selectors available to authenticated routes. |
| `APPLICATION_STATIC_HERMES_QUERY_SHADOW_BPS` | `0` | Query shadow sampling in basis points for admitted cache-miss leaders and direct administrator/system no-cache queries. |
| `APPLICATION_STATIC_HERMES_MUTATION_SHADOW_BPS` | `0` | Mutation shadow sampling in basis points. |
| `APPLICATION_STATIC_HERMES_QUERY_WASM_PRIMARY_V8_SHADOW_BPS` | `0` | Query V8-verifier sampling in basis points behind a Wasm primary. |
| `APPLICATION_STATIC_HERMES_MUTATION_WASM_PRIMARY_V8_SHADOW_BPS` | `0` | Mutation V8-verifier sampling in basis points behind a Wasm primary. |
| `APPLICATION_STATIC_HERMES_SHADOW_CONCURRENCY` | `8` | Shared immediate shadow concurrency. |
| `APPLICATION_STATIC_HERMES_SHADOW_TIMEOUT_MILLISECONDS` | `30000` | Positive shadow wall-clock deadline. |
| `APPLICATION_STATIC_HERMES_SHADOW_MAX_ACTIVE_ROUTES` | `2048` | Bounded opaque route-evidence capacity, applied independently to each runtime direction. |

The maintained workflow sets
`CONVEX_STATIC_HERMES_WASM_GATE_SOURCE_KEYED_DEPLOYMENT=1`. Any nonzero verifier
sampling rate requires an authenticated registry. The process-wide gate must be
`1` before either per-UDF primary switch can enable Wasm authority. An unset
per-UDF switch follows that gate for compatibility. For each UDF type, its
V8-primary shadow rate must be zero when Wasm primary is selected, and its
Wasm-primary V8-verifier rate must be zero when V8 primary is selected. Query
and mutation directions may differ. Zero verifier rates or a sample miss run
selected Wasm-primary routes without a verifier. Contradictory settings fail
startup. A shadow-only generation cannot be selected while any normal routing
is enabled.

Verifier admission uses immediate try-acquire and never queues. Eligible query
verifiers are admitted cache-miss leaders and direct no-cache queries invoked by
an administrator or the system. Mutation verifiers run in independent
transactions whose writes are discarded. Only the configured primary may
publish or commit.

Environment configuration is process-start state. In source-keyed mode the
backend polls `source-catalog.json` for authenticated descriptor changes. It
does not activate a descriptor through `current`. Non-source compatibility
mode continues to poll `current` for authenticated whole-generation
replacement.

Before the first artifact publication, a configured registry may be an existing,
private, empty directory while Wasm-primary routing is disabled. Startup and
reload then retain an unpublished state with no executable Wasm routes. The
normal polling path admits the first complete publication without restarting
the backend. A missing directory or malformed nonempty registry still fails
startup; an invalid reload retains the last admitted state. An empty directory
after a generation or catalog was admitted is not treated as unpublished.

Ordinary V8 deployments remain compatible with older clients that send no
source-keyed activation contract. They clear the selected Wasm generation in
the new source-package record, so retained source-keyed artifacts do not
authorize shadows for that publication. Clients explicitly requesting paired
activation still require the exact load-verified generation and expected-prior
checks. Enabling shadow sampling does not require every client to use paired
deployment.

The snapshot directory must be canonical, absolute, owned by the backend's
effective user, exact mode `0700`, writable, executable, and disk-backed. Linux
`tmpfs` and `noexec` mounts are rejected. The directory supports retained
compatibility package formats; graph artifacts are reopened and digest-checked
immediately before deserialization. It remains required while any registry is
configured.

## Registry preflight

Mount the registry read-only for the backend. Its exact graph-only root entries
in maintained source-keyed mode are:

```text
current
source-catalog.json
generations/
module-graph-cache/
packages/
```

The compatibility `current` file remains in the exact root layout but has no
source-keyed activation authority. Current generations live at:

```text
generations/<deployment-sha256>/<generation-sha256>/
```

Generation-v9 and generation-v10 are rejected from the older flat generation
layout. Registry directories require exact mode `0700`; files require exact
mode `0600`. The backend rejects extra entries, symlinks at opened boundaries,
non-regular files, non-canonical JSON, incomplete markers, record ordering
errors, and all size or digest disagreement.

Source-keyed startup authenticates only the root structure and bounded
`convex-wasm-runtime-registry-source-catalog-v2`. It validates 1 to 4,096
unique sorted descriptors without opening the named generation directories.
Each descriptor authenticates the runtime-content, deployment,
generation-manifest, and generation digests plus the generation-manifest size.
Descriptor presence alone is not readiness or execution authority.

The first exact selection authenticates the named generation, deployment-v8,
v2 graph binding, v2 cohort contracts, v5 graph packages and provenance, cache
entries, every Core Wasm payload, and every supplied AOT payload. For an AOT
record whose payload was intentionally omitted, it compiles the authenticated
Core Wasm with the production engine and verifies the exact expected AOT size
and SHA-256. It then deserializes and validates every selected routed module
before publishing readiness. AOT loading is therefore lazy by generation, but
complete for the selected generation at the readiness boundary; routed
requests cannot perform destination compilation.

Before a selected invocation can derive host-secret selectors, the
function-runner authenticates the active module and latest source-package
runtime-content digest inside its transaction. Execution repeats the check
before routed module use. Source mismatch fails closed before package or AOT
access.

## Publication, reload, and retention

For every candidate:

1. publish all immutable v5 graph packages and artifacts;
2. publish the nested complete generation-v9 or generation-v10 directory;
3. construct a v2 source catalog containing every existing descriptor plus the
   candidate descriptor;
4. write and sync the new catalog as a private sibling, atomically replace
   `source-catalog.json`, and sync the registry root; and
5. wait for an `accepted` registry reload event before requesting readiness.

The backend loads and authenticates only the bounded catalog during reload.
It accepts a nonempty successor that adds or removes selectors, but a selector
that remains must keep its exact authenticated descriptor, including the
generation-manifest size. A changed retained descriptor or malformed catalog
is rejected without changing the installed catalog. A racing publication is
retried from the next observed catalog state. An added descriptor is not
opened during reload.

Do not replace `current` for source-keyed publication. That instruction applies
only to non-source compatibility mode, where the backend authenticates the
complete candidate, confirms that `current` still names it, and atomically
replaces the active generation.

## Readiness and paired activation

The deploy-key-protected readiness operation is:

```text
POST /source_keyed_runtime_readiness
```

Its request names the exact four-digest selector:

```json
{
  "deploymentSha256": "<lowercase-sha256>",
  "generationManifestSha256": "<lowercase-sha256>",
  "generationSha256": "<lowercase-sha256>",
  "sourcePackageRuntimeContentSha256": "<lowercase-sha256>"
}
```

`ready` means the exact generation is resident, completely authenticated,
lane-admitted, load-verified, and route-preloaded. `notStaged` means only that
the exact descriptor is absent. If a present descriptor cannot be loaded or
validated, the request fails as an operational error; it must not be treated
as absence.

The deploy-key-protected active-state operation is:

```text
GET /source_keyed_runtime_active_pair
```

It reads the latest source-package record from one database snapshot and
ensures the exact generation paired with its runtime-content digest. `paired`
therefore reports database-visible source identity plus a load-verified
generation, not the newest catalog descriptor.

For a paired `finish_push`:

1. read the active pair and retain its exact source-package ID, source archive
   digest, optional runtime-content digest, and optional generation as the
   expected prior state;
2. publish the target immutable generation and additive catalog;
3. wait for catalog reload and request exact readiness until it returns
   `ready`;
4. submit `finish_push` with the expected prior state and the exact target
   generation; and
5. read the active pair again and require the committed source and generation
   to equal the requested pair.

`finish_push` repeats readiness before mutation and checks the expected prior
inside the commit transaction. A concurrent source or runtime change causes an
expected-prior or OCC failure instead of pairing the target with the wrong
source. Retry the complete read-stage-activate sequence with a fresh expected
prior; do not reuse a stale activation request.

After every backend restart, call the active-pair operation and require
`paired` before enabling Wasm-primary traffic or shadow sampling. Startup is
descriptor-only, so this check also loads and preloads the database-active
generation before request routing can encounter a cold exact selection.

## Residency and retirement

Source-keyed full-generation residency is a capacity-two LRU shared by
readiness, active-pair reporting, and invocation selection. A third exact
generation evicts the least-recently used generation even when an in-flight
invocation still owns an `Arc` to it. The active owner remains valid, but the
registry no longer owns a residency slot.

Eviction retires the exact generation incarnation, removes only routed-module
cache entries for that pointer, and prevents checkout of pooled instances from
that incarnation. This remains true when a later load has the same deployment
and generation digests. Retirement releases readiness-only compiled-module
ownership while active routes retain their own compiled modules. Idle cleanup
may complete later; an instance returned after retirement is destroyed instead
of pooled.

Exact cold loads use one single-flight per selector and one process-wide cold
generation permit. Caller cancellation does not abandon the shared load.
Catalog and residency locks are not held during filesystem access, hashing,
deserialization, or route preload.

Residency retirement alone does not authorize disk deletion. After the new
source/runtime pair commits, an operator may publish a catalog retaining only
that pair, confirm that the backend admits the smaller catalog and the active
pair remains ready, then remove unreferenced graph packages, artifacts, and
generation directories. If `current` names a generation being removed, point
it at the retained complete generation first; it remains compatibility-only
and grants no source-keyed routing authority. This needs no backend restart.
An older in-flight invocation may still own a retired generation; catalog
removal is not an in-flight liveness lease.

## Generated memory policy

| Variable | Default |
| --- | ---: |
| `CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_INSTANCE_HARD_CEILING` | `200` |
| `CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_SOFT_BUDGET_BYTES` | `4294967296` |
| `CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_HARD_BUDGET_BYTES` | `6442450944` |
| `CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_SAFETY_RESERVE_BYTES` | `536870912` |
| `CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_UNATTRIBUTED_PER_SLOT_BYTES` | `2097152` |
| `CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_COLD_FORECAST_BYTES` | `67108864` |
| `CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_WARM_IDLE_TARGET` | `8` |
| `CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_MAX_IDLE_SECONDS` | `300` |
| `CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_ADMISSION_WAIT_MILLISECONDS` | `30000` |
| `CONVEX_STATIC_HERMES_WASM_GATE_ACTIVE_CPU_CONCURRENCY` | required with a configured registry |

The hard instance ceiling is `1..=200`. The soft budget must be below the hard
budget, the safety reserve below the soft budget, cold forecast plus reserve
must fit the hard budget, and the warm target cannot exceed the instance
ceiling. Active CPU concurrency must be between one and the instance ceiling.

Initial file-backed module admission reserves:

```text
authenticated AOT bytes + authenticated Core Wasm bytes
```

An authenticated in-memory module cold load reserves:

```text
2 * authenticated AOT bytes + authenticated Core Wasm bytes
```

The second AOT term covers the authenticated input buffer retained while
Wasmtime creates its code mapping.

The final fixed module charge is:

```text
max(Wasmtime image range, authenticated AOT bytes)
    + authenticated Core Wasm bytes
```

Shared base, shard, and leaf code is deduplicated by engine compatibility
digest and AOT digest while an owning routed graph remains live. Route
identities and pools remain generation-scoped. Cache eviction does not prove
native code was released while a Store, routed module, or shared module retains
ownership.

Cgroup pressure hysteresis uses
`LOCAL_BACKEND_MEMORY_PRESSURE_ENTER_HEADROOM_BYTES` and
`LOCAL_BACKEND_MEMORY_PRESSURE_EXIT_HEADROOM_BYTES`, defaulting to `3 GiB` and
`5 GiB`. Missing or unlimited cgroup samples do not disable instance, byte,
forecast, or reserve enforcement.

Async host waits release active guest CPU but retain the Store, memory permit,
forecast, and live-byte charge. Default values are implementation priors; they
must be calibrated under the actual cgroup, artifact sizes, V8/Wasm mix, and
reuse policy.

## Observability

Primary and verifier executions use the same timestamped database inputs, but
record their own read dependencies; verification does not replay the primary's
reads. Before handler execution, the verifier also receives the primary's
document-ID generator state and creation-time cursor. This preserves the
primary's normal allocation behavior while giving the non-committing verifier
the same allocation inputs, including index boundaries when a handler reads
its own inserts. A detached V8 verifier retains that initial state, not the
primary's advanced state after it finishes. Ordinary independent transactions
continue to allocate independently. Comparison still requires one consistent
inserted-ID relation across results, reads, and writes; sharing allocation
inputs does not replace read-set comparison or hide additional verifier reads.
Uncommitted inserts supplied before the invocation remain fixed inputs: their
IDs, creation times, and read dependencies are not lane-local allocations.
Encoded byte values stay opaque during identity comparison, including bytes
inside decoded scheduled arguments; Base64 text is never treated as an ID.
When both lanes use the same inserted IDs, comparison first checks that exact
relation across results, reads, and writes. Other relations still use the
bounded search; identical large batches do not consume its permutation budget.

Holding the shared reader does not pin historical data indefinitely. The
primary receives its normal post-execution retention check, and a detached
verifier receives a separate check after its reads finish. If that final check
fails, the attempt is recorded as `invalid_shadow`, not agreement or semantic
divergence. This also takes precedence over a completed verifier execution
failure in either direction. The check runs within the bounded background
deadline and cannot delay or retract the already returned primary result.

Execution and registry metrics include:

- `convex_local_backend_static_hermes_wasmtime_gate_phase_seconds`;
- `convex_local_backend_static_hermes_wasmtime_gate_guest_fuel_operations`;
- `convex_local_backend_static_hermes_wasmtime_gate_instance_pool_total`; and
- `convex_local_backend_static_hermes_wasmtime_gate_registry_reload_total`.

The gate phase histogram separates retained-context read-set validation, guest
initialization and selected-entry preparation, handler export execution, and
result finalization and cleanup. Its timers close on both success and error
paths. These phases are host-observed wall-clock intervals, not guest CPU time.

WASI stdout and stderr are diagnostic-only output. The runtime rejects a write
that would make their combined retained output exceed the fixed result-byte
limit for one invocation. The check applies cumulatively across both streams
and occurs before the pending write buffer is allocated.

Reload events are `accepted`, `rejected`, `poll_failed`, and `retired`.
`accepted` covers an authenticated source-catalog change or non-source current
replacement. `retired` records attempted idle cleanup; it does not prove that
every old handle or mapped module was released.

Memory metrics include:

- `convex_local_backend_generated_wasm_memory_events_total`;
- `convex_local_backend_generated_wasm_memory_instance_policy_total`;
- `convex_local_backend_generated_wasm_memory_instances_info`;
- `convex_local_backend_generated_wasm_memory_bytes`;
- `convex_local_backend_generated_wasm_memory_cgroup_headroom_bytes`; and
- `convex_local_backend_generated_wasm_memory_cgroup_sample_available_info`.

The instance-policy gauge uses the `setting` label with
`hard_instance_ceiling` and `warm_idle_target` values.

Metric labels omit deployment, generation, route, package, module, function,
request, user, and tenant identities. Two authenticated route-level surfaces
retain opaque generation and route digests without application paths or
execution payloads.

`/api/app_metrics/query_shadow_evidence` returns retention-bounded comparison
counts, timings, and diagnostics. A memory diagnostic appears only for a
forecast overrun, denied growth, or successful discard while reuse is enabled.
These raw anomaly examples remain correlated with the route and direction but
do not calculate memory percentiles. Timing summaries include exact completed
sample counts, p50, p90, p99, and the retained maximum. A zero sample count has
null timing values; a positive sample count has all values, ordered from p50
through the retained maximum. Each retained diagnostic also includes its Unix
millisecond recording time.

`/api/app_metrics/generated_wasm_memory_statistics` returns the generated-memory
controller's cumulative statistics for every started authenticated execution
whose route record remains in the current backend process. Query and mutation
populations are read separately. Each route contains an aggregate and distinct
primary and shadow populations. Counts distinguish fresh and reused execution,
pool return, forecast overrun, denied growth, successful discard, and terminal
outcome. Fresh and reused populations also retain exact maxima.

Byte p50, p90, and p99 values are conservative upper bounds from logarithmic
histograms. `maximum` is the exact largest observed value; it does not age out
after a fixed number of later calls. A quantile bucket upper bound can exceed
the exact maximum, and a sufficiently rare exact maximum can exceed p99.
The overflow bucket above 6 GiB reports the platform's largest unsigned byte
value (on 64-bit backends, `18446744073709551615`), not a measured allocation.
JavaScript JSON readers round this bound upward to `2^64`.
The route-statistics store retains at most 4,096 least-recently-used records.
`routeRecordEvictionCount` reports record loss. A backend-process restart loses
all route statistics. The endpoint scope
`backend_process_route_record_lifetime` states this boundary. The response
aggregate covers every observed selected route, not only the current page.

The endpoint requires deployment metrics permission and accepts `udfType=query`
or `udfType=mutation` (default `query`), a `limit` from 1 through 100 (default
25), and an optional generation-bound `cursor`. It returns only observed route
records, ordered by route digest. Each page is an atomic controller snapshot,
not a retained snapshot shared by later requests. Consumers must reject a
generation, selection, capacity, eviction-count, or aggregate change across
pages and verify that the collected routes merge to the aggregate. Use a quiet
interval when a stable paginated read is required. Registry or controller
failures return a generic error without private diagnostic context.

Controller memory observations are independent of semantic comparison
completion and sampling. A
failed, timed-out, cancelled, or discarded execution retains the accounting
available at cleanup, but an invocation that does not enter Wasmtime emits no
memory observation. Statistics and anomaly diagnostics do not grant readiness,
admission, correctness, or promotion authority. A memory anomaly does not make
a route semantically observed by query-shadow `evidenceCount`.

The byte fields are controller accounting, not process RSS or a direct heap
measurement. `peakAdmittedBytes` excludes denied growth, but can include an
allocation that the controller admitted and the allocator later failed.
`requestedPeakBytes` includes growth requests reported to the controller as
denied, and is the peak used for controller learning. A resource-specific
limit can reject an operation before the controller observes it.
`forecastGrowthBytes` is the admission forecast above the checkout baseline
and underpins the initial admission liability; it is not a memory measurement.
`peakGuestBytes` and `peakHostBytes` are separate admitted controller peaks, so
their sum need not equal the combined peak.
`completionBytes` is the last combined accounting before pool return or
discard. `returnedToPoolBytes` is the accounted size at the pool-return
transition, and is zero when `returnedToPool` is false; it is not a promise
that those bytes remain resident. `reusedInstance` describes the Store that
actually executed. It can be false with a nonzero checkout baseline when a
stale pooled Store was discarded before a fresh Store executed.

`successfulExecutionDiscarded` identifies successful executions discarded
while reuse was enabled; diagnostic fresh-instance policy does not turn every
successful invocation into an anomaly. The fixed `terminal` value classifies
the controller completion as success, developer error, system error,
cancellation, timeout, resource limit, or memory limit.

A retained host-operation trace mismatch includes comparison-eligibility flags
and at most 16 differing operation/status counts for the primary and shadow
lanes. The operation and status names are fixed enums; syscall arguments,
paths, IDs, values, and error text are not retained. `omittedDifferingCount`
reports any additional count categories beyond that bound.

An `invalid_shadow` terminal carries `invalidReason`: `snapshot_validation`,
`lane_type_mismatch`, `query_writes`, `outcome_type`, `missing_handler_reads`,
`incomplete_host_trace`, or `write_normalization`. These fixed categories identify
the rejected contract without retaining the underlying error or transaction.
They do not turn an invalid observation into agreement or divergence. Older
backends omit the reason; that absence does not identify a cause.

Wasm-shadow initialization timeout, handler execution timeout, and instruction
budget exhaustion are `shadow_failure` terminals with fixed reasons
`initialization_timeout`, `execution_timeout`, and `instruction_budget`.
These causes come from native execution outcomes, not guest error text. They
are incomplete verification, not semantic divergence; initialization can stop
before handler-read capture begins. The corresponding reason counts contribute
to the total shadow-failure count. Wasm-primary keeps its canonical UDF error
behavior, and developer errors with timeout-like text remain ordinary outcomes.

Generated Wasm execution can also report the data-free typed reasons
`host_owned_bytes_limit`, `opaque_handle_limit`,
`opaque_handle_space_exhausted`, `aggregate_memory_limit`, `operation_limit`,
`host_abi_invariant`, and `wasmtime_trap`. The first host resource or ABI
failure survives later Wasmtime trap wrapping. An ordinary handler trap uses
`wasmtime_trap`, while selector and preparation traps remain
`generated_export_dispatch`. These reason fields extend the broader legacy
shadow-failure reason fields; they do not replace them.

A retained `wasmtime_trap` terminal also carries `wasmtimeTrapDiagnostic`.
`code` is a fixed Wasmtime trap category. The capsule retains at most 64
backtrace frames, with an explicit truncation flag. Each frame contains only an
authenticated module role and ordinal, function index, and optional function
and module offsets. It also records the invocation's opaque execution
correlation, whether the Store was reused, fuel limit and remaining fuel, and
current invocation resource counts and limits. These capsules are retained for
handler traps. Selector and selected-entry preparation failures keep their
separate generated-export dispatch classification.
When host-operation tracing was enabled, the capsule retains only the last 32
fixed operation/status pairs and an explicit truncation flag; otherwise it
marks the trace unavailable. Stderr evidence is only the fixed `empty`,
`static_hermes_uncaught_exception`, or `unclassified` category, byte count, and
SHA-256. The Static Hermes category requires the exact fixed runtime marker;
other content remains unclassified. The capsule never retains
function names, source paths, guest values, arguments, raw stderr, or raw error
and backtrace text. Older evidence with only `functionIndex` and
`functionOffset` remains readable by the maintained downstream tool.

Result diagnostics retain the first bounded structural difference for queries,
using array indexes or sorted object-field ordinals, never field names or values.
For error outcomes in either UDF type, `primaryError` and `shadowError` can
retain a fixed message category, presence of custom error data, and SHA-256 of
the exact UTF-8 message. The fingerprint is null when the message exceeds the
64 KiB inspection bound. Operators can compare known fixed source/runtime error
messages with these fingerprints without retaining raw error text or stacks.
Categories describe message syntax, not an authenticated failure origin; the
diagnostic never changes agreement or substitutes for mutation-ID normalization.
Fingerprints are private correlation evidence, not anonymous data or metric
labels. Successful mutation values still require the proven inserted-ID mapping
before structural inspection and omit this diagnostic.

Timing percentiles without completed sample counts are not evidence. Route
sampling is traffic- and query-cache-biased. Use deterministic route coverage
for promotion decisions.

## Focused verification

Run the narrow supplied-AOT deserialization regression:

```sh
scripts/run_cargo.sh test -p wasmtime_deserialize_regression
```

Run the focused source-keyed lifecycle checks:

```sh
scripts/run_cargo.sh test -p isolate \
  --features static-hermes-wasmtime-gate \
  source_keyed_lazy_load_and_additive_reload_dedupe_1421_capability_selectors

scripts/run_cargo.sh test -p isolate \
  --features static-hermes-wasmtime-gate \
  descriptor_only_startup_does_not_open_an_unused_generation

scripts/run_cargo.sh test -p isolate \
  --features static-hermes-wasmtime-gate \
  source_keyed_residency_evicts_the_lru_even_while_an_invocation_pins_it

scripts/run_cargo.sh test -p isolate \
  --features static-hermes-wasmtime-gate \
  delayed_retirement_does_not_evict_a_later_same_digest_incarnation

scripts/run_cargo.sh test -p isolate \
  --features static-hermes-wasmtime-gate \
  generated_wasm_warm_entry_preparation_preserves_environment_dependencies

scripts/run_cargo.sh test -p isolate \
  --features static-hermes-wasmtime-gate \
  paired_insert_then_first_read_dependencies

scripts/run_cargo.sh test -p function_runner \
  --features static-hermes-wasmtime-gate,testing \
  query_shadow
```

Run focused application and local-backend tests for paired `finish_push`,
readiness, and active-pair authorization separately. External publisher
acceptance must set `CONVEX_WASM_MODULE_GRAPH_REGISTRY_V5_FIXTURE` for the
existing ignored cross-boundary test, pass `-- --ignored`, use an actual
generation-v9 or generation-v10 registry, and exercise at least one
authenticated graph route. The environment variable name is retained test
compatibility; it does not change the maintained contract. A skipped ignored
test is not acceptance evidence.

A disposable backend validation must compare V8 and Wasm for:

- packed results and deterministic developer errors;
- reads, query streams, writes, scheduler effects, accounting, and OCC retry;
- caller identity, invocation time, randomness, environment input, and logs;
- timeout, cancellation, fuel, guest memory, host memory, handle, and operation
  limits;
- invalid, missing, wrong-kind, tampered, and engine-incompatible artifacts;
- source deployment changes before and during paired activation; and
- cold and warm execution, reuse, pressure, restart, residency eviction,
  retirement, and rollback.

Production-like memory, native-code, concurrency, cold-start, and pressure
calibration remain separate gates.

## Validation sequence

1. Build and identify a feature-enabled backend image.
2. Produce a complete deployment-v8 graph closure with the pinned engine and
   publish a generation-v10-shadow-only descriptor through an additive v2
   source catalog.
3. Start in source-keyed mode with normal routing disabled, both V8-primary
   shadow rates at zero, both inverse verifier rates at zero, and reuse disabled.
   Verify descriptor-only startup, exact readiness, and a paired active-state
   response.
4. Enable a small query shadow rate. Verify immediate shadow admission, source
   authentication, bounded evidence, cancellation, and rollback to zero.
5. Enable a small mutation shadow rate. Verify that only V8 commits and shadow
   writes are discarded across success, timeout, error, and cancellation.
6. Complete deterministic parity for every selected route. Traffic evidence
   cannot substitute for unobserved routes.
7. Calibrate memory, CPU concurrency, and the chosen reuse policy under a
   representative cgroup and mixed workload.
8. Only after an explicit promotion review, publish a complete generation-v9
   descriptor, require exact readiness, pair it with the matching source, set
   the V8-primary shadow rates to zero, and enable normal routing for a
   controlled Wasm-primary population. Set the inverse rates to zero for no
   verifier or to the desired bounded V8-verifier samples.

## Rollback

For V8-primary shadow validation, set both V8-primary shadow basis-point
settings to `0` and restart. V8 remains authoritative throughout, so no data or
schema rollback is needed.

For Wasm-primary execution, setting both inverse verifier rates to `0` disables
only V8 verification. It does not change primary authority.

For Wasm-primary routing, the kill switch is:

```text
CONVEX_STATIC_HERMES_WASM_GATE_ENABLED=0
```

Set both inverse verifier rates to zero in the same process configuration; the
per-UDF primary switches cannot override a disabled process-wide gate. The kill
switch requires restart. Keep the registry mounted during the first rollback so
startup can authenticate the source catalog while every route stays on V8. If
registry loading itself prevents startup, remove the registry-root setting and
mount as one configuration change.

A source-keyed generation rollback does not use `current` for activation.
If the previous exact descriptor and its files remain, request readiness and
run paired `finish_push` with a fresh expected prior and that descriptor as the
target. If retention removed them, transfer the previous immutable generation
and publish its descriptor again before requesting readiness. Verify the
resulting active pair. A malformed or unavailable generation fails readiness
and remains inactive.

Only non-source compatibility mode rolls back by atomically republishing a
valid `current` pointer for an already complete immutable generation. Do not
hand-edit a source catalog, generation, graph package, cache entry, artifact,
or completion marker.

Before restoring an image without this patch, disable normal routing and drain
generated execution. Then restore the image and configuration together. A Wasm
timeout, memory rejection, source mismatch, package failure, or runtime error
is not automatic rollback and never reroutes that invocation to V8.
