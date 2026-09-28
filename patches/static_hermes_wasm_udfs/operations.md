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

The build starts its own sccache server before Cargo and stops it when the build
exits. It disables the server's idle timeout because the interval between cache
requests can include a long active compilation. Expiring that interval can
orphan the compiler and make the client launch a duplicate compilation.

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

The binary's capabilities response advertises `sourceKeyedPreparedPools`
separately from artifact ABI support. Clients requiring initialized capacity
must check this capability; older load-only readiness has the same response
shape and is insufficient. Successful readiness responses also advertise
`Convex-Source-Keyed-Prepared-Pools: 1`; remote clients must require this header
from the serving process. The v2 body remains unchanged so retained historical
records can be read, but those records do not establish current capacity.

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
lane-admitted, load-verified, and prepared. Every selected entry in each group
has initialized under configuration-only authority. This is a point-in-time
capacity observation, not a reservation held for the HTTP client. Activation
prepares one baseline instance per group; selected-generation maintenance adds
optional demand-derived replicas. `capacityUnavailable` means residency,
memory, or CPU capacity cannot currently satisfy preparation; callers may retry
within a bounded deadline. `notStaged` means only that the exact descriptor is
absent. Artifact, initializer, registry, and invariant failures remain errors.

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

`finish_push` repeats preparation, then reserves the exact chosen initialized
instances and their checkout headroom before beginning publication. If that
inventory is no longer available, publication fails before database mutation.
The reservation pins the exact generation incarnation and removes its instances
from ordinary checkout and eviction. Each commit attempt checks the expected
prior and applies those instances' initialization dependencies to the transaction.
A concurrent source or runtime change causes an expected-prior or OCC failure
instead of pairing the target with the wrong source.

An owned publication task retains the reservation through the database outcome,
even if the HTTP caller disconnects. On commit, it installs timestamp-ordered
selection before releasing inventory and before fallible audit conversion.
Failure releases the reservation without changing selection; historical replay
also leaves current selection unchanged. Retry the complete read-stage-activate
sequence with a fresh expected prior; do not reuse a stale activation request. Postcommit reconciliation verifies the pair
and registry without demanding another idle-capacity peak.

Clients recovering from a lost finish response must not treat a snapshot of
the expected prior as proof that publication has stopped. Deployment operation
capabilities advertise `publicationStatusProtocol: 1`. Submit owned `start_push`,
persist its operation ID and process session before sending finish, and include
both `operationId` and `operationSessionId` in that finish request. Preparation
and schema waiting must fit within the advertised operation lifetime.

For recovery, send the operation and session IDs to the operation cancel endpoint.
Its `publicationState` is `active` while an accepted finish retains its lease,
including after cancellation or expiry. Poll the session-bound status endpoint
until it reports `revoked`, then inspect committed state again. `available`
means further finish authority remains; an unknown unexpired operation is an
error because a delayed submission may still arrive. A changed process session
or an absent expired operation cannot grant new finish authority. Legacy controls
without a session retain their analysis-status contract.

After a backend restart, active-pair reporting establishes the database-selected
identity and loads its compiled routes; it does not establish initialized
capacity. The background worker discovers and prepares that selected generation.
Ordinary primary requests wait for prepared capacity within the admission bound;
shadows may skip unavailable capacity. An operator requiring preparation before
traffic must also request exact readiness and wait for `ready`.

## Residency and retirement

Source-keyed full-generation residency is a capacity-two LRU shared by
readiness, active-pair reporting, and invocation selection. LRU reclamation
excludes the database-selected generation and any incarnation pinned by an
in-flight activation reservation.
If neither resident generation is reclaimable, candidate readiness reports
`capacityUnavailable`. A reclaimed generation may still have an in-flight
invocation owning an `Arc` to it; that owner remains valid, but the registry no
longer owns a residency slot.

Eviction retires the exact generation incarnation, removes only routed-module
cache entries for that pointer, and prevents checkout of pooled instances from
that incarnation. This remains true when a later load has the same deployment
and generation digests. Retirement releases readiness-only compiled-module
ownership while active routes retain their own compiled modules. Idle cleanup
may complete later; an instance returned after retirement is destroyed instead
of pooled.

Exact cold loads use one single-flight per selector and one process-wide cold
generation permit. A caller's deadline bounds its wait and admission from the
cold-load queue. After acquiring that permit, the loader owns the complete finite
load through publication even if its waiters expire; otherwise reconstruction
that exceeds one caller's budget would be discarded and repeated indefinitely.
Catalog identity, selected-generation protection and activation pins still
govern publication and reclamation. Activation and maintenance share one preparation flight per
generation incarnation. An activation joining maintenance reassesses its own
idle-capacity requirement afterward. Caller cancellation does not abandon
already-started initialization or teardown. Preparation uses one batch deadline,
and maintenance stops starting work when the generation loses selection.
A live activation flight protects one idle baseline per group from age eviction
while later groups initialize. On completion, including a capacity-limited
partial result, those baselines begin their ordinary idle-age grace period so
the next bounded request can retain that progress. An abandoned candidate has no
permanent age protection; pressure, admission, and retirement still apply.
Catalog, residency, and preparation-table locks are not held during filesystem
access, hashing, deserialization, database reads, or guest initialization.
The worker replenishes only the database-selected generation. A failed group's
bounded retry delay leaves other groups eligible for preparation. Due initializer
retries run after healthy groups have replenished their capacity; an expired
backoff does not put a slow failing group ahead of healthy work again.
Maintenance attempts each available baseline before adding replicas. A group
whose baseline cannot currently fit does not prevent smaller healthy groups
from adding useful capacity. Among eligible groups, maintenance starts with
those whose initialization has not run recently, so repeated successful but
expensive replacements cannot consume every batch ahead of a later group.
When retained idle state prevents an otherwise
feasible baseline, maintenance can release its own later groups and prepare them
again in measured transient-memory order. A missing group for which capacity was
reclaimed keeps the next turn across a batch deadline. Before reclaiming capacity
for a particular group, the controller selects the smallest oldest-first set of idle
Stores whose removal permits that group's next admission, and removes that
whole set before preparation resumes. Removing only part of the required set
would let maintenance rebuild an earlier baseline before enough memory is free.
It never takes checked-out or
activation-reserved Stores. A staged candidate cannot use this operation to
reclaim the serving generation's baseline.

If the preparation worker stops, existing prepared capacity remains usable.
Ordinary primary pool misses fail explicitly and shadows skip; worker failure
never enables request-owned initialization. Requests for older snapshots still
retain their separate reconstruction path.

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

The generated byte budgets cover Wasm runtime charges **and process-wide live
packed-buffer backing**. The latter is reported as
`generated_wasm_memory_bytes{component="shared_packed_buffers"}`. It includes
packed documents and retained `StringBuffer` backing held by database caches,
V8 readers, Wasm readers, or parsed documents; it is not additional memory
allocated by each Wasm invocation. Keep enough budget for this shared population
before sizing the prepared-instance pool.

`packed_value::memory::PACKED_BUFFER_MEMORY` charges each backing allocation
once. `Bytes::from_owner` keeps the charge alive across clones, slices and
zero-copy parsed strings, and releases it only after the backing is freed.
Cache eviction therefore cannot hide allocations still held by readers.
Whole buffers are tightened before tracking, so a later document `shrink`
does not copy an already compact shared allocation. No allocation-address
registry, cache-to-execution transfer, or per-reader reference ledger is used.

Opaque invocation limits still count the logical size of every referenced
document, including aliases. Only aggregate admission and private growth
forecasts exclude those shared bytes. Private result containers, cursors,
encoded buffers and copies in guest linear memory remain charged. Releasing
the last backing owner wakes admission even when that owner is outside Wasm.

For each slot, the retained baseline is raw guest capacity minus verified static
credit plus retained private host bytes. Active slots keep their checkout
baseline in that sum; their additional liability is the greater of current
private growth above checkout and the invocation forecast. Preparation adds
separate future-checkout headroom. The full projection adds all retained
baselines, active liabilities, preparation headroom, idle growth reservations,
module charges, one process-wide packed-backing count, per-slot allowances and
the safety reserve. Do not also add active current capacity to this projection.

External packed owners can move the projection above the generated hard budget.
Releases and reconciliation of failed allocations still reduce the ledger in
that state. A private host allocation already committed after preflight remains
charged even when its post-allocation check rejects execution. Budget eviction
preserves a matching reusable Store and removes other idle Stores only when the
selected group can make the waiting admission fit.

Cache capacities remain retention-policy limits; do not add their packed
contents to this live shared charge again. The startup cache-capacity sum is
a separate configuration-feasibility estimate, not an allocation ledger or a
combined cache-plus-Wasm maximum. The generated controller checks the shared
counter at admission and growth boundaries; it does not make database packing
fallible or impose a new allocation cap on V8/database work. Existing cache
limits and cgroup pressure control still govern those paths. Temporary packing
buffers, reference-count headers, allocator overhead, and other native
allocations remain covered by conservative reserves and actual cgroup headroom,
not by a claim that this counter measures RSS.

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
    + (authenticated Core Wasm bytes - certified emitted static payload)
    + separate initialization-image allowance, when needed
```

Only authenticated compiler declarations qualify for static-payload deduction.
Per-instance immutable-page credits additionally require verified copy-on-write
backing. Writable data and artifacts without these declarations keep their full
conservative charge.

Uncertified compatibility graphs may keep their declared memory export names.
Only a module carrying immutable-static declarations must export its defined
memory as `memory`; optional accounting does not add this requirement to older
graphs.

The generated engine reserves the full wasm32 address space so credited pages
cannot move into a private copy during growth. File-backed initialization uses
the original AOT file only under the pinned Wasmtime implementation and the
explicit `force_memory_init_memfd(false)` setting; a file-backed compilation
image alone does not establish that property. Disabled COW grants no Store
credit and needs no separate image allowance. Hosts without a mapping verifier,
including Linux hosts that deny access to procfs, retain conservative accounting.

The first constructed memory for a retained Module establishes its COW evidence.
That construction uses conservative initial admission; it does not claim a
pre-allocation mapping proof that the Wasmtime API does not expose. The Emscripten
base receives its credit immediately after base instantiation, before side-module
placement grows memory. Subsequent Stores reuse evidence owned by that exact
Module and apply the credit atomically with initial allocation admission. Raw
per-Store limits still apply, and a rejected allocation installs no credit.
A compatibility graph whose memory owner is not its first dependency keeps
post-instantiation accounting, because an earlier dependency can allocate a GC
heap before the certified linear memory.
Peak accounting snapshots admitted and requested private demand immediately
before instantiating the memory owner. A later sharing credit adjusts only the
population containing those pages, preserving earlier combined host/GC peaks
for initialization forecasts. This adds one short controller-lock operation
per fresh Store; warm executions perform no additional snapshot or scan.

Evidence lookup is a nonblocking OnceLock read, with no module-cache or pool
lock. Unobserved constructors inspect their own mappings outside controller,
pool and module-cache locks; concurrent constructors do not wait for a verifier.
There is no per-request procfs scan, probe Store or new admission queue. A newly
loaded Module needs new evidence even when its artifact digest is unchanged.
The first construction can still fail a raw initial-capacity check near the
budget; that conservative bootstrap limit is distinct from repeated charges
for already-proven backing. Prepared deployment constructs the baseline before
activation, so ordinary traffic uses retained evidence.

Budget-denied module loads and charge increases first check whether eligible
cache reclamation can satisfy admission. Shared graph owners count once; code
pinned by Stores or preloaded generations is not reclaimable. If reclamation
cannot suffice, the cache stays intact. Otherwise a sufficient LRU prefix is
removed and admission is retried once against current ownership. External
allocations can still cause that retry to fail; they do not trigger further
purging in the same attempt. This ownership check runs only on denied admission
and uses nonblocking module-charge lock reads. Explicit cgroup-pressure eviction
remains independent of this feasibility check.

Shared base, shard, and leaf code is deduplicated by engine compatibility
digest and AOT digest while an owning routed graph remains live. Route
identities and pools remain generation-scoped. Cache eviction does not prove
native code was released while a Store, routed module, shared module or retained
Wasmtime error backtrace retains ownership. Charge owners outlive Store and
Module destruction, including the direct leaf reference in a graph descriptor.

Prepared idle Stores retain a growth reservation for their next checkout in
addition to their initialized memory. Checkout transfers that reservation to
the active invocation; it does not add the same allowance twice. Actual growth
remains subject to the hard byte limit. The `idle_growth_reservation` memory
gauge reports the reserved allowance separately from retained bytes.
Returning a Store can keep or reduce its admitted reservation even if external
packed backing has since crossed the soft budget. An increase still needs
soft-budget admission, and active cgroup pressure still prevents retention.

Cold and warm histories remain separate. Successful and developer-error
executions can reduce the corresponding learned forecast; canceled, timed-out,
limited and system-error executions contribute only when their observed demand
is at least that history's prior forecast. Interrupted initialization therefore
cannot erase a learned cold requirement. Lower samples still appear in route distributions.
Forecast decreases also refresh a Store returned without execution after waiting
for CPU. A stale initialization context discards the old Store and its permit,
then admits a replacement with a fresh cold forecast and zero checkout baseline.

Preparation admits its initialization forecast plus future checkout headroom.
Before retaining a Store, it checks the actual initialized size and reserves
the next allowance within the soft budget. If this cannot fit, it discards the
Store instead of publishing unusable prepared capacity. Demand-derived extra
instances are optional and share the same instance and byte limits as serving
work; historical concurrency is not an unconditional readiness requirement.

Cgroup pressure hysteresis uses
`LOCAL_BACKEND_MEMORY_PRESSURE_ENTER_HEADROOM_BYTES` and
`LOCAL_BACKEND_MEMORY_PRESSURE_EXIT_HEADROOM_BYTES`, defaulting to `3 GiB` and
`5 GiB`. Missing or unlimited cgroup samples do not disable instance, byte,
forecast, or reserve enforcement.

Async host waits release active guest CPU but retain the Store, memory permit,
forecast, and live-byte charge. Default values are implementation priors; they
must be calibrated under the actual cgroup, artifact sizes, V8/Wasm mix, and
reuse policy.

Store destruction frees linear memory and the GC heap before releasing its
invocation ledger entry, including early errors and cancellation. A denied
growth remains terminal even if Wasmtime's internal GC retry subsequently
satisfies an allocation without growing. Host-operation admission and epoch
checks preserve that failure, and the lifecycle refuses further preparation or
handler entry. That Store is discarded without guest teardown.
Idle-eviction batches also retain each slot's baseline and allowance until its
Store is destroyed. Dropping a cleanup future, including before its first poll,
releases both the physical Stores and their evicting slots.

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

### Paired execution observations

`GET /api/app_metrics/execution_observation` reads the current process-local
session with `ViewMetrics` authority. `POST` to the same endpoint also requires
`Deploy` authority and accepts either:

- `{"action":"arm","routes":[{"udfType":"query","routeKey":"GENERATION:ROUTE"}],
  "durationSeconds":600,"pairsPerRoute":256,"sampleEvery":1}`; or
- `{"action":"stop","sessionId":"ID"}`.

Each route key contains authenticated generation and route SHA-256 digests.
Select one to eight distinct query/mutation routes from the active generation.
Duration is 1–1800 seconds, budget is 1–4096 pairs per route, and `sampleEvery`
is 1–1024 eligible admitted pairs. Selection is limited to V8-primary/Wasm-shadow
execution and does not change admission or authority. Every session is initially
disabled until explicitly armed; the former startup environment selector is no
longer used. Expiry, budget exhaustion, stop, or observation of a generation
change stops new selection. A read also evaluates expiry and generation validity.

The response wraps `session`, or `null` when none has been armed. It carries a
session ID, generation, timestamps, limits, state and per-route records/counters.
Each route retains a uniform reservoir of up to 16 completed observed pairs and
up to eight pairs with the largest measured Wasm wall duration. These sets may
overlap: deduplicate by route and sequence. They are independent of error/admission
diagnostics. The representative set represents only the selected population,
which can exhaust its budget before expiry. It does not estimate all traffic.

`selected`, `completed`, `abandoned`, `inFlight`, `withoutComparison`,
`incompleteAccounting`, and `notRetained` describe collection and retention.
In-flight means a selected pair has not reported or abandoned its observation;
it is not the number of engine tasks. Terminal snapshots may still show active
or cancelled tasks, and must not be presented as complete CPU breakdowns.
Stopped sessions continue accepting already selected completions. Replacing
retained data requires `replaceSessionId` matching the current stopped/expired/
exhausted/invalidated session, with no remaining in-flight pair reports.

CPU counts belong to synchronous polls on the observed thread. Provider CPU
excludes remote service and separately spawned tasks. Lane suspension overlaps
runtime phases. `wakeToPollNanos` is a subset of suspension after the first
recorded wake; it is neither additional elapsed time nor database service time.
Resumes without a recorded wake are counted explicitly. Native object-layout
counts are attached to the same observed execution. V8 GC callback spans cover
observed synchronous callbacks; they do not measure background worker CPU.
Hermes heap counters cover the handler and cleanup, excluding preparation, and
can be absent after a trap. GC CPU uses the WASI thread CPU clock for synchronous
collection sections; `gcCpuNanos` is absent on hosts without that clock. The WASI
CPU clock accumulates synchronous poll CPU for the retained guest Store. Epoch
yields can suspend a GC section and resume it on another executor thread, so raw
executor-thread lifetime clocks must not be exposed to the guest. Suspended time
and unrelated executor work are excluded, and pooled invocation reuse retains the
clock's accumulated value.
GC spans are included in enclosing execution time;
do not add them again. The collectors' counts and CPU scopes differ.

Timing responses from `query_shadow_evidence` include `exactTotalNanos` decimal
strings for elapsed histograms. These sum original durations before histogram
quantization or clamping, over the same retained paired population as
`exactSampleCount`. Ratio histograms have no exact elapsed sum. `timingWindow`
identifies the timeline base, requested start/end, bucket width and retention
limit. Totals cover retained buckets intersecting that window. Pages are separate
snapshots; aggregate and route totals overlap, and subtracting rolling-window
sums does not generally produce exact interval totals. Process restart, timeline
reset and bucket eviction can remove history.

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

The separate read-only endpoint
`/api/app_metrics/generated_wasm_memory_inspection` exposes live controller
inventory and the underlying per-bucket counts without changing the statistics
endpoint's existing protocol. It accepts `udfType=query|mutation`, `limit=1..100`
(default 100), and an optional lowercase route digest cursor. Deployment metrics
permission is required. It never checks out a Store or executes guest code.
Runtime startup must already have initialized configuration; inspection cannot
initialize or compile a generation on demand. The controller lock captures bounded
ownership records and the selected route page; identity hashing, histogram response
construction and serialization happen after release, so diagnostics do not hold
admission behind that work.

`controllerId` identifies the accounting lifetime; `controllerElapsedMilliseconds`
is each page's monotonic observation time. The global ledger reports private
retained baseline, active liability (including preparation headroom), idle growth
reservation, module charges, process-wide packed backing, unattributed allowances
and safety reserve. Their sum is `projected`; the soft and hard budgets are
comparison thresholds, not additional terms. The packed count
is one atomic observation alongside locked controller state; database owners can
change it immediately afterward. Cgroup headroom is the controller's latest
sample; `cgroupSampleAvailable=false` means its pressure state may be retained
from an earlier sample. The existing pressure sampler also refreshes backing
metrics when only non-Wasm owners allocate or release buffers.

Slots include raw guest capacity, shared-static credit, private host charge,
checkout baseline, reservation and active liabilities. Raw capacity includes
linear memories and the internal Wasmtime GC heap, not virtual address reservation
or compiled code. Slot `moduleChargeIds` refer to shared ownership records, not
additional per-instance charges. A module charge stays visible after cache
removal until its last charge owner is destroyed. `cached=false` can represent a
retained evicted module, a retired-generation load, or a load still in progress;
`kind` distinguishes loading reservations. Image bytes are absent for loading
and descriptor-only records. Charge totals include estimated metadata and possible
separate initialization-image allowances; image bytes alone are not that total.
An error with a Wasmtime backtrace can retain a module record after its last
Store disappears. A slot reused for another entry retains its original physical
module links. Shared-base accounting is computed at load and reused on cache hits.
The module inventory is limited to 512 rows with explicit `omittedModuleCharges`.

The global inventory includes retained generations. Routes include only observed
selected entries of the active generation. Opaque memory identity digests link
routes to slots and cold/warm forecasts; these keys are diagnostic within this
controller lifetime, not a serialized artifact contract. Prepared slots can exist
before their first route observation. Absent forecast records mean unavailable
history, not zero growth. No application paths, native addresses, document values
or artifact filesystem paths are exported.

Histogram counts are per bucket, not cumulative bucket prefixes. All fields share
`bucketUpperBounds`; its final null denotes unbounded overflow. Route distributions
combine fresh/reused executions and separate primary/shadow roles. Forecast growth
histograms separately represent eligible cold and warm learning observations;
they are current cumulative learning state, not interval distributions.
Cumulative exact maxima cannot generally determine interval maxima. A consumer may subtract counts
only after confirming unchanged controller/generation, unchanged eviction count,
complete ordered route coverage and nondecreasing counters; it must never subtract
percentiles or maxima. Newly observed routes are zero in a prior complete capture
only when no record loss occurred. Disappearing records invalidate an interval.

Pages have independent timestamps and are not a frozen report. Selected/observed
route counts and eviction counts permit detecting incomplete or changing coverage
while existing counters advance. A consumer must preserve per-page times and
reject restarts, generation changes, record loss or inconsistent coverage. Read
query and mutation inventory as repeated observations of the same controller;
never sum global inventory across pages, UDF types or functions. Keep physical
process/cgroup metrics and logical cache retention statistics alongside this
accounting evidence rather than treating their sum as physical RAM.

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
`peakGuestBytes` is raw admitted guest capacity before static credit;
`peakHostBytes` is private host accounting after excluding shared packed backing.
They can peak at different times, and the combined peak also subtracts static
credit. Their sum therefore need not equal the combined peak.
`completionBytes` is the last combined accounting before pool return or
discard. When `growthDenied` is true, it can exceed `peakAdmittedBytes`: a
container's capacity is measured after allocation, and rejected capacity can
survive cleanup. Such capacity stays charged without entering the admitted
peak. `returnedToPoolBytes` is the accounted size at the pool-return
transition, and is zero when `returnedToPool` is false; it is not a promise
that those bytes remain resident. `reusedInstance` describes the Store that
actually executed. When a stale pooled Store is discarded, its fresh replacement
starts with a zero checkout baseline and a cold admission forecast.

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

A failed Wasmtime guest call poisons its instance, including initialization,
selection, preparation, execution and host-import failures. Finalization revokes
host capabilities and releases host resources, then drops the Store without
calling guest teardown or other guest exports. A native allocation trap can leave
VM or allocator invariants broken; resetting fuel does not make guest cleanup
safe. Poisoned instances never return to the pool. A guest that returns an error
status normally still receives bounded teardown when discarded.

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
   shadow rates at zero, both inverse verifier rates at zero, and instance reuse
   enabled. Source-keyed preparation requires reuse even while sampling is zero.
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
