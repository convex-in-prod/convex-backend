# Static Hermes Wasm UDF Operations and Validation

This guide covers backend configuration, registry installation, activation, and
diagnosis for the [Static Hermes Wasm UDF patch](README.md). Build application
artifacts with the companion compiler's
[project build workflow](https://github.com/convex-in-prod/convex-wasm-compiler/tree/main#project-artifact-build).
The compiler produces a local registry; the steps here validate it on the
receiving backend and pair its generation with the matching source deployment.
Building the backend below enables runtime support but does not compile or
activate application functions.

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

The maintained path uses an authenticated deployment-v8 module-graph registry
and source-keyed selection. Explicit primary-capable publication produces
`convex-wasm-runtime-registry-generation-v9`, which supports V8 or Wasm as
primary, with optional verification in the other runtime. The runtime settings
choose the primary independently of publication. Publishing authenticated
artifacts does not require historical semantic execution proof; source, graph,
artifact, native compatibility and readiness checks still apply.

Explicit shadow-only publication produces
`convex-wasm-runtime-registry-generation-v10-shadow-only`. That generation remains
eligible only for verification, under either live primary selection. A shadow-only generation
must be republished and paired as primary-capable before it can execute as Wasm primary;
a primary-capable generation needs no rebuild when changing runtime direction.

Normal routing makes Wasm primary only for selected routes.
Those routes may run a sampled V8 verifier or may run without a verifier.
Unselected routes, V8-only source packages, and quarantined routes remain
V8-only. Only the selected primary or its permitted V8 recovery attempt publishes
or commits. A V8 verifier timeout, failure, overload rejection, cancellation,
divergence, or late completion is discarded; it never becomes fallback and never
delays or replaces a successful Wasm result.

Configured Wasm-primary execution can retry once in V8 after a classified,
safely disposed engine failure, including recoverable guest-memory OOM and
host-classified initialization or active execution timeouts. Recovery reconstructs
the original transaction inputs and uses normal V8 admission. Its bounded queue
allowance fits within the total wall deadline fixed before Wasm starts; Wasm
execution time does not expire that entire queue allowance. Application exceptions,
exhausted fuel/operation limits, aggregate pressure, admission rejection,
cancellation and independent host-operation or cleanup failures do not authorize
recovery. Forced-Wasm diagnostics and verifier
lanes remain strict. See the [recovery contract](README.md#current-architecture)
for classification, transaction and deadline details. No compiler or artifact ABI
change is required for this backend behavior.

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

## Live runtime selection

With the `static-hermes-wasmtime-gate` feature, authenticated administrators can GET or POST
`/api/wasm_runtime_policy` using a deployment key with deploy permission. GET
returns `snapshot` (`revision` and `policy`), `durable` (storage is configured),
`quarantinedSelectors`, and `quarantineInitialized`. An uninitialized or ambiguous
quarantine store blocks all Wasm even when the selector count is zero. POST accepts the revision from that snapshot and a complete
query/mutation policy, for example:

```json
{
  "expectedRevision": 3,
  "policy": {
    "query": {"primary": "wasm", "shadowBps": 500},
    "mutation": {"primary": "v8", "shadowBps": 0}
  }
}
```

`shadowBps` always samples the opposite engine, from zero through 10000. Zero is
primary-only. Direction and sampling are captured together before waiting for
application execution admission. Queries capture after function validation;
mutations capture with their permit acquisition, before opening the ordinary
mutation transaction and validating the function. Scheduled and cron mutations
use the same permit boundary with their existing transactions. Queued calls retain
their selection. Each OCC retry and reactive query reexecution is a new execution
attempt and captures the latest policy; cache hits and followers do not execute
another verifier. Nested transactional queries and mutations retain the existing
V8 recursion path and do not resample policy. An action's database calls are
separate execution attempts and capture their own policy.
No deployment or restart is needed.
The policy is atomically persisted beside the quarantine file as
`wasm-runtime-policy.json` for URL-backed databases; SQLite uses
`.<database-file>.wasm-runtime-policy.json` beside its database. Each file has one
backend process writer. Storage remains available without installed artifacts
when the database directory or `DATA_DIR` exists. A URL-backed V8-only server
without `DATA_DIR` reports `durable: false` and rejects writes. Builds without
the native feature do not expose this endpoint.
Request dispatch never waits for policy filesystem work.
A stale revision receives HTTP 409. Read the current policy after a conflict or
an uncertain update response instead of blindly retrying an old selection.
Rename publishes the new live snapshot. A subsequent directory-fsync failure
returns an error without restoring the preceding snapshot; the replacement is
visible but its survival across a host crash is unconfirmed. Client disconnect
does not cancel an update already submitted to the blocking writer.

Startup environment settings supply the initial policy only. A persisted policy
overrides those defaults on restart without parsing them. An invalid stored
policy fails startup. Registry and resource configuration remain
process settings. Updates cannot manufacture missing pairs, promote shadow-only
generations, or clear route quarantines. Those routes continue using V8. A mode
switch does not cancel or drain existing invocations.
For a cold source-keyed generation, authenticated publication and export
eligibility are checked before native preloading. An explicitly V8-only export
does not need Wasm capacity merely because another export in its source package
is paired.

A V8-to-Wasm promotion prepares the retained pair's applicable groups before
publication, including quarantined routes. An absent pair, authenticated
shadow-only admission, or no selected destination-primary kind permits a future
policy without warmup. A selected pair missing its descriptor or required source
content identity rejects promotion. Status, demotion and sampling-only updates
skip preparation. Final source and initialization checks share a ten-second
budget under the publication gate; the submitted filesystem write retains that
gate and its exact idle inventory through completion. Policy persistence is not
transactional with other database configuration writes. Each checkout still
validates initialization dependencies against its own transaction, so a later
configuration change can invalidate previously reserved capacity.

## Configuration

Boolean gate defaults accept only `0` or `1`. Without a stored runtime policy,
invalid or inconsistent direction/sampling defaults fail startup.

| Variable | Default | Contract |
| --- | ---: | --- |
| `CONVEX_STATIC_HERMES_WASM_GATE_ENABLED` | `0` | Startup Wasm-primary default; stored runtime policy takes precedence. |
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
`CONVEX_STATIC_HERMES_WASM_GATE_SOURCE_KEYED_DEPLOYMENT=1`. An authenticated
registry supplies verifier routes. For the environment defaults, the process-wide
gate must be `1` before either per-UDF primary switch can enable Wasm authority. An unset
per-UDF switch follows that gate for compatibility. For each UDF type, its
V8-primary shadow rate must be zero when Wasm primary is selected, and its
Wasm-primary V8-verifier rate must be zero when V8 primary is selected. Query
and mutation directions may differ. Zero verifier rates or a sample miss run
selected Wasm-primary routes without a verifier. Contradictory settings fail
startup. A shadow-only generation may be retained for verification but remains
ineligible for primary execution, independently of the live policy.

The verifier sampling gate uses immediate try-acquire and never queues. An
admitted V8 verifier uses the ordinary V8 worker queue; a Wasm verifier requires
immediate runtime-resource admission. Eligible query verifiers are admitted
cache-miss leaders and direct no-cache queries invoked by an administrator or
the system. Mutation verifiers run in independent transactions whose writes are
discarded. Verifier results cannot publish or commit, including when the primary
uses its separate V8 recovery attempt.

Environment configuration is process-start state. In source-keyed mode the
backend polls `source-catalog.json` for authenticated descriptor changes. It
does not activate a descriptor through `current`. Non-source compatibility
mode continues to poll `current` for authenticated whole-generation
replacement.

Before the first artifact publication, a configured registry may be an existing,
private, empty directory under either primary selection. Startup and
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

The compiler's [registry publication and transfer commands](https://github.com/convex-in-prod/convex-wasm-compiler/tree/main#publish-and-transfer-a-registry)
construct a local registry and transfer a preflighted generation. Backend
readiness and paired activation remain separate operations below. Compiler
[build-cache maintenance](https://github.com/convex-in-prod/convex-wasm-compiler/tree/main#cache-maintenance)
manages local compilation outputs, not the serving registry or runtime residents.

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
from the serving process. The v2 identity and status fields remain unchanged;
pending responses may also include capacity diagnostics. Retained historical
records can still be read, but those records do not establish current capacity.

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
lane-admitted, load-verified, and prepared. Every selected entry in each applicable group
has initialized under configuration-only authority. This is a point-in-time
capacity observation, not a reservation held for the HTTP client. Preparation
establishes one initialized instance per applicable group first. When the
destination policy uses Wasm primary, it then prepares a population from a fixed
snapshot of recent group demand. Applicable groups contain a selected query or
mutation whose destination primary is Wasm; quarantine does not remove them
from preparation. Ordinary shadow-only preparation keeps the
one-instance target. Compatible already-serving Stores count toward the target
during a same-generation policy update. Demand uses five one-minute occupancy
buckets, including asynchronous waits and detached cleanup. Queued requests,
cache hits and followers, verifier copies, and nested recursion add no primary
demand. V8 bootstrap observations use retained, validated metadata for the last
database-confirmed selected generation; they do not load source or artifacts.
Kind-specific history transfers only through shared exports of that kind when
groups change. Combined history keeps a conservative group estimate, without
multiplying inherited estimates or extrapolating a shadow sampling percentage.

Baseline construction uses available slots incrementally rather than waiting
for all baseline slots to be free at once. Every construction still passes the
existing CPU, instance, aggregate-memory and transient-construction admission.
Aggregate memory denial during construction becomes a capacity shortfall only
after disposal; guest limits, initializer exceptions and independent cleanup or
host failures remain errors.
Additional population uses the least-fulfilled proportion of each fixed target
for at most approximately 30 seconds. At most two jobs globally hold preparation
admission, and `inProgress` counts those owners rather than semaphore waiters.
A queued job rechecks total group capacity after CPU admission. Both the fixed
allocation and this live check include activation-reserved Stores for the exact
generation incarnation, so serving and reservation transfers do not turn a
captured idle deficit into unnecessary copies. Reservation occupancy adds no
primary demand, inherited peak or public `active` count.
Candidate growth preserves useful serving stock. A disconnected or expired
waiter leaves started initialization and teardown with their preparation owner.
Readiness and successful policy-update
responses may include `population`, with opaque `group` identities, `target`,
`prepared`, `active`, `inProgress`, and `shortfall`, plus `preparationElapsedMs`.
Readiness baseline capacity failures also include the captured population once route
discovery has completed. A `ready` result or successful
switch can therefore explicitly report fewer Stores than its optional target. `capacityUnavailable` means residency,
memory, or CPU capacity cannot currently satisfy preparation; callers may retry
within a bounded deadline. `capacityUnavailable` responses include `capacity`:
the flight's last wait reason (`loading`, `preparing`, `initializing`,
`awaiting_idle_baseline`, `deadline`, `cpu`,
a memory budget, pressure, or a slot/record limit), current projected/soft/hard bytes, instance
count/limit and available cgroup headroom. These are diagnostic observations,
not admission grants or a proof that a budget deficit is permanent.
`awaiting_idle_baseline` means held Stores occupy the captured target while no
idle initialized Store is available for the publication check. The optional
population and timing fields extend the retained v2 response; consumers that
validate its keys also need to accept those fields. `notStaged`
means only that the exact descriptor is absent. Artifact, initializer, registry,
and invariant failures remain errors.

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
instances before each source-package commit. It reassesses capacity under the pool
lock: an optional copy that yielded to pressure is a reported shortfall, while
loss of minimum group coverage still rejects publication. A same-generation
policy promotion counts active compatible Stores without freezing their work
during warmup. Their backing remains charged and their
forecasts remain in shared readiness headroom. If that inventory is no longer
available, publication fails before committing database changes.
The reservation pins the exact generation incarnation and removes its instances
from ordinary checkout and eviction. Its per-group population ownership transfers
with the physical Stores under the pool lock, including on failure and return.
Concurrent readiness and maintenance count that occupancy against their targets;
another publication still needs its own idle initialized Store and dependency
validation. Each commit attempt checks the expected prior and applies those
instances' initialization dependencies to the transaction.
Wasm configuration reads resolve individual table names and tablets once
`_tables.by_name` is enabled. Bootstrap retains a dependency on the complete
table catalog before that index is enabled. Creating an unrelated table thereafter
does not discard prepared instances or initialized images. Changes to observed
environment values, absent variables becoming present, canonical URLs, or the
table mappings and indexes actually used still invalidate initialization.
The name dependency conservatively includes matching names in every namespace.
A concurrent source or runtime change causes an expected-prior or OCC failure
instead of pairing the target with the wrong source.
An OCC failure releases the publication gate and reserved inventory before retry
backoff. The next attempt reserves again under the gate and checks the captured
policy revision, so a demotion during backoff rejects stale paired preparation.

An owned publication task retains the reservation through the database outcome,
even if the HTTP caller disconnects. On commit, it installs timestamp-ordered
selection before releasing inventory and before fallible audit conversion.
Failure releases the reservation without changing selection; historical replay
also leaves current selection unchanged. Retry the complete read-stage-activate
sequence with a fresh expected prior; do not reuse a stale activation request. Postcommit reconciliation verifies the pair
and registry without demanding another idle-capacity peak.

Legacy unpaired deployment clears the selected generation at commit. Its
detached publication task retains the existing analysis, native publication and
Node capacity owners; losing the HTTP waiter does not admit another analysis
while the task still owns source bytes or reevaluates authentication configuration.

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

Eviction retires the exact generation incarnation and removes only routed-module
cache entries for that pointer. A later incarnation cannot check out its pooled
instances, even when the deployment and generation digests match. Requests that
already captured the old incarnation can still use its remaining instances or
reconstruct their own Store. Retirement releases readiness-only compiled-module
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
already-started initialization or teardown. Baselines use the ordinary admission
deadline. Wasm-primary activation adds up to 30 seconds for optional population,
with at most two preparations across all generations. The initial baseline pass
retains measured serial memory-cost ordering; extra copies from independent
groups can run concurrently, with one producer at a time for each group's image.
Each clone reconstructs independent Store-native references and tables and uses
the group's validated initialized COW image. No mutable Store state crosses the
image boundary. A deadline after minimum coverage returns the completed stock;
the owned flight finishes any already-started guest work and cleanup. A failure
already recorded at that deadline remains an error. A still-running optional
initializer can fail after the response; its owner records that failure for
later preparation and admission. Artifact failures remain errors. Maintenance stops starting work
when the generation loses selection.
A live activation flight protects its captured population target from age
eviction while later groups initialize. On completion, including a
capacity-limited partial result, all retained copies begin their ordinary
idle-age grace period so
the next bounded request can retain that progress. An abandoned candidate has no
permanent age protection; pressure, admission, and retirement still apply.
Catalog, residency, and preparation-table locks are not held during filesystem
access, hashing, deserialization, database reads, or guest initialization.
The worker replenishes only the database-selected generation. A failed group's
bounded retry delay leaves other groups eligible for preparation. Primary pool
misses for that group report the known initialization failure without waiting
for an overload deadline; existing prepared instances remain usable. Capacity-only
retries preserve that failure until preparation succeeds. Due initializer retries
run after healthy groups have replenished their capacity; an expired backoff does
not put a slow failing group ahead of healthy work again.
Preparation attempts each available baseline before adding replicas. A group
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
It never takes checked-out or activation-reserved Stores. Activation preserves
the serving generation's recent demand, subtracting its checked-out Stores from
the required idle stock. During same-generation promotion, it also preserves
the captured destination allocation, even when the new primary kinds need more
Stores than the current serving policy. Candidate extras yield under pressure
instead of repeatedly reclaiming and rebuilding useful serving capacity. During a live
activation flight, serving maintenance keeps baseline coverage but suspends
optional replication, including rechecking after CPU waits. Both generations'
retained state, shared execution headroom and construction peak must still fit
the existing controller's limits. Baseline loss can block publication; optional
population shortfalls cannot keep it waiting indefinitely.

Demand uses five one-minute peak buckets, so completed bursts expire after
approximately four to five minutes. Actual primary Wasm observations count the
lifetime of each admitted Store, including suspension. They exclude queue
backlog and shadow executions; the separate bounded saturation hint can guide
maintenance but never raises an activation target. Eligible selected V8 primary
execution is counted from worker assignment through dirty-isolate destruction
and context preparation, including quarantined routes, to
provide an estimate before promotion. Cache hits, followers, verifiers and the
existing nested V8 recursion path do not add primary demand. Observation grants
no execution authority and never changes quarantine. Query, mutation and
combined occupancy have separate histories. Actual relevant Wasm history takes
precedence over the V8 estimate; query-only Wasm history does not replace the
estimate for a newly promoted mutation in the same pool. Five-percent shadows
are never multiplied by twenty. Group history follows exact selected
module/export/kind membership and only
original observations contribute when generations regroup, so inherited
estimates do not multiply themselves.

Each preparation attempt captures the peak itself, with a floor of one per
applicable group and no percentage or speculative replica reserve. After the
baselines, allocation favors the least fulfilled fraction of each group's
target, subject to global instance and memory admission. V8 execution duration
is only an estimate of future Wasm Store occupancy. Checked-out Stores from
verifiers or other kinds consume the available slot budget without raising a
captured target. The slot allocation includes idle surplus eligible for ordinary
preparation reclamation; a physically full pool alone does not limit an attempt
to baselines. Serving protection and byte admission are rechecked for each
construction. Regrouping can be conservative. A process restart loses this
history. An unseen workload begins at the floor, and a later increase belongs
to maintenance rather than an
unbounded extension of the deployment attempt. Memory forecasts retain their
existing safety margins; the population target does not replace byte admission
or promise simultaneous execution in every prepared Store.

A live policy promotion prepares when the authenticated pair can supply a
selected destination primary route. No selected pair, shadow-only admission,
or no selected destination primary kind permits setting a future policy without
preparation. A selected pair with a missing descriptor, a missing source content
identity, or unavailable required staging rejects the update. Quarantine keeps
execution on V8 but does not skip preparation: clearing quarantine must not
expose an unprepared primary population. Preparation holds no policy writer or
source-publication lock.
At publication, the policy revision and exact prior source are rechecked under
a gate shared with source commits, and the achieved idle inventory is
reserved and its configuration dependencies are checked. Final policy source
and dependency checks share a ten-second budget; paired source publication also
bounds each dependency check pass at ten seconds. Expiry releases the reservation
before submission. A concurrent update rejects stale intent. Legacy source auth
evaluation runs before this gate; its existing transaction/OCC loop owns the
gate only through each commit attempt. The legacy publication task retains the
native publication guard and Node capacity across caller disconnects, and clears
unpaired runtime selection immediately after a successful commit, before audit
conversion. Policy persistence is not a database transaction: concurrent configuration writes
can invalidate initialized state after its dependency check, so ordinary checkout
validation remains authoritative. The owned persistence task retains that reservation through
the write outcome even if the HTTP caller disconnects. V8 demotion and
sampling-only changes skip warmup; they can supersede a warming promotion. The
CLI bounds policy POSTs at 75 seconds without replaying an ambiguous response;
status GETs have a ten-second timeout and do not acquire the publication gate.
The ordinary baseline budget starts before generation loading and is 30 seconds
by default; optional population adds at most 30 seconds. The final ten-second
validation budget starts after acquiring the gate. HTTP admission, waiting for
another publication and durable I/O can exceed the remaining client time. A
submitted commit or filesystem write retains the gate through its actual outcome;
a rollback can wait for that outcome even though rollback skips preparation.
The backend HTTP timeout defaults to 300 seconds after HTTP admission and does
not cancel an already detached publication. The 75-second client timeout is a
response deadline, not a promise that publication has stopped.

Policy rejection codes distinguish missing content identity, unavailable staging,
baseline shortage, lost initialized inventory, changed dependencies, validation
timeout and stale source or policy intent. Baseline shortage includes how many
discovered groups have initialized capacity; zero discovered groups can mean
loading did not complete. Artifact or initializer failures remain operational
errors and retain their diagnostics in backend logs. Successful `population`
counts describe this reservation and its observed compatible active Stores.
Every applicable group contributes at least one idle initialized Store whose
configuration dependencies are validated. Active and reserved Stores reduce
optional copies. Active Stores can still be initializing or tearing down and do
not establish that baseline. Another publication's reserved Stores are also
unavailable as initialization evidence and are excluded from this response's
`prepared` count.
If held Stores leave room within the captured target, activation
can construct the missing idle baseline. If the target is fully occupied,
activation waits for an idle return within the existing deadline rather than
adding a replica reserve.
Active Stores remain free to finish or fail and are not idle reservations.
Calls retained from an earlier selection also count for that exact incarnation
during rollback preparation; a reloaded incarnation cannot inherit their capacity.
`inProgress` is captured when preparation returns; it does not count toward the
reserved baseline. `preparationElapsedMs` excludes the later publication-gate
wait, final validation and durable write.

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

## Guest scheduling intervals

| Variable | Default |
| --- | ---: |
| `CONVEX_STATIC_HERMES_WASM_GATE_EPOCH_TICK_INTERVAL_MILLISECONDS` | `10` |
| `CONVEX_STATIC_HERMES_WASM_GATE_CONTINUOUS_POLL_LIMIT_MILLISECONDS` | `50` |

Both accept positive integer milliseconds and are process-start settings.
Zero and malformed values are rejected. Applying a change requires restarting
the backend, with no compiler change, AOT invalidation or new application pair.

The first controls the engine clock for cancellation, timeout and memory checks.
The second controls when a healthy continuous poll yields; a pending host call
or admission wait resets that poll's allowance. It does not change execution
timeouts. Yielding occurs at the next epoch check after the poll allowance is
spent, so a larger tick interval also makes yielding and interruption coarser.
These are cooperative timings, not hard real-time bounds: the ticker must run
and guest execution must reach an instrumented check.

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
| `CONVEX_STATIC_HERMES_WASM_GATE_PROTECTED_ACTIVE_CPU_MINIMUM` | `0` |
| `CONVEX_STATIC_HERMES_WASM_GATE_DEGRADABLE_ACTIVE_CPU_MINIMUM` | `0` |

The hard instance ceiling is `1..=200`. The soft budget must be below the hard
budget, the safety reserve below the soft budget, cold forecast plus reserve
must fit the hard budget, and the warm target cannot exceed the instance
ceiling. Active CPU concurrency must be between one and the instance ceiling.

Wasm CPU admission uses the same service policy as V8 with independent capacity
and metrics. Both service floors must be zero (compatibility mode), or positive
with their sum no larger than Wasm CPU concurrency. Under contention each class
recovers its floor as permits are released and borrows unused capacity otherwise.
Query classes come from cache admission; dependency ownership takes precedence,
and suspension preserves the class. High mutations retain bounded priority within
protected capacity. Detached verifiers use immediate ordinary admission.
The preceding instance/memory queue uses the same class policy and floors against
its own admitted-instance occupancy. Suspended calls and cancellation cleanup
retain that occupancy with their memory permit. The queue selects eligible
compatible instances and forecasts, so an unavailable group does not block a
ready group. Memory byte limits remain independent of class floors. These floors
and dependency preference do not reserve physical instances or memory bytes:
eligible work recovers service as owners release resources, while unavailable
work remains bounded by its original admission deadline.

Wasm pending admission uses `ISOLATE_QUEUE_SIZE` and the dependency allowance
from `ISOLATE_DEPENDENCY_WORKER_RESERVE`, in a queue separate from V8 workers.
The queue reuses the isolate delay controller and its configured target, interval
and shed threshold. `ISOLATE_QUEUE_DELAY_CONTROL_ENABLED` controls adaptive
shedding; the absolute admission deadline is bounded by the shorter of the Wasm
admission wait and `ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS`, starting before route
loading. Setup and initial CPU admission share that deadline. Optional shadows
use authenticated cached modules; a module-cache miss returns optional capacity
unavailable without loading or reclaiming modules. They do not enter the queue,
bypass eligible primaries, or evict retained instances to make room. Primary
admission retains bounded reclamation outside queue locks. If a failed admission
attempt wakes an eligible queued request, it waits instead of reclaiming the idle
capacity that made that request eligible.
Reclamation planning accounts for idle instances already selected for destruction
before selecting another batch. Their pending byte and slot releases can prevent
redundant eviction, but cannot admit new work: physical ownership remains charged
until disposal finishes and wakes waiting requests. If pending releases are
insufficient, the planner can select the additional capacity required.
An observed teardown failure remains a cleanup error even if the rest of the
reclamation batch later reaches its admission deadline.

`wasm_resource_admission_requests` reports pending and retained resource owners by
service class. Retained means a checked-out request, including suspension or
cleanup, rather than an idle Store. `wasm_resource_admission_events_total` records
queue entry, grants, backlog rejection, delay shedding and withdrawal;
`wasm_resource_admission_wait_seconds` reports time through grant or withdrawal.
These are resource transitions, which can repeat within one execution after an
invalidated Store. Withdrawal does not classify an execution's terminal outcome.

`database_execution_attempts_total` records one terminal function-runner outcome
per query/mutation attempt, labeled by runtime, primary/verifier/fallback role,
service class, outcome and a bounded admission reason. `database_execution_in_flight_requests`
reports callers still waiting; `database_execution_attempt_seconds` includes
source selection, setup and admission. A runtime of `selecting` means selection
failed or the caller cancelled before a runtime was known. Source exclusions
that select V8 are recorded as V8, without counting a second attempt. Resource
retry and suspension likewise do not count as new demand.

`database_wasm_primary_fallbacks_total` counts entry into V8 recovery by bounded
engine-failure reason; it does not count successful recoveries. The failed Wasm
attempt keeps its own outcome. The recovery outcome appears in
`database_execution_attempts_total` with runtime `v8` and role `fallback`, including
V8 admission rejection, cancellation or failure. Recovery ends the inverse
comparison rather than recording a V8/V8 pair.

These observations end at the function-runner boundary, before transaction commit
and application retries. `function_error` includes guest errors and guest-enforced
limits; existing execution and memory diagnostics retain their detailed causes.
V8 queue-full, queue-expiry and initial CPU-wait rejections retain their existing
`RejectedBeforeExecution` metadata and count as `overloaded`, with reasons
`backlog`, `queue_expired` and `cpu`. A dirty-isolate rejection remains a system
error. The same typed capacity rejections in a V8 verifier produce `capacity_drop`
instead of a verifier execution failure.
A dropped caller records cancellation even if a detached worker still owns
resources; use the resource-owner gauges to see that remaining work. Verifier
attempts here begin when the admitted lane is polled. Existing shadow selection,
admission-drop and timeout counters also cover verification skipped or cancelled
before entering that lane. Typed setup deadlines and unavailable source-generation
residency before lane entry use the existing `capacity_drop` terminal;
`route_resolution_failure` retains actual setup/authentication failures.
Do not infer total verifier demand from executed pairs or these lane-attempt
counters alone.

Application degradable leaders and analysis share
`APPLICATION_MAX_CONCURRENT_DEGRADABLE_QUERY_LEADERS` across engines. V8 dispatch
separately enforces `ISOLATE_DEGRADABLE_QUERY_ANALYSIS_CAPACITY`, which inherits
the application cap when unset. Configuring the two independently permits more
Wasm demand without allowing degradable V8 work to occupy the protected worker
base. V8 reservations survive caller cancellation through worker cleanup and
preparation for reuse;
analysis waits fairly for both gates, and dependencies bypass the root gate.
The V8 capacity is reported as
`isolate_scheduler_capacity_info{capacity_kind="degradable_query_analysis"}`;
dispatch rejection uses the existing scheduler rejection counter with reason
`degradable_query_analysis_capacity`. Analysis reports its additional wait in
`isolate_analysis_v8_capacity_wait_seconds`.

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

For an ordinary slot, the retained baseline is raw guest capacity minus verified
static credit plus retained private host bytes. An initialized-image slot instead
retains recorded private linear exposure, internal Wasmtime GC capacity and
private host bytes; the image has its own shared charge and replaces static
credit. Raw per-Store capacity limits still apply to both kinds of slot.
Active slots keep their checkout baseline in the ledger. Their liability covers
the admitted forecast and growth above checkout, including the initialized
recorder's reserved write allowance, not just its observed exposure. Outstanding
fault-side overflow loans are charged separately until reconciliation transfers
them into that liability without granting capacity. Let `E` be the sum of running
handler liabilities, `P` the sum of construction/return liabilities, and `R` the
largest readiness forecast across idle Stores and preparations. Growth reservation
is `P + max(E, R)`. Thus construction remains separate while initialized idle Stores
share enough headroom to admit one handler. Concurrent handlers each retain their
full liability; readiness does not promise execution in every Store simultaneously.
The full projection adds all retained baselines, this growth reservation,
outstanding overflow loans, module charges, one process-wide packed-backing count,
per-slot allowances and the safety reserve. Do not also add active current capacity
or unassigned overflow credit to this projection.

The controller maintains retained and active reservation totals incrementally.
Equal readiness forecasts have separate counted owners, so removing one maximum
does not release another Store's headroom. Reservation changes and page-loan
transfers synchronously update the fault-side reservation word. Telemetry never
supplies allocation authority.

An invocation updates current usage and peaks under its own lock while its
existing charged allowance covers the change. Increasing ownership still checks
live packed backing and outstanding page loans. Allowance growth, whole-page
refunds and lifecycle transfers coordinate with the controller. Small refills
round up to 4 KiB, falling back to exact-byte admission near the hard limit;
less than one page of additional unused ordinary credit can remain until the
next refund or lifecycle transition. This credit is included in active liability.
Initialized write grants remain reserved separately from host-value growth.

External packed owners can move the projection above the generated hard budget.
Releases and reconciliation of failed allocations still reduce the ledger in
that state. A private host allocation already committed after preflight remains
charged even when its post-allocation check rejects execution. A denied invocation
cannot regain growth authority when shared backing subsequently falls. Budget
eviction preserves a matching reusable Store and removes other idle Stores only
when the selected group can make the waiting admission fit.

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

An initialized application image is an optional host optimization for an exact
preparation group and generation incarnation. The producer prepares every entry
and dependency without request authority, then runs the authenticated guest
maintenance boundary. Publication waits for successful preparation finalization.
No request-warmed Store becomes a shared image. Unsupported Core state and denied
image or recorder admission retain ordinary preparation.

Linear bytes alone cannot reconstruct a Store. The loader inspects authenticated
Core once and retains eligibility with its compiled Module. Each view rebuilds
its own tables, GOT bindings and function references in the authenticated graph
order, restores proven scalar globals and stack limits, and privately maps the
sealed bytes. Reference globals, hidden unrestorable state and table/segment
mutation are ineligible. Start functions are ineligible except for a proven
single zero-fill of the sole imported `env.memory`, addressed by a nonnegative
constant offset from the immutable i32 `env.__memory_base`. The loader checks the
complete instruction sequence, not a function name; calls, local state and other
effects remain ineligible. This linker initialization runs in ordinary memory
before restoration replaces it with the prepared image. Core validation and
ordinary instantiation still enforce types and memory bounds.
Reference-valued function interfaces and non-function element segments are also
ineligible. This proof also depends on the registered host imports creating no
Wasmtime GC roots; numeric host handles require the separate empty-value and
empty-operation checks at preparation. All guest calls must have completed, and
the capture and attachment boundaries must have no pending Wasm exception.
Caught exceptions can leave unreachable objects and allocated Wasmtime GC heap
capacity. Neither is image state, and collection success is not a liveness proof.
That heap remains separately charged and Store-local, and is never copied into
the image. Each view reconstructs its own exception tags as well as its functions.
The guest maintenance decoder accepts versions 1, 2 and 3. Version 2 preparation
certifies the entire heap without requiring nursery reclamation or granting
discard ranges. Version 1 deferred preparation still retains ordinary instances.
Version 3 additionally separates prepared allocation storage and supports pressure
collection, dynamic free payload, empty-nursery observation and weak-slot trimming.
A restored Store discovers the version through the older allocator export before
issuing version-3 operations; an unknown-operation trap is never negotiation.
Deploy matching host support before using a new result version. A host that
uses the export must reject unknown result versions; hosts that ignore this
optional optimization can continue ordinary execution.
A replacement validates and applies the
complete initialization read set before restoration; changed configuration removes
only the expected cache owner, while existing views retain their own owners.

The whole sealed capacity has one shared charge, including sparse zero pages.
Each view retains its recorder and copied dependency metadata charges through
Store destruction. Attachment replaces ordinary static-page credit. Successful
invocations retain useful private state only under admitted context reuse and
enforced memory limits; errors and cancellation dispose the Store without guest
destruction. Invocation providers, opaque values and asynchronous operations still
receive ordinary host cleanup. Image copying, validation, mapping and owner
destruction run outside cache and preparation-table locks. Portable Core Wasm
does not require this host capability or a specific Wasmtime release.

Opaque-value copies and crypto/WASI random output validate arguments before
admitting the actual destination through `Memory::write` or
`Memory::data_mut_range`. A whole-memory mutable borrow grants
write access to every page, even if the caller later selects a small subrange;
it can overcharge private exposure or panic when admission is denied. Opaque
payloads copy directly from their host-owned allocation, and invocation randomness
fills the admitted guest range in place. Invalid input retains its host-invariant
error; write denial preserves `MemoryAccessError::is_write_denied` and writes no
prefix. These synchronous transfers hold exclusive Store ownership throughout;
growth, reclamation and guest reentry cannot overlap them. Writable-page exposure
is a conservative permission charge, not a measurement of resident bytes.

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

Prepared idle Stores retain a forecast for the next checkout, not separate
exclusive memory reservations. The `ready_growth_reservation` gauge is the
additional shared headroom `max(0, R - E)`. Inspection schema v2 exposes per-slot
`idleGrowthForecastBytes` and `readyGrowthBytes`; they must not be summed as charges.
Actual retained backing, write grants and outstanding loans stay charged until
their owning lifecycle releases them. Shared readiness cannot authorize a fault,
allocation or concurrent execution without its ordinary admission check.
Returning a Store can keep or reduce the total charge even if external packed
backing has crossed the soft budget. An increase still needs soft-budget admission,
and active cgroup pressure prevents retention. A reconciled baseline decrease may
cover readiness before completion; that transfer must not count the same released
bytes again as pending reclamation.

Cold and warm histories remain separate. Only successful executions can reduce
the corresponding learned forecast; developer-error, canceled, timed-out,
limited and system-error executions contribute only when their observed demand
is at least that history's prior forecast. Interrupted initialization therefore
cannot erase a learned cold requirement. Lower samples still appear in route distributions.
Forecast decreases also refresh a Store returned without execution after waiting
for CPU. That return cannot restore readiness bytes released at route checkout;
it keeps at most the admitted route reservation. A stale initialization context
discards the old Store and its permit, then admits a replacement with a fresh
cold forecast and zero checkout baseline.

Warm checkout selects a forecast for the authenticated route and exact
generation/runtime memory identity. Recent observations are conditioned on
one-MiB baseline bands for guest and retained host memory. Initialized images
use private exposure and separate internal-GC capacity instead of raw linear
capacity. These bands classify evidence; they do not establish available heap
or interchangeable private pages. Each route retains at most 128 observations;
32 completed matching samples permit using its own p99 plus headroom. Sparse,
missing or evicted histories use the group prior, raised by observed route
demand when necessary. Lower truncated samples do not age useful history.
Their cutoff includes earlier completions, but excludes any increase of the
group prior caused by that same sample, so a high failure cannot reject its own
route observation.
Budget eviction uses the same selected route and baseline as admission.
Group readiness uses a separate 128-observation window. Cumulative histograms
remain available for diagnostics, independently of those bounded predictions.
The completing call's peak does not impose an additional readiness floor:
released host buffers and expansions already retained in the baseline are not
new demand. Larger future route reservations still pass checkout admission.

Preparation admits its construction forecast separately and contributes its
future-handler forecast to shared readiness. Before retaining a Store, it checks
the actual initialized size and combined return projection through ordinary
retention admission. If retention is refused, it discards the Store instead of
publishing unusable prepared capacity. Demand-derived extra instances are
optional and share the same instance and byte limits as serving
work; historical concurrency is not an unconditional readiness requirement.

Prepared-image construction has a separate forecast based on the cached image's
linear capacity and observed source host state. The ordinary limiter charges
linear capacity until attachment completes, and image/recorder metadata keeps
its separate owner. This forecast does not certify initialization dependencies:
validation can discard the candidate and ordinary initialization then remains
subject to checked allocation limits. Cheap clone observations do not lower the
initializer population. Inspection reports this forecast source as `image_clone`.

Successful initialized-image calls ordinarily retain useful private backing.
After the handler's first reconciliation, retention projects the completing
slot's retained charge and readiness forecast through `P + max(E, R)`, replacing
its checkout baseline and active liability. Crossing the aggregate soft
budget selects reclamation toward 90% of that budget; a per-route p99 is neither
a retention limit nor permission to allocate. Pending reclamation claims prevent
duplicate maintenance without discounting admission charges before physical
release. Active cgroup pressure instead selects direct disposal because idle
return would be rejected regardless of collection.

For maintenance versions 1 and 2, selected reclamation frees native allocator
payload before conditional young collection. Version 3 also separates prepared
allocation storage from subsequent allocation. Prepared cells retain ordinary
tracing, sweeping and finalization; their holes are not reused for request
allocation. The host checks dynamic occupied bytes at each successful completion
and requests collection above 2 MiB, trying young collection before ordinary full
collection unless the nursery is already empty. A lower-threshold retry therefore
does not repeat nursery root visitation when only old/native storage remains.
The occupied-byte check is constant-time and does not walk the heap
or acquire an admission lock.

After collection, private exposure above 6.75 MiB selects release toward 6.25 MiB.
These are release-policy thresholds, not active-memory limits or evidence about
physical residency. Release uses allocator and collector free payload and the
tail of an already empty nursery, preserving a 2.5 MiB nursery prefix for reuse.
An unsuccessful attempt can be retried after the next pressure-triggered
collection, even when physical exposure has not grown: changed demand can free
payload inside existing backing. Aggregate pressure can select earlier
maintenance. Only an aggregate-required target permits one additional collection
attempt with a 512 KiB occupied threshold after an insufficient export. A proactive
target may be below the live floor, so an optional release does not justify that
retry. Ranges are reacquired after collection and weak-slot trimming,
and their ownership must be disjoint before any page is discarded. Partial release
is useful even when the proactive target cannot be met; ready admission still
charges the retained backing. Failure to meet an aggregate-required target
continues to select disposal.

Maintenance runs within the successful call's original CPU, fuel and deadline
limits, while finalizer resources remain valid, without controller/pool locks
across guest calls or mapping operations. It requires a second reconciliation.
An error poisons the Store and preserves the original typed execution failure
even if subsequent reconciliation also fails. Image disposal runs no guest
destructor. Claims remain
owned until readiness transfer publishes the physical savings; any savings
still covered by the checkout baseline remain claimed until completion or
Store disposal. There is no idle guest collection or per-call whole-image reset.

Cgroup pressure hysteresis uses
`LOCAL_BACKEND_MEMORY_PRESSURE_ENTER_HEADROOM_BYTES` and
`LOCAL_BACKEND_MEMORY_PRESSURE_EXIT_HEADROOM_BYTES`, defaulting to `3 GiB` and
`5 GiB`. Missing or unlimited cgroup samples do not disable instance, byte,
forecast, or reserve enforcement.

Async host waits release active guest CPU but retain the Store, memory permit,
forecast, and live-byte charge. Default values are implementation priors; they
must be calibrated under the actual cgroup, artifact sizes, V8/Wasm mix, and
reuse policy.

Cancellation produces no transaction-bearing result. A nested UDF can own the
transaction when its host future is dropped, so cancelled finalization revokes
authority and clears operations, values and queries without reading or recovering
that transaction. Ordinary result finalization still requires its transaction.
Once execution starts, cancelled instances are disposed, never returned to the
pool. When guest destruction applies, a live instance uses the invocation's CPU
permit for bounded destruction. An interrupted guest call poisons the instance
and permits only host cleanup and Store disposal. Instances using initialized
images also skip guest destruction.
Detached execution retains its shadow admission guard until disposal completes,
even after the caller abandons the result receiver.

An interruption-derived host error does not turn cancellation into a cleanup
failure. Failed revocation, finalization or destruction takes precedence over
cancellation in the memory terminal outcome. Memory completion occurs once after
disposal; a recorded memory-growth denial retains its memory-limit classification.
Test observations may lack a transaction write count after a cancelled nested
call; unavailable state is distinct from zero writes. Post-guest cancellation
with an intact transaction still exposes its discarded write count.

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

### Execution timing scopes

`UdfOutcome.user_execution_time` and the user-time histogram subtract pauses
from wall time. Database-provider work is paused before its first poll in both
engines, including cached results and errors. Wasm releases CPU admission only
if that poll is pending; V8 releases it before polling. Resumption waits count
as system time and must reacquire admission even after the request timer fires.
Same-isolate V8 nested guest execution temporarily resumes the shared user
clock; its database operations pause that clock again. Initialization and
module loading keep their existing conditional pause after the first pending
poll, so this boundary does not exclude every host operation from user time.

The complete intervals still differ. V8 starts its user clock with request
setup, before context validation, provider initialization, module evaluation
and export lookup, and reports it after result decoding and the final microtask
checkpoint. Static Hermes checks its separate preparation deadline, then rearms
the active user clock after selected-entry preparation and the second entry
selection, before capability issuance and handler read capture. Its reported
active interval includes handler execution, successful guest reclamation and
host result/value finalization up to timeout extraction; later outcome assembly
and runtime destruction are outside that interval. Preparation system time is
retained against the same cumulative system budget. Background preparation does
not emit a synthetic user-time sample. If invocation preparation fails before
rearming, an emitted user-time sample still covers preparation.

The observation collector's handler phase starts immediately before the V8
wrapper invocation or Wasm handler export. On ordinary completion it ends
before host result finalization, making it narrower than either reported
user-time interval. An early exit that bypasses a phase transition can leave
cleanup attributed to the preceding phase. The collector's thread CPU counts
exclude scheduler suspension, whereas reported user time can
include descheduling outside an explicit pause. Provider decoding, batching and
result handling outside the common provider future remain host work with
engine-specific costs. Compare matched phases and populations; aligning the
database pause boundary does not establish full runtime or preparation parity.

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

Memory gauges are published at controller initialization and by the existing
memory-maintenance sampler, rather than on each value update. The sampler captures
reservation state under the controller lock and publishes after releasing it;
capture and publication are serialized so an older capture cannot overwrite a
newer one. Current usage is sampled under each invocation's own lock and can
change while other invocations are sampled. These usage fields are diagnostics,
not additional reservations. Maintenance work can delay the next sample, and a
scrape can fall between individual gauge writes. Use the on-demand inspection
for a coherent reservation equation rather than treating separately scraped
gauges as an atomic inventory.

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
retained baseline, `activeLiability` (`P + E`), `readyGrowthReservation`
(`max(0, R - E)`), module charges, process-wide packed backing, unattributed allowances,
safety reserve and `initializedOverflowBorrowed`. Their sum is `projected`;
`initializedOverflowAvailable` is unassigned credit and is not added. The soft
and hard budgets are comparison thresholds, not additional terms. The packed count
is one atomic observation alongside locked controller state; database owners can
change it immediately afterward. Cgroup headroom is the controller's latest
sample; `cgroupSampleAvailable=false` means its pressure state may be retained
from an earlier sample. The existing pressure sampler also refreshes backing
metrics when only non-Wasm owners allocate or release buffers.
`retentionPressureActive` records aggregate retention hysteresis, separately from
cgroup pressure. `reclamationInProgressBytes` is planned work still owned by active
invocations, not freed capacity or a subtraction from `projected`.

Slots include raw guest capacity, shared-static credit, private host charge,
checkout baseline, readiness forecasts and active liabilities. Schema v2's
`idleGrowthForecastBytes` and `readyGrowthBytes` contribute to the readiness
maximum, not a sum of per-slot charges. Historical schema v1 instead reports
separate idle reservations and includes preparation headroom in active liability;
readers must not mix schema versions within a capture or subtract their ledger
terms across an interval. Raw capacity includes
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
they are cumulative eligible observations, not the bounded prediction window
or interval distributions. `recentWarmSamples` describes group-window occupancy.
Active slots expose `forecastSelection` with the selected source, matching sample
count and starting baseline. Route `demandHistory` reports recent populations
and their growth p99 upper bounds (null denotes the unbounded overflow bucket).
Its baseline bucket indices denote one-MiB bands; `initialized` distinguishes
private exposure from retained capacity.
`imageChargeId` separates initialized images, including replacement after a
configuration change without a source-generation change.
It identifies the owner selected at checkout. Disposal can release the last
image owner just before the outer invocation permit completes, so its charge
row may already be absent while the active slot still reports that selection.
These history snapshots can age downward and must not be subtracted as counters.
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
to the total shadow-failure count. Verifier lanes never recover. A configured
Wasm primary may recover from the two host-classified timeouts after successful
disposal; exhausted fuel remains terminal. System-time exhaustion during host
waits and independent host or cleanup failures also remain terminal, including
when they race a timer. Developer errors with timeout-like text remain ordinary
outcomes.
An active timeout can interrupt a nested host call at its pause boundary after
that call takes the transaction. Disposal then needs no transaction-bearing
result or completed read comparison. Independent nested-provider failures retain
their own error, and any observed cleanup failure still prevents recovery.

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
separate generated-export dispatch classification. Raw trap codes remain typed
through initialization and module-ownership wrappers so exhausted fuel and
stack limits cannot authorize recovery. An interrupt requires independent typed
host-timer evidence; its phase or trap code alone cannot authorize a timeout retry.
When host-operation tracing was enabled, the capsule retains only the last 32
fixed operation/status pairs and an explicit truncation flag; otherwise it
marks the trace unavailable. Stderr evidence is only the fixed `empty`,
`static_hermes_uncaught_exception`, `hermes_heap_out_of_memory`, or `unclassified`
category, byte count, and
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

Run the timeout and continuous-poll checks. These use synthetic guests and do
not require an external compiler artifact; they establish interruption and
suspension behavior, not complete-handler performance. They cover synchronous
expiration without watchdog progress, pause/resume deadline preservation,
rejected activation after preparation expiration, and guest epoch callbacks
during instantiation, resumed host waits, and destruction:

```sh
scripts/run_cargo.sh test -p isolate \
  --features static-hermes-wasmtime-gate \
  timeout::tests

scripts/run_cargo.sh test -p isolate \
  --features static-hermes-wasmtime-gate \
  generated_epoch_checks_timeout_and_poll_budget_without_watchdog
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
2. Produce a complete deployment-v8 graph closure with a compatible engine and
   publish a generation-v9 descriptor through an additive v2 source catalog.
   Use generation-v10-shadow-only instead when publication should exclude
   primary Wasm execution.
3. Start in source-keyed mode with normal routing disabled, both V8-primary
   shadow rates at zero, both inverse verifier rates at zero, and instance reuse
   enabled. Source-keyed preparation requires reuse even while sampling is zero.
   Verify descriptor-only startup, exact readiness, and a paired active-state
   response. Read runtime-policy status as well: stored policy overrides the
   environment defaults, including after a restart.
4. Enable a small query shadow rate. Verify immediate shadow admission, source
   authentication, bounded evidence, cancellation, and rollback to zero.
5. Enable a small mutation shadow rate. Verify that only V8 commits and shadow
   writes are discarded across success, timeout, error, and cancellation.
6. Complete deterministic parity for every selected route. Traffic evidence
   cannot substitute for unobserved routes.
7. Calibrate memory, CPU concurrency, and the chosen reuse policy under a
   representative cgroup and mixed workload.
8. For a controlled Wasm-primary population, use the paired generation-v9
   and select Wasm primary through the live policy API. A
   generation-v10-shadow-only must first be republished as v9, checked for exact
   readiness and paired with its matching source. Set `shadowBps` to zero
   for no verifier or to the desired V8-verifier sampling rate. Verify
   transaction, OCC, timeout and cancellation behavior in this direction too.

This sequence validates behavior; it does not add a historical execution-proof
prerequisite to primary-capable publication.

## Rollback

For V8-only operation on a backend supporting live runtime selection, run:

```text
convex runtime-mode --primary v8 --shadow-percent 0
```

Read status to confirm the applied policy. Newly dispatched executions use V8;
existing executions finish with their captured selection. No restart or schema
rollback is needed. Setting only shadow sampling to zero leaves primary authority
unchanged.

On older backends without the live API, set the process gate, both per-UDF primary
switches and all shadow rates to zero and restart. On a backend with stored policy,
changing those defaults does not override it. Keep the registry mounted during
the first routing rollback so its authenticated artifacts remain available. If
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
is not automatic rollback. Configured-primary recovery is separately limited to
classified, safely disposed engine failures; forced-Wasm diagnostics remain strict.
