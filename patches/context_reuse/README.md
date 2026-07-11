# Context Reuse

This patch lets a self-hosted Convex backend keep an initialized V8 JavaScript
context for later calls, avoiding repeated module evaluation. It supports queries,
mutations, actions running in the Convex runtime, and HTTP actions. Node.js actions
use a separate runtime and are outside this patch.

Module and global state survive reuse; caller identity, transactions, and other
request state are replaced for every call. Applications must enable reuse only
when one request cannot leave JavaScript state that changes later requests. Read-only module
constants can be retained; saving the current caller in a module variable cannot.
The backend checks lifecycle and initialization dependencies, but it does not
prove application code is safe to reuse.

Three parts work together:

- This backend patch reads the deployed reuse policy and manages cached contexts.
- The [CLI bundling patch](https://github.com/convex-in-prod/convex-js/tree/main/patches/default_database_context_reuse)
  can emit that policy from `convex.json` and group entries that share initialization.
- The compiler's [context-reuse analyzer](https://github.com/convex-in-prod/convex-wasm-compiler/tree/main#context-reuse-analysis)
  supplies source-graph and dependency-policy checks for applications adopting reuse.

This feature runs JavaScript in V8. The separate
[Static Hermes Wasm UDF patch](../static_hermes_wasm_udfs) consumes compiled Wasm
artifacts; it also uses context-reuse analysis, but has its own runtime and caches.

## Application policy

An entry module can opt into each execution kind independently. For a manually
managed policy, export:

```js
export const experimental_reuseContext = {
  queries: true,
  mutations: true,
  actions: true,
  httpActions: true,
};
```

The old `experimental_reuseContext = true` form remains compatible and means
`{ queries: true, mutations: true }`. An omitted property means `false`. The four permissions are
module-wide and do not vary by exported function; the request's validated UDF type selects the
applicable permission. HTTP actions use the same analyzed module policy and no longer require a
startup-time backend permission knob.

When the CLI emits policy through `bundler.experimentalContextReuse`, configure
it in `convex.json` instead of also exporting a source marker. The
[CLI configuration guide](https://github.com/convex-in-prod/convex-js/tree/main/patches/default_database_context_reuse#purpose)
describes defaults, exclusions, and action selection. The backend receives the
resulting module policy, not an application allowlist.

Review the complete static and dynamic import graph before enabling a permission. Do not enable it
for code that retains arguments, identities, transactions, documents, request or response objects,
callbacks, errors, promises, streams, or other request-derived state in module globals or imported
package state. The same review applies to third-party packages and import-time initialization.

## Grouped initialization

Grouping lets several entry modules reuse one initialized dependency graph. For
example, two generated entries that only re-export functions from the same
shared chunk can use one context instead of retaining a copy per entry.

The typed policy accepts a versioned initializer selection:

```js
initializationModule: {
  version: 2,
  database: "_deps/context_init_<database-hash>.js",
  action: "_deps/context_init_<action-hash>.js",
}
```

Queries and mutations use `database`; ordinary actions use `action`. Each optional module
statically imports a fixed group of entry modules. A missing path retains per-entry reuse;
actions never fall back to the database path. At least one path must be present, each must be
a canonical root-level `_deps/*.js` path, and its execution kind must have reuse permission.
Unknown versions and fields are rejected during analysis and persistence decoding.
The backend evaluates the selected graph during cold initialization and uses its path as the
cache and scheduler-affinity key within the execution kind. The field grants no reuse permission
and does not change HTTP-action cache keys. The legacy string-valued `initializationModule`
continues to select the same root for every permitted query, mutation, or ordinary action.

A warm invocation must find its original entry already loaded; it cannot expand the group. All
member metadata and dependencies participate in the cold initialization read set, including
members never invoked, so changing any member invalidates the resident. Function lookup,
authorization, arguments and logging continue to use the original function path. Components and
execution kinds remain separate; concurrent calls can still initialize independent replicas.

The CLI's [shared initialization groups](https://github.com/convex-in-prod/convex-js/tree/main/patches/default_database_context_reuse#shared-initialization-groups)
automatically group outputs containing only imports and re-exports with identical
ordered static chunk imports, when reuse is enabled. The initializer imports those
entries without rebundling them. This shares an already-common dependency graph while adding only
the forwarding entries and policy exports. It does not combine arbitrary modules with different
initialization behavior. Group membership and contents determine the generated path. Ordinary
action entries are explicitly selected with `bundler.experimentalContextReuse.actions.entries`.
Database groups include all eligible entries, regardless of action permission. Separate action
groups include only eligible selected entries. `actions.grouping: false` keeps actions per-entry
without splitting their database group. All action exports in each selected module share the
permission and retained context. Database and action contexts remain different classes because
their retained host callbacks bind different Rust environment state.

For database UDFs and actions, grouping adds entry-metadata reads and broadens invalidation to
all initialized group members. Actions retain a component source snapshot for later imports,
but that preload does not make unrelated modules initialization dependencies. Loading a new module
during an action handler prevents retaining that context. Grouping can reduce duplicate contexts;
every member must remain safe for reuse, and count limits still do not imply equal RSS.
See [selective publication invalidation](selective_invalidation.md) for the separate module,
runtime-selection and query-result contracts, including the conservative Wasm dependency scope.

The maintained single-root backend rejects an object-valued `initializationModule` during
analysis. Upgrade it before deploying version 2 output; adding an ignored action-specific sibling
field to a legacy string would incorrectly initialize database-only wrappers for actions.
Earlier typed-policy backends that ignore the whole initializer retain per-entry reuse, and
boolean-only backends treat the typed policy as disabled. Internal protocol producers send split
roots only in the new versioned field, with the legacy root absent, so old workers retain safe
per-entry initialization. Persisted version 2 objects cannot be read by the single-root backend;
restore legacy metadata with a compatible deployment before rolling back that backend.

## What is bounded and safe

Each V8 isolate has one context cache. A retained context is called a resident.
Queries and mutations share the database cache kind; ordinary actions and HTTP
actions each have a separate kind. All three compete for the same resident budget.

Each isolate retains at most one probationary context plus the configured number
of protected contexts. New candidates enter the probationary slot; frequency-based
admission favors repeatedly used contexts in the protected segment. The default
is five protected residents plus one probationary resident (`5+1`).
`ISOLATE_CONTEXT_CACHE_PROTECTED_RESIDENTS_PER_ISOLATE` changes the protected segment; the shared
pool bound and optional `ISOLATE_CONTEXT_CACHE_MAX_RESIDENTS` setting are derived and validated by
the existing cache-capacity machinery. Contexts from different kinds cannot alias.

Reuse is best effort. A miss creates a fresh context, and cache frequency, memory pressure, worker
capacity, isolate recreation, and concurrent use can all prevent retention. The scheduler advertises
only resident keys through a thread-safe mirror; V8 roots stay on their owning isolate thread.

Before reusing a context, the backend validates the module metadata, environment,
and other dependencies read during initialization (its initialization read set).
Changes to those dependencies force a fresh context.
Every request gets fresh identity, transaction, callbacks, streams, task state, and timeout state.
Mutation file-download caches also belong to the invocation and are shared with its nested
mutations, never retained in a reusable context. Sampled runtime diagnostics distinguish file-storage
retries from OCC and write-limit retries and record their backoff.
Database UDFs and actions also carry caller-drop cancellation to the save boundary. A context is
published only after successful execution, a final microtask checkpoint, clean termination, no
pending request-owned work, and a valid read set. Ordinary actions additionally require successful
return-schema validation using the caller's validation snapshot, delivery to the function-runner
response channel, and a final cancellation check after that send.
HTTP actions additionally require a successfully
streamed response, an open isolate response stream at finalization, and successful delivery of the
outcome to the function-runner response channel. Later forwarding from that channel to an outer HTTP
transport is not a cache-publication boundary.

Clearing an action timer releases its native promise resolver. A canceled sleep completion
already in transit is consumed without invoking JavaScript, while normal execution-permit
reacquisition still occurs. Unused `AbortSignal.timeout` deadlines release their roots after
native tasks stop and before the final microtask checkpoint. Deadline coercion and timeout
reasons retain the runtime's existing behavior.

Outbound fetch cancellation channels may remain open after useful work completes. They are
tracked separately from response and upload bodies and do not alone prevent reuse. Pending
fetch promises, unread response bodies, unfinished uploads, ordinary timers, and unrelated
stream listeners still reject context retention. Application and package code must not retain
request signals or controllers in module state. Actual V8 tests cover timer cancellation,
late completions, deadline firing and disposal, consumed fetch bodies, and unfinished uploads.

## V8 shadow executions with Wasm primary

With the [Static Hermes Wasm UDF patch](../static_hermes_wasm_udfs/README.md#primary-and-verifier-modes),
a Wasm-primary query or mutation can run a V8 shadow execution for comparison.
That verifier uses the ordinary V8 execution path and the deployed module's
reuse policy. It can take a valid cached context and retain a context after
successful execution, subject to the same initialization checks, request cleanup,
and cache limits as a V8 primary. The verifier role does not force a fresh context
or create a separate context-cache namespace.

Each verifier still has its own transaction and request state; its writes are
discarded. These transaction boundaries do not require repeating module
initialization. Shadow traffic can therefore keep V8 contexts warm, and timing
comparisons should distinguish initialization misses from reused executions.

## Adoption and rollback

Apply this patch after backend memory resilience and the current isolate scheduler/cache-capacity
machinery. The required metrics are emitted automatically; establish fresh-versus-reused,
module-evaluation, cache, isolate-memory, termination, and recreation baselines before enabling
application permissions.

Roll out the backend first, then enable reviewed modules or execution kinds.
Disable reuse at the place that supplies the policy: remove the source property
or marker for a manually managed module, or update the CLI's defaults, exclusions,
or action selection. Redeploy the affected modules so the backend sees the new
policy. This prevents future reuse for the disabled kind. Restart backend workers
only if already retained contexts must be discarded immediately. There is no
backend-wide HTTP reuse switch.

See [the design reference](design_reference.md) for lifecycle ordering, protocol compatibility,
metrics, scheduler affinity, cancellation, and interactions with dependency and degradable-query
admission.

## Non-reusing action placement

A V8 or HTTP action without context reuse prefers an idle worker for the same
client with no saved contexts, searching from most recently completed (MRU) to
least recently completed. Its separate fresh context
prevents overwriting residents, but running the action on a populated worker
would still make those residents unavailable to concurrent requests, including
the action's database calls. The search stops at the first empty cache in MRU
order; if there is none, the existing MRU fallback applies.

This preference uses only existing same-client idle workers. It never waits for
a busy worker, creates a worker for cache diversity, or changes context ownership,
admission, dependency reserves, or action limits. Reusable actions retain the
ordinary affinity/miss policy; fresh queries, mutations and deployment requests
retain their existing placement. The preference is independent of the optional
reusable-miss policy below.

The preference changes worker utilization and idle lifetimes. A populated worker
can now expire during action-only traffic instead of being kept alive by those
actions. Compare mixed traffic and quiet intervals followed by returning hot
modules, including initialization CPU, completed throughput, tail latency and
RSS. Unchanged worker and resident limits do not guarantee unchanged memory use.
Scheduler tests cover empty-worker ordering, same-client fallback with unused
worker capacity, busy/other-client exclusion, dependency actions, and preserving
warm query placement. They do not measure V8 costs or production benefit.

`isolate_scheduler_non_reusing_action_placement_total` counts accepted V8/HTTP
action dispatch attempts to existing same-client idle workers, with fixed
`pool_name` and `outcome` labels:

- `mru_empty`: the first worker's mirror was already empty;
- `preferred_empty`: a later empty mirror was selected after observing populated workers;
- `no_empty`: every candidate mirror was populated, so MRU fallback was used.

New workers, stolen workers and reuse-eligible requests are excluded. These are
placement observations, not a simultaneous cache snapshot or a count of avoided
initializations. Idle recreation and pressure can clear mirrors during the scan.

## Initialization diagnostics and optional placement

`ISOLATE_CONTEXT_CACHE_CAPACITY_PLACEMENT` defaults to false. When enabled, exact
idle affinity remains first. On a miss, the scheduler prefers an existing idle
same-client worker with room backed by a global ownership token, then one able
to replace its probationary resident. A nominally empty cache without a token
does not outrank replaceable probationary capacity. The hint is rechecked by
the real save path; it neither reserves a token nor creates a worker.
Growth ties use the most recently completed worker. Replacement ties use the
least recently completed eligible worker, so overflow copies do not repeatedly
compete beside the same frequently used protected entries while older idle caches remain
unused. This compares scheduler completion order, not local frequency counts
whose aging clocks differ between workers. If every cache is unavailable, the
existing most-recently-completed fallback applies.
During cgroup pressure, miss-capacity preference is disabled because new
contexts cannot be retained; exact idle affinity still takes precedence.

Completion recency does not establish that a cache is cold. A long cold scan can
rotate replacements through idle caches, age their protected residents and evict
contexts that MRU placement would leave untouched. Using those workers also
postpones their idle recreation. Measure initialization, eviction and memory
costs when a prior hot set returns; this heuristic does not dominate MRU.

Compare policies with unchanged workload, worker, context and memory budgets
before enabling this option. The synthetic trace models serial
cache operations over a hot set, cold scan, changed hot set and pressure. It
occasionally excludes a worker from selection, but does not model concurrent
execution, taken roots, validation, V8 initialization, or real save finalization.
A second serial trace covers full-budget overflow with empty or contested MRU caches.
Its favorable miss count is not evidence of production tail-latency or RSS
improvement. An enabled-policy comparison still needs successful work,
initialization cost, tail latency and memory observations. Existing opt-in,
read-set validation, request reset, microtask cleanup and client/execution-kind
boundaries still apply.

The scheduler emits `isolate_scheduler_context_miss_placement_total` for
accepted reusable-context miss dispatches to an existing same-client idle worker. Its fixed
labels identify `policy=mru|capacity` and selected/best observed retention
(`unavailable|replace_probationary|grow`). Disabled placement still observes
alternatives, exposing cases where the selected worker cannot retain a miss
but another idle cache could. Exact hits, new workers, and stolen workers do
not increment this family. Observations precede execution and are not promises
of successful retention; they do not measure replica lifetime or per-module
popularity. Counts from `mru` and `capacity` generations must be kept separate.
These are backend attempts, including retries, rather than logical requests.

Metric migration: `reason="admission_replacement"` is split into
`protected_replacement` and `probationary_rejection`. Sum the two when comparing
against the old combined counter. The new eviction age is a logical local
insertion/return count, not seconds; frequency is the aged local estimate. Scheduler miss
outcomes distinguish a mirrored resident on a busy worker, the same key in
flight without an observed resident, and no idle affinity. Mirrors are hints;
failed read-set validation is recorded separately at execution. First-use and
reassigned workers have no current mirror until completion; an idle recreation
can also leave the scheduler holding an empty old mirror. Absence of an observed
resident is not proof that no busy worker owns one.

### Sampled runtime diagnostics

`runtime_attempt_diagnostic` samples queries, mutation attempts and actions at
approximately 1/128 eligible attempts, capped at 60 starts per minute and eight
concurrent observations. Sampler-lock contention skips observation. Caller records
can finish before queued or detached runtime tasks, which keep the sample slot
until their observation drops. Paths
are bounded to 512 bytes. Records contain phase CPU/poll wall, suspension,
context validation, mutation commit and OCC timing, with no arguments or
environment values. Module initialization spans are included in their enclosing
phase, and lane suspension overlaps runtime work; do not sum overlapping fields.
Query result-cache hits do not enter this sampler: use the existing query-cache
metrics. An empty observation is not evidence of a cache hit. Nested or paired
runtime observers can suppress an outer sample's runtime detail.

The record message is `runtime_attempt_diagnostic {JSON}` with `version: 1`.
Collectors retaining text can extract the JSON payload; a JSON tracing formatter
wraps that message in `fields.message`. The observation uses the existing camelCase
serialized execution-observation fields. Outcome values are snake_case. Commit,
selected/elapsed backoff and conflict-wait milliseconds are nullable: null means
no completed measurement, including interruption. Conflict wait excludes the
backoff sleep. Missing or invalid CPU accounting is not zero CPU; runtime tasks
still active at snapshot mean incomplete runtime evidence. HTTP actions bypass
this sampler, and Node actions do not acquire an isolate-runtime observation.
Ordinary V8 actions do carry an observation through their initialization,
handler and finalization. Their background task-executor CPU is excluded;
task-response waits appear in other suspension. Same-isolate nested UDF calls
mark phase accounting invalid, as separately scheduled nested runtime tasks do.
The log record contains no errors, arguments, results or environment values.
