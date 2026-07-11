# Context Reuse

This patch makes V8 context reuse one per-module feature covering queries, mutations, ordinary
Convex-runtime actions, and HTTP actions. It keeps one bounded isolate-local cache for all three
execution kinds and reinstalls request-owned Rust state on every invocation.

The cache is an optimization, not a correctness boundary. Reuse preserves JavaScript module and
global state, so an application must opt in only for module graphs that are safe when state survives
between executions. The backend does not contain an application module allowlist.

## Application policy

An entry module can opt into each execution kind independently:

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

Review the complete static and dynamic import graph before enabling a permission. Do not enable it
for code that retains arguments, identities, transactions, documents, request or response objects,
callbacks, errors, promises, streams, or other request-derived state in module globals or imported
package state. The same review applies to third-party packages and import-time initialization.

## What is bounded and safe

Each isolate retains at most one probationary plus the configured protected number of contexts. The
default is five protected residents (`5+1`).
`ISOLATE_CONTEXT_CACHE_PROTECTED_RESIDENTS_PER_ISOLATE` changes the protected segment; the shared
pool bound and optional `ISOLATE_CONTEXT_CACHE_MAX_RESIDENTS` setting are derived and validated by
the existing cache-capacity machinery. Database-UDF, ordinary-action, and HTTP-action contexts
have distinct cache kinds and cannot alias, but they compete for the same bounded resident budget.

Reuse is best effort. A miss creates a fresh context, and cache frequency, memory pressure, worker
capacity, isolate recreation, and concurrent use can all prevent retention. The scheduler advertises
only resident keys through a thread-safe mirror; V8 roots stay on their owning isolate thread.

After taking a resident, the backend validates its initialization read set. Deploys, environment
changes, component resources, or other initialization changes therefore force a fresh context.
Every request gets fresh identity, transaction, callbacks, streams, task state, and timeout state.
Database UDFs and actions also carry caller-drop cancellation to the save boundary. A context is
published only after successful execution, a final microtask checkpoint, clean termination, no
pending request-owned work, and a valid read set. HTTP actions additionally require a successfully
streamed response, an open isolate response stream at finalization, and successful delivery of the
outcome to the function-runner response channel. Later forwarding from that channel to an outer HTTP
transport is not a cache-publication boundary.

## Adoption and rollback

Apply this patch after backend memory resilience and the current isolate scheduler/cache-capacity
machinery. The required metrics are emitted automatically; establish fresh-versus-reused,
module-evaluation, cache, isolate-memory, termination, and recreation baselines before enabling
application permissions.

Roll out the backend first, then enable one reviewed module or execution kind at a time. To roll
back semantic reuse, remove the corresponding property (or the legacy marker), redeploy the module,
and restart backend workers so process-local residents are destroyed. There is no backend-wide HTTP
reuse switch.

See [the design reference](design_reference.md) for lifecycle ordering, protocol compatibility,
metrics, scheduler affinity, cancellation, and interactions with dependency and degradable-query
admission.

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
