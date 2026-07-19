# Bounded Multi-Context Reuse

This patch replaces Convex's single saved V8 context per isolate with a bounded cache of reusable
database-UDF and HTTP-action contexts. It is intended for self-hosted installations that have
reviewed several application entry modules for context reuse and find that the upstream one-slot
policy churns between unrelated hot module keys.

The cache is isolate-local because V8 contexts and their handles are isolate-affine. The scheduler
uses only a thread-safe mirror of resident keys; it never moves a context between worker threads.
No application module, function, route, component, client, or deployment name is encoded in backend
policy or metric labels.

## Patch composition

This patch changes cache shape, admission, pressure eviction, and scheduler affinity as one unit.
Applying only the multi-entry container without changing scheduler fallback would still let cache-key
diversity create isolate workers. Changing scheduler fallback without separating the transient fresh
context would let ineligible database UDFs and ordinary actions clobber the residents that fallback is meant
to preserve.

Database reuse eligibility remains owned by upstream's `experimental_reuseContext` marker and the
maintained
[`cancellation_safe_database_context_reuse`](../cancellation_safe_database_context_reuse/README.md) safety patch.
Marked queries and mutations may reuse. This patch does not weaken that boundary.

HTTP action reuse remains independently disabled unless the operator enables
`REUSE_HTTP_ACTION_CONTEXTS`; see
[`reuse_http_action_contexts`](../reuse_http_action_contexts/README.md). Database and HTTP contexts
share capacity but use distinct cache-key kinds.

The companion
[`context_reuse_observability`](../context_reuse_observability/README.md) patch is required for a
production rollout. Its cache lifecycle and scheduler-affinity meanings are updated by this patch.
The cgroup-pressure consumer additionally requires
[`backend_memory_resilience`](../backend_memory_resilience/README.md), which owns the controller,
allocator actions, shared signal, and Node response. This context patch is applied after that
generic memory patch and owns only the isolate/cache response.

## Upstream starting point

Upstream commit `fa5836a1b` replaced an unbounded map of database UDF contexts with one saved slot.
That slot can hold either a fresh pre-created context or one reusable context. The change bounded
retained contexts, but it also means that two unrelated reusable module keys on one worker replace
one another.

The one-slot scheduler tries to avoid that replacement. For a reusable miss it prefers a same-client
worker with no reusable resident, then creates a worker while physical capacity remains, and only
then steals an idle worker. As applications mark more entry modules, the number of warm keys can
therefore affect worker creation even when serial request concurrency does not require another
worker.

This patch keeps the upstream bound explicit and finite while removing cache-key diversity from the
worker-creation decision.

## Cache shape

By default, each isolate can retain at most six reusable contexts:

- one probationary window resident;
- five frequency-protected main residents.

`ISOLATE_CONTEXT_CACHE_PROTECTED_RESIDENTS_PER_ISOLATE` configures the protected segment and
defaults to five. Values below two are rejected so pressure can preserve its separate two-protected
layout without exceeding the configured normal capacity. The one-entry probationary window is not
part of the configured value.

The fresh pre-created context is separate. An unmarked query or mutation, an action or analysis
request, or a reusable miss can take or create that fresh context without clearing the reusable
cache. After the request, the worker can prepare another empty fresh context for low-latency
dispatch.

The reusable key is:

```text
(database_udf | http_action, component id, canonical entry-module path)
```

The exported function name is not part of the key. Two eligible query exports from one entry module
can use the same context. The same module path in two components is two keys. A database context and
an HTTP context cannot alias even if their canonical paths are equal.

The configured per-isolate limit applies to idle reusable residents. A context is removed while a
request uses it and can return only after successful execution. Same-isolate nested execution can
temporarily have an outer reusable context in flight while an inner context is cached; the shared
resident-token budget still bounds retained and in-flight reusable ownership across workers.

## Admission policy

The cache uses a deliberately small W-TinyLFU-style policy.

Every eligible reusable lookup, including a miss, increments an exact worker-local frequency
counter. After 16 observations per configured reusable position, all counters are halved and zeros
are removed. The finite aging window lets recent popularity replace old popularity and bounds stale
one-hit keys without a probabilistic count-min sketch.

A newly saved miss enters the one-entry probationary window. When another new candidate replaces
it, the displaced window entry competes with the weakest protected main resident:

- free main capacity admits it directly;
- otherwise it must have strictly greater recent frequency than the victim;
- the existing protected resident wins a frequency tie;
- equal-frequency victims are ordered by least recent successful access.

One-hit outliers therefore occupy the probationary window briefly but normally cannot evict a
proven resident. A genuinely new hot key accumulates frequency even on misses and eventually wins
admission.

A protected context that was taken for a request remains a protected candidate when successful
execution returns it. Same-isolate nested execution can fill its former position before it returns.
In that case the returning protected context competes with the current weakest resident; recent
successful access breaks a protected-frequency tie. The cache never exceeds its configured idle
reusable capacity and never retains duplicate keys.

If nested execution saved the same key, the returning context replaces that duplicate in its
existing segment. The key already owns a slot and resident token, so making it compete again could
discard valid warm state or evict an unrelated protected entry without improving key diversity.

The implementation scans the configured protected segment for victim selection. It does not add
Moka, Caffeine, a count-min sketch, hill climbing, or adaptive segment sizing.

## Scheduler-wide resident budget

Every isolate cache in one scheduler shares a resident-token budget. The default capacity is the
structural maximum:

```text
(configured protected residents per isolate + 1 probationary resident) × maximum isolate workers
```

An operator can set `ISOLATE_CONTEXT_CACHE_MAX_RESIDENTS` to a smaller positive decimal value. The
backend rejects zero, malformed values, overflow, and values greater than the structural maximum at
startup. Omitting the variable permits full population but remains bounded by the per-isolate and
worker limits.

A token follows a reused context while it is in flight. Validation failure, cancellation, execution
failure, rejected re-admission, cache clear, and isolate destruction release it. A fresh candidate
requires a free token unless the full pool still permits a one-for-one exchange with an idle local
probationary resident. That exchange reuses local token ownership even when an in-flight context
leaves an empty structural slot; it must not fill that slot without a token.

Failure paths keep the token until the owning V8 scopes, context root, and module-map roots have
been dropped on the isolate thread. Cache removal likewise clears the scheduler mirror and destroys
the removed roots before shared capacity becomes available to another worker.

For database UDFs, a successful candidate remains outside the cache through database-environment
argument, warning, outcome, and transaction finalization. HTTP candidates likewise remain private
through isolate-local warning and result finalization. Both paths recheck locally visible
cancellation or receiver closure and termination immediately before synchronous insertion, so the
token is dropped with the candidate when one of those local fences fails.

These final local checks are the publication linearization points. Database retention validation,
application transaction merge, return validation, and query-result finalization still occur after
database isolate publication. The in-process HTTP forwarding wrapper can likewise discover an
external delivery disconnect after a clean action has been published. Those later outcomes do not
retract an already clean reusable context; this is the existing reuse contract, not a pending
cross-layer confirmation protocol. See the database-UDF and HTTP companion essays.

The token budget is a count bound, not byte accounting. V8 does not expose reliable retained bytes
per context in this path. Operators should choose a lower cap when their worker count and memory
allocation cannot support full population.

The lower cap is a safety ceiling, not a process-global eviction cache. It does not reach into
another isolate to reclaim a context. When the pool is full, a worker without an idle local
probationary resident rejects a fresh candidate until some token is released; the
`reject_pool_capacity` operation reports that condition.

## Heap pressure

The V8 isolate heap limit remains the byte-level hard bound. This patch does not change
`ISOLATE_MAX_USER_HEAP_SIZE` or `ISOLATE_MAX_HEAP_EXTRA_SIZE`.

Between requests, Convex requires at least one user heap's worth of available V8 heap before the
isolate can safely serve arbitrary fresh work. The one-slot implementation exempts an isolate from
recreation whenever any reusable context is saved, which can preserve a hot context but does not
reclaim selectively.

The bounded cache responds to isolate-local V8 heap pressure in this order:

1. drop the unused fresh context;
2. ask V8 to collect unreachable context state;
3. evict the probationary reusable resident if pressure remains;
4. evict the weakest protected resident, ordered by frequency and then recency;
5. collect and recheck after each reusable eviction;
6. recreate the isolate only if the required free heap remains unavailable after the cache is
   empty.

This retains as much useful warm state as the actual isolate heap permits. It also guarantees that a
saved context is not an indefinite blanket exemption from the existing free-heap requirement.

The separate backend-memory controller can publish cgroup pressure before external HTTP shedding.
On that signal, each idle isolate drops its fresh context, removes the probationary resident and the
weakest protected residents, retains at most the two strongest protected residents, and requests a
V8 low-memory collection after removing roots. New probationary admission remains suppressed while
the signal is active. A protected context returning from an in-flight request competes by frequency
and recency for one of the two retained positions. A context rejected or replaced after the
transition schedules another collection after its removed roots have been dropped. Clearing
pressure restores the normal six-entry bound but does not prewarm contexts.

A request can finish while its worker is unable to poll the pressure watch. The synchronous context
save therefore holds a shared pressure-state guard through admission, mirror updates, root
destruction, permit reconciliation, and operation accounting. Controller publication takes the
corresponding exclusive guard. A returning context is consequently published entirely before a
pressure transition or reconciled against the new state; it cannot sample one state and insert after
the controller has published another.

The cache does not poll process RSS on every save and does not estimate context bytes. It consumes a
shared pressure signal whose cgroup sampling, hysteresis, allocator trim, and Node retirement
contract is documented in
[`backend_memory_resilience`](../backend_memory_resilience/README.md).

## Scheduler behavior

For an eligible reusable request, an idle worker is selected in this order:

1. same client and exact resident key;
2. any idle worker for the same client;
3. a new worker, only when no same-client worker is idle and the configured physical worker set has
   not yet been created;
4. the existing least-recently-used idle worker after the physical set is full.

Non-reusable requests also take any idle same-client worker. Their fresh contexts are independent of
the reusable residents, so no empty-cache preference is needed.

The client-isolation boundary is unchanged. A worker selected for another client recreates its
isolate before serving that client. Dependency reserve, action-shell capacity, queue lanes, delay
control, hard expiry, and per-client worker limits are unchanged.

Frequency accounting remains worker-local. A shared scheduler-global popularity table would add a
contended update to every eligible request and couple cache admission to scheduler ownership. Once
serial misses stop allocating workers based on key diversity, the selected worker's request stream
is the relevant local admission stream. A module can still have several warm contexts when actual
concurrency requires several workers.

## Metrics

The patch keeps metric labels closed and application-independent.

Existing decision, lookup, validation, fresh/reused initialization, module evaluation, heap,
recreation, queue, and latency metrics keep their meanings. Cache lifecycle changes as follows:

- `isolate_context_cache_operations_total{context_kind,operation}` records `save`, `take`,
  `reject_pool_capacity`, `reject_frequency`, and `reject_memory_pressure`.
- `isolate_context_cache_cleared_total{context_kind,reason}` distinguishes app-definition clear,
  cache drop, isolate-heap memory pressure, cgroup memory pressure, duplicate replacement,
  pool-capacity replacement, and frequency admission replacement. Validation failure remains
  represented by the database lookup outcome after the context has been taken.
- `isolate_context_cache_entries_info{context_kind}` remains the number of idle reusable residents;
  it does not count an in-flight hit.
- `isolate_context_cache_capacity_info{pool_name,scope}` exposes the per-isolate structural capacity
  and effective shared pool capacity.
- `isolate_context_cache_owned_info{pool_name}` periodically reports reusable contexts that own
  shared pool capacity, including contexts currently in flight.
- `isolate_memory_capacity_bytes{pool_name,capacity_kind}` exposes configured per-worker and pool
  heap and ArrayBuffer ceilings before native runtime overhead.
- scheduler affinity changes from the one-slot `empty_worker` outcome to
  `same_client_worker`. `hit`, `new_worker`, and `stolen_worker` retain their meanings.

No module path or client identifier is added to these metrics. Application attribution continues to
use function-execution logs and controlled marker rollout.

Read the observability essay for the final metric names, cardinality calculation, and reset-aware
operator queries.

## Correctness boundaries

This patch is an eviction and scheduling optimization. It does not make mutable module state safe.
Application code must remain correct when:

- a request uses any one of several warm worker-local contexts;
- another request previously changed module or global state in that context;
- the requested key misses while other residents remain cached;
- validation discards an old context after a deployment or system-metadata change;
- pressure, idle timeout, maximum lifetime, client reassignment, or process restart removes some or
  all residents;
- concurrent requests evaluate the same module in separate isolates.

Do not use reusable module state as durable storage, a lock, an authorization source, a monotonic
counter, or a transactional mutation cache. Publication before commit is not by itself unsafe for a
source-pure context, but application review must reject retained state derived from a mutation
attempt whose transaction may later fail or retry.

## Expected benefits

This patch is useful when a worker repeatedly serves several eligible entry modules. Expected
effects are:

- fewer fresh module evaluations after ordinary warmup;
- less cache replacement between unrelated hot modules;
- fewer isolate workers created solely to preserve inapplicable one-slot residents;
- lower evaluation CPU and request latency during ordinary traffic and post-deployment recovery;
- retained memory that grows to explicit limits rather than an unbounded module map.

It does not eliminate the first cold evaluation after a deployment and does not prewarm contexts.
It does not improve provider/network time inside Node actions. It does not make more application
modules eligible by itself.

## Adoption

Before applying the patch:

1. Carry and verify backend memory resilience, cancellation-safe database context reuse, and
   context-reuse observability.
2. Review every marked entry module and its transitive runtime graph for retained request state,
   top-level mutation, timers, listeners, import-time asynchronous work, and third-party singleton
   behavior.
3. Record the maximum worker count, V8 user/extra heap limits, backend memory limit, and desired
   `ISOLATE_CONTEXT_CACHE_MAX_RESIDENTS` value.
4. Establish a one-slot baseline for evaluation count/time, affinity outcomes, resident count, V8
   physical/used/available heap, native contexts, recreation, CPU, latency, queueing, and errors.

Deploy the backend patch without simultaneously expanding the application marker cohort. Verify the
existing marked traffic first. Only then broaden application adoption.

No schema migration, data rewrite, worker-runtime update, or application protocol migration is
required. Backend replacement still has the availability and authorization requirements of the
self-hosted installation.

## Rollback

The immediate semantic rollback is an older backend image with the one-slot cache. A backend restart
destroys all worker-local contexts. Lower the resident cap before rollback only if the older image
understands the same knob; otherwise restore the complete previous image and configuration together.

Application code remains correct with reuse absent, so removing markers is not required merely to
return to one-slot behavior. Current upstream has no backend-wide database reuse switch. If
correctness is in doubt, remove the marker from every opted-in database-UDF entry in one complete
application deployment and restart the backend to clear saved contexts before restoring traffic.
Disable HTTP reuse separately with `REUSE_HTTP_ACTION_CONTEXTS=false`.

## Verification

Focused unit tests cover:

- probationary admission and one-hit rejection;
- the exact 96-observation aging boundary and changing popularity after aging;
- frequency and recency victim ordering;
- bounded nested/in-flight return behavior;
- duplicate-key replacement;
- pool-token acquisition and release, plus configured-cap validation;
- memory-pressure eviction order;
- same-client fallback without cache-key-driven worker creation.

Source tracing additionally verifies that validation, execution, cancellation, and local
finalization failures retain a hit token until the associated V8 scopes and roots are destroyed.
The public checkout has no V8 lifecycle harness that can assert that destruction order directly.
It also verifies that context read-set capture and validation use `UdfInitialize`. If either future
blocks, the timeout releases the active-JavaScript permit, records the blocked interval as system
time, and records permit reacquisition separately. A synchronously ready cache path keeps the
permit and does not create a pause. Lazy non-system module loading uses the same conditional release
under `LoadModule`.

The complete isolate library suite and an isolate all-target check cover the surrounding build and
control-flow boundaries. This public checkout has no executable database/HTTP reusable-context
lifecycle harness, so initialization validation, cancellation, publication, key-kind separation,
and worker recreation also require source tracing and ordinary canary traffic rather than a claimed
end-to-end regression test.

Use ordinary application traffic for production-shaped verification. This patch does not require a
synthetic stress fixture or a timing-dependent manual procedure.

## Rejected alternatives

- **Restore the unbounded context map:** no finite retained-memory ownership.
- **Plain LRU:** one-hit outliers evict useful residents immediately.
- **Lifetime LFU:** historical winners do not adapt to traffic changes.
- **One resident per worker:** safe but mechanically prone to churn under broad adoption.
- **Movable process-global V8 contexts:** incompatible with isolate-affine V8 handles.
- **Scheduler-global popularity in the first version:** adds shared hot-path coordination without
  demonstrated need.
- **Moka/Caffeine as the context store:** does not solve V8 affinity and adds a general concurrent
  cache where a fixed six-entry local policy suffices.
- **Count-min sketch or adaptive W-TinyLFU:** disproportionate for six residents and a modest key
  universe.
- **Per-context byte limits:** reliable retained-byte attribution is unavailable at this boundary.
- **Prewarming:** separate CPU, deployment, freshness, and cancellation semantics; current cold-miss
  evidence does not justify it.
