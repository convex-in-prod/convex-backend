# Context reuse design reference

This note records the implementation contract behind the concise
[context reuse](README.md) adoption description.

## Policy and propagation

Analysis reads the entry module's `experimental_reuseContext` export. A literal `true` retains the
upstream query/mutation behavior. An object reads the boolean `queries`, `mutations`, `actions`,
and `httpActions` properties; missing or non-true properties are disabled. The analyzed policy is
stored with module metadata, carried through validation, and serialized in a typed protocol field.

The old `reuse_context` protocol field remains for rolling compatibility. New producers send both
the typed policy and a safe legacy database projection: the bit is true when both query and mutation
permissions are true, even if action permissions are also present. A narrower database policy cannot
be represented without granting extra authority, so its legacy bit is false. New consumers prefer
the typed policy and decode an old true bit as query/mutation permission. HTTP consumers deliberately
ignore the legacy bit because it historically never granted HTTP reuse; old producers therefore
remain safely fresh until they send the typed policy.

Validation checks the exported function type after reconstructing a trusted protocol value. The
scheduler repeats the same type-specific check when deciding context affinity. This prevents a
stale or mixed-version producer from turning a database marker into action or HTTP reuse.
Trusted protocol reconstruction also clears all reuse permissions for system-module paths; system
code cannot opt itself into retaining a reusable request context.

An optional canonical `_deps` path in the policy's `initializationModule` property is stored in
analyzed metadata and carried in `ValidatedPathAndArgs.context_initialization_module`. Its absence
means per-entry reuse. Consumers apply it only to a query or mutation whose corresponding reuse
permission is enabled; system paths clear it and actions ignore it. Older consumers omit or ignore
the field and keep their previous per-entry behavior.

For a group, cold initialization evaluates the initializer inside the existing read-snooping
boundary, including every entry's module metadata. Both cold and warm calls require the original
entry in that module map. A missing entry fails the invocation and cannot publish a resident.
Database UDFs already prohibit handler-time dynamic imports, so warm calls cannot add modules
outside the saved initialization read set. No incremental group expansion or read-set merging is
needed. The initializer path changes cache identity only; the validated function path still owns
function selection, visibility, arguments, and request accounting.

## Request lifecycle

The three cache kinds share a lifecycle but have different final success contracts:

1. Admission selects an isolate and the request claims a worker permit.
2. An eligible request removes its `(kind, component, module)` resident from the cache before
   validating the saved initialization reads. A validation miss or error destroys the taken roots;
   a valid resident installs fresh request state over the retained module map and V8 context.
3. A miss creates or reuses only the separate empty fresh context. If reuse is enabled, initialization
   runs with read snooping so its system reads can be validated on the next invocation.
4. Execution shuts down request task executors and performs a final microtask checkpoint. This is
   required because the checkpoint itself can run user microtasks and invoke native functions.
5. Termination, caller cancellation, pending promises, dynamic imports, streams/listeners, and the
   execution result are checked before making a candidate. The read set and module map are held out
   of the scheduler mirror until all fallible outcome finalization has completed.
6. Successful candidates are inserted through the bounded cache admission policy. Rejected or
   invalid candidates drop their V8 roots before returning shared resident capacity.

Token guards outlive both V8 scopes and request-owned resolver roots on failure. A returned
database environment must clear unresolved syscall resolvers before leaving that guard's scope.
Action finalization keeps the token outside its candidate until warning and outcome construction
have succeeded, because the enclosing handle scope still roots the context during those steps.

Database UDFs publish a successful query or mutation candidate. Ordinary actions publish only after
their successful `ActionOutcome` has been delivered to the response channel, with no pending action
task promises or request-owned async work. HTTP actions publish only after their successful result
has been delivered to the function-runner response channel and only for a successfully streamed
response, with no pending task, dynamic-import, stream/listener, or unhandled-rejection state, no
execution error, and an open isolate response sender. Buffered body forwarding to an outer HTTP
transport can fail after publication; that transport is not an owner of the isolate-local candidate.
HTTP's client-disconnect abort listener is intentionally excluded from the pending-listener check;
the listener is request infrastructure and is destroyed with fresh request state.

The database and ordinary-action callers use a drop guard around the response wait. Dropping a
caller sets an atomic cancellation flag and wakes the execution loop. Synchronous JavaScript cannot
be interrupted from another thread, so the database save boundary checks the flag again after the
final checkpoint. A cancellation can still race after that last check; it cannot retract a database
candidate that has already been synchronously published. Ordinary actions additionally keep their
candidate outside the cache until the worker's response send succeeds, so a closed action response
channel cannot publish a warmed context.

## Cache and scheduler

The cache is isolate-affine. Its resident key includes the cache kind, component, and canonical
entry-module path, or the fixed initializer path for grouped database contexts; the exported
function name is not included. Scheduler affinity, cache lookup, and publication use the same key.
A shared bounded budget accounts for
residents and taken/in-flight reusable roots. A thread-safe mirror contains complete resident
keys and whether a probationary slot exists, and is cleared before the owning V8 roots are destroyed.

The cache has one frequency-admission window and a protected segment. Under isolate heap pressure it
drops the separate fresh context, collects, then evicts the probationary and weakest protected
residents until the free-heap check passes or the cache is empty. Under process cgroup pressure it
drops the fresh and probationary contexts, retains at most the two strongest protected residents,
suppresses new resident admission, and requests collection after roots leave the save path. A save
holds the shared pressure-state guard through its complete synchronous admission or rejection, so it
cannot publish against a stale pressure state. These actions are independent of the application
policy: policy controls eligibility, while cache admission controls retention. The resident budget
is a count bound rather than per-context byte accounting; the V8 isolate heap remains the byte bound.

On a reusable request, affinity first prefers a same-client idle worker with a matching resident.
The disabled-by-default `ISOLATE_CONTEXT_CACHE_CAPACITY_PLACEMENT` option then ranks eligible misses
by free local room backed by currently available global ownership, followed by a replaceable
probationary resident. Growth ties preserve same-client MRU order; replacement ties choose the
least recently completed eligible worker. This lets overflow replicas avoid repeated local
competition with the same frequently used protected entries. Local frequency counts are not compared across
workers because their aging clocks advance at different rates. Pressure disables that miss
preference, and all-unavailable selections retain the MRU fallback.
Without a preferred cache, a reusable request uses the MRU same-client idle worker before creating
or stealing a worker. It never waits for a busy resident or creates a worker for cache diversity.

Non-reusing V8/HTTP actions prefer the first empty cache in same-client idle MRU order, falling back
to MRU when none is observed. A separate fresh context protects saved roots from replacement, but
does not make them accessible while an action occupies their worker. Keeping such actions on empty
caches can preserve warm execution for concurrent database requests and action descendants. This
preference never selects a busy or other-client worker over an available same-client worker and
does not reserve a separate action pool. Fresh queries, mutations and control-plane requests retain
MRU placement. The action preference is independent of the reusable-miss placement option.

Completion recency is not a popularity estimate. A cold scan can spread replacements and frequency
aging across idle caches that MRU would leave untouched, increasing misses when their earlier hot
set returns. Reusing those workers also postpones idle recreation. Both policies inspect idle
mirrors on eligible misses, stopping when the highest retention class is found; this synchronous
diagnostic work holds the already-acquired active permit. Exact hits and fresh requests skip the
miss-retention scan; non-reusing actions instead scan for an empty cache, stopping at the first
one. Keeping actions off populated workers may let those workers reach idle recreation sooner.
Evaluate returning hot modules after action-only intervals as well as concurrent mixed traffic;
fixed ownership limits do not imply equal RSS. Keep the miss-placement option disabled until a
fixed-load comparison includes its costs.

The mirror is only a hint: save rechecks ownership and pressure, and a request still validates the
cache entry on the owning worker. A hit in one kind does not satisfy a request in another kind.
Client changes recreate the isolate before execution. Busy-worker mirrors and matching requests in
flight are separate observations: a matching request can be initializing fresh state or rejecting
an invalidated resident, while another key can remain resident on that worker. New, reassigned and
recreated workers can lack a current scheduler mirror until completion. Diagnostic absence means
no observed affinity, not global absence or validation failure.

## Metrics and interpretation

The typed decision counter is labeled by `query`, `mutation`, `action`, or `http_action` and records
the effective allowed/disabled decision after validation. Cache lifecycle operations and entries are
labeled by `database_udf`, `action`, or `http_action`; reusable initialization attempts are labeled
by UDF type. Database-UDF read-set lookup outcomes and the read-set validation trace property expose
whether a resident was found and accepted. Existing module-load/evaluation, isolate heap, memory,
termination, and recreation metrics remain the primary cold-versus-warm and memory signals.

The unprefixed registry families for rollout are:

- `context_reuse_decision_total` and `reusable_context_init_total`;
- `database_udf_context_reuse_lookup_total`;
- `isolate_context_cache_operations_total`, `isolate_context_cache_cleared_total`, and
  `isolate_context_cache_entries_info`;
- `isolate_context_cache_capacity_info` and `isolate_context_cache_owned_info`;
- `isolate_scheduler_context_affinity_total` and `isolate_memory_capacity_bytes`;
- `isolate_scheduler_context_miss_placement_total`, labeled by `pool_name`, `context_kind`,
  `policy=mru|capacity`, `selected_retention` and `best_retention`. Retention values are
  `unavailable|replace_probationary|grow`. This counts only accepted same-client idle misses;
  exact hits, worker creation and stealing are excluded. Both policies observe alternatives,
  but save still rechecks ownership and pressure. Best observed capacity is a scheduling hint,
  not a reservation, simultaneous global snapshot, or measurement of replica lifetime.
- `isolate_scheduler_non_reusing_action_placement_total`, labeled by `pool_name` and
  `outcome=mru_empty|preferred_empty|no_empty`. This counts only accepted non-reusing V8/HTTP
  action dispatch attempts to existing same-client idle workers, excluding new and stolen workers.
  Outcomes describe the observed empty-cache preference, not avoided misses; mirror clears during
  idle recreation or pressure mean the scan is not a simultaneous snapshot.

The exported Prometheus names include the executable prefix. The maintained self-hosted Compose
configuration defaults `DISABLE_METRICS_ENDPOINT` to `true`, so an operator must enable and scrape
the endpoint before relying on these series. No family adds a module path, function path, client,
identity, or route label.

These are attempt and resident counters, not logical request counters. Mutation OCC retries and
validation retries can increment them more than once per user-visible request. A missing series may
mean no matching activity, not a zero-valued feature.

## Interactions

Dependency-capacity admission remains highest priority for nested calls that unblock an ancestor.
Context reuse does not create a worker pool or bypass dependency reserves. The isolate queue and
class-aware active-JavaScript admission operate before context affinity; degradable root queries may
use the same shared worker/cache machinery when their module policy permits query reuse.

Deployment analysis and control-plane requests remain fresh and retain their own queue policy. They
can evict or compete with reusable residents only through ordinary bounded worker/cache scheduling;
no context policy is inferred for them.

Local Node executor actions have a separate process-generation cache and pool policy. This V8
context-reuse feature applies to ordinary Convex-runtime actions handled by the isolate backend,
not to Node executor generation state.

## Verification and rollback

Focused coverage should test typed policy analysis, old/new protocol round trips, per-kind scheduler
affinity, read-set invalidation, successful and failed saves, caller drop during queued and executing
actions/UDFs, final-checkpoint termination, pending streams/imports/promises, and cache pressure.
Production rollout should compare module-evaluation rate, cache hit/validation outcomes, isolate
heap/native memory, request latency, termination/recreation, and application error rates before and
after enabling each policy property.

Removing one property disables new reuse for that kind after redeployment, but already saved roots
are process-local. Restart workers for an immediate clean cache. Removing the legacy boolean disables
database query/mutation reuse in the same way.
