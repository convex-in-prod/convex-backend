# Context Reuse Observability

This patch adds bounded backend metrics for deciding whether reusable V8
contexts are being allowed, found, retained, cleared, and selected by the
isolate scheduler. It is intended for self-hosted operators evaluating database
UDF context reuse or the separate opt-in HTTP action context reuse patch.
It also clears stale scheduler mirrors when a cache is destroyed. The later
[`bounded_multi_context_reuse`](../bounded_multi_context_reuse/README.md) patch extends these
signals for multi-entry admission and changes same-client miss affinity so cache-key diversity does
not allocate workers.

The patch does not enable context reuse, change the application marker, add a
module allowlist, or change cache capacity. It adds no application module,
function, component, route, client, or deployment value to a metric label.

## Patch composition

Database UDF context reuse is an upstream experimental feature selected by an
entry module's `experimental_reuseContext = true` export. This observability
patch is designed to accompany
[`cancellation_safe_database_context_reuse/README.md`](../cancellation_safe_database_context_reuse/README.md),
which permits marked queries and mutations to reuse contexts and prevents canceled executions from
publishing them.

The shared isolate cache in this maintained branch also supports the independent
[`reuse_http_action_contexts/README.md`](../reuse_http_action_contexts/README.md) patch. Cache and
scheduler metrics therefore use `context_kind={database_udf,http_action}`.
Database decision and read-set validation metrics remain database-UDF-specific.
HTTP action context reuse is still controlled only by
`REUSE_HTTP_ACTION_CONTEXTS`.

When the bounded multi-context patch is also carried, this observability patch's cache-operation,
clear-reason, capacity, and scheduler-affinity enums must be updated with it. The bounded patch does
not create module-path labels or a parallel observability system.

Operators carrying only upstream database context reuse can retain the database
metric paths and omit the HTTP-specific branches when rebasing this patch. No
HTTP behavior is required for the database metrics.

## Existing signals retained

The backend already exposes several signals that answer parts of the rollout
question. This patch reuses them rather than adding parallel metric families:

- `reusable_context_init_total{udf_type,reused}` counts marked database UDF
  context-initialization attempts after request-environment initialization.
  `reused="true"` is a validated context hit; `reused="false"` is a fresh
  reusable-context candidate. It is emitted before entry-module
  lookup/evaluation and handler execution, so failures at those later boundaries
  remain counted.
- `udf_isolate_load_user_modules_seconds{udf_type,is_dynamic,status}` records
  user-module load count and duration by function type.
- `udf_isolate_evaluate_module_seconds{status}` records actual JavaScript
  module evaluation count and duration.
- Existing isolate heap histograms and aggregate gauges report used heap, heap
  size and limit, native and detached contexts, external memory, physical size,
  malloced memory, and ArrayBuffer memory.
- `recreate_isolate_total{reason}` reports explicit isolate-recreation causes
  such as idle timeout, client change, heap upgrade, maximum lifetime, and
  failed cleanliness checks. It is not emitted for plain worker shutdown or
  every early request failure that causes the worker to discard an unclean
  isolate.

The existing `fastrace` properties named `reusable_context`, `is_reused`, and
`reuse_success` are useful only when a deployment installs a `fastrace`
reporter. The stock self-hosted backend does not export those properties into
its ordinary tracing or Prometheus pipeline, so they are not a substitute for
the metrics below.

## New metrics

### Effective database decision

`database_udf_context_reuse_decision_total{udf_type,decision}` counts successful
validation attempts for marked database entry modules.

- `udf_type="query", decision="allowed"` means the module marker is effective for a query.
- `udf_type="mutation", decision="allowed"` means the module marker is effective for a mutation.

Unmarked modules do not increment this metric. Actions and HTTP actions are not
database UDF reuse candidates and do not increment it. The counter is emitted
after function type, visibility, arguments, and validators have succeeded, so
rejected calls before that boundary are not included.

This is an attempt counter, not a logical-request counter. A mutation retry
after OCC increments `allowed` again if it reaches validation. A
write-throughput retry does so only when its pre-execution throughput check
passes and the attempt reaches validation. A query-cache retry that selects
another `Go` operation can likewise validate and increment `allowed` more than
once for one caller request.

Current upstream always supports the marker and has no backend-wide enable
knob. Unmarked modules do not increment this counter; application module policy
is the effective activation and rollback boundary.

The query cache validates a function only after selecting its `Go` path. A
cache-served query therefore does not increment this counter. An allowed
attempt normally proceeds toward isolate execution, although later admission,
scheduling, transaction, or internal failures can still stop it before context
lookup.

### Database cache lookup and validation

`database_udf_context_reuse_lookup_total{udf_type,outcome}` records each
database reusable-context lookup that reached an isolate worker:

- `not_found`: the selected worker had no saved context for the entry module;
- `validation_failed`: a context was found, but its saved initialization reads
  no longer matched current system data;
- `validation_error`: starting or performing validation of those reads returned
  an internal error;
- `hit`: the saved reads validated and were applied to the current transaction.

Queries and mutations can both produce normal lookup outcomes. Actions and HTTP actions cannot.

A context is removed from the idle cache before read-set validation. A failed
or errored validation therefore discards that context and the request evaluates
fresh or fails, respectively. The existing
`reusable_context_init_total{reused}` counter supplies the final reused-versus-
fresh initialization-attempt split without duplicating it here.

### Cache lifecycle and occupancy

`isolate_context_cache_operations_total{context_kind,operation}` counts:

- `save`: a reusable context was admitted to its isolate cache;
- `take`: a resident context was removed for request use;
- `reject_pool_capacity`: a fresh candidate could not obtain a shared resident token and had no
  safe local probationary exchange;
- `reject_frequency`: a returning protected candidate lost admission after same-isolate nested work
  filled its former cache capacity;
- `reject_memory_pressure`: cgroup pressure suppressed admission of a new or probationary
  reusable context, or a returning protected context lost the pressure-limited two-entry
  competition.

`isolate_context_cache_cleared_total{context_kind,reason}` counts contexts, not
clear calls. Its reasons are:

- `admission_replacement`: frequency admission replaced a protected resident or rejected a
  probationary resident;
- `pool_capacity_replacement`: the pool was full and a new probationary resident directly replaced
  the cache's sole resident without growing total ownership. When protected residents exist, the
  displaced probationary resident instead undergoes the normal frequency comparison and the
  resulting removal uses `admission_replacement`;
- `duplicate_replacement`: same-isolate nested execution produced the same key while an outer
  context was in flight, and the returning context removed the duplicate;
- `memory_pressure`: incremental isolate-heap reclamation evicted the probationary resident or the
  weakest protected resident;
- `cgroup_memory_pressure`: the backend-wide cgroup pressure signal removed a reusable resident to
  converge the isolate on its two strongest protected entries;
- `app_definition_evaluation`: app-definition evaluation cleared the shared
  context cache before using arbitrary evaluation contexts;
- `cache_drop`: the owning `ContextCache` was destroyed, normally because its
  isolate worker was recreated or shut down.

Only displaced reusable contexts increment this counter. Dropping the separate empty fresh context
under pressure has no `context_kind` and does not increment it. A candidate rejected before it
became resident increments the operation counter but not the clear counter.

Correlate `cache_drop` with `recreate_isolate_total{reason}` when the worker
records an explicit recreation reason. `cache_drop` also covers ordinary worker
shutdown and early or unclean request failures that do not emit a recreation
reason. Propagating recreation state through the cache API would duplicate the
existing reason metric and make the patch substantially more invasive.

`isolate_context_cache_entries_info{context_kind}` is the current process-wide number of idle saved
reusable contexts. A context is subtracted while a request uses it and added again only if the
request successfully republishes it. The gauge does not count a reusable context while it is in
flight, although its shared resident-budget token remains owned until the request publishes or
drops it.

With the bounded cache patch, each isolate has one probationary plus five protected reusable
residents by default. The protected count is configurable. Database and HTTP contexts share those
positions but use distinct keys. The empty fresh context is separate. Cache insertion, take, clear,
pressure eviction, and destruction update the gauge in paired deltas.

`isolate_context_cache_capacity_info{pool_name,scope}` exposes the configured per-isolate reusable
capacity and the effective shared pool resident-token capacity. Its `scope` values are
`per_isolate` and `pool`.

`isolate_context_cache_owned_info{pool_name}` reports reusable contexts that currently own shared
pool capacity, including contexts in flight. It is sampled with the aggregate isolate-heap report,
so operation counters and idle-entry occupancy remain the sources for changes between samples.

`isolate_memory_capacity_bytes{pool_name,capacity_kind}` reports the configured V8 capacity used
for startup planning. Its four `capacity_kind` values are `heap_per_worker`, `heap_pool`,
`array_buffer_per_worker`, and `array_buffer_pool`. These are configured ceilings before native V8
and runtime overhead, not current allocation.

The scheduler uses a thread-safe mirror of the saved reusable key. That mirror
can outlive a worker's `ContextCache` during isolate recreation, so the drop
path clears it before destroying the V8 contexts and recording `cache_drop`;
otherwise the scheduler could advertise a context that no longer exists.

### Scheduler affinity

`isolate_scheduler_context_affinity_total{pool_name,context_kind,outcome}`
records worker selection for a reusable-context-eligible request:

- `hit`: an idle worker for the same client advertised the requested context;
- `same_client_worker`: no matching idle context was available, so the scheduler selected an
  ordinary idle worker for the same client. That worker may retain unrelated reusable keys;
- `new_worker`: no same-client worker was idle, so the scheduler allocated another worker before
  the physical worker limit was reached;
- `stolen_worker`: no same-client worker was idle and the physical limit was reached, so the
  scheduler selected an idle least-recently-used worker. A client change recreates the isolate
  before serving the request.

The scheduler first looks for a matching warm worker, then uses any idle same-client worker. It
allocates a worker only when no same-client worker is idle and physical capacity has not yet been
created, and otherwise steals the least-recently-used idle worker. Fresh work no longer clobbers
reusable residents, so unrelated keys are not a reason to create another worker.

This counter describes the scheduler's worker selection, not successful
read-set validation. For database contexts, only
`database_udf_context_reuse_lookup_total{outcome="hit"}` proves actual reuse.
For example, an advertised context can be selected and then fail validation
after a deployment changes its initialization reads.

The metric is emitted only after the selected worker's request channel accepts
the eligible request. Queue rejection or expiry before worker selection, and a
selection whose worker channel has already failed, do not increment it.

## Cardinality and ingestion

The context kind, outcome, operation, reason, UDF type, and decision labels are
closed sets represented by Rust enums, booleans with exhaustive matches, or
exhaustive matches over `UdfType`. `pool_name` comes from static isolate
configuration rather than application input or a closed enum. The maintained self-hosted layout
has one `funrun` pool.

In the maintained module-wide, HTTP, and bounded-cache composition, the expected normal maximum
with one pool is 42 counter series plus nine gauge series before standard process and resource
labels:

- two effective decision combinations;
- eight database lookup combinations across two UDF types;
- ten cache operation combinations;
- fourteen cache-clear combinations;
- two occupancy gauges;
- two capacity gauges for the current pool;
- one owned-context gauge for the current pool;
- four isolate-memory capacity gauges for the current pool;
- at most eight scheduler pool, context-kind, and outcome combinations in the current pool layout.

The five labelled counter vectors explicitly use `Duration::MAX` as their eviction TTL. The
current-layout hard bound is 42 counter series. Each additional statically configured isolate pool
can add at most eight scheduler series
and seven gauge series: two cache-capacity, one owned-context, and four isolate-memory capacity
series. These vectors do not register with the inactivity sweeper or add corresponding
`metrics_evictable_cardinality_info{metric}` and
`metrics_series_evicted_total{metric}` label sets. The two occupancy gauge
series are also not inactivity-evicted.

No histogram or module-path label is added. Application attribution should use
one marked module per deployment and correlate these aggregate counters with
function-execution logs, which already carry the canonical function path.

The backend Prometheus endpoint exports the counters and gauges without
metric-specific configuration once the endpoint is enabled. The stock Docker
Compose default is `DISABLE_METRICS_ENDPOINT=true`; a scraper deployment must
set it to `false`. A downstream scraper or collector can add resource labels or
transform metric names and types, so verify its effective output before relying
on these series.

Metric names in this document are the unprefixed registry names used in Rust.
Convex prefixes exported names with the executable name. The stock Docker
binary is `convex-local-backend`, so its Prometheus endpoint uses names such as
`convex_local_backend_database_udf_context_reuse_lookup_total`. Operator queries
must use the effective exported name.

The five new counters are cumulative for the process lifetime and reset only
when the process restarts; `CONVEX_METRICS_EVICTION_TTL_SECONDS` does not change
their explicit no-eviction setting. Reset-aware `rate` and `increase` queries
still handle process restarts. The existing `reusable_context_init_total`
counter retains the stock labelled-counter TTL and can reset after inactivity,
so queries that combine it with the new counters must remain reset-aware. The
occupancy gauge can return to its earlier value before the next 15-second scrape,
so use operation and clear counters to explain short-lived cache activity.
Metrics do not appear until a matching path first records a label set; absence
before application opt-in, or after a process restart before the first matching
event, is expected.

## Operational interpretation

For a marked database-UDF rollout, use this funnel:

1. `database_udf_context_reuse_decision_total{decision="allowed"}` confirms
   that a query or mutation attempt passed validation with its marker retained.
2. `database_udf_context_reuse_lookup_total` separates no saved context,
   invalid initialization reads, validation errors, and validated hits.
3. `reusable_context_init_total{reused}` confirms fresh versus reused
   initialization for attempts that pass request-environment initialization.
4. Cache operation, clear, and occupancy metrics explain whether contexts were
   retained between requests.
5. Scheduler affinity explains whether dispatch found the matching warm worker, used another idle
   same-client worker, allocated a worker, or stole an idle worker at physical capacity.
6. Existing module-evaluation, latency, query-cache, queue, shedding, heap, and isolate-
   recreation metrics determine whether reuse produced a useful performance
   result without unacceptable memory retention.

For a mixed marked module, both query and mutation traffic may increment
`decision="allowed"` and produce lookup or reusable-initialization outcomes.
Cache and scheduler metrics are intentionally aggregate by context kind, so
either UDF type in the same deployment can increment those series.

Ordinary staged canary traffic is sufficient to exercise this funnel. The
patch does not require a synthetic load generator, a timing-dependent OCC
workload, or a manual restart procedure.

## Runtime cost

The patch adds counter or gauge updates only at validation, reusable-context
lookup, cache lifecycle, and scheduler-selection boundaries. It adds no
per-request histogram observations, timers, background tasks, or
high-cardinality labels. On a reusable miss, worker selection scans the same-client idle-worker
deque once for an exact key, then takes its ordinary most-recent idle fallback. The bounded cache
performs at most a six-entry key scan and a five-entry victim scan. Exact frequency counters age
every 96 eligible lookups.

Cache accounting reuses the scheduler mirror lock already acquired for save
and take. Clear and drop acquire that lock once to remove stale advertisements.
Replacing an occupied reusable slot performs one clear transition followed by
one save transition. Metric updates occur after the mirror lock is released.
The expected cost is small relative to V8 context creation and module
evaluation, but operators should still compare backend CPU and request latency
before and after carrying the patch if reuse is disabled and these paths are
unexpectedly hot.

## Adoption and rollback

Apply this patch before marking an application database-UDF module. Verify
that the new metric names appear on the backend `/metrics` endpoint after
ordinary marked traffic, then verify the same series and labels in the remote
metrics store. Local exporter visibility alone does not prove collector or
ingestion correctness.

No schema, data migration, application source change, or new metric-specific
environment variable is required for the observability patch itself. The bounded-cache companion
adds its optional resident-cap variable. The existing metrics endpoint still has to be enabled for
scraping. Removing this patch removes the instrumentation and drop-time scheduler-mirror cleanup;
removing the bounded-cache patch separately restores its one-slot scheduler policy. Neither change
alters application markers or clears contexts in an already running process. Restart backend
workers after rollback if a clean cache baseline is required.

The drop-time mirror cleanup is a generic scheduler-correctness fix, not a
metric. If upstream supplies equivalent signals and operators stop carrying the
metric families, they should retain that cleanup or an upstream equivalent so
an idle worker cannot advertise contexts destroyed during isolate recreation.

If metric volume or behavior is unexpected, first remove dashboards or alerts
that assume absent label sets are zero. Then compare operation counters with the
occupancy gauge and recorded isolate recreation reasons. Do not add module names
to backend labels as a debugging shortcut.

## Verification boundary

Package-scoped Rust formatting, compilation, the focused same-client scheduler regression, and
existing isolate and UDF tests cover the changed build and control-flow boundaries. The patch does not add a
manual fixture or dedicated stress test. Production-shaped verification uses
ordinary canary traffic and confirms the emitted Prometheus and remote-ingested
series before relying on them for rollout decisions.
