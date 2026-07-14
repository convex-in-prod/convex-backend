# Context Reuse Observability

This patch adds bounded backend metrics for deciding whether reusable V8
contexts are being allowed, found, retained, cleared, and selected by the
isolate scheduler. It is intended for self-hosted operators evaluating database
UDF context reuse or the separate opt-in HTTP action context reuse patch.
It also clears stale scheduler mirrors when a cache is destroyed and makes a
reusable miss prefer a same-client worker without a reusable context before
allocating or stealing a worker.

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

- `save`: a reusable context was stored in the isolate's single saved slot;
- `take`: a saved context was removed for request use.

`isolate_context_cache_cleared_total{context_kind,reason}` counts contexts, not
clear calls. Its reasons are:

- `fresh_context_clobber`: a request that could not use the saved reusable
  context needed a fresh context and discarded the saved slot;
- `reusable_context_replacement`: a reusable context finished while another
  reusable context already occupied the one cache slot. This can happen when a
  same-isolate nested marked query saves before its marked parent finishes; the
  parent's later save replaces it;
- `app_definition_evaluation`: app-definition evaluation cleared the shared
  context cache before using arbitrary evaluation contexts;
- `cache_drop`: the owning `ContextCache` was destroyed, normally because its
  isolate worker was recreated or shut down.

Only displaced reusable contexts increment this counter. Clearing or replacing
a fresh prewarmed context has no `context_kind` and does not increment it.

Correlate `cache_drop` with `recreate_isolate_total{reason}` when the worker
records an explicit recreation reason. `cache_drop` also covers ordinary worker
shutdown and early or unclean request failures that do not emit a recreation
reason. Propagating recreation state through the cache API would duplicate the
existing reason metric and make the patch substantially more invasive.

`isolate_context_cache_entries_info{context_kind}` is the current process-wide
number of saved reusable contexts. A context is subtracted while a request uses
it and added again only if the request successfully republishes it. The gauge
does not count a reusable context while it is in flight.

Each isolate has one saved context slot total. A fresh prewarmed context or one
reusable database-UDF or HTTP-action context can occupy it; database and HTTP
contexts therefore compete for the slot. Concurrent traffic can still retain
contexts of either kind on separate workers. Cache insertion, take, clear, and
destruction update the gauge in paired deltas.

The scheduler uses a thread-safe mirror of the saved reusable key. That mirror
can outlive a worker's `ContextCache` during isolate recreation, so the drop
path clears it before destroying the V8 contexts and recording `cache_drop`;
otherwise the scheduler could advertise a context that no longer exists.

### Scheduler affinity

`isolate_scheduler_context_affinity_total{pool_name,context_kind,outcome}`
records worker selection for a reusable-context-eligible request:

- `hit`: an idle worker for the same client advertised the requested context;
- `empty_worker`: no matching idle context was available, but an idle worker
  for the same client advertised no reusable context. Its local slot can still
  hold a fresh prewarmed context, which the scheduler does not advertise;
- `new_worker`: no matching context or same-client worker without a reusable
  context was available, so the scheduler allocated another worker before the
  physical worker limit was reached;
- `stolen_worker`: neither same-client option was available and the physical
  limit was reached, so the scheduler selected an idle least-recently-used
  worker. It can belong to the same client when that client's idle workers hold
  inapplicable warm contexts; a client change recreates the isolate before
  serving the request.

The scheduler preserves idle workers with inapplicable warm contexts instead of
proactively selecting and clobbering them. It first looks for a matching warm
worker, then for a same-client worker without a reusable context, then allocates
a worker when physical capacity remains, and otherwise steals the
least-recently-used idle worker.

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
configuration rather than application input or a closed enum. The current
production layout has one `funrun` pool.

In the maintained module-wide and HTTP composition, the expected normal maximum
with that one pool is 30 counter series plus two gauge series before standard
process and resource labels:

- two effective decision combinations;
- eight database lookup combinations across two UDF types;
- four cache operation combinations;
- eight cache-clear combinations;
- two occupancy gauges;
- at most eight scheduler pool, context-kind, and outcome combinations in the
  current pool layout.

The five labelled counter vectors explicitly use `Duration::MAX` as their
eviction TTL. The current-layout hard bound is 30 counter series. Each
additional statically configured isolate pool can add at most eight scheduler
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

For a marked query rollout, use this funnel:

1. `database_udf_context_reuse_decision_total{decision="allowed"}` confirms
   that a query attempt passed validation with its marker retained.
2. `database_udf_context_reuse_lookup_total` separates no saved context,
   invalid initialization reads, validation errors, and validated hits.
3. `reusable_context_init_total{reused}` confirms fresh versus reused
   initialization for attempts that pass request-environment initialization.
4. Cache operation, clear, and occupancy metrics explain whether contexts were
   retained between requests.
5. Scheduler affinity explains whether dispatch found the matching warm worker,
   used a same-client worker without a reusable context, allocated a worker, or
   stole an idle worker at physical capacity.
6. Existing module-evaluation, latency, query-cache, queue, shedding, heap, and isolate-
   recreation metrics determine whether reuse produced a useful performance
   result without unacceptable memory retention.

For a marked module, query and mutation traffic can both increment
`decision="allowed"` and produce database lookup and reusable-initialization
outcomes. Cache and scheduler metrics are intentionally aggregate by context
kind, so either UDF type in the same deployment can increment those series.

Ordinary staged canary traffic is sufficient to exercise this funnel. The
patch does not require a synthetic load generator, a timing-dependent OCC
workload, or a manual restart procedure.

## Runtime cost

The patch adds counter or gauge updates only at validation, reusable-context
lookup, cache lifecycle, and scheduler-selection boundaries. It adds no
per-request histogram observations, timers, background tasks, or
high-cardinality labels. On a reusable miss, worker selection scans the
remaining same-client idle-worker deque once more to find a worker without a
reusable context before allocating or stealing a worker.

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
environment variable is required. The existing metrics endpoint still has to
be enabled for scraping. Removing the patch removes the instrumentation, the
drop-time scheduler-mirror cleanup, and the preference for a same-client worker
without a reusable context on a reusable miss; it does not change the marker or
clear contexts in an already running process. Restart backend workers after
rollback if a clean cache baseline is required.

The drop-time mirror cleanup is a generic scheduler-correctness fix, not a
metric. If upstream supplies equivalent signals and operators stop carrying the
metric families, they should retain that cleanup or an upstream equivalent so
an idle worker cannot advertise contexts destroyed during isolate recreation.

If metric volume or behavior is unexpected, first remove dashboards or alerts
that assume absent label sets are zero. Then compare operation counters with the
occupancy gauge and recorded isolate recreation reasons. Do not add module names
to backend labels as a debugging shortcut.

## Verification boundary

Package-scoped Rust formatting, compilation, the focused scheduler regression
test for a worker without a reusable context, and existing isolate and UDF tests
cover the changed build and control-flow boundaries. The patch does not add a
manual fixture or dedicated stress test. Production-shaped verification uses
ordinary canary traffic and confirms the emitted Prometheus and remote-ingested
series before relying on them for rollout decisions.
