# Function usage counters

The runtime observability patch includes an optional exporter for per-function
usage history. Completed execution records update cumulative counters in backend
memory. An external collector pulls those counters; function completion performs
no network requests and does not depend on the collector or its storage.

## Activation and access

`FUNCTION_USAGE_METRICS_ENABLED=true` enables collection. An unset value or
`false` disables collection; other values fail initialization. The exporter uses
`/metrics/function_usage` and its own registry. Ordinary `/metrics` scrapes do
not collect or serialize the function counters. `DISABLE_METRICS_ENDPOINT`
disables both routes.

The endpoint uses the existing unauthenticated metrics route mechanism. Operators
restrict `/metrics` and `/metrics/*` to trusted local or private-network clients
before enabling collection. Function and component labels disclose application
structure. No admin key protects this endpoint.

## Sparse collection contract

The first scrape includes every retained function. Subsequent scrapes include
only functions updated since the preceding collection. Once an hour, the next
scrape includes every retained function again, including idle functions. Every
included function exports all its counter families. Global exporter-health
metrics remain present on every response.

Counters retain their cumulative values when collected. Only the pending-export
flags are cleared. Flags are cleared before reading atomic counters, and each
completion publishes its flag after updating counters. A concurrent update is
therefore visible in that collection or a subsequent collection.

The endpoint has one intended consumer. A manual scrape also clears pending
flags. A failed response can consume flags without delivering counters; a later
function update or hourly snapshot sends the cumulative values again. There is
no acknowledgement or replay queue. This is best-effort operational telemetry.

Omission means unchanged, so ordinary Prometheus staleness handling does not
implement this protocol. A consumer retains the latest counter state between
updates and handles observed counter decreases as process resets.

For example, single-node VictoriaMetrics v1.153.0 can use these scrape settings:

```yaml
scrape_configs:
  - job_name: convex-function-usage
    scrape_interval: 5m
    scrape_timeout: 10s
    metrics_path: /metrics/function_usage
    no_stale_markers: true
    max_scrape_size: 256MiB
    stream_parse: true
    static_configs:
      - targets: ["127.0.0.1:3210"]
```

The corresponding `-streamAggr.config` file reconstructs counters with their
original names and labels:

```yaml
- match: '{__name__=~"convex_local_backend_function_usage_.+_total", function!=""}'
  interval: 5m
  staleness_interval: 24h
  outputs: [total_prometheus]
  keep_metric_names: true
  flush_on_shutdown: true
```

The namespace above is for the local backend. Other service binaries use their
own metric namespace. The function label matcher excludes exporter-health
counters from aggregation. Unmatched metrics use ordinary storage behavior.

`total_prometheus` establishes a baseline from the first received sample, then
stores a derived cumulative counter every five minutes, including intervals with
no input. State expires after 24 hours without input; hourly snapshots refresh
idle functions retained by the backend. A VM restart or changed aggregation rule
also loses aggregation state. The first sample after state loss becomes a new
baseline instead of recounting prior process-lifetime work. Stored history is
independent of aggregation state; `-retentionPeriod=30d` retains it for thirty
days without enterprise downsampling.

Full and partial responses share the same scrape limit. Stream parsing limits
VM's response-buffering cost, but the backend still builds selected metric
objects and response text in memory. The default 16 MiB scrape limit can reject
a full snapshot; collection does not automatically split oversized responses.

## Identity, measurements, and bounds

Names start with `function_usage_` after the service namespace. Domain labels
are `component`, `function`, and `udf_type`. An internal `label_lengths` label
separates otherwise ambiguous concatenations in the pinned metric vector's
label hash. Group queries by the domain labels.

The root component label is empty. Function labels use canonical module paths
without `.js`, followed by `:export`; default exports use the module path alone.
HTTP actions use the matched method and route pattern. System functions and
unmatched HTTP requests are excluded. Request IDs, arguments, document IDs,
identity data, and error text are not labels.

The exporter retains at most 4,096 function label combinations per process.
Component and function labels are each limited to 1,024 bytes. Existing counters,
including counters for removed functions, remain until process exit. Recording
uses nonblocking map admission; scrapes hold that map only while selecting and
cloning counter handles. Capacity, label-length, contention, and invalid-duration
omissions increment `dropped_observations_total{reason="..."}`.
`retained_functions` reports retained identities.

The counter families measure:

- Calls and failures from terminal completion records, including query cache
  hits and subscription reruns; attempts requesting a retry are not terminal.
- Non-cached attempts, retries, query cache hits, and query cache misses.
- Execution seconds, available user-execution seconds, and the count of attempts
  with user-execution timing. These are elapsed durations, not operating-system
  CPU time. Node actions do not provide user-execution timing.
- Database bandwidth bytes, database I/O bytes, document counts, and written index
  rows, following the existing execution record's accounting.
- Attributed file-storage read/write bytes and network egress bytes.

Cache hits do not repeat the cached execution's time or resource usage. Nested
transaction work follows existing function attribution; action callbacks have
their own completion records. Existing system-error exclusions remain in effect.
HTTP error responses chosen by application code are not execution failures.

Backend restarts lose unsampled increments. A reset is invisible if the new
counter exceeds its previous value before collection; multiple intervening
resets cannot be reconstructed. Initial usage before a consumer baseline is
also omitted. Derived counters repeat during failed scrapes, so a flat series
does not establish zero activity while scrape health is missing or failing.
Aggregation can add another output interval of delay. These counters describe
approximate usage, not billing or a durable event ledger.

Queries use `increase()` or `rate()` over the derived counters. For example:

```promql
sum by (component, function, udf_type) (
  increase(convex_local_backend_function_usage_calls_total[7d])
)
```

## Verification and rollback

The focused Rust tests are `application`'s `function_usage_metrics::tests` and
`common`'s `infrastructure_scrapes_do_not_collect_function_usage`. They exercise
cache/retry accounting, bounded admission, label separation, concurrent updates,
sparse collection, hourly response repair, and infrastructure-scrape isolation.
Consumer verification includes empty successful responses, visible resets,
restart baselines, expiry, and full snapshots larger than the default scrape cap.

Disable collection and restart, or restore a backend without the exporter, to
stop new usage input. Consumer state expires according to its configured policy;
stored history follows the external store's retention. No schema migration or
dashboard rollback is required for the exporter.
