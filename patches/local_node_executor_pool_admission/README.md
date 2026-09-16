# Local Node Executor Pool Admission Policy

Status: implemented as an optional operator policy within the complete
application-declared local Node executor pools adoption unit.

This document describes independent-action concurrency limits, queue-delay
observability, event-loop unresponsiveness budgets, and optional RSS and
generation-retirement thresholds for the default and named local Node executor
pools. An application
declaration selects a process pool, while this subordinate policy lets an
operator constrain how work enters that pool, choose how long its main event
loop may stop responding before replacement, and set its sampled direct-child
RSS allowance.

This policy applies only to application `Execute` work. The internal `_system`
child serializes `Analyze` and `BuildDeps` through its fixed FIFO admission
contract and does not use per-pool dependency overflow, concurrency, warning,
or event-loop policy from this map.

The design rationale, liveness analysis, and rejected alternatives are in
[`design_reference.md`](design_reference.md).

## Runtime-pools composition

This policy composes with
[`pinned_local_node_executor_pools`](../pinned_local_node_executor_pools/README.md)
for named routing, resident-generation ownership, cutover fencing, and the
shared RSS budget. It also requires
[`scheduled_action_admission`](../scheduled_action_admission/README.md) so a
scheduled or registered-cron action waits for runtime capacity while its
durable job is still `Pending`.

The application-wide `APPLICATION_MAX_CONCURRENT_NODE_ACTIONS` limit remains
the hard aggregate Node action bound. A configured per-pool limit is an
additional child bound; it never creates capacity above the global limit.

## Configuration

`LOCAL_NODE_EXECUTOR_POOL_POLICIES` is a strict JSON object keyed by a valid
application-declared pool name. The reserved `default` key configures modules
without a named assignment. For example:

```json
{
  "planning": {
    "maxConcurrency": 1,
    "maxRssBytes": 2684354560,
    "maxOldSpaceSizeMib": 1536,
    "memoryPressureMinRssBytes": 2147483648,
    "maxEventLoopUnresponsiveSeconds": 30,
    "queueWarningSeconds": 10,
    "maxGenerationAgeSeconds": 21600,
    "backgroundDrainTimeoutSeconds": 30
  }
}
```

All fields are optional within a non-empty pool policy:

- `maxConcurrency` limits independent actions admitted to that pool. It must
  be positive and no greater than
  `APPLICATION_MAX_CONCURRENT_NODE_ACTIONS`.
- `maxRssBytes` sets that pool's sampled direct-child RSS retirement threshold.
  It must be positive and remain above that pool's effective V8 old-space and
  cgroup-pressure RSS thresholds.
- `maxOldSpaceSizeMib` overrides the pool's V8 old-space allowance. Pools
  without it use `LOCAL_NODE_EXECUTOR_MAX_OLD_SPACE_SIZE_MIB`.
- `memoryPressureMinRssBytes` overrides the pool's direct-child RSS floor for
  retirement during sustained cgroup pressure. Pools without it use
  `LOCAL_NODE_EXECUTOR_MEMORY_PRESSURE_MIN_RSS_BYTES`.
- `maxEventLoopUnresponsiveSeconds` is the maximum elapsed interval for which
  the pool's resident Node main event loop may fail health probes before the
  backend retires it. It is not a CPU-time quota.
- `queueWarningSeconds` records one warning metric when an ongoing
  pool-admission wait reaches the configured duration. It requires
  `maxConcurrency`; it does not reject a scheduled job or change its durable
  state.
- `maxGenerationAgeSeconds` sets the healthy generation-age retirement limit.
  Omission inherits `LOCAL_NODE_EXECUTOR_MAX_GENERATION_AGE_SECS`; explicit
  `null` disables only age retirement for that pool. RSS, package, pressure,
  watchdog, and deployment/topology retirement remain active.
- `backgroundDrainTimeoutSeconds` bounds the process-generation resident
  cleanup callback. It defaults to 30 seconds and is used when a generation
  registers the callback; callback errors and expiry are recorded before the
  child is reaped.

Unknown fields, invalid pool names, empty policies, zero values, a queue
warning without a pool concurrency limit, and a per-pool concurrency limit
above the application-wide limit fail backend startup. The map accepts at most
nine entries, matching the default pool plus the routing protocol's eight named
pools. A valid policy may name a pool that is not present in the current
application topology, which permits operator configuration before application
deployment.

Omitted pools preserve the existing global-only admission and global memory,
RSS, and watchdog behavior. Omitting one field preserves that field's existing
behavior. The reserved `_system` pool is not addressed by this map and always
uses the global memory and RSS thresholds; it recycles cold and does not use
pool-local application admission policy.

## Admission behavior

For a configured pool, an action acquires capacity in this order:

1. the per-pool independent-action permit;
2. the application-wide Node action permit;
3. the exact resident-generation admission used by Node routing; and
4. for durable scheduled work, the monotonic `Pending -> InProgress` claim.

The pool permit is retained through invocation. Pool-first ordering prevents a
backlog for one pool from holding every global Node permit. All Node paths use
the same order, so the two semaphore levels cannot form a permit cycle.

Direct action requests use the existing bounded application-runner wait. Pool
and global admission share one absolute deadline, so adding a pool policy does
not double that wait. Scheduled and registered-cron actions may wait without
that direct-call timeout, but remain durably `Pending` until all runtime
admission required by the start barrier succeeds.

Dependency work uses the existing bounded dependency-overflow model. A
concurrency-one pool therefore admits one independent root plus only the
bounded descendant work needed to unblock an admitted ancestor. This avoids a
parent-child capacity deadlock; `maxConcurrency` is deliberately the
independent-root limit rather than an absolute count including dependencies.
The application-wide Node limit remains the aggregate hard bound.

Cancellation drops both permits. No new durable queue, claim state, retry
protocol, or recovery state is introduced.

## Watchdog behavior

The local Node `/health` request is served by the same main event loop that
accepts `/invoke`. Failed probes therefore provide evidence that the process
cannot currently make invocation progress. A per-pool unresponsiveness budget
changes how long that evidence is tolerated; it does not move the probe to an
isolated worker and does not claim to measure the action's consumed CPU time.

Short probes and first-miss diagnostics remain active. The configured budget
also bounds the first in-flight probe from the time its request starts, even
when the ordinary probe timeout is longer. A successful response before the
deadline clears the failed interval. A completed failed probe retains that
request start as the interval origin, and the watchdog races the remaining
budget independently of later probe intervals and request timeouts. Once the
budget expires, the backend identity-fences, terminates, and replaces that
generation through the existing local-executor lifecycle. An action interrupted
by hard retirement retains the existing conservative outcome because external
effects may already have started.

A larger budget is appropriate only when the operator intentionally accepts a
longer interval without Node callback or invocation progress. It does not make
an unbounded CPU-bound algorithm safe; such actions still need an algorithmic
bound or explicit yielding.

## Observability

Metrics report, by pool:

- configured independent concurrency;
- the configured queue-warning duration, with zero meaning disabled;
- active and waiting pool-admission requests;
- admission wait duration and outcome;
- waits that reached the configured warning duration;
- the effective V8 old-space allowance, RSS retirement threshold, and
  cgroup-pressure RSS threshold;
- the configured event-loop unresponsiveness budget, healthy generation-age
  threshold, and resident callback timeout;
- the existing health-probe, consecutive-miss, generation, and request
  lifecycle signals;
- `local_node_executor_background_drain_outcomes_total{pool_name,reason,outcome}`
  records callback presence, completion, error, timeout, response, and
  cancellation outcomes.

Queue warnings are evidence for capacity tuning. They do not automatically
fail or retry work.

## Activation and rollback

Apply the routing and scheduled-admission prerequisites first. Configure only
the pools that need an override. Verify each `maxConcurrency` against the
application-wide Node limit and each pool's effective memory settings against
its `maxRssBytes`. Set the total Node RSS budget to cover the effective pools
in the committed topology and one largest application surge allowance.
Restart the backend, then verify the configuration, per-pool
memory-threshold, startup-budget, and queue metrics before increasing traffic.
Invalid threshold ordering or a total below the default/system/surge minimum
fails startup; a later topology whose effective sum exceeds the total fails
deployment.

To roll back policy without changing application routing, remove
`LOCAL_NODE_EXECUTOR_POOL_POLICIES` and restart the patched backend. To remove
the patch itself, remove the setting before restoring an earlier image. No
schema or data migration is required. Existing `InProgress` jobs retain their
normal conservative recovery contract.
