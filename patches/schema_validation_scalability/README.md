# Schema Validation Scalability

This patch reduces avoidable application-write conflicts and CPU allocation work
while a deployment validates existing data against a new schema. It is useful
for deployments with large tables, complex union validators, or live writes
that can invalidate a pending schema.

## Adoption

Apply this patch to a backend with upstream's per-table validation attempts and
separate progress counters, then deploy the backend through the normal image
replacement procedure. The behavior activates automatically. No additional
setting, data migration, or coordinated CLI, dashboard, or application release
is required. It does not depend on another downstream patch or function runtime.

The existing `SCHEMA_WORKER_PAGE_SIZE` and `SCHEMA_WORKER_WALK_CONCURRENCY`
settings retain their meaning and defaults: 1,000 documents per page and one
table walk at a time. Keep them unchanged when comparing this patch's effects.

## Effects on application writes and validation

An application write can satisfy the active schema while violating the schema
being deployed. Convex then fails the pending schema while allowing the write
under the active schema. Previously, removing validation progress in that same
transaction could make the write conflict with a worker's progress checkpoint
and retry repeatedly.

With this patch, cleanup runs separately from the application write. The schema
state still determines whether validation may continue: failure or replacement
cancels obsolete work, and a stale checkpoint cannot override that decision.
Staged validators can continue checking data after their owning schema becomes
active. Ordinary optimistic concurrency control (OCC) remains enabled.

Failed or replaced validation attempts can remain visible until background
cleanup runs. Cleanup selects at most 32 inactive attempts per pass; finding
them may still require scanning a longer prefix of active attempts. Historical
failed and overwritten schemas retain the one-hour age threshold, with at most
32 deletions per cleanup transaction. These limits bound cleanup batches rather
than total storage or every discovery read.

Successful document validation also avoids copying the full root validator.
Union matching skips construction of error details for rejected alternatives
whose errors will be discarded. This reduces allocation work, particularly for
large nested unions.
The same optimization applies to document writes, function arguments and
returns, and component validation. Accepted values and reported validation
errors remain unchanged.

## Performance limits and verification

The patch does not eliminate required full-table scans. Total validation time
still depends on document count and size, validator complexity, database read
latency, and available CPU. It adds no scan parallelism and changes no deployment
timeout. No numerical speedup has been established for this patch.

Compare validation of equivalent schemas over the same data under similar
traffic and resource limits. Check how many documents were actually scanned:
Convex can skip tables when existing schema or shape information already proves
compatibility. A deployment that skips validation is not a useful throughput
comparison.

Use existing schema progress and backend metrics:

- `convex_local_backend_schema_validation_documents_validated_total` shows live
  scan progress; its rate indicates aggregate documents validated per second.
- `convex_local_backend_schema_validation_table_walk_documents_sum` and
  `convex_local_backend_schema_validation_table_walk_seconds_sum` record work
  when a table walk exits, including canceled or failed walks. Compare their
  increases over equivalent runs rather than treating them as live progress.
- Correlate scan progress with backend CPU, database read latency, and application
  mutation latency and OCC failures. This patch removes one source of contention;
  unrelated application conflicts can still occur.

The included regression tests cover concurrent schema failure and progress
updates, schema replacement, staged validation, and validator acceptance and
diagnostics. A synthetic benchmark isolates validator CPU work; it does not
measure database access or establish end-to-end deployment speed.

## Related patches

- [Request-local operation failures](../terminal_occ/README.md) controls how
  exhausted OCC errors reach callers. It complements this patch's reduction of
  schema-progress contention; neither patch requires the other.
- [Paced isolate module analysis](../deployment_analysis_pacing/README.md) limits
  module-analysis pressure during deployment. Module analysis is a separate phase
  from scanning stored documents, so slow analysis requires separate diagnosis.

## Rollback

Restore the preceding backend image through the normal replacement procedure.
Stored schemas and progress records need no conversion. The previous allocation
costs and application-write contention during schema failure return. No client,
dashboard, application-code, or configuration rollback is needed for this patch.
