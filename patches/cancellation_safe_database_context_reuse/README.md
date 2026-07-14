# Cancellation-Safe Database UDF Context Reuse

This patch hardens upstream database-UDF context reuse against caller cancellation while preserving
the upstream module-wide policy: a module whose analyzed namespace contains
`experimental_reuseContext = true` may reuse contexts for both queries and mutations. Actions and
HTTP actions remain ineligible at the database-UDF validation boundary.

The patch is generic. The backend contains no application module names or deployment allowlist.
Applications decide which module graphs are safe and emit the ordinary upstream export.

## Why maintain this patch

Upstream passes the caller receiver's close future into isolate execution. That observes caller drop
while a database UDF waits in its async-syscall loop, but it does not give same-isolate nested UDFs a
shared signal they can check at their own context-save boundary. Synchronous JavaScript also does
not poll the receiver-close future.

Without the hardening, an execution whose caller has gone away can finish and publish its JavaScript
context for later reuse. This patch creates one cancellation signal per request, propagates it
through same-isolate descendants, and checks it immediately before synchronous cache insertion.
The full ownership, memory-ordering, nested-call, and timing design is preserved in
[`cancellation_design_reference.md`](cancellation_design_reference.md).

## Effective eligibility

The analyzed module bit is carried through function validation and the trusted function-runner
protocol. Consumers combine it with the actual UDF type:

- query: may reuse when the analyzed module bit is true;
- mutation: may reuse when the analyzed module bit is true;
- action: fresh;
- HTTP action: fresh at this database-UDF boundary.

The UDF type is checked again after protocol reconstruction. A stale or mixed-version producer
therefore cannot make an action or HTTP action eligible merely by sending a true bit.

Current upstream always supports the analyzed marker and no longer has a backend-wide enable knob.
Eligibility is therefore controlled by application module policy and the UDF-type boundary above.

## Mutation correctness boundary

A mutation context can be published before the enclosing transaction commits. That fact alone does
not make reuse incorrect. A source-safe module that retains no request-derived or transaction-
derived mutable state behaves correctly after success, failure, or OCC retry.

The real hazard is conditional: JavaScript state written during a failed attempt may survive into a
retry or later mutation even though the database transaction did not commit. Applications must not
mark graphs that retain `ctx`, arguments, documents, identities, errors, promises, callbacks, or
derived mutable values across executions. They must also review third-party package globals and
import-time work. Queries require the same source discipline because they can also leave failed or
canceled execution state in a reusable context.

Commit-aware publication would be required only for an application that deliberately retains state
whose validity depends on successful transaction commit. It is not needed for source-pure module
graphs and is outside this patch.

## Save boundary

A database context is saved only when:

- the module and UDF type are effectively eligible;
- initialization produced or retained a valid context read set;
- isolate execution completed successfully;
- the final microtask checkpoint did not expose V8 or isolate termination; and
- caller cancellation was not visible at the final non-awaiting check.

There is no await between the final cancellation check and cache insertion. Caller drop before that
check prevents publication. Caller drop after it does not retract a context that already reached the
save boundary. A failed or canceled taken context is discarded because it is absent from the cache
while executing.

The signal does not interrupt synchronous JavaScript from another thread. Such work continues until
an existing timeout, syscall cancellation point, termination check, or final save check. The
scheduler discards a request when selection observes a closed response and cancels a selected
request's pending active-permit acquisition if closure occurs there. External canceled entries can
remain retained while ineligible until selection or their bounded queue deadline; direct internal
entries are pruned when their buffer is polled. Caller drop can still race the final pre-dispatch
check or happen after execution begins, but the canceled execution cannot republish its context.

## Composition

This patch composes with:

- `bounded_multi_context_reuse`, which changes cache admission and eviction but preserves the same
  eligibility and save checks;
- `context_reuse_observability`, which records allowed query and mutation decisions, fresh/reused
  initialization, validation, cache lifecycle, affinity, occupancy, and memory;
- `reuse_http_action_contexts`, whose HTTP execution cache is a separate runtime policy and must not
  leak its marker through database-UDF protocol reconstruction.

## Verification

Focused protocol coverage proves that the reuse bit survives serialization for queries and
mutations and is rejected for actions and HTTP actions. Focused cancellation coverage verifies
shared clone visibility, drop-guard publication, and disarming. The nested-call plumbing and final
save-boundary ordering remain source-traced because the public checkout has no executable reusable-
context lifecycle harness. An upstream or private harness should additionally exercise queued and
executing caller drop, synchronous handlers, same-isolate recursion, snapshot queries, separately
scheduled children, termination during the final checkpoint, mutation OCC retries, and isolate-
system failure.

## Adoption and rollback

Before activation, inventory marked modules and review complete runtime import graphs. Establish
fresh/reused, evaluation, OCC, error, latency, cache occupancy, isolate memory, and recreation
baselines. Roll out application policy separately from the backend image when practical so evidence
can identify which semantic change caused a regression.

Normal per-module rollback removes the emitted upstream marker and redeploys that module. There is
no backend-wide disable knob in current upstream. Emergency rollback must remove the marker from
every opted-in module, deploy those application changes, and restart backend workers to clear
process-local saved contexts before traffic resumes.
