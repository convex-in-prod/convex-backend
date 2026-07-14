# Database UDF Context Reuse Cancellation Hardening

> Detailed design reference. This is not a separate adoption unit. Module-wide eligibility and
> cancellation-safe context publication are maintained together in
> [`README.md`](README.md). This reference
> preserves the original signal-ownership, memory-ordering, save-boundary, and timing analysis.

This maintained patch hardens the caller-drop boundary for reusable database
UDF contexts. It is generally applicable to upstream database context reuse;
it is not specific to a UDF-type gate. The maintained patch in
[`README.md`](README.md)
depends on this patch so that its documented cancellation behavior is true.

## Upstream gap

The isolate client waits for a database UDF result through a one-shot channel.
Upstream passed `response.closed()` into the isolate as its cancellation
future. That detects caller drop while the database UDF is waiting in its
async-syscall loop, but it does not provide a signal that a same-isolate nested
UDF can check at its own context-save boundary. Synchronous JavaScript also
does not poll the channel-closure future.

As a result, a reusable database UDF could finish after its caller was dropped and
publish module state from an abandoned execution. This is independent of the
query or mutation type and applies to any reusable database context.

## Signal ownership and propagation

Each `IsolateClient::execute_udf` request creates one `CancellationSignal` and
keeps a caller-side `CancelUdfOnDrop` guard while waiting for the response.
The guard is declared after the pinned response future. Rust drops locals in
reverse declaration order, so cancellation is published before the response
receiver is dropped when the waiting future is canceled. The guard is disarmed
after any completed response, including an execution error or sender error,
because no execution remains for that caller to cancel.

The signal is part of `RequestType::Udf`, so it follows a request while queued
and while executing. A top-level database UDF and every same-isolate nested
query, snapshot query, or mutation clone that signal. A separately scheduled
child creates its own signal in the direct `IsolateClient` callback path.
Dropping the parent child-request future sets the child's signal before dropping
the child's response receiver.

The shared state contains an `AtomicBool` and one `AtomicWaker`. Cancellation
stores with release ordering and checks load with acquire ordering. The wait
future checks the flag, registers its waker, and checks the flag again, so a
cancellation between the first check and registration cannot be lost.

One waker is sufficient under the current database syscall contract. `runUdf`
is an unbatched syscall, so same-isolate recursion has one active descendant
chain. Several ancestor cancellation waits can remain pending, but the deepest
registered frame is enough: waking it makes that frame fail and wakes each
ancestor as the recursive result unwinds. Batching or concurrently executing
same-isolate `runUdf` calls would invalidate this assumption and would require
a multi-waiter notification primitive.

The signal has no ownership edge back to the caller guard or request. Stored
wakers are dropped with the signal after the request completes, so the design
does not create a persistent ownership cycle.

## Save boundary

Database UDFs continue to poll cancellation while waiting for an async syscall.
Reusable executions additionally perform a non-awaiting cancellation check
after the final microtask checkpoint, request-state extraction, and termination
handling, immediately before the context-save gate.

The final microtask checkpoint can run user code. The database path therefore
rechecks V8 termination after the checkpoint, matching the existing HTTP reuse
requirement, so termination exposed by that checkpoint cannot pass through the
save gate merely because the worker will recreate the isolate afterward. An
asynchronous isolate termination that is not visible at this check is a
post-check race. A later worker or next-request cleanliness check that observes
the termination discards the isolate and its context cache, but those checks
are optimistic rather than synchronized with the background timeout task. This
patch excludes termination visible at the final check; it does not claim that
cache insertion or worker advertisement is linearized against a termination
that becomes visible afterward.

A context is published only when all of the following are true:

- the module is effectively eligible for database context reuse;
- initialization produced or retained a valid context read set;
- isolate execution completed with `Ok(Ok(_))`;
- the final checkpoint did not expose V8 or isolate termination; and
- caller cancellation was not visible at the final check.

The final caller check is the cancellation linearization point. There is no
await between it and the synchronous cache insertion. A caller drop that wins
before the check prevents the save. A caller drop after the check does not
retract a context that has reached the save point, just as retention,
transaction merge, return validation, query-result finalization, and mutation
commit occur too late to retract it.

Thrown or rejected handlers, observed cancellation, and termination or
isolate-system errors visible at the save gate do not publish a reusable
context. A taken context is absent from the cache while it executes, so any
such failure also discards that context.

## Cancellation timing and non-goals

The signal does not terminate V8 from another thread. Synchronous JavaScript,
module initialization, and context-read-set validation may continue until they
reach an existing timeout, syscall cancellation point, termination check, or
the final save check. This preserves upstream execution and timeout behavior
while closing the reusable-context publication hole.

Non-reusable UDFs retain upstream cancellation semantics: cancellation is
observed while waiting for async syscalls, but synchronous JavaScript is not
interrupted by this patch. Their contexts are never candidates for the cache.

The scheduler discards requests whose closed response is visible at selection
and cancels a selected request's active-permit acquisition when closure becomes
visible during that wait. An ineligible external entry can remain retained until
selection or its finite queue deadline; the internal callback buffer prunes
closed entries whenever it is polled. Caller drop can still race after the final
pre-dispatch check or occur after execution begins. Such an execution can spend
work and consume a taken warm context before the final save check discards it,
but it cannot publish the canceled execution. Returning an untouched taken
context remains a separate cache-lifecycle change.

## Verification boundary

The public checkout has no database-UDF lifecycle harness that can run normal
deployed JavaScript through the application, function-runner, isolate,
recursive-call, and cache-save layers. The source trace establishes the
ownership and save ordering above. Behavior-level regression coverage still
belongs in an upstream or private lifecycle harness and should cover queued and
executing caller drop, synchronous handlers, same-isolate recursion, snapshot
queries, separately scheduled children, termination during the final
checkpoint, and isolate-system failure.
