# Terminal OCC across transport layers

## Purpose

The application mutation runner owns the bounded optimistic concurrency control (OCC) retry
budget. Once it returns an exhausted OCC error, the transport must return that error to the caller.
A Node callback retry loop or sync socket reconnect would otherwise begin another mutation retry
cycle and delay unrelated pending work.

## Behavior

The Node callback half recognizes HTTP 503 responses with the exact structured
`OptimisticConcurrencyControlFailure` code and a string message. It passes a positively identified
body to the existing response error handler without another callback attempt. Inspection consumes
the original stream, is limited to 64 KiB and one second, and respects action disposal. Ordinary
5xx, transport, oversized, incomplete, malformed, and unclassified responses retain the existing
retry policy and stable per-callback mutation identifier. Non-idempotent action callbacks retain
their existing retry rules.

The sync half handles exhausted OCC returned by either the public or admin application mutation
API. It sends the existing `MutationResponse` with the original request ID, a generic identified
OCC error message, no commit timestamp, and empty log lines. Table and document diagnostics are
excluded regardless of log visibility. The worker remains connected so following mutations and
query-set changes can progress. Successful mutations, redacted function errors, and non-OCC
application failures keep their existing response or failure behavior. HTTP OCC errors remain 503
and do not become deterministic user errors.

The patch adds no retry API, protocol message, configuration, scheduler priority, or process
supervision policy. A caller may choose to submit a separate mutation after receiving the error;
that choice remains with the caller.

## Adoption and rollback

The Node half extends the existing callback retry loop; the sync half uses the ordinary application
and sync interfaces. Neither requires the scheduler or native-resident supervision patches.
Apply the backend and its Node executor source together. Activation is automatic for positively
identified exhausted OCC. Restore the previous backend and Node executor together to roll back;
that restores callback retry amplification and socket closure on these failures.

## Verification

The focused Node syscall tests cover terminal OCC propagation, ordinary retry budgets and
identifiers, malformed and stalled bodies, UTF-8 boundaries, and action disposal. The sync tests
inject exhausted system and user OCC at the application API boundary into the real worker's
public and admin mutation paths. They assert identified, redacted responses, no resubmission,
following mutation and query-set progress on the same worker, and unchanged non-OCC/HTTP
classification. These worker tests do not induce database conflicts or prove committed-result
retention or deduplication.
