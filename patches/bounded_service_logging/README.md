# Bounded service logging

The [runtime observability patch](../runtime_health_dashboard/README.md) sends
service tracing output through a shared bounded stdout queue.
An output worker owns the blocking write, so a stalled collector cannot block a
runtime worker that emits a lifecycle warning. It applies to `config_service()`
callers independently of native-resident supervision. Tool tracing keeps its
synchronous stderr behavior.

The stdout queue holds at most 256 records. Saturation and output write failures
discard records; aggregate loss is reported after output resumes. Loss summaries
use the same queues, are best effort, and do not generate more summaries when
they are dropped. The optional trace-file output retains its existing format,
filter and 128,000-record capacity. These are record bounds, not byte or RSS
limits, and diagnostic delivery does not establish application progress.

Output workers are created during logging initialization. Failure to start one
fails service startup instead of silently disabling an output. Keep the returned
tracing guard alive until service work and its runtime have shut down. Each
output guard waits at most one second for draining, without writing, flushing or
joining on the caller's thread. A blocked worker may remain until process exit;
pending records are not guaranteed to survive shutdown.

The pinned tracing appender's shutdown guard can synchronously print to stdout
when its queue is full. The bounded writer here avoids that fallback for both
service stdout and the optional file output.

## Adoption and rollback

Bounded service logging is part of the runtime observability adoption unit,
not a separate patch. It is active for service tracing without an environment
switch or schema change and does not require function-usage collection or a
dashboard rollout. Formatter selection, filtering and Sentry breadcrumb policy
are unchanged.

Apply runtime observability before enabling native-resident stderr forwarding. The native-resident
integration test exercises forced reaping and shutdown with backend stdout held
full, using an external process deadline. Reverting the writer restores
synchronous service output and its backpressure; remove native diagnostic
forwarding first if that lifecycle isolation must be retained.
