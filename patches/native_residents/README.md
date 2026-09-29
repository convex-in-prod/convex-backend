# Supervised native residents

This patch extends the coordinated local executor owner with one optional native
resident steady slot. Native and Node replacements share the existing surge
capacity. The process implements a bounded lifecycle protocol and calls application
APIs as an ordinary client; it is independent of the function execution runtime.

The [native resident contract](../../crates/node_executor/NATIVE_RESIDENTS.md)
defines artifact publication, configuration, deployment envelopes, protocol messages,
capacity, shutdown, bounded stderr forwarding and verification. The
[bounded service logging](../bounded_service_logging/README.md) implementation is part
of the [runtime observability patch](../runtime_health_dashboard/README.md). Output backpressure
and draining do not authorize child cleanup or capacity release. Apply that patch,
coordinated local executor pools and deployment operations first. The matching CLI
must advertise and validate native-resident protocol 1 before sending a publication
envelope.

Function publication compares the expected prior descriptor and records the selected
descriptor in the root source package. Deployment metadata reads use the function's
transaction, so ownership checks conflict with concurrent selection changes. Live
process status is convergence evidence and cannot replace that durable authority.

Enable the adapter only after installing immutable executable/configuration files and
reserving the native steady allowance plus the largest shared replacement allowance.
Keep active, staged, draining and rollback inputs. An omitted envelope is allowed only
when no native resident is selected; an explicit null target retires that selection.
An incompatible application contract requires an intervening compatible publication
with null selection and confirmed process cleanup.

The patch retains direct-child and capacity ownership through canceled preparation,
retirement, forced replacement, memory pressure and shutdown. The process fixture
checks these boundaries using an actual local executable. It does not exercise a
particular application's external side effects or establish production throughput.

Native termination uses the backend's critical cgroup headroom signal, not the earlier
optional-cache reclamation signal supplied to Node pools. The controller retains the
existing HTTP-shedding thresholds and hysteresis without a native-specific timer or
new capacity knobs. Critical pressure remains effective while allocator trim is pending
and when HTTP shedding is disabled but reclamation is enabled. Controller tests cover
both signals, recovery hysteresis and unchanged-sample notification behavior; native
process tests retain interruption, restart and confirmed-reaping coverage.
