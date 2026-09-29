# Local native residents

A native resident is a directly supervised executable, independent of Node source
packages. It implements lifecycle protocol 1 and uses application APIs as an ordinary
client. It does not implement a query, mutation, or action runtime.

## Configuration and publication

Set all three backend variables to enable the adapter:

- `LOCAL_NATIVE_RESIDENT_ARTIFACTS_DIR`
- `LOCAL_NATIVE_RESIDENT_CONFIGURATIONS_DIR`
- `LOCAL_NATIVE_RESIDENT_MAX_RSS_BYTES` (at least 16 MiB)

Set `LOCAL_NODE_EXECUTOR_TOTAL_RSS_BUDGET_BYTES` to cover the Node steady slots, one
native steady slot, and one surge slot sized for the largest Node or native replacement.
The default total covers only the default and system Node slots and a Node replacement;
it does not grow automatically when the native adapter is enabled. Startup and deployment
validation reject an insufficient total. Node and native candidates and draining
generations use the same surge owner.

Publish the complete executable to `<artifacts>/<sha256>/resident` and its runtime JSON
configuration to `<configurations>/<sha256>.json`. Install files atomically, keep them
immutable, and retain active, staged, draining, and rollback versions. Configuration is
limited to 32 KiB and executable size to 512 MiB. The backend checks both hashes before
spawn. On Linux it executes the verified file descriptor through `/proc/self/fd`, sets
parent-death termination, and passes configuration through owned pipes with an empty
environment. Credentials belong in configuration, not executable arguments or artifacts.

The finish-push body accepts `nativeResident`:

```json
{
  "expectedPrior": null,
  "target": {
    "artifactSha256": "<64 lowercase hex characters>",
    "configurationSha256": "<64 lowercase hex characters>",
    "lifecycleProtocol": 1,
    "applicationContract": "example-v1"
  },
  "applicationContract": "example-v1"
}
```

All three envelope fields are required; a null value is explicit. The source-package
transaction compares `expectedPrior` and records `target` with the
function deployment. An omitted envelope is accepted only when no native resident is
selected. A null target selects ordinary execution. Both V8-only and paired deployment
use this boundary. Queries and mutations read `nativeResident` through
`getDeploymentMetadata` in their own transaction, so application ownership checks
participate in OCC with publication.

A retained process and a target process must support the declared application contract.
The backend serializes the live-process compatibility check with function publication
and refreshes committed selection under that guard. A canceled publication caller cannot
leave a stale compatibility decision authorizing an incompatible publication.
The guard covers publication after capacity admission and is released before Node drain.
Native staging uses the same bounded deployment surge admission and force policy as Node;
`forceNodeCutover` can reclaim a draining surge owner before native staging begins.
To change it incompatibly, first publish compatible functions with a null native target,
wait for drain or confirmed termination, and then publish the incompatible deployment.
Unchanged executable/configuration descriptors preserve the running process across
function-only publications. Backend restart and lost HTTP replies recover from the
latest committed source package, including an empty package.

## Control and lifecycle

`POST /api/deploy2/native_resident` accepts an administrative key and a typed command:

```json
{"adminKey":"...","command":{"type":"inspect"}}
```

The other commands are `{"type":"prepare","descriptor":{...}}` and
`{"type":"forceRetire","generation":"..."}`. Prepare verifies inert readiness and
reaps the trial before returning. Force retirement must match the exact current process
generation and does not alter durable selection. Inspection returns lifecycle protocol,
whether the adapter is enabled, `committedSelection` (descriptor or null),
`committedSelectionTs` (transaction timestamp as a decimal string), and selected/active
process status. The committed selection comes from the latest root source package in a
fresh read transaction. `status.selected` reports the supervisor's last reconciled selection
and can lag that transaction. Publication CAS and uncertain-reply resolution use the
committed selection; process status reports convergence. Deployment capabilities expose
`nativeResidentProtocol: 1`; enabling the adapter is a separate configuration requirement.

Control uses newline-delimited JSON over child stdin/stdout. Each message is at most
64 KiB. `generation` is an opaque string; protocol and nonce are unsigned integers.
The child reserves stdout for the protocol.

Stderr carries application-sanitized diagnostic lines. The supervisor forwards valid UTF-8
lines of at most 8 KiB, without control characters, at up to 100 lines per second per child.
Oversized, malformed, incomplete and excess lines are discarded with aggregate drop reporting;
pipe failure is diagnostic only. Each forwarded line has the `native_resident_log` message
prefix and a `native_generation` field in backend logs. Application JSON remains within the
message for the operator's log collector to parse. The reader has bounded buffers, runs
independently of lifecycle reads, and is canceled at termination rather than delaying reaping.
Completed losses are summarized at the next one-second window boundary, including when stderr
stays open without another write. Summary delivery is best effort under output saturation.

[Bounded service logging](../../patches/bounded_service_logging/README.md) uses one shared
256-record stdout queue and one output thread, so native
diagnostics and backend lifecycle warnings cannot block runtime workers on the stdout pipe.
There is no separate native output queue or lazy sink initialization. Output workers are
required at service startup; failure to create one fails startup visibly before supervision.
Queue saturation and output write failures discard records and report aggregate loss after
output resumes. Summary loss does not generate further summaries. The optional trace-file
writer retains its existing 128,000-record buffer and format/filter policy, with the same
bounded shutdown behavior. Tool tracing still writes to stderr synchronously.

Each output guard attempts to drain for at most one second without printing, flushing, or
joining on the caller's thread. A blocked output worker may remain until process exit, and
queued diagnostics are not guaranteed to survive shutdown. These output bounds do not limit
bytes read from resident stderr or establish process liveness, completion, or ownership.
Applications use bounded nonblocking logging when stderr backpressure cannot delay their work.

| Direction | Message |
| --- | --- |
| Backend → child | `{type:"initialize",generation,descriptor,configuration}` |
| Child → backend | `{type:"ready",generation,artifactSha256,configurationSha256,lifecycleProtocol,applicationContract}` |
| Backend → child | `{type:"activate",generation}` |
| Backend → child | `{type:"probe",generation,nonce}` |
| Child → backend | `{type:"progress",generation,nonce}` |
| Backend → child | `{type:"retire",generation,deadlineMs:180000}` |
| Child → backend | `{type:"drained",generation}` |
| Child → backend | `{type:"failed",generation,code}` |

Readiness is inert: no application ownership or external work starts before activate.
Progress acknowledges the exact probe on the thread that owns application decisions;
a responsive I/O thread does not establish coordinator progress. Healthy idle residents
still service probes. Readiness has a ten-second deadline and probes a five-second
response budget. Retirement permits the application's episode-aware drain for at most
180 seconds. Failed drain, forced retirement, process failure, and memory pressure all
retain termination authority. A successful drain reply does not prove process exit.
The backend confirms direct-child reaping before returning capacity.
RSS sampling continues while readiness and progress replies are pending. Memory pressure
and shutdown interrupt stalled replies and retirement, including both the predecessor
and an inert replacement. An inert replacement continues to answer coordinator probes
while its predecessor drains. Promotion resumes any outstanding probe with its original
nonce and deadline before activation.
The incumbent remains supervised during replacement verification and readiness. Protocol
writes retain their offsets and deadlines across cancellation and receive RSS checks;
retirement interrupts cover blocked writes too. A failed inert candidate is terminated
even if it subsequently sends a valid reply.

The local backend supplies the critical cgroup pressure signal to native supervision,
separate from early optional-cache reclamation. Critical pressure uses
`LOCAL_BACKEND_MEMORY_PRESSURE_ENTER_HEADROOM_BYTES` and
`LOCAL_BACKEND_MEMORY_PRESSURE_EXIT_HEADROOM_BYTES`, with the same hysteresis as HTTP
shedding. It applies when either backend reclamation or HTTP shedding is enabled,
including when HTTP shedding itself is disabled, and never waits for allocator trim.
Early reclamation alone leaves a healthy native process running. Critical pressure
terminates owned native children and prevents activation until headroom recovers.
The independent native RSS limit and progress watchdog remain active at either level.

Generation identities remain distinct across backend restarts and PID reuse. Stale replies
cannot name a successor. Canceled startup and cleanup retain the child and
shared capacity until reaping. Shutdown tracks canceled preparation cleanup and blocking
artifact verification. Canceling a shutdown caller does not detach the supervisor or
report cleanup complete. A backend shutdown terminates owned children; Linux
parent-death signaling prevents an active resident outliving its supervisor.
Repeated shutdown callers observe the same supervisor and reconciliation completion,
including any terminal failure.

## Verification

The native integration fixture is an ordinary ELF executable. With the testing feature,
`cargo test -p node_executor --features testing --test native_resident` exercises the
actual verified-descriptor spawn, inert readiness, activation, function-only reuse,
candidate replacement, forced retirement fencing, graceful drain, canceled preparation
and shutdown, RSS enforcement before readiness and after activation, memory pressure
during stalled progress and replacement drain, watchdog replacement, parent-death
termination, JSON-formatted stderr forwarding, and reaping. A separate parent-process fixture
holds backend stdout full during force retirement and shutdown, checks direct-child reaping,
and applies its deadline outside the supervised backend process. Native unit tests additionally
cover bounded stderr framing and quiet-stream loss reporting, progress mismatch,
disconnect, stalled progress, canceled probe resumption, canceled startup, and incomplete
artifact inputs. Follow the checkout's resource-guard instructions when running these
commands.
