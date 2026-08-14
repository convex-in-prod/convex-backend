# Bounded deployment operations

Deployment analysis has one active whole-job owner per in-process application, up to four
queued jobs, a 30-second admission wait, and a 768 MiB aggregate logical input
reservation. Queued deployment precedes preflight, with a four-deployment burst
limit. An already active preflight keeps its reservation until execution and
cleanup finish. `start_push`, `evaluate_push`, schema prediction and legacy
configuration pushes share this gate. Root analysis retains the existing
query/analysis gate and `ANALYZE_CONCURRENCY` bound.
Different backend processes or application instances have independent gates.

The byte estimate includes source copies, reconstructed unchanged modules and
the configured analysis cache allowances. Components are analyzed sequentially.
These are retained-data estimates, not RSS bounds: runtime heaps, allocator
overhead, HTTP decoding and native libraries still need host headroom.
Schema prediction charges all supplied module sources while its complete
configuration is retained, even though it evaluates only schemas. Spawned
package preparation and prediction tasks retain the job lease until their
futures actually stop after cancellation.

## HTTP and execution admission

Exact deployment POST routes with `Authorization: Convex <admin key>` use three
separate HTTP slots carved from ordinary capacity: analysis/submission,
schema-wait/finish, and status/cancellation/capabilities. A long analysis or
schema wait cannot consume the status slot. Authorization is checked before body
decompression, with a two-second authentication timeout. Handlers also
authenticate their body credential. Body-only legacy clients use ordinary HTTP
admission. Total HTTP capacity and dependency overflow stay bounded. A second
request for an occupied slot receives 429; authentication timeout returns 503.
Waiting for total capacity has a separate two-second deadline and returns 429.
Dependencies retain access to the entire HTTP total, including these allowances;
the three lane slots are not exclusive physical capacity under callback load.
HTTP total must exceed the dependency reserve plus three deployment slots.

The analysis slot also covers the exact POST routes `get_config`,
`get_config_hashes`, `deploy2/prepare_external_deps` and
`deploy2/download_external_deps` under `/api/`. Configuration reads and dependency
package preparation are prerequisites for analysis in deployment clients; leaving
them in ordinary admission would stop those clients before the reserved analysis
route. Archive downloads release HTTP admission at the response head, as other
streaming responses do. They retain their separate body-stream deadline.

With the control-plane queue enabled, `ISOLATE_CONTROL_PLANE_QUEUE_CAPACITY`
reserves space inside `ISOLATE_QUEUE_SIZE` as well as bounding its lane. It must
leave ordinary queue capacity. Dependency overflow retains access to the total
queue to release ancestors. FIFO selection among eligible requests remains;
worker eligibility can skip an ordinary backlog to select configuration work.

`ISOLATE_CONTROL_PLANE_WORKER_RESERVE`
(default 1) preserves worker capacity inside the existing base limit. It is
clipped to leave at least one ordinary slot. Dependency overflow retains its
existing limit. The scheduler reserves a worker before exposing an initial
JavaScript waiter, with separate pending slots for effective service classes.

Configuration evaluation uses a distinct `control_plane` JavaScript queue. It
alternates grants with ordinary protected work whenever both are waiting, inside
the existing protected group's service floor and elastic share. Resumptions
precede initial starts within each queue; ordinary resumptions cannot hide
deployment demand. Dependency work retains precedence, and the degradable floor
is unchanged. Without protected/degradable floors, the enabled control-plane
lane still alternates with ordinary work. Disabling the lane restores the prior
JavaScript policy. No extra permits or preemption are introduced. For occupancy
comparisons against the protected minimum, sum `protected` and `control_plane`.

These are non-preemptive service and finite-queue bounds. Existing executions,
dependency callbacks, shared query/analysis permits and external resources must
finish or time out before their capacity is returned. They do not establish a
wall-clock deployment deadline through database, storage or host failure.

When cgroup shedding is enabled, authenticated deployment intake remains
eligible while ordinary intake is shed until headroom reaches
`LOCAL_BACKEND_DEPLOYMENT_MIN_HEADROOM_BYTES` (default 2 GiB). Configure this
alongside the existing shedding thresholds and measured runtime heaps. It is a
stop for new intake, not an allocation guarantee for already running work.
The deployment threshold must remain below the finite cgroup limit. Enabling
only internal reclamation does not enable this HTTP stop; with shedding enabled,
the stop is evaluated even while ordinary soft shedding is currently inactive.
The self-hosted Compose template passes through the deployment headroom and
control-plane worker-reserve settings without overriding their backend defaults.

## Opt-in operation protocol

Legacy endpoints remain supported. New clients explicitly negotiate this
protocol; it is not enabled by retrying arbitrary POST requests.

All operation endpoints are POSTs under `/api/deploy2/operations/`. Every call
requires an `adminKey` body field and Deploy permission. Supply an authorization
header with Deploy permission for the same deployment to use the reserved HTTP
lane. The header and body credentials are authorized independently and may be
different valid keys. Revocation affects later API calls; it does not revoke
work already accepted by the server. An operation ID alone grants no authority.

1. `capabilities`, body `{ "adminKey": "..." }`, returns `protocolVersion: 1`,
   a process-specific `sessionId`, and retention limits.
2. `submit` accepts `adminKey`, `sessionId`, `operationId`, `kind` and `config`.
   `kind` is `start_push` or `evaluate_push`. `config` is the ordinary start-push
   payload with `adminKey` omitted. The operation ID is Unix seconds followed by
   `:` and 32 lowercase hexadecimal nonce characters. Its lifetime is ten
   minutes, with at most five seconds of client clock lead.
3. `status` and `cancel` accept `adminKey` and `operationId`.
4. Once `start_push` completes, pass its returned result to the existing
   `wait_for_schema` endpoint. Finish through `/api/deploy2/finish_push`, using
   that unchanged result as `startPush` and the same `operationId`.

Submitting the same ID and input returns the current status. Different input
returns `DeploymentOperationInputMismatch`. Identity compares JSON with sorted
object keys; omitted defaults and explicit defaults remain distinct. Credentials
are excluded from the digest. Status is `running`, `completed` with `result`,
or `failed` with `code` and, where available, an authenticated error message.
Status does not yet report individual root progress.

There are at most eight retained operations, 384 MiB of aggregate logical
input/result reservations, and 32 MiB per encoded result. Serialization stops at
the encoded limit. Prepared source bytes remain charged while retained,
including by a finish admitted before cancellation or expiry. Completed data
expires even without later HTTP traffic. Operation time advances monotonically
within a process, so a wall-clock rollback cannot make a removed ID reusable.
Expiry requests cancellation; ownership is released only after the active future
drops. Disconnection detaches the HTTP subscriber. Explicit cancellation stops
analysis but does not undo schema/index preparation or an activation already
committed through finish. It does not roll back external effects from Node
import-time code. Module, schema, auth-configuration, component-definition and
initializer evaluation all bind cancellation to their current execution identity;
the dedicated Node owner retires its system generation on explicit cancellation
or workflow exit and retains admission through confirmed cleanup. Legacy Node
subscriber loss continues to use the original terminal request deadline.

Status replies share the encoded result allocation instead of serializing a
new copy for each poll. Registry expiry releases its copy; an HTTP response
already in transport can still hold those bytes. Transport buffers, request
decoding and transient JSON values are outside the registry's logical budget.

Poll the same operation with bounded jitter and an overall deadline. Treat
validation failure as terminal. A backend restart changes `sessionId`, so an
old submit cannot silently repeat preparation. Uncommitted analysis and pending
preparation responses are process-local. Inspect deployment/schema state before
creating a new operation after a restart; automatic resubmission is unsafe.

## Authoritative finish and historical replay

The server retains the validated sources and a digest of its preparation
response. An altered client echo cannot use those sources. Finish reuses the
server-owned sources without archive downloads, and retains environment,
schema, topology, external-dependency and paired-runtime validation. Its
transaction also checks the root source-package generation captured before
preparation. A newer activation, including a legacy or empty-package deployment,
rejects an uncommitted operation with `DeploymentOperationSuperseded`.
Retained preparation also records canonical URL environment overrides. The
activation transaction rechecks them alongside user environment variables,
since both can affect module analysis and auth configuration.
Source-package insertion reads the prior record transactionally and advances
its creation time, so concurrent legacy writes and equal source bytes cannot
hide a later activation from this check.

Finish checks retained generation and canonical URL authority before source
resolution and cutover admission, then repeats those reads in its activation
transaction. An already-superseded preparation cannot enter force-capable
cutover admission. A matching receipt is returned before these checks, so a
newer deployment does not invalidate historical replay.

Only one finish at a time may hold a retained operation's prepared sources.
A concurrent finish receives `DeploymentOperationFinishInProgress` immediately,
before source resolution or Node cutover admission. Retry that same finish after
the owner releases its lease; an exact committed replay then returns its receipt
before source resolution or cutover, including after restart. The lease remains
exclusive through cancellation, expiry and handler cleanup. Transaction receipt
checks alone cannot provide this exclusion: a duplicate that entered cutover
admission before the first commit could otherwise force the first owner's
draining Node generation to stop or wait unnecessarily for its capacity.

Finish identity includes `dryRun`, `message`, `forceNodeCutover` and the exact
source-keyed activation selector. It excludes credentials and the operation ID.
Dry-run finishes still require retained preparation with unchanged generation
and canonical URL authority, and check for conflicting activation receipts;
they create no activation receipt. Cancellation before finish acquires retained
sources prevents activation. An already acquired
finish may commit, and cancellation cannot undo pending schema preparation.

An activation and its receipt commit in the same transaction. A full bounded
receipt-table read participates in OCC, preventing concurrent retries from
activating twice. The `_deployment_receipts` system table is initialized by the
normal system-table initializer. It holds at most 16 live receipts with a
128 KiB activation diff each. A full table rejects before activation; expired
receipts are pruned transactionally. Expired operation IDs cannot be reused.

Replaying an already committed finish returns its original commit timestamp and
diff, even after restart or a newer deployment, without activating old code.
`activationReplay.nodeCutoverCompletion` is `unverified`: the receipt proves
historical activation, not current deployment or completion of the original
Node cutover. Without a receipt or retained preparation, finish fails explicitly.

## Preflight packaging

V8-only `evaluate_push` resolves and validates sources in memory and avoids
object-storage uploads. It streams the canonical ZIP into a bounded discard
sink to preserve the existing compressed-size limit; compression CPU remains.
The ZIP compressor can still buffer an individual entry before writing to the
sink. The registry's logical byte budget is not a process memory bound.
Node or external-dependency consumers keep the archive path. Canonical module
names, topology, dependency identity and package-size checks apply to both paths.

CLI changes are needed to negotiate operation support and poll the same ID.
The CLI's schema-endpoint fallback must distinguish a positively unsupported
capability from authorization, overload, invalid schema and malformed responses.
These backend endpoints do not change that client fallback policy.


## Bounded operational evidence

`deployment_finish_events_total{outcome="activation_committed|historical_replay"}`
records observed non-dry-run finish outcomes after the transaction, including
both early receipt lookup and a concurrent receipt discovered in the transaction.
Polling and attachment do not count as activation. A commit is recorded before
Node cutover and can precede a failed cutover; use the existing Node cutover
metrics and authenticated result separately. A crash between commit and emission
or a telemetry gap can hide the event, so absence never disproves activation.

`http_deployment_hard_stop_total` counts deployment intake refused for hard
memory pressure. `deployment_operation_retention_rejections_total` counts new
operations refused by the finite retained-count/byte budget. Whole-job and root
pacing evidence is described in [analysis pacing](../deployment_analysis_pacing/README.md).
The HTTP dependency boolean is callback classification and does not identify
deployment intake. The ordinary main-service ceiling is `H - D - 3`; the total
remains `H`. Base occupancy is emitted even when only the deployment reserve is
configured. These observations never own workflow progress or retry policy.
