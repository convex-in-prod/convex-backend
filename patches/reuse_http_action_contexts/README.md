# Reuse HTTP Action Contexts

This patch adds an opt-in mode for reusing V8 contexts for HTTP actions. It is
intended for self-hosted deployments that have a small number of hot HTTP action
entry points, where the HTTP actions mostly route requests, call Convex queries,
mutations, or actions, and build HTTP responses. For that shape of application,
repeatedly creating and initializing a fresh JavaScript realm for each HTTP
request can be a large fixed cost. Reusing the already-initialized context can
materially improve throughput and tail latency.

The patch is deliberately behind `REUSE_HTTP_ACTION_CONTEXTS`. It changes the
semantics of HTTP actions in a way that is acceptable for some deployments and
not acceptable as a silent global default. It preserves fresh per-request Rust
state, request state, response state, cancellation state, and callback state,
but it can preserve JavaScript module and global state across HTTP action
requests that land on the same cached context.

This note uses "reuse" for the behavior and "cache" for the backend data
structure that stores a reusable context between requests. A cached context is
not a user-facing cache API. It is an implementation detail of the isolate
worker.

## What is being reused

Convex uses several "context" objects at different layers. This patch is about a
V8 context, not the user-facing JavaScript `ctx` argument and not just a Rust
dependency-injection object.

A reused HTTP action context contains:

- a `v8::Context`, which is a JavaScript global realm;
- the JavaScript global object for that realm;
- already-installed Convex native syscall and op functions on the realm;
- already-evaluated JavaScript modules and their module-level state;
- a `ModuleMap`, which maps module specifiers to V8 module handles;
- a captured `ContextReadSet` for the system-table reads used while initializing
  the module graph.

The per-request state is not supposed to be reused. Each request still gets a
fresh `ActionEnvironment`, `RequestState`, `RequestScope`, HTTP request stream
state, response streamer, identity, callback map, task response receiver,
cancellation path, and syscall trace. Those objects are installed into V8
context slots before the request runs and removed when the request scope is
dropped.

The cache save path also refuses to store an HTTP action context after a request
that ends with a Convex/JavaScript execution error, records a V8 termination, or
leaves unresolved action task promises, dynamic imports, unhandled promise
rejections, or non-abort streams. A non-abort stream must be closed, have no
unread buffered chunks, and have no listener before the context can be saved.
This prevents an ignored or partially read request body, fetch body, storage
body, or response body from carrying request-owned work into a later invocation.
The request abort stream is the one exception: its listener is expected to
remain pending until the client disconnects and is detached when the request
state is dropped.

`TextDecoder` resources follow their JavaScript objects through V8 weak handles
and are not request-scoped stream work. A decoder deliberately retained in
module or global state therefore remains usable with the rest of that state;
one that becomes unreachable is reclaimed with its JavaScript object.

The context is inserted into the cache only after all fallible request
finalization, including warning emission and result extraction, has completed.
A request that returns a system error therefore cannot leave its candidate
context in the cache even if JavaScript execution itself completed cleanly.

Termination is checked after the final microtask checkpoint because that
checkpoint can still run user code. Error HTTP status codes deliberately written
by application code are different: if the action successfully streams that
response, the context can still be saved. A handler or response-body stream
error, including exceeding the response-body limit, that occurs after the
response head has started is still an execution error and prevents reuse, even
though the HTTP result is already represented as streamed. The guard is about
failed or unfinished execution paths where request-owned JavaScript promises or
streams may still be attached while the Rust task, callback, cancellation, and
response state is being dropped.

The action task executor is already closed when that final checkpoint runs. If a
late microtask tries to start another action task, task registration removes the
new promise resolver and returns an error instead of panicking on the closed
task channel. The syscall boundary records termination, so that context is not
saved.

There is a separate request-local diagnostic guard in
`IsolateHandle::push_context` in `crates/isolate/src/termination.rs`. For a new
root request, it clears the saved HTTP request stream byte count used when
formatting OOM errors. Nested calls preserve the parent value, because the
parent HTTP request body is still the relevant diagnostic context. This reset
keeps an OOM in a later reused context from reporting the body size of an
earlier HTTP request. The request abort signal uses a separate ordinary stream,
so creating it does not replace the body stream's byte counter.

The important semantic change is that JavaScript module and global state can
persist. A module-level variable set by one request can be visible to a later
request if that later request runs on the same cached V8 context. Applications
should not depend on this for correctness, because contexts can be cleared under
memory pressure, invalidated by deploy or configuration changes, or simply not
be the one selected by the scheduler. But with this patch enabled, the behavior
is possible and expected.

## Cache shape in this patch

The context cache introduced by this patch is local to an isolate worker. It is
not a single process-global HTTP action context.

Each isolate worker owns a `ContextCache`. HTTP action contexts are cached by
`CanonicalizedComponentModulePath`, which means component plus module path.
While a request is running, the context is taken out of that worker's cache. If
the request succeeds and the context is still reusable, it is saved back into
that same worker's cache.

For a deployment with one HTTP router module and eight isolate workers, the
normal warm steady state is therefore up to eight cached HTTP action contexts
for that router module, not one context per URL route and not one global context
for all workers. If the application uses multiple HTTP modules or components,
each worker may cache more than one module-path context over time.

## Why the key is the module path

A single shared HTTP action context could be made to work for some applications,
but it would be a wider semantic change. It would allow module-level state from
one HTTP entry module to exist in the same JavaScript realm as another entry
module that did not import it. That may be harmless for simple applications, but
it is not the smallest correctness boundary.

The module-path key is a conservative compromise:

- it matches the existing Convex database-UDF context reuse boundary;
- it avoids sharing module-level globals between unrelated HTTP entry modules;
- it lets the scheduler prefer an isolate worker that already has the requested
  module warmed;
- it keeps invalidation tied to the system-table reads used to initialize that
  module graph.

For many self-hosted applications, all HTTP routes live in one router module. In
that common case this patch effectively gives one reusable HTTP action context
per isolate worker for the whole HTTP router, not one context per URL route.

## Why this can be fast

Without this patch, an HTTP action request may reuse a V8 isolate, but it still
builds a fresh V8 context and module map for the request. For a hot, simple HTTP
path, that setup work can dominate the actual application work. The request may
spend more time creating the JavaScript realm, wiring native Convex functions,
loading module metadata, evaluating the HTTP router module, and resolving
imports than doing the useful request-specific operation.

This is especially visible when the HTTP action is mostly a thin adapter around
Convex function calls. The expensive part is not necessarily the query or
mutation. The expensive part can be paying the JavaScript initialization cost on
every request before reaching the cheap or cached operation behind it.

Reusing the V8 context keeps the evaluated router and imported module graph
warm. The next request can install fresh per-request state into the same realm
and enter the already-evaluated code. This removes a large fixed cost from the
hot path.

In one self-hosted stress test with one hot HTTP action endpoint,
`HTTP_SERVER_MAX_CONCURRENT_REQUESTS=192`, and a `30s` warm run at `200 rps`,
disabling HTTP action context reuse produced `587/6000` successful responses,
`5413` HTTP `503` responses, and a p95 latency of `6651 ms`. Enabling reuse on
the same backend candidate produced `6000/6000` successful responses with p50
`46 ms`, p95 `110 ms`, and p99 `296 ms`. These numbers are not a universal
benchmark, but they show the kind of fixed per-request cost this patch is meant
to remove.

## What the read-set validation means

The `ContextReadSet` uses Convex's normal transaction read tracking machinery.
During HTTP action context initialization, Convex reads system tables for data
such as module metadata, source package metadata, environment variables,
component resources, canonical URLs, and UDF configuration. The patch can snoop
those initialization reads and store enough information to validate them later.

Before a cached HTTP action context is reused, the backend checks that the
relevant system-table ranges still hash the same way. If those reads no longer
validate, the cached context is discarded and the request initializes a fresh
context.

This is not a separate deployment-version token. It is a use of Convex's
existing transactional read-set concept for system metadata. That matters
because deploys, environment variable changes, resource changes, component
changes, and canonical URL changes are represented as writes to Convex system
tables. If those writes affect the ranges read during context initialization,
validation fails and reuse stops for that old context.

The current implementation was reviewed specifically for freshness paths around
deploys, source package changes, environment variables, component resources,
canonical URLs, and component metadata. The important property is that the
cached context is not reused until the saved initialization reads validate
against the current transaction. The core validation helper is
`ContextCache::validate_and_apply_context_read_set` in
`crates/isolate/src/context_cache.rs`, and the HTTP action reuse call site is
`ActionEnvironment::run_http_action` in
`crates/isolate/src/environment/action/mod.rs`. This does not prove that no
subtle invalidation bug can exist, which is one reason the behavior remains
explicit and opt-in, but the main known stale-router failure mode is addressed
by validating the system metadata reads before reuse.

## Why this is not an obvious upstream default

We do not know why this optimization is not already upstream. The most plausible
reason is that it is not only an implementation optimization; it is a contract
change.

Fresh HTTP action contexts imply a simple model: module/global JavaScript state
does not live across HTTP requests. Reusing contexts changes that model. A
hosted product cannot assume every application treats module globals as
stateless implementation detail. It also has to consider worker stickiness,
memory retained by arbitrary module graphs, and support cases where behavior
depends on whether a request hit a warm worker or a fresh one.

For self-hosted deployments, the operator usually owns both the code and the
runtime policy. That makes the trade easier to evaluate directly. If the code
does not store request-specific state in module globals, and if rollback is
available by disabling `REUSE_HTTP_ACTION_CONTEXTS`, the optimization is a
reasonable self-hosted default candidate.

## When this patch is a good fit

This note is written for self-hosted operators. In that setting, the patch is a
good fit for the common Convex HTTP action style: routes validate an HTTP
request, call Convex queries, mutations, or actions, and prepare an HTTP
response. That code shape usually should not keep request-specific mutable state
in module globals anyway, so the semantic change is mostly "the router and its
imports stay warm" rather than "the application now depends on in-memory cross
request state."

The main self-assessment is simple: inspect HTTP action modules for top-level
mutable state that is intended to be per request. If there is none, the patch is
usually a reasonable self-hosted optimization to test. If there is request-owned
state in module globals, move it into the handler scope before enabling this
knob. In particular, do not retain a request, response, action context, stream,
or promise from one handler in a module global. If the application intentionally
uses module globals as an in-process cache, treat that cache as best-effort
only; isolate recreation, memory pressure, deploys, and scheduler choices can
all clear or bypass it.

For the standard self-hosted Docker Compose file, set
`REUSE_HTTP_ACTION_CONTEXTS=true` in the Compose environment or the `.env` file.
The Compose file passes the variable through without supplying a default, so an
unset value preserves the backend's default of `false`.

## Memory behavior

V8 does not expose a simple, precise "bytes owned by this context" value through
this code path. The backend can observe isolate-level heap statistics such as
used heap, total heap, available heap, external memory, native context count,
and detached context count. Per-context attribution is much harder because
contexts share one isolate heap.

The practical memory guard is therefore isolate-level rather than exact
per-context accounting. The backend already checks isolate heap availability
between requests. If the isolate does not have enough available heap and cached
contexts exist, the context cache is marked under memory pressure and contexts
are cleared before constructing a fresh context. If memory is still
insufficient, the isolate is treated as not clean and is recreated.

A matching cached context can still be taken directly while the cache is marked
under memory pressure; this is the existing behavior shared with reusable
database-UDF contexts and avoids allocating another V8 context. Therefore,
memory pressure does not guarantee immediate eviction of a hot HTTP context.
The isolate heap limit, idle timeout, and maximum lifetime remain the backstops
for a repeatedly reused context. Operators should not interpret the cache's
memory-pressure handling as a per-context memory cap.

Operators adopting this patch should treat context reuse as a memory-throughput
tradeoff. The maximum idle warm state is one saved reusable context per isolate
worker, shared with database-UDF reuse. Increasing isolate worker count can
therefore increase the number of retained contexts. Multiple hot modules change
which contexts occupy those worker slots rather than multiplying the per-worker
slot count. Module graph size and top-level caches should still be measured.

The companion
[`context_reuse_observability/README.md`](../context_reuse_observability/README.md) patch exposes
HTTP cache save, take, clear, occupancy, and scheduler-affinity
signals with bounded `context_kind="http_action"` labels. It deliberately does
not add route or module labels.

For a rough estimate, assume one hot HTTP router module, eight isolate workers,
and a single-digit MiB retained context size after warmup. That is usually tens
of MiB of retained heap, not GiBs. Even if the warm context is closer to `10
MiB`, an eight-worker deployment is around `80 MiB` for the main HTTP router
context. On a `4 vCPU / 16 GiB` self-hosted server, that is unlikely to be the
dominant memory term. The risk becomes more meaningful with many separate HTTP
modules, large import graphs, or top-level in-memory caches.

The practical operating check is concrete: after enabling the knob, run a warm
load test for the hot HTTP path and compare p50/p95/p99 latency, HTTP `503`
count, isolate used heap, isolate available heap, native context count, detached
context count, and isolate recreation reasons. Also verify that a deploy or
relevant environment/resource change is visible after the next request, which
exercises the read-set invalidation path.

Application code should still be correct when contexts are not reused. Context
reuse is a performance optimization with observable state-retention semantics,
not a durable in-memory cache API.
## Interaction with isolate queue delay control

HTTP action context reuse can reduce action initialization time and therefore
change isolate service time, queue sojourn, and retained heap per worker. It
does not change whether an HTTP action is an independent action or whether a
nested call is a dependency. The opt-in queue policy is documented in
[`isolate_queue_control/README.md`](../isolate_queue_control/README.md).

Keep the two controls independently reversible. A queue-policy comparison
should hold context reuse constant, and a context-reuse comparison should hold
worker, admission, and queue policy constant. Mixed-load validation should
cover warm and cold HTTP actions, nested queries and mutations, isolate heap
pressure, context invalidation, queue rejections, and dispatch sojourn.
