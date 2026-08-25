# JS Runtime Environment

There are a few ways user code can interact with our system.

1. There's a global `Convex` object that's created very early in
   `initialization::setup_context` and populated soon after when executing
   `setup.js`. We pass this global object as the first argument to all UDFs.
2. The `Convex.syscall` method, installed in `initialization::setup_context` and
   implemented within `syscalls.rs` provides the API for the user to interact
   with the database.
3. Helpers within `setup.js` provide bindings, like `Convex.get` for interacting
   with the database without having to use `Convex.syscall` directly.
4. The user can also import system modules under `convex:/system` that will
   eventually include code derived from our npm package. For example, we'll
   eventually have a custom `Int64` object that will be available for the user
   to create themselves within UDF execution.

# Argument and return value serialization (as of 2021-11-10)

```
                             Arguments                     Return value

                       ┌───────────────────┐           ┌───────────────────┐
                       │ Convex Value (JS) │           │ Convex Value (JS) │
                       └───────────────────┘           └───────────────────┘
                                 │                               ▲
                          convexReplacer                         │
                                 │                         convexReviver
 Browser                         ▼                               │
                   ┌──────────────────────────┐    ┌──────────────────────────┐
                   │ JSON-serializable object │    │ JSON-serializable object │
                   └──────────────────────────┘    └──────────────────────────┘
                                 │                               ▲
                          JSON.serialize                         │
                                 │                          JSON.parse
                                 ▼                               │
                          ┌─────────────┐                 ┌─────────────┐
─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┤   String    ├ ─ ─ ─ ─ ─ ─ ─ ─ ┤   String    ├ ─ ─ ─ ─ ─ ─ ─ ─ ─
                          └─────────────┘                 └─────────────┘
                                 │                               ▲
                        serde::Deserialize                       │
                                 │                       serde::Serialize
                                 ▼                               │
                     ┌──────────────────────┐        ┌──────────────────────┐
 Rust                │ Convex Value (Rust)  │        │ Convex Value (Rust)  │
                     └──────────────────────┘        └──────────────────────┘
                                 │                               ▲
                         serde::Serialize                        │
                                 │                      serde::Deserialize
                                 ▼                               │
                        ┌────────────────┐              ┌────────────────┐
─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┤     String     │─ ─ ─ ─ ─ ─ ─ ┤     String     │─ ─ ─ ─ ─ ─ ─ ─ ─
                        └────────────────┘              └────────────────┘
                                 │                               ▲
                            JSON.parse                           │
                                 │                        JSON.serialize
                                 ▼                               │
                   ┌──────────────────────────┐    ┌──────────────────────────┐
                   │ JSON-serializable object │    │ JSON-serializable object │
                   └──────────────────────────┘    └──────────────────────────┘
                                 │                               ▲
                           convexReviver                         │
 V8                              │                        convexReplacer
                                 ▼                               │
                       ┌───────────────────┐           ┌───────────────────┐
                       │ Convex Value (JS) │           │ Convex Value (JS) │
                       └───────────────────┘           └───────────────────┘
                                 │                               ▲
                                 │                               │
                                 │    ┌─────────────────────┐    │
                                 │    │                     │    │
                                 └───▶│    User UDF code    │────┘
                                      │                     │
                                      └─────────────────────┘
```


# HTTP mutation priority

`POST /api/mutation` accepts `priority: "normal" | "high"`; omission means normal.
The hint applies to that call and its OCC retries. It grants no authorization and does
not change HTTP admission, transaction concurrency, total workers, or CPU capacity.
All callers use the same endpoint authentication and function authorization.

High priority requires `ISOLATE_QUEUE_DELAY_CONTROL_ENABLED=true`. Deployment operation
capabilities expose `mutationPriorityProtocol: 1` when available, or `0` with the legacy
CoDel queue. A high request on the legacy configuration fails with
`MutationPriorityUnavailable`; normal requests retain the existing behavior. The
WebSocket protocol and its connection mutation serialization are unchanged.

Physical isolate selection gives high mutations at most two turns before an eligible
ordinary request. A high request cannot cross an older eligible dependency or deployment
control request. Ineligible high requests do not block other work. High requests use
ordinary queue and worker capacity, with the same expiration and overload rules; they
cannot consume dependency or control-plane reserves.

Initial and resumed JavaScript admission use the same bounded preference within the
protected service group. Deployment control-plane alternation and protected/degradable
service floors are preserved. Class-aware dependency admission still precedes both
application queues. Compatibility admission without service floors or control-plane
support retains its existing collapse of dependency callbacks into ordinary resumptions;
those callbacks receive the bounded ordinary service turn. Resumptions precede initial
starts within each selected class. Cancellation returns queued or granted capacity.

Primary Wasm invocations use this policy in their separate active-CPU limiter. The
execution retains its class across asynchronous suspension and resumption. Opportunistic
shadow admission remains immediate and never queues behind the priority hint.
Detached V8 verification uses ordinary service without inheriting the primary
mutation's hint.

Priority changes admission order. It does not bound network, database, or transaction
commit latency, and it never preempts a running function.

Active-JavaScript class and queue metrics distinguish `high_priority_mutation`.
Protected-group occupancy includes ordinary protected, high mutation, and control-plane
permits, including unclaimed grants. Wasm admission uses its own CPU metrics.
