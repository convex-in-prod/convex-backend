# WebSocket transport timing metrics

## Purpose and adoption

Separate sync WebSocket queue delay, message serialization, socket flush, and
ping/pong round-trip observations so a self-hosted operator can identify which
transport boundary is slow. This is an independent backend patch and does not
require the runtime-health dashboard, bounded service logging, or Wasm execution.

Apply the patch and build the backend normally. Metrics become active when the
patched backend starts; no schema or data migration, Caddy change, worker change,
or configuration change is required. Existing metrics collection can continue
unchanged. Add queries for the new histograms when their detail is useful.

## Compatibility and liveness

WebSocket control-frame pings carry an eight-byte, connection-local sequence ID.
The payload is only a diagnostic correlation ID, not authentication, application
state, or an acknowledgement contract. Older clients remain wire-compatible:
standard WebSocket implementations echo ping payloads without changes to the
application sync protocol. No client upgrade or protocol negotiation is required.

Every received frame still updates the existing liveness timestamp. Empty,
malformed, unknown, expired, and duplicate pong payloads count in
`backend_pong_total` and retain their ordinary liveness effect, but produce no
round-trip sample. A matched pong consumes its pending entry, so a duplicate
cannot count twice. Delayed and out-of-order replies use their own ping's start
time, even when newer replies have already arrived.

Each connection retains at most 32 pending pings and discards the oldest entry
when full. The five-second heartbeat and 120-second client timeout remain
unchanged. Clients that send other traffic without echoing pings cannot grow
diagnostic state indefinitely. Missing correlation affects only RTT observations;
it does not create a new disconnect condition.

## Metric boundaries

All durations are seconds. Endpoint labels retain the bounded server-message
variants; payloads, connection IDs, and application identities are not labels.

| Metric | Boundary and interpretation |
| --- | --- |
| `backend_ws_send_delay_seconds{endpoint}` | Elapsed time from the sync worker's message timestamp to the start of serialization for each emitted message. This retains the existing observation point. For chunked transitions it includes the initial transition serialization and chunk preparation; later chunks also include earlier chunk sends. It is not a pure channel-wait measurement. |
| `backend_ws_serialize_seconds{endpoint,status}` | Time converting a server message to JSON and encoding it. Includes the initial serialization of an oversized `Transition` before chunking, then each `TransitionChunk` envelope separately. Status is `success` when encoding finishes and `error` otherwise. |
| `backend_ws_socket_send_seconds{endpoint,status}` | Time awaiting the WebSocket sink's send and flush for an encoded text message. Status distinguishes `success`, `error`, and `canceled`. Completion does not mean browser receipt, application processing, or browser acknowledgement. Control-frame ping sends are outside this histogram. |
| `backend_ping_pong_seconds` | Elapsed time from recording a ping immediately before its socket send to receiving the matching pong. It includes local send delay, transport time, and peer handling; it is not a network-only latency measurement. Unmatched replies do not contribute. |

Queue-delay observations, serialization samples, and socket-send samples have
distinct boundaries, but chunking makes their counts and intervals differ. Do
not sum histogram quantiles or assume the three distributions partition a
single end-to-end latency. `backend_ws_out_total` retains its existing count at
the start of each emitted message's serialization; it is not a delivery count.

## Rollback

Restore the previous backend image. No client, configuration, schema, or stored
data rollback is needed. The new serialization and socket-send histograms stop
receiving samples, and the previous ping/pong metric again measures elapsed
time since the most recent ping without matching its reply. Queries consuming
the new metrics should tolerate their absence during rollback.

## Verification

Run the focused tracker tests through the repository Cargo wrapper:

```sh
scripts/run_cargo.sh test -p local_backend --lib subs::ping::tests
```

These tests cover delayed and out-of-order replies, malformed and unknown
payloads, duplicate replies, and bounded pending state. They test correlation
only; they do not exercise a live socket or prove client liveness behavior.

Build the backend and verify that an existing sync client connects without an
upgrade. During a bounded socket check, compare matched-pong samples with pong
counts and verify that unmatched pongs preserve the existing timeout behavior.
Exercise ordinary and chunked messages, then inspect endpoint and status labels
for serialization and socket-send histograms. A slow reader or canceled send
can exercise socket backpressure and cancellation; a successful flush alone
must never be treated as browser acknowledgement.
