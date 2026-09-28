# Typed values and packed document delivery

Status: implemented typed host operations and value transfer, with remaining
legacy SDK adapters. The backend supports native scalar, get, string, write,
nested-UDF, scheduling, collection, pagination, and stream-open requests. Values
use `CVA1`; compound queries use `CQR1`. Packed query results remain packed
through provider delivery, and binary final results decode directly into
host-accounted pending values. Matching SDK/compiler sources and linked Wasm
artifacts exist. Ordinary SDK execution, semantic comparison, and representative
performance measurements remain necessary before treating the complete path as
validated. Synchronous ID normalization, audit-log request encoding, count
lowering, and stream identifier/cleanup adapters still need conversion. A linked
artifact or supported import alone does not establish that an SDK operation uses
the typed path.

The design belongs to the [Static Hermes Wasm UDF patch](README.md), which owns
the runtime integration. Its query and provider changes are shared with V8;
the binary transfer into Wasm memory is specific to the Wasm adapter.

## Decision

Use one typed operation/value contract with:

- direct Core Wasm scalar imports for small operations;
- fixed little-endian records for compound operations;
- a small tagged encoding for newly constructed and pending Convex values; and
- a host-result variant carrying an existing FlexBuffers document unchanged.

Native Hermes code traverses guest values and constructs received JS values.
Rust decodes requests directly into typed operation arguments. Shared providers
return typed or packed values before any runtime-specific serialization. Keep
JSON adapters at the consumers that require them, rather than converting the
new binary path back into JSON inside the provider.

The initial direct scalar call `convex_capability_start_scalar` takes the current
invocation capability and one operation code. Codes 1 through 6 respectively
mean `authGetUserIdentity`, `getFunctionMetadata`, `getDeploymentMetadata`,
`getTransactionMetrics`, `getRequestMetadata`, and
`storageGenerateUploadUrl`. The host applies the same authorization,
operation-mode, accounting, and async completion rules as the request-handle
path. Invalid codes fail before an operation is queued. These codes are a
fixed part of the new compatibility identity.
`convex_capability_start_get` takes an ID, an optional table, and a system-table
flag as direct typed fields. `convex_capability_start_string` takes one UTF-8
argument and a distinct operation code: codes 1 through 4 mean storage URL
lookup, storage metadata lookup, storage deletion, and scheduled-job cancellation;
codes 5 through 7 create a function handle from a name, reference, or existing
handle. Empty function addresses are invalid.
`convex_capability_start_write` takes a write kind, optional table and ID, and a
bounded typed value frame where the kind requires one. These direct starts enter
the existing authorized operation queue. `convex_capability_start_run_udf`
takes a nested UDF kind, one of name/reference/function-handle address kinds,
the UTF-8 address, a `CVA1` argument frame, and an optional `CVA1` transaction
limits frame. The host decodes both frames into typed values and calls the shared
nested-UDF provider without constructing an intermediate JSON request. The
transaction limits frame contains nonnegative safe-integer fields from the
existing supported limit set; omitted fields retain the provider defaults.
`convex_capability_start_schedule` takes the mutation-scheduler kind (delay or
absolute time), a millisecond time value, the same address kind and UTF-8 address,
and one bounded `CVA1` frame containing a committed argument object. The host
uses the existing time conversion, address resolution, argument validation,
transaction scheduling, and async completion path. Pending commit timestamps
are rejected before submission. The scheduled ID returns as a typed value.
Action scheduling and legacy/V8 callers retain their existing adapters.
Other compound operations require typed
records rather than a JSON request envelope. The exact direct-import signatures
and codes belong to the new compatibility identity; they are not extensions of
an application schema.
An SDK context captures its invocation capability. Native scalar, get, string,
write, nested-UDF, and scheduling callbacks exposed through that context must reject a call after its
captured identity differs from
the current invocation, before forwarding to a global native callback. Global
SDK entry points read the current capability directly. This distinction prevents
a retained context from gaining a later invocation's authority.
The `convex_performance_now` import takes the current invocation capability and
returns a nonnegative millisecond value directly. It returns `-1` for a stale
capability; the guest maps that reserved value to the existing stale-capability
error. The host still charges the operation and observes time through the shared
provider. It uses no request or result handle.

The operation/value protocol is independent of application schemas. New tables,
document fields, nested object shapes, and function argument/result shapes do
not require a backend rebuild or redeploy. New runtime operations or value kinds
remain explicit compatibility changes.

## Recursive value bytes

The initial shared codec is in [`value::wasm_abi`](../../crates/value/src/wasm_abi.rs).
A value frame begins with ASCII `CVA1`, followed by one recursively encoded
value or host-result container. Multi-byte lengths and numbers are little-endian.
Lengths and container counts are unsigned 32-bit integers; strings and object
names are length-prefixed UTF-8 without a terminator. Floating-point payloads
retain their IEEE 754 bits, including negative zero and NaN. The tags are:

| Tag | Payload |
| ---: | --- |
| 0 | Null; no payload. |
| 1, 2 | Boolean false or true; no payload. |
| 3 | Binary64 bits, eight bytes. |
| 4 | Signed Int64, eight bytes. |
| 5, 6 | String or bytes: length, then bytes. |
| 7 | Array: count, then that many values. |
| 8 | Object: count, then each field name and value. |
| 9 | Unresolved commit timestamp; allowed only where pending values are legal. |
| 10 | Existing packed document: byte length and FlexBuffers bytes; host result only. |
| 11 | Query document collection: count, then individual packed or pending document entries; host result only. This outer container does not count toward a document's Convex value nesting. |
| 12 | Delete this field; allowed only as a direct value of the top-level patch object. Distinct from null and never a stored value. |

The receiver rejects duplicate/invalid field names, invalid UTF-8, oversized
frames or Convex values, excessive nesting, unknown tags, and trailing bytes.
Object field order is not an authority rule: the host stores fields in its
validated Convex object representation. The guest encoder must still preserve
the SDK's observable property-read and getter order while constructing the
frame. Delete, omitted, and filter-undefined states belong to their operation
records; they are not stored Convex value tags.

The SDK snapshots query literals and range/search values when a query is built.
The typed path must preserve that time of observation: capture each present
value into an owned `CVA1` buffer at construction, with undefined represented
separately. Retaining a caller's mutable object until query execution would
change query semantics. A later query request record can include the captured
bytes without traversing or converting that value again. V8 retains its
existing tagged-JSON builder representation.

The source-connected typed query record begins with ASCII `CQR1`. It uses little-endian
unsigned 32-bit byte lengths/counts and unsigned 64-bit limits. Its fixed prefix
is terminal (collect 1, first 2, unique 3, stream 4, paginate 5), source (full
scan 1, index range 2, search 3), order (default 0, asc 1, desc 2), and a
length-prefixed UTF-8 table. An index range adds an index name and constraints;
each constraint has an operator (eq 1, gt 2, gte 3, lt 4, lte 5), field path,
and an optional captured `CVA1` frame. Search adds an index name and filters;
the first filter is a search string and subsequent filters are field equalities
with optional captured frames. Query operators are filters or numeric limits.
Filter expressions use fixed tags for literals, fields, binary arithmetic and
comparison, unary operations, and logical lists. A literal and each equality
operand has a distinct presence byte so undefined cannot become null. The
paginate terminal appends optional cursors, optional read limits, and a page size.
The reader rejects unknown tags, excessive counts/nesting, invalid UTF-8, bad
`CVA1` frames, trailing bytes, and search ordering. The record has no
application-specific schema or borrowed guest-memory lifetime. Its exact tag
table is kept with the decoder and native encoder until the new producer/consumer
identity is bound; `CQR1` is not yet a supported deployed import. The current
opaque ABI identity is version 3. The source now imports
`convex_typed_value_abi_v1` when preparing the new native runtime, and the host
requires that versioned import for binary results and typed write, nested-UDF,
scheduling, and query starts. A module importing the versioned ABI must use the
binary final-result transfer; older modules retain the JSON result import.
`CVA1` and `CQR1` remain their exact frame
versions. This leaves historical version 3 artifacts interpretable under their
original contract while rejecting a producer that needs the new import on an
older host. Linked producer/consumer execution remains to be verified.

## Problem in the current path

The published query pipeline obtains packed documents, constructs owned Rust
documents, converts them into JSON values/text, and delivers those responses to
the runtime. SDK conversion then restores Convex-specific values. The Wasm
boundary also constructs and validates request envelopes and encodes their text.

Changing only the final byte format would retain much of that work. The decision
changes the representation through the shared provider and the native runtime
adapter, preserving database semantics while removing intermediate trees and
unnecessary serialization.

The local worktree now has these intermediate interfaces:

- [`QueryStreamNext`](../../crates/database/src/query/mod.rs) carries either a
  packed or materialized document. Ordinary index and table rows stay packed.
- [`IndexRange::start_next`](../../crates/database/src/query/index_range.rs)
  records a read on the packed document and keeps ordinary rows packed.
- [`query_batch`](../../crates/isolate/src/environment/udf/async_syscall.rs)
  implements SDK gets and query-stream reads using a packed or typed provider
  result. Pagination retains the same representation through the provider.
- [`AsyncSyscallResult`](../../crates/isolate/src/environment/udf/async_syscall.rs)
  carries the provider result. The [V8 execution loop](../../crates/isolate/src/environment/udf/mod.rs)
  still serializes it to the SDK's JSON string contract, directly from a packed
  document walker where available. Insert, patch, and replace results locally
  remain typed through this carrier. The binary Wasm result encoder consumes
  owned values rather than cloning them before encoding. The local guest-promise
  Wasm completion path retains packed or typed results without an intermediate
  JSON tree; its native reader constructs guest objects from the result frame.
  Linked guest execution has not been verified.

## Shared document representation

Carry packed or materialized documents through query execution and typed
provider results. Use one shared implementation of query semantics. Existing
callers that require owned documents can materialize at their adapter; they
must not force all runtime consumers through that conversion.

[`PackedDocument`](../../crates/common/src/document.rs) already retains the
document's logical size, resolved identity, and a
[`PackedValue`](../../crates/packed_value/src/lib.rs). Its value buffer contains
the developer-facing body, including `_id` and `_creationTime`; the resolved
tablet identity is separate metadata. For ordinary concrete documents,
`to_developer()` does not rewrite the body.

The backing [`ByteBuffer`](../../crates/packed_value/src/buffer.rs) owns
reference-counted bytes. An async result can retain that ownership without
borrowing transaction memory or copying the complete document. Continue to
account for retained backing storage, including any larger allocation retained
by a slice.

| Path | Representation and required behavior |
| --- | --- |
| Ordinary gets and index/table queries | Keep the existing packed document through delivery. Preserve authorization, namespace resolution, read sets, accounting, cursors, and ordering. |
| Query limits, collection, and pagination | Carry packed rows through the existing completion/container logic. Return individual length-delimited document payloads; do not repack them into a new FlexBuffers array. |
| Database filters | Evaluate the existing expression language using a shared field-lookup abstraction. Read referenced fields/subtrees from the packed document while retaining its original bytes. |
| Search results | Expose packed results from the shared document lookup core, preserving candidate revision checks and read accounting. |
| Virtual system tables | Preserve the physical-to-virtual transformation and return its resulting typed value. The original physical bytes are not the response. |
| Unresolved staged writes | Return the true pending value at delivery. Do not expose the concrete index/query view's placeholder substitution as an ordinary integer. |

[`Expression::eval`](../../crates/common/src/query.rs) reads the source document
through field expressions; its other operators work on the resulting values.
[`PackedValue::get_path`](../../crates/packed_value/src/lib.rs) already opens a
path without unpacking the whole document. Generalize the source field access
while retaining one implementation of arithmetic, comparisons, missing fields,
and short-circuit behavior. An object-valued operand may still require decoding
that subtree; it does not require replacing or repacking the source document.

Read accounting already accepts packed documents through
[`UserFacingModel::record_read_document`](../../crates/database/src/bootstrap_model/user_facing.rs).
Keep charging logical document size where that is the current database contract;
encoded FlexBuffers byte size is a different quantity.

Pending-write semantics require particular care. `PendingDocument` uses a
concrete pre-commit view with unresolved commit timestamps substituted for
query/index processing. `developer_document_to_json` currently restores the
pending representation for JS delivery. Move this representation selection into
the typed result boundary. Preserve both the existing query behavior and the
unresolved value seen by application code.

## Runtime adapters

### Wasm and Hermes

For an unchanged packed document, the intended path is:

```text
existing FlexBuffers document -> guest memory -> native Hermes object construction
```

The result frame carries a bounded, appropriately aligned slice for each packed
document. The native decoder traverses it and constructs ordinary Hermes values
directly. It does not build a tagged JSON tree or call SDK JSON restoration.
A collection can contain packed documents and typed pending/transformed values.

Guest-origin arguments, writes, and final results use the generic tagged encoding.
The packed-document variant is a host-result optimization; it need not introduce
arbitrary guest-supplied FlexBuffers as another host input surface. Recognizing
unchanged JS objects through mutation tracking is outside this design.

Copying bytes into guest memory and allocating final JS objects remain real costs.
This removes host unpacking and re-encoding; it does not make the Hermes heap and
Rust heap interchangeable or provide transparent lazy JS documents.

### V8

The shared query/filter changes apply to V8 as well. Its existing JSON adapter
can serialize directly from packed values, without constructing an intermediate
`ConvexObject` or `serde_json::Value` tree. The existing
[`ConvexValueWalker`](../../crates/value/src/walk.rs), packed walker, and
[JSON serializer](../../crates/value/src/json/mod.rs) provide the relevant
representation-independent traversal.

V8 can also construct native values directly from the packed reader using its
embedding API. That would remove JSON generation/parsing and SDK restoration,
and would not require copying the packed document into Wasm linear memory.
It requires a typed V8 syscall/SDK result interface; returning a native object
where the current SDK expects a JSON string is not compatible.

The initial shared refactor must keep the existing V8 contract working and let
its JSON adapter benefit from packed traversal. Direct V8 materialization is a
separate adapter extension, not a prerequisite for the Wasm ABI. Its performance
must be assessed rather than assumed: V8's JSON parser is optimized, and repeated
embedding-API calls have costs too.

## Value and ownership contract

The generic encoding represents null, booleans, binary64 numbers, signed int64
BigInt, strings, bytes, arrays, and string-keyed objects. Preserve negative zero,
infinities, and the established NaN behavior. Pending timestamps, patch deletion,
filter undefined, omitted fields, and null have distinct positional semantics.
Encode integers and bytes directly, without base64 or numeric text wrappers.

Specify fixed widths, little-endian byte order, value tags, record fields,
container lengths, and alignment explicitly. Never transmit native Rust or
Hermes object layouts. Matching endianness does not make unaligned typed pointer
casts valid; use safe byte access or guaranteed alignment. Little-endian matches
Wasm memory operations and common host targets; direct scalar imports have no
serialized byte order.

Guest-native traversal must preserve SDK-observable behavior. In particular,
snapshotting immediate object entries before recursive conversion can affect
getter execution, mutations, and exception order. Remove JavaScript descriptor
objects and per-character UTF loops without dropping necessary native snapshots
or changing property semantics. Preserve accepted prototypes, field-name rules,
enumerability, sparse arrays, cycles, unsupported types, and Unicode behavior.
Use rooted handles across Hermes allocation/GC and separate native ASCII/UTF-16
access where appropriate.

Fuse structural validation and typed construction. The host still validates
untrusted lengths, tags, duplicate keys, value bounds, capabilities, and operation
legality before submission. Native guest encoding does not grant host authority.
Special-number canonicalization required by value semantics remains separate
from any canonical byte encoding used for artifact identities.

Decode or take ownership of requests before guest scratch can be reused. Do not
retain borrowed guest-memory pointers across guest execution, memory growth,
async suspension, or cancellation. Results and stream/completion handles retain
one consuming lifetime and are released on success, error, cancellation, and
instance retirement. Preserve existing batching, microtask checkpoints, cleanup,
transaction ordering, and accounting. Avoid per-field imports and separate
size/copy/release calls where one bounded owned delivery suffices.

## Format choice and alternatives

The selected design uses a small native tagged codec for fresh values and reuses
FlexBuffers where the bytes already exist. These are explicit variants of one
ABI, selected by the available value rather than an operator configuration.

| Alternative | Reason for the decision |
| --- | --- |
| FlexBuffers for every value | Existing packed documents avoid encoding entirely. Fresh requests would still pay builder sorting, width/offset selection, and native conversion; pending values also need an extension contract. Reuse existing buffers without requiring every value to be built as one. |
| Cap'n Proto or FlatBuffers | Direct encoded access is useful, but generic recursive documents still need tags/names and the receiver must construct ordinary JS objects. Pointer/layout machinery does not remove that work. |
| SBE | Suitable for generating fixed operation records and potentially equivalent to handwritten scalar access. It does not supply the recursive Convex value model. Standard int64/null and optional-float/NaN conventions need adaptation, and generated Rust indexing is not itself fallible malformed-input handling. A schema generator remains an implementation option, not the value format. |
| MessagePack or CBOR | Credible sequential encodings with existing libraries; they require a Convex-specific type profile and native adapters. Their big-endian multibyte numbers add conversion on little-endian targets. Byte order alone is not evidence that they are slower overall. |
| Application-specific generated host schemas | Would couple application evolution to backend builds unless schemas are loaded dynamically. A permanent generic value contract already preserves independent deployment without that additional runtime machinery. |

The expected gain follows from removing work across the complete path. There is
no measured serializer ranking or guaranteed end-to-end improvement attached to
this decision. A native implementation of any format that retains the old JSON
provider round trip does not satisfy the design.

## Compatibility and implementation boundary

The current opaque value ABI and JSON request/result contracts remain supported.
The source-connected typed ABI adds a versioned host import and `CVA1`/`CQR1`
frame headers without changing the meaning of opaque ABI version 3. The compiler
and host must agree on the exact import signature and byte layouts, and artifacts
must authenticate the emitted import list. The host rejects missing versioned
imports for typed operations before execution; a new layout requires a new
identity rather than silently reusing these symbols. Artifact/registry JSON
canonicalization is not replaced by this runtime value protocol.

Compiler and backend releases are independently deployable when these contracts
remain supported. The backend's `capabilities` command reports
`convex-local-backend-capabilities-v3`, with a `wasmRuntime` record listing
supported deployment and module-graph formats, opaque-value and capability-request
ABI versions, and host import names with parameter/result types. These lists come
from the same definitions used by artifact admission. A feature-disabled binary
reports `staticHermesWasmtimeGate: false` and `wasmRuntime: null`.

Admission validates producer inventory and runtime-policy hashes as provenance
within the authenticated artifact closure; it does not compare them with an
exact compiler-build identity embedded in the server. Hashes still protect
artifact bytes, source pairing, and internal contract consistency. A compiler
rebuild or compatible optimization therefore needs no backend rebuild, restart,
or operator allowlist update. Startup configuration can restrict capabilities
already implemented by the binary; it cannot add an unsupported ABI.

Preflight compares required formats and ABI semantics with supported versions
or explicit capabilities. Use a version range only if every version in it is
supported. Check required import signatures without requiring unused optional
producer capabilities. The loader validates the actual linked imports and the
versioned marker's operation/wire semantics. Wasmtime native AOT compatibility
remains separate: a compatible Core Wasm module may need local AOT reconstruction
for the host engine and target. None of these checks depends on application table
or argument schemas.

Ordinary V8-only application deployment remains supported. Application schemas
are data to the generic runtime contract; no per-application Rust code generation,
backend restart, or dynamic transport-schema registry is required.

Implement the shared query/provider representation, native guest codec, SDK
entry/exit integration, and compatibility wiring as a coherent change. Extend
existing semantic coverage for the new boundary, including packed/materialized
equivalence, filters, pending writes, malformed frames, and cancellation/reuse
ownership. Assess the completed path using representative application execution,
runtime profiles, and memory/failure behavior. The design does not require a
separate deployment or benchmark program for every codec or sub-optimization.
