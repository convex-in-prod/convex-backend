# Selective publication invalidation

Module metadata and runtime activation have separate responsibilities. A module row
identifies its source and source map, execution environment, analyzed functions,
reuse permissions and optional initialization group. Publishing a newer package
preserves that row when these fields are unchanged. For isolate modules, Node
versions, external dependency packages and Node pool topology are unrelated;
Node modules retain those package compatibility checks. Wasm and native resident
generation descriptors belong to the latest source-package activation record and
do not force unchanged JavaScript module rows to be rewritten.

This uses the existing module-content comparison, not a proof that different
JavaScript programs are equivalent. Source-map or analyzed-policy changes can
still invalidate a module even if its successful return values would be equal.

## Runtime authority and query dependencies

Every actual source-keyed Wasm execution selects the exact committed generation
at its transaction's start timestamp. Source authentication checks the entry
module and package identity at the same snapshot before execution. Catalog
selection, cohort selection, retained generation ownership and authentication
remain required even when code was unchanged by publication. Older transactions
continue to select their older generation. The source-selection helper uses a
snapshot clone only when there are no pending module or package writes; otherwise
it reads through the caller's transaction so standalone and pending publications
remain visible.

Source authentication rejects pending module-table writes: the persisted package
digest cannot prove the contents of an independently edited transaction-local
graph, even when its entry module is unchanged. Normal publication executes
routes after commit. Ad-hoc packages have no paired generation and select V8;
pending activation-only records with unchanged module rows can still authenticate.

Activation reads alone do not become query-result dependencies. This relies on
an explicit execution invariant: authenticated artifacts and engines for unchanged
JavaScript implement the same application semantics. A generation descriptor is
execution authority, not application-visible result data. Code, initialization
configuration, observed environment variables and application data still need
complete dependency coverage. An explicit application read of deployment metadata
continues to read the activation record transactionally, so native-resident
metadata changes still invalidate that observation and participate in OCC.

V8 records the imported module graph through ordinary module reads. Generated
Wasm artifacts currently authenticate a complete package without exposing a
validated per-route JavaScript import closure to the query read tracker. Their
invocations therefore record the full module-metadata index interval as a
behavioral dependency, without fetching every row. This includes additions and
deletions and preserves query validity across generation-only publication when
module metadata is unchanged. **Unrelated module changes still invalidate Wasm
query results.** Narrowing this scope requires a complete authenticated import
closure, including negative dependencies; an entry-module hash alone is insufficient.
This conservative coverage is independent of Wasm resident/cohort cache identity.

The query cache refreshes the complete transaction token before returning a hit;
subscriptions observe the same indexed read dependencies. A result-cache hit
executes no function, so it needs no runtime selection or generation authentication.
Context reuse alone never permits returning a cached query result. Application-data
writes continue to invalidate results and subscriptions, while leaving an initialized
context reusable when its initialization inputs did not change.

## Action initialization

Actions preload source metadata and package references at their initialization
snapshot because later imports must still see that snapshot after the transaction
ends. Those preload reads are separate from resident validation. The initialization
transaction records point reads for loaded modules and observed user environment
names, including absent names. A grouped initializer records every loaded member;
the entry module, component and execution kind retain their existing contracts.
Component environment bindings, canonical URLs, component arguments and import-time
RNG/time configuration remain conservatively tracked. Task resources include the
full function inventory; they are only resolved during execution and are reloaded
for each invocation without becoming initialization dependencies.

Before entering an action handler, the backend captures initialization hashes and
drops the transaction. A successful HTTP response without a matching route also
finishes initialization. Warm invocations validate and apply the saved read set
before using their module map; they keep that saved set when retained again.
Module loading during execution revokes retention for both cold and warm actions:
new module state may depend on execution-time RNG, time or request state and must
not be saved with the previous initialization dependencies. Imports of already
initialized modules do not expand the graph. Fresh request environment values,
identity, callbacks and task state remain invocation-owned.

Pending module or environment writes use the original transaction where a snapshot
clone would lose them. These exceptional paths can retain broader dependencies;
initialization hashes that cannot be captured safely prevent retention.

## Focused regression coverage

The isolate context-reuse tests exercise unchanged-source republication with new
Wasm/native generation metadata, unrelated modules and unused environment names,
imported-code changes, actual data-change invalidation, live subscription tokens,
observed environment absence/presence, action configuration changes, cold/warm
lazy-import revocation and an action deployment-snapshot race. Existing grouped
tests cover uncalled members, missing group entries, reuse opt-out, execution-kind
and component separation, cancellation and response-delivery boundaries.

The generated-source test exercises unchanged module publication, activation
selection before and after a commit, pending package/module writes, explicit
deployment-metadata invalidation and conservative dependency additions. These are
behavior tests requiring the native isolate test build; documentation is not a
claim that they have been executed or that runtime performance is accepted.

## Execution-kind initializer policy

Analyzed module equality includes the complete initializer selection, including the distinction
between legacy shared roots and version 2 database/action roots. A policy-only change therefore
replaces module metadata and invalidates residents that loaded that module, even if its JavaScript
bytes and package compatibility are unchanged. The stable JavaScript package identity and
transaction-authenticated runtime selection are unaffected.

Database initialization can include eligible entries both with and without action permission.
Action initialization loads only its selected action group. Its existing import point reads do
not include the additional database-only wrappers, so changing one of those wrappers invalidates
the database resident while preserving an action resident whose loaded graph is unchanged.
A dependency loaded by both groups, or metadata of an entry in both groups, invalidates both.
