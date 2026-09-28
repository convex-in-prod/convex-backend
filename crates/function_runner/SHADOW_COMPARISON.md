# Mutation shadow identity comparison

Mutation comparison relates newly inserted IDs consistently across writes,
results and read dependencies. Existing document identities and invocation inputs
remain fixed. It does not compare concurrent allocation order as application
semantics.
Existing read comparison excludes singleton allocation checks for fresh IDs;
reads of existing documents and index ranges still agree.

`FUNCTION_RUNNER_MUTATION_SHADOW_IDENTITY_CONTRACTS` optionally supplies a JSON
object keyed by root function paths. Selectors are canonicalized, including `.js`
aliases; aliases and repeated literal JSON keys cannot declare the same route
twice. Duplicate keys are rejected before JSON map construction can discard an
earlier declaration. The configuration
is read once per process. Absence preserves ordinary comparison. Invalid
configuration prevents fallback and produces a fixed diagnostic; it cannot
change the primary result. Contracts are operator-owned assertions about field
semantics and require maintenance when those semantics change. They grant no
normalization authority over another component's tables or scheduled jobs.

Example:

```json
{
  "members:activate": {
    "serializedFields": [
      {"table": "projections", "field": "digest", "payloadFields": ["owner", "jobs"]}
    ],
    "opaqueScheduledTokens": [
      {"function": "jobs:run", "field": "executionToken"}
    ]
  }
}
```

The fallback operates only after ordinary result/read/write comparison fails:

- A serialized field is parsed and checked against its declared payload fields
  in the same document. Missing optional payload fields are omitted. Numeric
  payload values follow JavaScript's binary64 representation. The parsed graph
  participates in ID mapping; a separate encoding representation retains every
  byte except complete unescaped string tokens for newly inserted IDs or declared
  opaque tokens, represented as typed references. Every such occurrence remains
  constrained by the same identity relation, even in duplicate JSON fields.
  Formatting, field order, ordinary text and inconsistent references still differ.
  A derivative cannot select another declared derivative as a payload field;
  validation never depends on declaration order.
  Reserved JSON object keys beginning with `$`, including escaped keys and
  occurrences hidden by duplicate fields, leave the proof unresolved. This
  preserves byte scalars and prevents raw text from supplying internal markers.
  Escaped fresh-identity values and fresh identities used as JSON keys also
  leave the proof unresolved, including occurrences hidden by duplicate fields.
  Literal encoding equality does not establish the identity's logical owner.
- An opaque scheduled token is accepted only in a newly inserted root-component
  scheduled job for the declared function. Its value occurs exactly once in the
  lane's host-generated UUID evidence and belongs to one job. The complete UUID
  populations, including multiplicities, agree across lanes. Each occurrence is
  represented by a typed reference to that job, using the same ID relation as the
  rest of the transaction. One token field per scheduled function is supported.
  Unknown, reused, inherited and input tokens do not receive fresh identity
  authority. UUIDs used as business values are not opaque identities and are not
  eligible for a declaration.
  The job and its newly inserted argument document belong to the root component;
  a root target function does not authorize a child-owned job.
- Logs, audit logs, error messages and undeclared string encodings retain their
  ordinary comparison. No arbitrary substring replacement is performed.

Identity substitution in object keys is unsupported and leaves the proof
unresolved. Keeping those keys literal during remapping could accept equal
spellings attached to different logical owners. Ordinary comparison with an
unchanged ID relation still accepts equal keys. Object values continue to use
typed identity references; the comparator does not introduce a second key relation.

UUID evidence is process-local, omitted from Debug and operator diagnostics, and
retained only for sampled comparisons. Normal executions do not collect it.
Nested invocation evidence is merged with the same 1,024-draw cap. Overflow drops
provenance rather than failing execution or accepting incomplete evidence.

Each fallback lane bounds written records to 256, structural work to 512 KiB and
nesting to 64. Search retains its 4,096-expansion bound and separately limits
document comparison work and result/read comparison work to separate 64-MiB
allowances. Candidate shapes are computed once.
The ordinary identity and zero-insert fast paths also admit document work before
copying it. Complete write comparisons are reused by result/read predicates.
Exhaustion never produces a successful comparison.

Detailed comparisons expose `outcome` (`matched`, `identity_equivalent`,
`unresolved`, `mismatch`) and a data-free `identityDiagnostic`. The latter keeps
the initial mapping failure and the fallback result/reason. Per-route and aggregate
summaries expose `comparisonOutcomeCounts`. Process counters use
`dimension="comparison_outcome"`. Existing aggregate mismatch counts continue to
mean nonmatching comparisons, including unresolved ones. An independent log or
other difference remains a mismatch even when result/read/write normalization
succeeds.
An exhausted proof or invalid derivative reports unresolved even without inserts.
After valid normalization, an exhaustive failure to find a candidate or relation
proves a difference. An independent error message or success/error difference
remains a mismatch even when identity normalization is unresolved. A difference
in whether an error carries custom data also remains a mismatch.
External read differences are proved using the complete allocation ID sets and
also remain mismatches when the identity proof cannot complete.
