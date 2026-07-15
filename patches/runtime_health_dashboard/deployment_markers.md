# Deployment marker pagination

Health charts need deployment timestamps, but the full audit-history query also
returns schema and component diffs. Large deployment records can force reactive
pagination to split a page. Applying a retention cutoff derived from the current
time to the index bounds changes the query fingerprint between requests, so the
split cursor is rejected and the client restarts pagination. Repeated restarts
can repeatedly transfer large audit records from an already-open dashboard.

The paginated audit-history query keeps the caller's index bounds unchanged and
applies the current retention cutoff to returned rows. Expired records stay
hidden. Descending pagination ends when a page reaches expired history, and its
split hints are cleared because no later records are eligible. Action and author
filters and the existing row and byte limits remain in effect.

Single-action filters use the existing action/time index, which is now also
declared in the TypeScript system-table schema. New member/time and
action/member/time indexes serve single-author filters, including a single
action combined with a single author. Multiple-value filters use the remaining
index prefix where possible and filter within that range. Index availability
is read through the metadata table's `by_id` index; `_index` has no creation-time
index for a default table scan.

Health charts use a separate permission-checked paginated query that returns
only creation-time numbers for deployment events. Each deployment action uses
its own indexed paginated stream; the dashboard merges their timestamps in
chronological order. Detailed audit-history views continue to receive full
records. The compact response reduces network transfer and browser memory;
database reads still include full audit records and remain subject to the
existing pagination limits.

An initial page that reaches a pagination read limit recommends a split and
retains its continuation cursor. Requiring a split can leave reactive clients
waiting indefinitely when one or two large records leave no usable split cursor.
Clients can accept that short initial page and continue reading. A page with an
existing end cursor still requires a split if its fixed range cannot be read
completely, so continuation never skips rows inside an established range.

## Adoption

Build the backend with the updated system UDFs, then publish the updated
dashboard. The new dashboard requires the compact marker query, so publish the
backend before or together with the dashboard. New system indexes backfill and
enable automatically on backend upgrade. Filtered queries use their best
available completed index while backfills run. Switching to a more specific
completed index can reset an active pagination session; time advancing does not
keep invalidating its cursors. This also covers upgrades from versions without
the existing action index. Confirm that all three indexes are enabled after
rollout. No configuration change, document-field change, or manual data
migration is required. Rebuilding only application functions does not publish
these system UDFs.

The backend change preserves the existing full audit-history API and can serve
an older dashboard. To roll back both halves, restore the dashboard before the
backend so clients do not request a missing query.

## Verification

Use deployment history containing a record large enough to split a page and a
time window older than the audit retention cutoff. Split and continuation
requests should complete without cursor errors, and no expired rows should be
returned. Health charts should receive timestamp-only pages and show deployment
markers without transferring schema or component diffs. Check the detailed
history view separately to confirm that its event details remain available.
Single-action, single-author, and combined single-action/author filters should
use their respective indexes after backfill completes. Multiple-value filters
should retain their existing matching behavior.
