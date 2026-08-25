# Degradable Active-JavaScript Admission

This design lets the degradable query leader cap exceed the number of isolates
actively executing JavaScript without admitting degradable query trees that can
start and then lose all execution progress. The implementation extends the
existing active-JavaScript limiter with protected and degradable service groups
and the backend-derived dependency class. The enabled deployment lane gives
configuration work a separate class inside the protected group. It does not add arbitrary weights,
per-function policy, preemption, or another worker pool.

## Resource model

The leader gate and the active-JavaScript gate control different resources:

- `APPLICATION_MAX_CONCURRENT_DEGRADABLE_QUERY_LEADERS` bounds the lifetime of
  admitted degradable cache-miss leaders together with paced isolate analysis.
  A leader retains this permit across database and other asynchronous waits.
- `FUNRUN_ISOLATE_ACTIVE_THREADS` bounds isolates that are executing JavaScript
  at one instant. An execution releases this permit around supported
  asynchronous waits and reacquires it before running JavaScript again.

Requiring the leader gate to be smaller than the active gate couples lifetime
admission to instantaneous CPU occupancy. Class-aware active admission removes
that coupling while retaining a finite CPU gate.

## Classification and propagation

The active gate has five classes:

- `Dependency` is work whose completion releases an isolate-holding ancestor.
- `Protected` is ordinary root work, actions, and every other request without a
  narrower classification.
- `HighPriorityMutation` is a mutation carrying the per-call high hint. It shares
  protected capacity with bounded preference over ordinary protected work.
- `ControlPlane` is typed backend configuration evaluation. With its lane enabled,
  it alternates grants with combined ordinary/high-mutation demand inside the protected floor
  and elastic share; otherwise it uses ordinary protected admission.
- `Degradable` is an admitted degradable root query cache-miss leader.

A client declaration alone never creates `Degradable` active work. The query
cache assigns `Degradable` only to a `CacheOp::Go` that still owns the acquired
degradable leader permit after the cache recheck. Cache hits, followers, normal
bypasses, and rejected admission do not carry the class into execution.

Backend-derived dependency ownership overrides every application class. This
keeps separately scheduled descendants on the ancestor-unblocking path even
when the root leader is degradable. The resulting class is stored in the
isolate request and in every suspended active permit. Reacquisition therefore
retains the execution's class instead of turning a degradable resumption into a
new protected start.

## Grant policy

Let:

- `A` be total active-JavaScript capacity;
- `P` be the protected minimum;
- `G` be the degradable minimum;
- `E = A - P - G` be elastic capacity.

The limiter applies these rules whenever a permit becomes available:

For these comparisons, protected demand and occupancy include `HighPriorityMutation`
and `ControlPlane`. After choosing that group, the limiter alternates application
and control-plane grants when both wait. Within application demand, high mutations
receive at most two grants before a waiting ordinary protected request. Resume
priority applies within each class. See the
[HTTP mutation priority contract](../../crates/isolate/README.md#http-mutation-priority).

1. A dependency waiter receives the next grant.
2. If only protected or only degradable work is waiting, that class receives
   the grant and can borrow the complete active capacity.
3. If both application classes are waiting and one is below its minimum, that
   class receives the grant. When both are below their minimums, the lower
   minimum-satisfaction ratio receives the grant; an exact tie alternates.
4. Once both minimums are met, the class with less elastic occupancy receives
   the grant; an exact tie alternates.
5. Within the selected class, a resumption precedes an initial start and each
   phase remains FIFO.

The minimums are non-preemptive service floors, not static reservations. The
limiter never keeps a permit idle for an absent class. Existing active work is
not interrupted when another class becomes runnable. A dependency backlog can
also occupy capacity needed for either floor because releasing active ancestors
has higher liveness priority. The application floors converge as permits turn
over after dependency demand clears.

Balancing current elastic occupancy is important during transitions. Merely
alternating new grants lets the class that borrowed all capacity retain most
elastic slots while its incumbents finish. Occupancy balancing converges to an
equal division of `E` under sustained two-class demand without imposing a
static partition.

For `A=28`, `P=4`, and `G=14`, the ten elastic permits converge to five per
class under sustained contention. The resulting occupancy is approximately
nine protected and nineteen degradable permits. Either class can still use all
28 when the other class has no waiter.

## Initial admission and worker reservations

The isolate scheduler cannot remove an arbitrary number of requests from its
external queue and then wait for active permits. Doing so would hide queued
demand from delay and expiry accounting and could let a large arrival wave from
one class occupy every permit wait ahead of the other class.

The scheduler exposes at most one external initial waiter for each active
class. Requests from that class remain in the queue until the exposed waiter
acquires a permit, times out at the original queue deadline, or loses its
caller. One direct internal dependency waiter is exposed separately and has no
external queue deadline because that callback cannot be retried safely.
When both service floors are zero, degradable requests use the protected slot.
An enabled deployment lane retains separate control-plane and dependency slots.
With that lane disabled too, non-high external classes collapse to the single
existing phase-only initial wait. High mutations retain their separate slot.
Unlimited active capacity has no permit wait.

Every exposed waiter reserves its global and per-client worker eligibility
while waiting for the active permit. The reservation prevents concurrent
waiters from collectively passing worker checks and then discovering that no
worker is available after a permit grant. A reservation is removed immediately
before worker assignment or terminal rejection. Queue entries that remain
queued continue to expire through the non-consuming expiry receiver.

## Configuration contract

`FUNRUN_ISOLATE_PROTECTED_ACTIVE_THREADS_MIN` and
`FUNRUN_ISOLATE_DEGRADABLE_ACTIVE_THREADS_MIN` are both zero or both positive.
Positive values require all of the following:

- finite positive `FUNRUN_ISOLATE_ACTIVE_THREADS`;
- configured `APPLICATION_MAX_CONCURRENT_DEGRADABLE_QUERY_LEADERS`;
- `P + G <= A` without integer overflow;
- `G` no larger than the degradable leader cap.

When both minimums are zero and the control-plane lane is disabled, service-floor
and dependency-class admission are disabled. Dependencies and degradable execution
use ordinary active admission for their phase. High mutations retain their separate
class with bounded preference; resumptions precede initial starts within each
effective class. In that mode the leader cap remains strictly below
any configured finite active capacity; `0` continues to mean unlimited. With
positive minimums, the leader cap can exceed `A` because `G` supplies admitted
degradable execution progress.

The leader cap remains strictly below the application query shared base and
the isolate worker shared base. Active admission does not replace lifetime and
worker bounds, and one admitted root can still create separately scheduled
dependencies.

The enabled control-plane lane retains separate fair admission even with zero
class minimums. Its grants alternate with combined ordinary/high-mutation demand; dependencies retain
precedence. No extra CPU permits are created. See
[deployment operations](../non_committing_codegen_analysis/deployment_operations.md).
When comparing occupancy with the protected floor, sum `protected`,
`high_priority_mutation`, and `control_plane`.

## Observability and tests

The active limiter exports bounded metrics for:

- configured total, protected minimum, and degradable minimum;
- held or granted occupancy by `dependency`, `protected`, `high_priority_mutation`,
  `control_plane`, and `degradable`;
- waiters by class and `initial` or `resume` phase;
- acquisition latency by the same class and phase labels.

The focused test matrix covers dependency precedence, both service floors,
work-conserving borrowing, elastic convergence, resumption precedence,
cancellation before and after grant, simultaneous scheduler exposure of both
application classes, queue-deadline ties, and pending worker reservations.

## Deliberately excluded extensions

The design does not add configurable weights, arbitrary class registration,
per-client shares, per-function priority, active-permit overflow, preemption,
or a dedicated degradable worker pool. The two service classes correspond to
the existing trust and overload contract. Additional policy requires evidence
that these classes and bounded metrics cannot express a measured scheduling
problem.


For protected-floor comparisons, `active_javascript_protected_group_occupancy_info`
records protected plus high-mutation plus control-plane occupancy before scraping. Do not add
separate class maxima, including maxima within the same aligned bucket: their
peaks need not be simultaneous. Older images without this gauge have missing
combined-occupancy evidence. The sum does not change dependency precedence,
non-preemptive turnover, or the degradable floor.
