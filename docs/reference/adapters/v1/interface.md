<!--
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
-->

# Public Adapter operation mechanisms

[Documentation](../../../README.md) / Public package reference

**Audience:** concrete Adapter authors, explicit application assembly and Framework
evidence receivers.
**Status:** implemented process-local public mechanisms, composed by concrete
configuration/database Adapters and Framework. Durable execution policy remains
separate and unimplemented.
**Package:** `github.com/frost-leo/fathomry/adapters/v1`.

## Responsibilities and dependency direction

This package owns operation admission, actual-work accounting and independent
evidence custody. It does not wrap Internal's shared engines or define a universal
SDK client. Production dependencies are the standard library and public
[failure](../../failure/v1/interface.md), [resource](../../resource/v1/interface.md)
and, transitively, [settings](../../settings/v1/interface.md). No Internal, concrete
SDK, i18n engine or Framework dependency is required to execute or report an error.

Concrete providers supply typed facts, public semantic failures, native execution
and safe non-owning facades. They translate native ownership rather than letting
this runtime guess completion, readiness, transaction context semantics or effects.
Both direct application use and Framework scenarios can consume those facades.
The primitives here are implementation/assembly authority, not boilerplate each
business operation should have to recreate.

`resource/v1` owns instances and Fixed/Follow replacement; this package owns calls.
Neither is a global service locator. There is no automatic SDK retry, source Watch,
client construction, control-database write, distributed quota or durable ledger.

## Bind once, then execute

1. Construct a Runtime with an explicit owner context and Options.
2. Construct an Inbox for required evidence. Bind an Endpoint with that Inbox and
   the component's Copy function; optionally bind an Observer.
3. Call Run for synchronous submission or Start for one owned producer goroutine.
   Both reserve evidence and admit work before invoking a producer.
4. The producer resolves one Outcome and retains a Guard for any work that outlives
   the submitter. Child/StartChild own related work in the same root family.
5. Direct callers observe a Receipt. The independent receiver claims the same
   private facts through the Inbox and acknowledges only after actual work releases.
6. Stop new traffic; Close the Runtime and retain it through timed-out joins.
   Drain/acknowledge required evidence and separately close instance owners under
   the application's declared shutdown policy.

Immediate Run/Start errors describe rejected admission; no producer ran and no
accepted record exists. A non-nil Receipt denotes acceptance, not success.
A subsequently unavailable resource or canceled dispatch is an accepted failure
with independent evidence. No mutation is rerun to recover a missing acknowledgement.

Bind captures opaque receiver states and the function values, not mutable caller
handle storage. Copies share the same underlying owner; do not concurrently
overwrite handles while using them. Overwriting one returned Receipt wrapper
does not alter the independent record or other wrappers.

The [executable example](../../../../adapters/v1/example_test.go) shows a finite
operation and explicit independent reception. Public SDK facades will own this
protocol for their callers; no concrete service Adapter is supplied here.

## Outcomes, actual work and instance borrowing

Call is producer authority; Scope grants bounded child creation, not Runtime
shutdown or native client access. Concrete consumer APIs must not expose either
merely because they return a result. A Receipt cannot close its Runtime.

A submitting stack always retains a work reference. An inline completion callback
cannot return admission capacity or a source lease while submission still runs.
Hold must precede additional callback/stream/session work; Guard.Release declares
that work actually ended, exactly once across copied guards. Context cancellation
is not release. Guards are permitted for adopting existing responsibilities while
a call remains owned; acquiring a guard does not authorize canceled native dispatch.

Child operations have their own outcomes and evidence, but share the root's work
envelope. A parent cannot release until all descendants and its own references end.
A handled child failure does not automatically become the parent's business failure.
Child contexts observe both the explicitly supplied context and parent cancellation.
Admission/dispatch inspect the bounded ancestor chain without relying on the timing
of asynchronous cancellation propagation.

RunWithLifetime, ChildWithLifetime and UsingWithLifetime separate admission/setup
from an explicit retained lifetime. Both contexts stop queued admission; after
acceptance the call follows its lifetime, parent and runtime. The producer still
uses the setup context for acquisition. This supports database preparations and
PostgreSQL Begin without treating a canceled, already-completed setup as retained
handle cancellation. MySQL Begin instead uses its caller context for the full
transaction lifetime. No canceled parent can be revived, and no context is
implicitly detached. UsingWithLifetime retains one source generation for the
entire family, just like Using.

Resolve copies present data outside locks and publishes once. It does not release
guards, children or the submitting stack. Quiescent work without a supplied outcome
becomes ErrOutcome, not successful absent data. Primary/Cleanup preserve exact
immutable error objects; ValueCopy invokes the component's independent copy contract.

Using/StartUsing explicitly borrow one public resource.Ref for the complete root
family. The source name and acquired generation are captured, not read later from
current settings. Old generations remain borrowed while streams, transactions or
callbacks are active. Release of that borrow is not proof of source-wide native
cleanup; resource owns retirement and retained cleanup separately. A component's
consumer value must not expose mutable clients or owning Close authority.
Zero Source means no successful borrow has been captured yet; it also covers
pending/failed acquisition and does not prove no binding was configured.

Wait observes publication; WaitReleased also joins actual work and source lease
release. Waiting uses its own context and never cancels work. Known completion may
win over an already-canceled wait. Inspect Snapshot.Err separately from the wait
error. A pre-release snapshot can lack a later lease-release cleanup error; final
cleanup-sensitive receivers use WaitReleased. Historical snapshots never change.

## Required evidence is not optional logging

Inbox reserves both a record slot and declared bytes **before** admission/dispatch.
Reserved, queued and claimed records share the same bounds. A missing receiver or
full Inbox rejects new work; facts are never silently dropped.

Next claims one accepted record, which may still be live. NextReleased instead
claims the oldest released queued record, skipping live owners without freeing
their reservations. Actual completion wakes the existing Inbox waiters; no
forwarding worker or second queue is introduced. Use this path when a long-lived
Open/Watch must not block reception of later finite results.

Copies of Delivery share
one claim authority. Ack refuses until actual work releases; Retry requeues the
same facts without releasing the reservation or repeating SDK execution. Stale
claims cannot acknowledge a newer claim. Receipt wrappers do not expose custody
authority.

DeliverOne claims, waits for release, rechecks receiver cancellation, invokes a
bounded sink outside locks, then acknowledges. Waiting/sink failure returns the
claim for later handling; the helper contains no retry loop. Cancellation can
still race with sink entry, so the sink must honor its context and declare actual
completion truthfully. A sink that writes and then returns an error may receive
the record again: idempotency and durable deduplication belong to that sink.

Seal rejects new reservations but honors accepted ones. Empty-queue EOF waits for
pending reservations, not claims held by other consumers. Claimed records can
still be returned after EOF; coordinate receiver shutdown and outstanding custody,
not EOF alone. Close never seals an Inbox automatically because separate runtimes
may explicitly share it. Joining Runtime.Close does not join external callers
still unwinding rejected admission; those callers return their reservations before
their methods return.
Successful Close confirms only the local join; operation failures stay in their
receipts/evidence, not an aggregate Close error.

Observer is a separate bounded, payload-free diagnostic queue. It attempts Admitted,
Resolved and Released transitions in state order, including implicit missing-outcome
resolution. Failed carries reported primary/cleanup failure as of that phase; it is
not a business disposition. Full/sealed queues count drops with a saturating counter.
Operation labels must be stable non-secret labels, not per-request metric dimensions.
There are no exporter callbacks in producer or locked paths.

## Options, bounds and copying

Options/EvidenceOptions are loadable JSON data, separate from runtime declarations.
Zero selects defaults except MaxQueued, where zero deliberately means no queue.

| Bound | Default | Accepted non-default range |
| --- | --- | --- |
| Active root operations | 16 | 1..1024 |
| Queued roots | 0 | 0..4096 |
| Active declared work bytes | 64 MiB | 1..2^40 bytes |
| Queued declared work bytes | active byte limit when queue enabled | 1..2^40 bytes; must be zero if queue disabled |
| Concurrent nodes in a root family, including its root | 64 | 1..4096 |
| Depth, with root at 1 | 8 | 1..32 |
| Explicit guards per node | 64 | 1..65536 |
| Evidence records, including reservations/claims | 64 | 1..65536 |
| Declared evidence bytes | 16 MiB | 1..2^40 bytes |
| Optional observer records | no observer | explicit 1..65536 |

Root work and each record's evidence bytes are positive reservations; children
declare zero work bytes and share their root's envelope. FIFO root admission does
not bypass a large head waiter; cancellation/closure returns its reservation.
Finalization never needs a new admission/evidence slot.

These bounds do not measure or limit arbitrary Go/native allocations, kernel memory,
remote operations or workload-wide quotas. Copy must isolate mutable data, never
mutate its input, and be bounded, concurrent-safe and non-panicking. Producers and
sinks also must not panic or wait for lifecycle progress requiring their own return.
The library cannot force uncooperative work to terminate or make arbitrary SDK
values concurrent-safe.

Request.Operation is a dotted lowercase label of at most 64 bytes. Optional ID is
opaque UTF-8 of at most 256 bytes with C0/DEL controls refused; explicit inspection
may expose it. Runtime Name is an optional non-secret ASCII label of at most 64 bytes.
Sequence is non-wrapping and local to one Runtime, not a durable/cross-runtime ID.
Released means local ownership ended, not committed, visible, rolled back or durable.

## Errors and localized logging

The logical owner is `fathomry/operation`, in core facility 0x005. This is an
[operation capability allocation](../../failure/v1/code-allocation.md), not an
Adapter-layer band. Definitions/Resources compose explicitly with failure/i18n.
Component-specific meaning and native errors remain deliberate causes; the generic
ErrOperation does not infer what a native operation accomplished. Concrete provider
facades should construct their own semantic occurrence rather than treating a
generic wrapper as a universal native-error classifier.

Creating an error never requires resource, settings or i18n lookup. At an explicitly
assembled presentation boundary, add the operation component to the catalog:

```go
i18n.Component{
    Module: "fathomry", Name: "operation", BaseLocale: "en",
    Resources: adapters.Resources(), Directory: "resources",
    Definitions: adapters.Definitions(),
}
```

Bind a Presenter to the intended settings.Reader once. A slog caller can then write:

```go
logger.ErrorContext(ctx, "adapter.operation.failed",
    "error", presenter.Present(operationError))
```

The structured error contains stable Code/Identifier, frozen localized text,
requested/actual language, fallback and any separate presentation issue. Plain
logging of the original error does not automatically translate it. Foreign errors
are not automatically classified or sanitized by Present; components must own
their safe public occurrence. Do not log an unrestricted native cause separately.

A settings-bound Presenter selects the current accepted language per presentation.
Changing only the locale does not require rebuilding an Adapter or Presenter.
A separately replaceable catalog/Presenter can use resource, but it is not required
for reporting failures. Missing configuration/resources retain the original safe
static explanation; localization failure must never erase operation failure or
recursively invoke the failing logger. A unified Framework logger facade is not
implemented by this common runtime.

Runtime handles, request/correlation metadata and typed outcomes refuse implicit
JSON persistence/reconstruction. Default fmt/slog projections do not invoke native
formatters or reveal their values. Explicit inspection and component-projected
human messages remain subject to their owners' privacy policies.

## Verification and limits

[Grouped core tests](../../../../adapters/v1/) exercise inline/late completion,
bounded trees, queued cancellation, copied guard/claim races, evidence retry/sealing,
privacy and malformed inputs. The lifecycle fuzz oracle independently models held
references, descendants, result presence and custody accounting.

[Integration tests](../../../../adapters/v1/integration_test.go) use a real loopback
HTTP body across public resource replacement, actual settings-backed slog
presentation, all English/Chinese definitions, and a separate consuming Go module.
Negative compilation checks reject generic tag conversions and consumer shutdown
authority; production dependency checks reject Internal/SDK/i18n coupling.

These checks do not qualify arbitrary native SDK migration, real service Adapters,
production throughput, crash recovery, durable exactly-once evidence, Temporal
failure transport or deterministic Workflow replay. Runtime goroutines and mutable
process preferences belong in Activities/process-local code, not Workflow logic.
