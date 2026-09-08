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

# Controlled calls and technical evidence

Status: implemented shared mechanisms for [Issue #5](https://github.com/frost-leo/fathomry/issues/5),
not concrete SDK/service support, distributed quotas, durable evidence storage or
a business execution engine. The implementation follows S01-S08 and S10-S11 of the
[accepted architecture](../architecture/internal-sdk-integration.md).

These mechanisms support both framework preparation/control and flexible business
I/O through adapters. They preserve facts needed for reliable decisions; they do
not decide Item/Run state, input completeness, retries or subscription progress.

## Packages and trusted boundaries

- `failure` owns the public error foundation and value-only execution attribution.
  Semantic owners declare their errors; SDK catalogs do not move into this kernel.
- `source` owns configuration, assembly and the authoritative resource record.
  Admission and borrowing extend that record rather than creating another owner.
- `operation` owns controlled-call completion, typed results, necessary evidence
  handoff and optional safe diagnostics. It depends on `source` and `failure`,
  not concrete Providers, adapters, workflow orchestration or exporters.

Public Go visibility enables external composition and typed capability results;
it is not a grant of unrestricted business access. Trusted composition/Provider
code receives `Access`, `Call`, `Scope` and the completion authority. Business
facades expose their permitted operations, typed data/handles and read-only
`Receipt`, not underlying SDK clients, `Assembly`, `Access`, `Scope` or `Call`.
The framework evidence receiver owns `Inbox` and its deliveries separately.
Go packages are not an untrusted-code sandbox.

See the [executable example](../../operation/example_test.go) and
[independent consuming-module test](../../source/boundary_test.go). Both are local
contract fixtures, not service integrations.

## Configure and bind once

Apply `source.WithLimits(selection, limits)` to an owned selection before
`Assemble`. Invalid policies fail preflight before constructors. Borrowing and
delegation inherit that record's policy; they cannot attach a fresh allowance.
Legacy selections remain usable for #4 composition but cannot obtain controlled
`Access` without an explicit policy.

Trusted integration code binds the selected capability with `source.Bind` and its
scoped admission authority with `source.AccessFor`. Its public facade uses
`operation.Begin` before entering native work. `Bind` alone does not wrap methods
or enforce admission automatically; exposing that path directly bypasses this
contract. Each concrete integration must prove its facade applies the mechanisms.

The immutable policy and current usage are separately inspectable through
`Access.Limits`, `Assembly.Snapshot` and operation results:

| Field | Unit and behavior |
| --- | --- |
| `Limits.Active` | Positive maximum admitted root uses on this record across its aliases |
| `Limits.Queued` | Maximum waiting root calls; zero rejects overload immediately |
| `Limits.Bytes` | Positive maximum sum of declared active working-byte reservations |
| `Limits.QueuedBytes` | Separate queued-byte ceiling; positive when queueing is allowed, otherwise zero |
| `Limits.MaxLeases` | Positive live borrowing-node ceiling per root, including ancestors retained by descendants |
| `Usage.Active/Queued` | Current logical root uses / queued callers, not Items, rows, connections or SDK attempts |
| `Usage.ActiveBytes/QueuedBytes` | Currently charged declared bytes, never cumulative wire traffic |
| `Status.Borrowers` | Borrowing assembly scopes; active call pins are separately projected in Usage |

Queueing is FIFO, including byte head-of-line blocking. A bounded oversized request
is rejected rather than queued forever. Waiting runs on the caller's stack;
there is no scheduler, worker pool or background reaper. Context is checked at
admission, and the finite helper checks again before native entry. Cancellation
after that entry remains cooperative.

A root's `Request.Bytes` is nonnegative and covers its entire nested working
envelope. Nested calls declare zero additional working bytes, not a new allowance.
Evidence has a separate positive reservation, described below. Framework bookkeeping
and count-bounded metadata are additional memory overhead, not payload bytes measured
by these reservations. Count and declared
byte bounds do **not** measure arbitrary SDK allocation, native memory, prefetch,
decompression, connection pools or external account quotas. Providers must bound
those independently and reject modes that cannot meet a required guarantee.

`Description.Revision` still identifies prepared Provider settings. It is not a
fingerprint of the later admission policy, execution budgets or build. Changing
these policies must not be disguised as an unchanged effective execution contract;
composition freezes the relevant settings and records their separate provenance.

## Per-call association and attempts

`Request.Name` uses the existing 1-64 character source-label syntax. Each call
supplies a `failure.Execution` value:

| Field | Meaning |
| --- | --- |
| `Call` | Required logical call ID; uniqueness belongs to the caller's documented scope |
| `Parent` | Optional logical parent, not the call itself; nested calls must name their actual parent |
| `Run/Item` | Optional framework execution attribution; Item requires Run |
| `Owner` | Optional opaque effect-ownership scope; neither quota scope nor authorization |
| `Attempt` | Execution attempt number; zero means unspecified, not SDK attempt zero |

Nonempty execution IDs use 1-128 ASCII letters, digits, dots, underscores or
hyphens. Validation bounds representation, not identity authenticity or privacy.
Background calls need a logical Call ID, not an invented Run. Source identity,
original composition scope and configuration revision come from the actual record;
aliases and user input do not overwrite them. Scope-label uniqueness remains
composition's responsibility, not a global registration guarantee.

`Call.Attempt` records a genuinely intercepted SDK attempt immediately before its
entry. It does not execute or retry anything. `AttemptsKnown=false, MaxAttempts=0`
means observations are lower bounds. Exact accounting requires proven complete
instrumentation and a positive local maximum. The configured attempt allowance
must include necessary cleanup; exhausting it does not establish rollback or
release. Parent and child calls have separate attempt observations; do not sum
physical batches or terminal Items by counting wrappers.

SDK-owned retries, authentication refresh, batching and background traffic need
explicit interception, native bounds or an unsupported status. A local logical-call
ceiling is not account-wide request rate control. This issue neither introduces
retries nor implements the future shared-limit backend.

## Budgets follow the execution shape

`Budget.Context(parent, phase)` produces a cooperative deadline: the earlier of
`now + Limit` and `parent deadline - Reserve`. Durations are Go nanoseconds;
negative values are invalid. A zero Limit inherits a parent deadline. Only
`Lifetime` may instead inherit an explicitly cancellable context with no deadline.
An uncancellable, unbounded lifetime is rejected. Reserve needs a parent deadline.

The caller owns the returned cancel function. Cancel it only when the SDK no
longer needs that context. No context is silently detached or stored as a mutable
instance-wide current caller. Outer cancellation can consume the intended reserve;
it is not a hard cleanup guarantee.
[Go context contract](https://pkg.go.dev/context#CancelFunc).

| Shape | Integration pattern and distinct obligations |
| --- | --- |
| Finite | Begin for admission, then synchronous `Call.Execute` with its execution budget; callback returns all outcomes/cleanup facts and confirms its own local use is over. Remaining users must already be retained; otherwise use manual reporting/release. No goroutine races it against a timer. |
| Stream/handle | Establish separately where the SDK supports it; retain the original call while consuming/finalizing the returned resource. Partial data and cleanup failure can coexist. |
| Async | Keep the receipt until native delivery evidence. Retain a submit-stack `Guard` **before** calling an SDK that can invoke completion before submission returns. Waiting and delivery use independent caller-owned budgets. |
| Session | Establish, own an explicit Lifetime, handle bounded messages with separate budgets/results, then stop/join native work and report cleanup. Establishment expiry is not session lifetime. |

`Execute` returns setup/budget/state errors; the callback's technical failures are
in the receipt's `Result.Err`, not its own return error. While Execute runs, it
exclusively owns completion: public Resolve/Finish/Release/Complete reject mixed producers
instead of silently dropping the callback's later data or cleanup failures.

The phase names describe purposes, not a universal SDK state machine. For example,
an HTTP request context can govern body consumption beyond receiving headers, and
request-body closure may happen asynchronously after `Do`. A Provider cannot
cancel a supposed establishment phase at headers if its SDK still uses that
context; it needs a suitable native timeout or a longer owning phase.
[Go HTTP contract](https://pkg.go.dev/net/http#Client.Do).

## Completion, nested work and shutdown

`Begin` owns one root borrowing lease. `Scope.Hold` creates a bounded child
obligation without acquiring another resource permit; `Guard.End` confirms that
use has actually ended. Copies share idempotent release. An ended scope cannot
create fresh children. Retained ancestors count toward the bound, preventing an
endless handoff chain from growing unbounded memory.

`BeginNested` creates a separately attributed result under an existing scope.
It reserves evidence nonblockingly and retains the same root use. A child ends
independently, while its parent cannot release the resource ahead of it. Nested
operations share the predeclared resource/byte envelope; this is not authorization
for extra physical connections or unbounded parallel fan-out.

Synchronous helpers and cleanup can use an already held scope and phase budget
without creating a child. Borrowing-node/evidence exhaustion returns immediately,
never waits behind the same held resource. Required cleanup must not depend on
creating a new root or child at shutdown.

Technical reporting and resource release are separate:

1. `Resolve(outcome)` publishes the first technical value/primary error and any
   already-known cleanup error. It retains the original lease **and** cleanup
   evidence capacity. `Receipt.Wait` may now return with `Final=false`.
2. `Finish(cleanupError)` records final technical/cleanup evidence without erasing
   prior errors **or releasing use**. `WaitFinal` can now receive a cleanup failure
   while local termination is still unconfirmed, even with every capacity at one.
3. `Release()` separately confirms this producer no longer uses the resource. A
   failed or timed-out SDK Close is not sufficient evidence. Descendants still pin
   the resource; no new permit or evidence slot is required for this confirmation.
4. `Complete(outcome)` combines reporting and release only when all technical facts
   and this producer's local-use completion are known. The finite helper uses this
   combined path after its callback returns; there is no automatic cleanup retry.

Resolution, final reporting and local confirmation are each one-shot; duplicates return false
without accepting the new value. `Guard.End` carries lifetime evidence only:
it is not a place to discard late errors. Late error-producing work must use
Resolve/Finish or an independently reserved child result.

`Result.Final` concerns this operation's final technical report. `Released`
requires positive local-use confirmation and every retained descendant to finish. Neither proves remote
cancellation, external rollback, committed/visible data or durable recording.
`Receipt.WaitReleased` observes local subtree completion. Ending any wait
never releases work or overwrites the technical error with a wait error.

Closing an assembly stops its new calls but permits existing scopes to finish
nested work and cleanup. Its active calls retain earlier ordered dependencies.
Existing borrowed assemblies continue independently after owner shutdown, as in
#4; their own Close cannot return their protecting lease while calls remain.
Delegation still rejects active borrowers/calls and now rejects queued work too.
Close is synchronous/cooperative and must be retried explicitly after pins end.
If an SDK needs flush/drain/stop work to settle pending callbacks, the integration
must drive that work through an existing held scope or its explicit owner-control
path. Putting the only progress action in the source's final Release callback
would deadlock: that callback is intentionally not invoked while calls still use
the resource. No universal SDK shutdown driver is supplied by these mechanisms.
Arbitrary callback panics, uncooperative native code and process loss are not
recoverable lifecycle guarantees.

## Typed results and necessary evidence

`Outcome[T]` preserves typed partial data together with separate Primary and
Cleanup errors. `Present=true` distinguishes explicit empty output from absent
data. The capability owns T's meanings: accepted/committed/visible/unknown effects,
per-Item versus aggregate granularity, native references and completeness. The
shared mechanism never expands an aggregate ACK into per-Item success.

Transferred T must be immutable and bounded. Result metadata is copied; generic
values and original native errors are **borrowed**, not automatically deep-copied.
Do not mutate a map/slice behind a transferred result or call its projection a
deep snapshot. Returning a copy of the outer struct does not clone its payload.
Providers must define these ownership/concurrency rules and budget native causes.

Errors use public `failure`, retain `errors.Is/As`, and add actual source/execution
attribution without printing native causes. `Result.Err` aggregates inspectable
primary/cleanup failures. Nil means no reported error, not proof of an effect,
complete empty output or business success. Retry, toleration, reconciliation,
localization and Item/Run disposition remain separate owners.

`Inbox[T]` is the framework's independent process-local evidence boundary:

- A positive count and declared byte capacity are reserved **before** SDK work.
  Full capacity rejects admission. A rejected Begin returns an error but no accepted
  operation receipt; no SDK work was entered through that call.
- `Next` receives an accepted operation's live receipt, potentially before results.
  Pending, queued, received-but-unreleased and finished deliveries remain charged.
- Complete/Resolve/Finish never invoke receiver code or await queue space. A serial
  SDK promise cannot become stuck behind an exporter or evidence callback.
  [franz-go promise contract](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.Produce).
- `DeliveryRecord.Release` refuses while local use remains, then idempotently
  relinquishes the slot. The framework must handle necessary facts first. Release
  itself performs no durable write and is not a storage acknowledgement.
- Business receipt access cannot release the independent delivery. Handling an
  error does not remove evidence. A recording failure leaves capacity occupied.
- Long sessions drain bounded child results incrementally, retaining only their
  current aggregate/session responsibility. Receivers must not serialize an endless
  session wait ahead of every child delivery. The library owns no receiver workers.

There is no shared unbounded completed-call history. Inbox backlog is explicit and
bounded; receipts retained after local release are caller-owned memory. A lost
completion or missing cleanup keeps a finite reservation occupied rather than
manufacturing success. Resource/evidence ownership must be handed to an accountable
receiver before an adapter abandons its caller wait.

This is an in-process handoff, not the full durable evidence protocol. Process
crashes, authoritative storage acknowledgement, duplicate/late evidence across
processes, archival and advancement after persistence failure remain framework
integration obligations. Do not use this queue alone to claim reliable recovery.

## Observation and data-version boundaries

A nil Observer disables diagnostics. Otherwise a fixed-capacity lossy queue receives
only shape, nested-use flag, elapsed duration, error-presence flags and attempt
counts. No source/name/Run/Item/account labels, arbitrary metadata, data, native
text or error graphs are exported. Results distinguish pending, disabled, queued,
dropped and suppressed observation; queue acceptance is not export or persistence.

`ExportOne` runs on the receiver's stack, with a context suppressing recursive
observation through other controlled calls/Observers. Export failures and panics
are returned separately and never sent back to that exporter. A slow exporter can
block its caller, not operation completion or cleanup. Integration code must
propagate the supplied context rather than replace it with Background.

Logs and telemetry cannot be the sole evidence authority: bounded queues and even
persistent telemetry pipelines have loss modes.
[OpenTelemetry resiliency](https://opentelemetry.io/docs/collector/resiliency/).

These are process-local Go contracts, not a new wire protocol. Runtime calls,
scopes, guards, receipts, outcomes, results, inboxes and delivery records reject
JSON encoding/reconstruction and use restricted ordinary formatting/logging.
The existing runtime `failure.Error` guard remains intact. Deliberate access to
a typed payload or native cause is not automatically sanitized.

Module/API compatibility, error semantic version, configuration format, source
revision, SDK/build/deployment and future data protocols remain distinct.
A future persisted DTO must separately specify schema identity/version, units,
bounds, missing/zero/unknown values and historical compatibility or rejection.
No Temporal converter, Workflow command or payload/history format changed here.

## Verification and remaining support obligations

Adjacent tests cover concurrent same-source calls, alias ceilings, FIFO/count/byte
overload, queue expiry, noncooperative finite callbacks, early/duplicate async
completion, partial stream reads, late cleanup at every limit of one, session
establishment/lifetime/message budgets, bounded child evidence, shutdown borrowing,
native causes, missing versus empty output and observation faults/feedback.

Run `go test -race -count=20 -timeout=2m ./...` and the repository's formatting,
tidiness, vet and build checks. The finite microbenchmark measures mechanism costs,
not SDK throughput; sustained mixed-shape tests check declared resource/evidence
high-water limits. Detailed evidence belongs to the implementation PR/reference
handoff and identifies the exact tested revision.

Fixtures, standard-library pipes and source comparisons do not establish Kafka,
Redis, database, HTTP service, object-store or Temporal support. Concrete Providers
still must prove native completion, shutdown progress, attempt interception,
bounded buffers/bytes/native resources, real service effects and their public facade.
