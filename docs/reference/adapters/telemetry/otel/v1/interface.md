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

# OpenTelemetry Adapter calling contract

[Documentation](../../../../../README.md) / Public package reference

**Audience:** independent Go consumers, Framework composition and Adapter maintainers.
**Status:** implemented bounded profile for [#128](https://github.com/frost-leo/fathomry/issues/128),
with isolated native/protocol and independent-consumer qualification.
This page does not certify a Collector, production backend or supported release.
**Package:** `github.com/frost-leo/fathomry/adapters/telemetry/otel/v1`.

The [telemetry category contracts](../../v1/interface.md) own SDK-independent
budgets, signals/effects and observation data. Existing budget/vocabulary names
remain aliases; sensitive provider observations retain their OTel diagnostics and
error codes. Native costs, Compose, Profile and source/span lifecycle stay here.

## Responsibilities and preparation

The [capability matrix](capabilities.md) connects selected native fields and
operations to their Internal/public routes and positive/rejecting tests.

The provider exposes bounded logs, traces, synchronous metrics, W3C propagation
and HTTP/protobuf export through the existing public operation/runtime and
resource mechanisms. It installs no global provider, logger, exporter or ambient
configuration. No concrete logging provider is required.

`Settings` is strict-loadable data, not live authority. `Dependencies` borrows the
public Runtime, required-evidence Inbox and optional Observer. Explicit JSON may
contain sensitive endpoints, headers and PEM material; ordinary fmt/slog output
is redacted. Optional nil fields retain native defaults; explicit pointer-to-zero
values are validated rather than silently defaulted. Durations use nanoseconds.
Empty endpoints disable a signal; at least one signal is required.

`Prepare` freezes the final resolved native selection and its authoritative
metadata before native construction. `Prepared.Policy` describes one source;
`Compose` combines the exact preparations for all simultaneous owners, including
retired generations. These are declared work, evidence and resident envelopes,
not measured RSS or remote capacity. Recommendations never resize caller-owned
runtimes. Active spans consume normal operation slots; no dedicated Flush capacity
is implied by a single saturated source.

Use `Open` or `Prepared.Open` with explicitly owned runtime/evidence mechanisms.
Open is local construction, not backend readiness. Even when Open returns an
error, a non-nil Owner retains cleanup responsibility. The construction context
owns the entire source lifetime, not only the setup wait.

## Direct and Fixed/Follow use

`Owner.Client` and `Owner.Handle` grant non-owning capabilities. Bind the Handle in
a typed `resource.Instance` whose Release delegates to the Owner. `Using` borrows
a Fixed/Follow Ref under a frozen Budget covering every admitted generation.
`WithID` produces a correlation view, not new resource or operation capacity.

StableDestination is a pure non-owning routing fact: direct/Fixed clients cannot
follow replacement endpoints between calls; Follow clients can. It does not
acquire a lease or promise source existence, readiness, admission or lifetime.
Explicit logging composition uses this fact to reject hidden destination changes
behind retained logger views. Standalone telemetry Follow behavior is unchanged.

AdmissionCanceled safely inspects the current public failure's exact native
entry/pre-Emit cancellation boundary (including its known invocation attribution
frame), not arbitrary cause text/callbacks. It proves cancellation before that
dispatch/enqueue only; target lifetime and retry policy remain separate. Error
codes, original identities and occurrence metadata are unchanged. Managed logging
uses this additive fact to avoid poisoning healthy independent-record targets.

Each accepted operation retains its actual source and public generation.
A larger replacement cannot bypass the Using budget. A live span remains on its
original source across Follow replacement; queued telemetry does not move to the
replacement endpoint. Failed candidates and obsolete generations retain their
own release responsibility.

Owners must not be copied. Client/Handle views are non-owning; they cannot close
the source, Runtime or Inbox. Returned facts and copy accessors do not expose
owning SDK providers, transports or mutable span objects.

## Data, signals and effects

The approved profile includes Emit, Start and retained span controls, eight fixed
int64/float64 synchronous metric families, Inject/Extract and Flush.
Bounded typed data preserves null, scalar, binary and closed collection/map values
without invoking arbitrary reflection, marshalers, Stringers or LogValuers.
Resource/scope configuration, event time, observation time and safe error
annotations retain their distinct meanings. See the
[Internal contract](../../../../internal/telemetry/otel/v1/interface.md) for
native capability limits and exact-source provenance; the public declarations
own their actual field/copy contracts.

A direct receipt denotes public admission, not success. Its Result and independent
evidence preserve source/generation facts and per-signal Accepted, Submitted,
Acknowledged, Rejected, TransportCalls, Sampled and Effect. Errors and partial
effects remain separate. TransportCalls counts observed HTTP RoundTrip entries,
not an exact physical wire-attempt total.

Local queue acceptance, OTLP receiver acknowledgement, backend persistence and
required-evidence acknowledgement are different facts. Submitted log/span batches
are not automatically retried after unknown effects. Cumulative metric collection
does not turn missing receiver acknowledgement into proof of no effect.

The source-lifetime Open receipt resolves after actual cleanup with its detached
source identity and original root attribution. This metadata is present even
without signal data: HasData remains false and SignalsCopy is empty. It does not
invent a resource-borrow generation or equate cleanup with export success.

## Span ownership and cleanup

Start retains one original public root and resource generation until actual native
End and evidence transfer complete. Span mutations consume their declared
cumulative limits and never acquire a replacement generation. Sampled-out spans
also require End.

A canceled End wait leaves the Span and original receipt live. Resume with a
separately usable cleanup context; cancellation is not proof of termination.
Repeated completed End observes the same receipt. Close cannot declare source
release while a live span still owns native work.

Close stops export scheduling, refuses new work, joins retained operations, then
performs bounded final native export and actual release. Timed-out waiting retains
the same Owner; `Release` reports completion separately from retained errors.
Final cleanup does not need fresh ordinary operation/evidence admission.

## Optional managed export

Manual Flush always remains available. `Owner.StartExport` explicitly starts one
scheduler for that physical source; nothing starts it implicitly. Both options
are required: Interval is 1ms..24h and Timeout is 1ms..1min.

The loop calls the Owner's direct public Flush, never a latest-following Ref.
There is no permanent resource lease or operation root between ticks.
One in-flight accepted export must actually release before another tick dispatches;
a canceled waiter does not authorize overlap. Elapsed ticks coalesce into bounded
Skipped accounting instead of an accumulated work queue.

A second live loop on the same owner is refused. Stop cancels scheduling and joins
the actual Flush stack and receipt. A Stop timeout leaves a resumable handle.
An explicitly stopped, joined schedule can be restarted until source shutdown
begins. Owner.Close also stops and joins its schedule; source shutdown permanently
prevents restarting it.

Status exposes saturating Attempts, Succeeded, Failures and Skipped counters.
Running denotes a currently owned attempt; Stopped means actual loop termination.
LastError preserves historical failure after subsequent success. Status and
optional diagnostics do not replace the independently reserved operation evidence.
Stop does not itself submit a final batch; final source cleanup owns that attempt
and undelivered reporting.

Start a schedule in each resource Build when managed operation is selected.
A borrowed retiring generation continues its own schedule and endpoint until its
Release begins. Generation reservations therefore bound managed loops; the loop
does not retain a lease that could prevent its own retirement.

## Required evidence and staged composition

Run independent required-evidence reception for every telemetry Runtime.
`framework.StartReceiver` uses released records, avoiding head-of-line blocking
behind a live source or Span. Finish receivers only after all their producers
and final cleanup have released evidence. Receiver/export diagnostics must end
out of band rather than recursively feed the same observed logging pipeline.

A logger-to-telemetry composition must preserve both telemetry lifetime and
admission while logger work is still active. The default composition uses
independently budgeted existing Runtime instances. Holding a logging root while
waiting for another root in the same exhausted Runtime is not safe composition.
Client.UsesRuntime provides a non-owning identity check for such compositions,
including Runtime aliases; it neither exposes shutdown authority nor proves
readiness or available capacity. A shared-runtime child route is not supplied.

Stop and join producers first; then stop export scheduling, release telemetry
owners and finish evidence reception. Framework.Close is not an inter-provider
dependency DAG, and resource declaration order does not provide this sequence.
A retained lease alone does not keep a canceled operation runtime admissible.
No general shutdown manager, durable spool or background event retry is supplied.

## Errors, privacy and verification

Observability facility `0x280`, component `telemetry_otel`, owns the provider's
stable public errors. Definitions and English/zh-CN resources are detached and
available to the actual offline CLI. Error mapping preserves deliberate native
`errors.Is/As` access while hiding private native text from ordinary presentation.
Shared admission/evidence failures retain their existing operation identities.

Runtime handles/results refuse implicit JSON persistence or reconstruction.
Explicit payloads and cause inspection remain sensitive; safe error projection
does not discover secrets in caller-authorized message content.

[Focused loop tests](../../../../../../adapters/telemetry/otel/v1/exportloop_test.go)
exercise receipt ownership, stop continuation, tick coalescing and evidence
saturation. [Error tests](../../../../../../adapters/telemetry/otel/v1/error_contract_test.go)
cover native/public identity, redaction, detached definitions and both locales.
The [independent consumers](../../../../../../adapters/telemetry/otel/v1/integration_test.go)
verify direct/Framework usage and the actual corrected dependency graph.
The [Adapter maintenance guide](../../../../../development/public-adapters.md)
defines continuing whole-tree, independent-consumer and actual-merge gates.
