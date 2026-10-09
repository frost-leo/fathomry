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


# Typed zerolog / OpenTelemetry composition

[Documentation](../../../../../../README.md) / Public Adapter reference

**Audience:** explicitly composed local/OTel/mixed logging applications.
**Status:** implemented qualified independent-event recovery; not backend durability.
**Package:** `github.com/frost-leo/fathomry/adapters/logging/zerolog/otel/v1`.

New borrows and copies a public OTel Client façade, requiring StableDestination:
direct or Fixed, not a telemetry Follow route that could retarget an old logger.
The logger itself still supports direct/Fixed/Follow. Construct a stable telemetry
destination for each physical logger generation. A copied wrapper grants no new
source, Runtime, queue or native SDK ownership.

Select a managed-record sink and provide this Sink through ManagedRecords to opt
into typed recovery. Selecting the legacy record route explicitly preserves its
any-error failure-stop contract. Neither route synchronizes or closes borrowed
telemetry; the bridge deliberately has no Sync/Close method.

Closed fields convert directly without JSON parsing or callbacks. All seven
severity mappings, original time/caller/context, resource and native correlation
are preserved. Binary stays bytes; uint64 must fit OTLP int64; local-only values
explicitly fail this destination rather than truncate/coerce local output.
Zero original time remains OTLP's unknown timestamp.

## Event versus target failure

Conversion/validation failure and known temporary queue, evidence or admission
refusal reject that event while permitting later independent legal events.
No original event is resent. Real source/Runtime closure, disabled log capability,
unknown/malformed outcome or uncertain pending state stops the destination.
The ordinary byte/file/legacy contracts are not globally relaxed.

Classification reads the receipt's actual Primary/Cleanup, not the generic
operation aggregate. Public OTel AdmissionCanceled proves its exact known
pre-dispatch/pre-enqueue cancellation boundary without exposing native owners
or interpreting arbitrary error text. A non-owning Profile distinguishes a
stopped capability from caller cancellation on a live target. Its Fixed resource
probe is immediate and releases its borrow; it does not acquire another root.
Known actual completed facts are not overwritten by a canceled waiter.

Every failed record retains its own error and per-output effects. Local success,
remote record rejection, native completion, export acknowledgement and independent
evidence custody are separate. Stopped/Rejected are not business retry policy.

## Runtime and shutdown

The typed sink rejects the same actual Runtime identity before source effects.
Every Using/retained operation rechecks its captured source against its actual
Runtime before native output. Names, apparent spare capacity or a caller wrapper
overwrite cannot authorize nested roots. Default composition uses separately
budgeted existing Runtime instances; no child-budget sharing alternative is claimed.

Stop/join producers and all retained logging families/owners first. Telemetry
must remain alive AND admissible through this stage. Then stop/join explicit
export scheduling and close telemetry for final bounded export and cleanup.
Only afterward seal/drain/ack both evidence receivers. Framework.Close and resource
declaration order are not a dependency DAG. Receiver/exporter errors terminate
out of band, never recursively through the same logging/evidence pipeline.

[Actual peer tests](../../../../../../../adapters/logging/zerolog/otel/v1) independently
decode records and qualify conversion refusal, queue/evidence/native-gate saturation,
healthy successors, disabled/closed destinations and cleanup. Synchronous fan-out
does not isolate latency from a slow target; no worker or retry is installed.
