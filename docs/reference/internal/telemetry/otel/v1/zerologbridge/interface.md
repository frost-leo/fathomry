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

# OpenTelemetry zerolog bridge interface

[Documentation](../../../../../../README.md) / Internal package reference

**Audience:** framework logging composition maintainers.
**Status:** implemented borrowed legacy and managed record outputs; not a provider/backend certificate.
**Package:** `github.com/frost-leo/fathomry/internal/telemetry/otel/v1/zerologbridge`.

## Responsibilities and call sequence

`New(client)` returns a concurrent `Sink` implementing the
[zerolog record contracts](../../../../logging/zerolog/v1/interface.md).
For explicit independent-event recovery, prepare a sink with
`Kind: "managed-record"`, then provide the bridge in
`BindingsV1.ManagedRecords` when selecting that frozen preparation. Select
`Kind: "record"` or legacy `SinkV1.Records` only when any returned error must
permanently stop that destination. Both routes retain selected local outputs.
This package imports that concrete immutable record contract and core telemetry,
not Zap. No shared logging abstraction or pooled SDK event escapes.

`WriteRecord` consumes the original context and immutable typed Record, never
parses its JSON. Original time, message, technical correlation and logging
provider/source/scope/revision survive. Logging metadata is nested under
`logging`, user attributes under `attributes`. Trace through Panic map to
OTel severities 1, 5, 9, 13, 17, 21 and 24; Fatal/Panic do not terminate or panic.

Closed arrays and groups remain nested; null and binary stay typed. Byte strings
remain strings, durations become integer nanoseconds, and time attributes UTC
RFC3339Nano. Original caller metadata is retained when supplied. Absent record
time remains OTLP time zero rather than being replaced with bridge time.
Unsigned values exceeding int64 explicitly reject this OTel record, without
truncation/stringification or suppressing later local sinks.

## Ownership, evidence and limits

The bridge borrows Client and owns no provider, exporter or maintenance methods.
Zerolog neither flushes nor closes borrowed record sinks. Composition must
separately schedule telemetry Flush, drain its independent evidence inbox,
and keep the telemetry dependency alive until logging consumers have stopped.

Acceptance is queue acceptance only. Later export failure does not rewrite
earlier logging receipts. The [core telemetry contract](../interface.md) owns
options, technical error identities, budgets, copying, queue and export behavior.
The existing logging integration retains local file, byte-writer and independent
per-sink evidence responsibilities.

The managed route reports `RecordAccepted` only for observed local queue
acceptance. `RecordRejected` preserves a failed event while permitting the next
independent event after conversion limits, queue/evidence saturation, temporary
admission refusal or known cancellation. It neither retries the failed event nor
promises that the next event will succeed. `RecordStopped` covers disabled/closed
destinations, incomplete cleanup, malformed acceptance evidence and unknown
failure state. Native zerolog latches that stopped destination; healthy local
siblings continue. Classification uses bounded technical error inspection,
without calling arbitrary formatting or error-matching hooks.

The legacy route still permanently stops its sink after any returned error,
including a managed-record rejection. Ordinary byte streams and owned files
retain their existing conservative failure-stop policy. A later successful
telemetry Flush never rewrites historical logging failures or resumes a stopped
sink; composition must stop/recreate that source when recovery is required.

[Focused tests](../../../../../../../internal/telemetry/otel/v1/zerologbridge/bridge_test.go)
exercise every severity through the real RecordWriter.
[Managed recovery tests](../../../../../../../internal/telemetry/otel/v1/zerologbridge/recovery_test.go)
independently decode local JSON and actual OTLP integers, saturate queue/evidence
and active-call capacity, and distinguish next-event recovery from terminal stop.
[Actual integration](../../../../../../../internal/telemetry/otel/v1/integration_test.go)
verifies typed OTLP correlation, local writer/file rotation, overflow refusal
and independent evidence when the receiver fails. Native slog.Handler support
and production backend guarantees are not supplied.
