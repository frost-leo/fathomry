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
**Status:** implemented borrowed RecordWriter; not a provider/backend certificate.
**Package:** `github.com/frost-leo/fathomry/internal/telemetry/otel/v1/zerologbridge`.

## Responsibilities and call sequence

`New(client)` returns a concurrent `Sink` implementing the existing
[zerolog RecordWriter](../../../../logging/zerolog/v1/interface.md).
Supply it as one `SinkV1.Records` while retaining all selected local outputs.
This package imports that concrete immutable record contract and core telemetry,
not Zap. No shared logging abstraction or pooled SDK event escapes.

`WriteRecord` consumes the original context and immutable typed Record, never
parses its JSON. Original time, message, technical correlation and logging
provider/source/scope/revision survive. Logging metadata is nested under
`logging`, user attributes under `attributes`. Trace through Panic map to
OTel severities 1, 5, 9, 13, 17, 21 and 24; Fatal/Panic do not terminate or panic.

Groups remain nested; durations become integer nanoseconds and times UTC
RFC3339Nano. Unsigned values exceeding int64 explicitly fail the OTel sink,
without truncation/stringification or suppressing later local sinks.

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

The existing zerolog contract permanently stops a sink after its first returned
error. This includes OTel queue/evidence saturation, unsupported values and
canceled bridge admission. A later successful telemetry Flush does not recover
that logging source's stopped sink. Composition must stop/recreate the logging
source; local sinks continue independently. This bridge does not change that
accepted fail-stop policy or silently swallow a rejection to avoid it.

[Focused tests](../../../../../../../internal/telemetry/otel/v1/zerologbridge/bridge_test.go)
exercise every severity through the real RecordWriter.
[Actual integration](../../../../../../../internal/telemetry/otel/v1/integration_test.go)
verifies typed OTLP correlation, local writer/file rotation, overflow refusal
and independent evidence when the receiver fails. Native slog.Handler support
and production backend guarantees are not supplied.
