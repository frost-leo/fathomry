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

# OpenTelemetry Zap bridge interface

[Documentation](../../../../../../README.md) / Internal package reference

**Audience:** framework logging composition maintainers.
**Status:** implemented borrowed bridge; not a provider or backend certification.
**Package:** `github.com/frost-leo/fathomry/internal/telemetry/otel/v1/zapbridge`.

## Responsibilities and call sequence

`New(client)` returns a concurrent `Sink` satisfying the existing
[Zap StructuredSink](../../../../logging/zap/v1/interface.md) by structural typing.
It imports native zapcore and core telemetry, not the Fathomry Zap provider,
zerolog or a third-party bridge. Supply it to the existing Zap `Select`
extension parameter; keep telemetry alive until all logging consumers close.

`Write` translates the actual supported primitive/binary/time/fault fields,
preserving caller context, time and severity. Original logger name, provider,
source, scope and opt-in caller location appear under `logging`; user fields
appear under `attributes`. Fixed technical correlation comes from the original
logger's framework fields, not new invented call IDs.

Durations remain integer nanoseconds, byte strings become text, binary stays
bytes, and times normalize to UTC. Float32 widens to float64. Integers
first retain their native Zap field widths; raw Bool fields
use Zap's equality-to-one meaning rather than nonzero truthiness. Unsigned values
must fit OTLP int64; larger values fail this sink rather than silently becoming
strings or changing local Zap output. Fault fields export safe technical
diagnostics, never native cause text. Raw errors, namespaces, reflection and
user marshalers are unsupported. Severity is limited to the existing
debug/info/warn/error product boundary.

## Ownership, evidence and limits

The bridge borrows Client and owns no queue, provider, exporter or close method.
`Sync` checks its caller context but has no private buffer to drain: it does
**not** invoke telemetry Flush. This does not weaken Zap's existing file Sync;
those sinks retain their independent maintenance.

A nil Write result means only OTel queue acceptance. Failures remain in both
the logging result and telemetry's independently owned inbox when admission
succeeded. Core telemetry's [data, evidence and lifecycle contract](../interface.md)
applies, including bounded queue rejection and explicit export scheduling.
The bridge never consumes required inbox deliveries on behalf of composition.

[Focused tests](../../../../../../../internal/telemetry/otel/v1/zapbridge/bridge_test.go)
cover translation, refusals, severities and borrowed Sync.
[Product integration tests](../../../../../../../internal/telemetry/otel/v1/integration_test.go)
verify native OTLP records alongside actual Zap multi-file rotation/gzip.
Unselected native Zap fields and production backend guarantees remain unsupported.
