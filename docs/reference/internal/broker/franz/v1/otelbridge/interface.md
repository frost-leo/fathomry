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

# Kafka OpenTelemetry propagation bridge

[Documentation](../../../../../../README.md) / [Kafka interface](../interface.md)

**Audience:** framework adapters explicitly associating Kafka records with traces.
**Status:** implemented bounded header translation; not automatic instrumentation.
**Package:** `github.com/frost-leo/fathomry/internal/broker/franz/v1/otelbridge`.

`Inject` and `Extract` reuse the actual
[OpenTelemetry context contract](../../../../telemetry/otel/v1/interface.md).
The core Kafka package imports no telemetry or logging provider.

Injection returns copied header storage, preserving ordinary binary values, nil/
empty values, duplicates and order. Existing W3C fields reject instead of silently
replacing context. Extraction ignores ordinary headers and rejects duplicate W3C
names case-insensitively. Input/output lists are bounded to 64 headers/64 KiB;
the OTel boundary additionally bounds W3C propagation to 8192 bytes.

Baggage is explicit opt-in. The caller owns trust decisions. Context association
does not authorize a source, alter key/partition routing, define Item/Run identity
or supply metric labels. No native kotel/kzap hook, global propagator, logger,
worker, exporter or resource owner is installed.

Native context round-trip and header/copy/duplicate tests are in
[bridge_test.go](../../../../../../../internal/broker/franz/v1/otelbridge/bridge_test.go).
Errors retain the product fault identity and underlying OTel cause without
formatting raw header values.
