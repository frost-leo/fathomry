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

# Telemetry contracts

[Documentation](../../../../README.md) / Public Adapter reference

**Audience:** telemetry consumers and concrete Adapter authors.
**Status:** implemented SDK-independent accounting and observation data.
**Package:** `github.com/frost-leo/fathomry/adapters/telemetry/v1`.

## Category and provider responsibilities

This category owns `Budget`, `Policy`, `Signal`, `Effect`, `SignalResult`,
`Info`, `LayerInfo`, `Attribution`, `Fact` and `Option`. It imports only
public operation contracts and the standard library. There is no exporter,
instrument, span, Client, provider registry or operation runtime.

[OpenTelemetry](../otel/v1/interface.md) retains its native configuration,
preparation/Compose formulas, detailed native BudgetInfo, Client/Span/source
ownership, frozen Profile and Result projection. Provider-specific APIs and
failure semantics are not replaced by a universal telemetry interface.

Budget and Policy describe caller-owned source/work/evidence allowances in bytes
and counts, not measured RSS, remote quotas or backend durability. Overlapping
owners and retired generations stay charged until their actual release. A
Policy is data, not admission or a hidden Flush/Close reservation.

## Observations, copying and privacy

Signals are logs, traces and metrics. `NotAttempted` differs from
`UnknownEffect`; acknowledgement means receiver acceptance, not indexing or
durable storage. `TransportCalls` counts observed transport entries rather than
physical wire attempts. `SignalResult.Err` remains an immutable borrowed cause
for deliberate inspection, not something ordinary formatting traverses.

Info identifies frozen configuration, separately from operation attribution and
borrowed resource generation. Layer provenance carries field names, not their
credential/endpoint values. `Info.Clone` copies both provenance slices and each
Fields slice. Nil outer provenance stays nil, nonnil-empty outer provenance stays
nonnil-empty, and empty Fields normalizes to nil. Other value fields copy normally;
error objects are not cloned or made safe for concurrent mutation.

Info, Attribution, SignalResult and Fact are deliberately inspectable but redact
ordinary fmt/slog and refuse implicit JSON serialization/reconstruction. Typed-nil
slog values are safe. Option values are explicitly non-secret selections supplied
by their owner. Numeric budgets remain ordinary data.

## Provider compatibility

Existing `otel.Budget`, `Policy`, `Signal`, `Effect`, `LayerInfo` and
`Option` names alias the category types, preserving exported literals and enum
values. Their reflected Go package identity follows the category; no unversioned
gob/interface type-name persistence is promised.

OTel Info, Attribution, SignalResult and Fact remain provider-defined types over
the common fields. Explicit category conversions are supported, while their
existing diagnostic marker and provider serialization error facility remain intact.
Info.Clone delegates the same container-copy semantics to the shared contract.

OTel Profile deliberately keeps its OTel Fact-typed fields and Clone method.
A direct alias to a category Profile would break existing Fact field assignment
and composite literals; no generic profile-storage abstraction is introduced.

## Executable boundaries

[Policy tests](../../../../../adapters/telemetry/v1/policy_test.go),
[metadata tests](../../../../../adapters/telemetry/v1/metadata_test.go) and
[the independent consumer](../../../../../adapters/telemetry/v1/testdata/consumer/main.go)
exercise the shared contracts without provider/SDK acquisition.
[OTel category controls](../../../../../adapters/telemetry/otel/v1/category_contract_test.go)
preserve policy formulas, field/enum compatibility, copying and diagnostic codes.
These controls do not qualify an external Collector or backend.

The [Adapter tree map](../../../../../adapters/README.md) owns role/file conventions.
