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

# OpenTelemetry capability and verification matrix

[Documentation](../../../../../README.md) / [OpenTelemetry Adapter](interface.md)

**Audience:** maintainers tracing native capabilities to bounded public behavior.
**Status:** implemented requirement/test matrix for #128.
This matrix does not certify a production backend or supported release.

The selected native APIs/SDKs are 1.47.0; the HTTP Logs exporter is 0.23.0.
Only the metric SDK and trace exporter core have
[documented metric](../../../../../../third_party/otel-metric/FATHOMRY.md) and
[trace](../../../../../../third_party/otel-trace/FATHOMRY.md) corrections.
Configuration format 1, public API v1, SDK versions, OTLP protocol 1.11.0 and
source/generation revisions are independent axes.

| Native capability / fields | Internal path | Public route | Positive and rejecting controls |
| --- | --- | --- | --- |
| Final defaults, overlays, explicit zero, endpoint/signal selection | preparation.go, options.go, Select/Prepared.Select | Settings, Prepare, Validate, Prepared.Open | TestPreparedFinalBudgetAndFrozenConstruction; TestPublicStrictPreparationAndFailures; strict all-role Adapter conformance |
| Source residence, queue, live spans, instrument/cardinality and work/evidence costs | Prepared.Metadata | Prepared.Metadata/Policy, Recommend, Compose, Using Budget | resident/signal-isolation preparation tests; preconstruction insufficient-runtime refusal; overlapping-generation consumers |
| String-message compatibility and explicit null/scalar/binary/array/map bodies | data.go, logs.go | LogRecord.Message/Body, Value, Emit | typed-log actual OTLP cases/copying; unsupported callbacks, ambiguous fields, malformed/oversized/deep values and no partial queue entry |
| Resource/scope identity, schemas/version, typed attributes | freezeIdentityAttributes, final settings, native providers | Settings identity fields, ConfigAttribute/ValueTree/ValueNode, Tree | typed identity native/wire and frozen-copy tests; strict settings tree loading; duplicate/reserved keys, unused/shared/cyclic nodes, noncanonical base64 and depth/count/byte refusals |
| Severity 0..24, event name, original event and observation timestamps | native log.Record | LogRecord, Emit | all-severity Internal controls; independent public OTLP time/body/context checks; invalid severity/time/UTF-8 refusals |
| Parent/NewRoot, kind, sampling, initial links, immutable SpanContext | traces.go Start | SpanInput, Link, Client.Start, Span.IsRecording/SpanContext | sampled/unsampled, parent/new-root and trace-state native/wire controls; unsupported kinds/invalid links and cumulative bounds |
| Name, attributes, dynamic links, status, event/error/end time | retained native Span | SetName/SetAttributes/AddLink/SetStatus/AddEvent/AddEventAt/RecordError/End/EndAt | extension native and actual OTLP tests, public all-signals/error redaction; rejected mutations, concurrent End/canceled waiting, safe known error projection |
| Int64 counter/updowncounter/gauge/explicit histogram | metrics.go, corrected aggregate comparison | Instrument manifest, MeasureInt64 | exact native sums and protobuf AsInt, small/large/negative gauge controls, cumulative collections; exact histogram bucket counts and math/big oracle; undeclared kind/dimension and negative counter refusals |
| Float64 counter/updowncounter/gauge/explicit histogram | native synchronous instruments | Instrument manifest, MeasureFloat64 | all eight public families independently decoded; unchanged float search; nonfinite inputs and mismatched instruments rejected |
| Cumulative temporality, explicit boundaries, cardinality overflow | manual reader and native fixed instruments | Settings.Instruments/MetricCardinality, Flush | native metric family/cardinality tests; sorted finite bucket limits; integer histogram wire Sum/min/max remain doubles and are not falsely asserted exact |
| W3C trace context and opt-in baggage | context.go | Inject/Extract | public direct consumer identity round-trip; native propagation/fuzz, bounded carriers and malformed-header ignore controls |
| HTTP/protobuf, explicit TLS/mTLS, none/gzip, limits and no redirect/retry | export.go, network.go | Settings transport fields, Flush | real Internal TLS/mTLS/gzip/partial/warning/malformed/oversized/protocol-switch peers; request bounds, unknown effects, cancellation, late-dial join and actual socket closure |
| Manual and explicit periodic export | controlled native Flush, no native batch processor | Client.Flush, Owner.StartExport, ExportLoop.Stop/Status | public managed start/restart/uniqueness, stopped/canceled join, coalesced ticks, historical failures, independent evidence saturation and returned partial failure |
| Old source queues, spans and endpoint ownership | original source, bounded capture, final release | Owner/Handle/Using, resource Fixed/Follow, per-owner schedule | independent Framework consumer and managed Follow tests: held old span, old/new endpoints, fixed policy, failed candidate, loop termination and final export |
| Safe errors and independent facts | technical fault/result/custody mapping | Result/SignalResult/Info/Attribution, Definitions/Resources, offline CLI | source-cleanup attribution, native/public error graphs, canary redaction, en/zh-CN catalogs, runtime serialization refusal and evidence retry without re-export |
| Independent distribution | two exact nested source modules and root graph | public-only direct/Framework fixtures, existing CLI dependency policy | independent module resolved/linked graph checks, forbidden Internal import, cold generated projects and exact replacement ZIP/checksum checks |

## Configuration and copying boundaries

The public `Settings.Version` field has JSON name `format`: zero selects native
format 1; unsupported nonzero formats reject. Name identifies the source, not the
SDK version. Pointer fields preserve omission versus explicit zero. Empty signal
endpoints disable those signals without changing the minimum-one-signal rule.

Resource/scope attributes retain legacy bounded string maps plus closed typed
trees. Body is a runtime recursive value; configuration uses a flat forward-only
tree because the existing strict loader intentionally rejects recursive schemas
and arbitrary codecs. This representation does not authorize custom marshalers.

Inputs are borrowed only during the relevant operation/preparation. SDK-retained
data is copied. Result slices/provenance have independent copy accessors; native
error objects retain intentional explicit inspection semantics. Source lifetime
observations contain metadata without inventing signal data or a resource borrow.

## What remains deliberately outside the profile

No asynchronous metric callback/dynamic scope registry, arbitrary views,
delta/exponential configuration, exemplars, gRPC/HTTP-JSON exporter, native global
installation, arbitrary callbacks/marshalers, spool, automatic event resend or
Collector administration is supplied. These are the approved non-goals, not
substitutes for required capability work.

Both native correction modules retain standalone tests and exact original-file
hashes. Root CI also invokes their regression tests through the actual consuming
selection. Module-cache mutation is never a delivery mechanism. Independent
projects receive explicit immutable replacements through the existing CLI path;
the parent module ZIP and a dependency's root replacements are not sufficient.

## Executable entries

- [Public direct/Framework fixtures](../../../../../../adapters/telemetry/otel/v1/testdata/).
- [Public all-signal and strict-data tests](../../../../../../adapters/telemetry/otel/v1/public_test.go).
- [Managed export and old-generation peers](../../../../../../adapters/telemetry/otel/v1/exportloop_integration_test.go).
- [Native/wire integer controls](../../../../../../internal/telemetry/otel/v1/metrics_precision_test.go).
- [Native consuming-graph qualification](../../../../../../internal/telemetry/otel/v1/native_compatibility_test.go).
- [Cold CLI and generated project checks](../../../../../../cmd/fathomry/internal/command/project/integration_test.go).

Historical backend experiments do not qualify this source upgrade. Change
acceptance requires the focused controls, normal full gates, fresh review,
required PR checks and verification of the actual normal merge into develop.
