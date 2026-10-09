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


# Zap capabilities and acceptance map

[Zap contract](interface.md)

**Audience:** maintainers and consumers evaluating the admitted logging profile.
**Status:** implemented native/public capability map with local protocol evidence;
not production/backend or release certification.

Selected upstream: [Zap v1.28.0](https://github.com/uber-go/zap/tree/5b81b37b81b8e2ed447a6f57991e372ee4fa5c8f).
Public API/configuration version, native SDK version, physical source revision and
logical generation/revision are independent axes.

| Selected native capability | Internal authority | Public route | Positive and rejecting evidence |
| --- | --- | --- | --- |
| Debug/info/warn/error synchronous entries | Log/LogEntry, one original Access | Client/View Log/LogEntry | public decoded local/OTLP tests; unsupported terminal levels, canceled direct calls |
| Native scalar widths, Float32, binary/time, nil/no-op | FreezeFields/FieldsValues, exact native tags | Native zapcore.Field plus Field(logging.Value) | Native width/control decoder, public uint64 max, nil/Skip controls |
| Closed null/array/map values | Bounded private fixed codecs | logging.Value constructors | Independent binary-array/base64/null/empty decode; callbacks never entered, duplicate/depth/node/byte refusal |
| Original caller/time/name | Entry/EntryMetadata, CallersFrames | Direct Log, explicit LogEntry, retained Named, Slog | Business caller/file line, preserved timestamp/PC, zero absence |
| Immutable fields/name | Copied native With/Named, shared cumulative bound | Owned Retain/With/Named families | Copy/retention saturation, no new native allowance or destination retarget |
| Per-output JSON/structured thresholds | Policy view thresholds, physical Core writes | Settings.Outputs / Structured / ExtensionLevel | Local-only/OTel-only/mixed peers, distinct filtered/failed/written states |
| Linux file rotation/gzip/count retention/Sync | Original rotatingFile/checkedWriter | Native file Settings, Sync | Native rotation/permission/restart/short-write tests; physical lock and failure history |
| Level-only reload | Prepared.PhysicalEquivalent / WithPolicy | Prepared.Adopt, Fixed/Follow Handle | Original old policy, lower new threshold, shared queue/Sync, last physical release |
| Final preparation and budgets | PrepareV1.Metadata/Select | Prepare/Metadata/Policy/Compose | Explicit-zero/refusal before effects; level-width accounting invariant |
| Safe structured errors | Native faults remain native; public projection before native entry | zap.Error(public failure/i18n) / shared SafeError | Framework localized/aggregate error tests and private formatter canaries |
| Restricted slog | Explicit bounded original Entry | Client.Slog / shared Handler | Standard chronology control plus rejected dynamic callbacks/duplicates; poison and hidden-error status |
| Typed telemetry composition | Closed structured record, independent telemetry source | zap/otel Sink | Actual OTLP protobuf, exact integer/null/binary/trace/time; shared Runtime rejection |
| Independent result custody | invocation + adapters Runtime/Inbox | Receipt/Result/SinksCopy/Attribution | Per-output partial facts, canceled/blocked cleanup, independent acknowledgements |
| Borrowed/owned cleanup | One physical root and bounded policy children | Owner Close/Release/ShutdownComplete | No lock release while policy survives; failure candidate/continuation tests |
| Public distribution | Existing exact module delivery | Standalone and Framework consumer modules | No Internal imports; exact selected graph and cold generated projects |

Zap exposes no Rotate, Trace, Fatal, Sugar, raw owning SDK logger/Core/CheckedEntry,
custom encoder registry, automatic sampling/retry or asynchronous logging worker.
These are explicit non-goals, not reclassifications of required capabilities.
Restricted ingress is not an unrestricted slog compatibility claim. Local tests
do not certify backend durability or a production deployment.
