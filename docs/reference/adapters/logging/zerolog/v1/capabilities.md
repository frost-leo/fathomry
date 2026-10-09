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


# Zerolog capability and acceptance map

[Public contract](interface.md)

**Audience:** maintainers and consumers evaluating the admitted profile.
**Status:** implemented native/public routes with local executable evidence.
Selected [zerolog v1.35.1](https://github.com/rs/zerolog/tree/116c8060e034e8d46855354d22db2acbc8df9e1e);
no source-version upgrade, native-handler substitution or concrete Zap dependency.

| Native capability | Internal authority | Public route | Positive/rejecting proof |
| --- | --- | --- | --- |
| Seven severities without actions | LogEntry / explicit level string | Log / Level | All severities emitted; native Disabled and binary build refuse |
| Existing scalar/null/group semantics | Legacy attr validation and JSON encoder | slog.Attr | Exact legacy 64-scalar budget and decoded SDK-equivalent output |
| Closed arrays/maps/binary/float32 | Value / bounded fixed encoding | shared Value / Attribute | Exact int64/uint64, nil/empty, byte-string, exponent/copy/depth/callback controls |
| Original metadata | Entry / Record PC/Caller | LogEntry / safe slog | Original time/PC including zero absence; actual business caller |
| Immutable derived values | With / shared physical retention | Retain/With | Bounded family storage, retained Follow destination and cleanup |
| Explicit byte/record/file outputs | Final kind/bindings | Settings/Dependencies | No unused target, typed nil/extra/missing refusal, independent thresholds |
| Qualified record recovery | ManagedRecordWriter/RecordOutcome | ManagedRecordWriter / typed OTel sink | Next legal event after conversion/queue/evidence/gate refusal; closed/unknown stops |
| Conservative legacy/file failure | Original failed/file state | SinkResult Stopped and causes | Short writes/panics, historical failures, marker recovery and no automatic replay |
| File Sync/Rotate/gzip/count | Original fileSink under one Access | Sync/Rotate/file settings | Actual file decoding, marker/FD release, no borrowed maintenance |
| Frozen policy reload | PrepareV1/WithPolicy | Prepare/Adopt/Fixed/Follow | Old/new source+sink filters, Log/Sync/Rotate shared saturation, last owner release |
| Original dependency identity | Exact bindings and physical owner | Captured handles / Runtime checks | No retargeted telemetry façade or root-within-root substitution |
| Restricted slog | Original explicit Entry, not SDK Handler | shared owned gateway | Native chronology defect witness vs standard/public control and visible refusal |
| Safe error and evidence | Existing fault/invocation | failure/errorbridge, Result/native correlation | Private-canary protection, distinct effect/cleanup/custody facts, offline CLI |
| Distribution | Selected immutable module graph | Independent and generated consumers | Direct/Framework local-only excludes Zap/OTel; mixed typed protocol tests |

No async diode/buffer workers, sampling, automatic retries/fallback, arbitrary
native hooks/formatters, raw JSON/CBOR input, pooled Events or arbitrary file-policy
hot reload are introduced. These are scoped exclusions, not deferred required work.
