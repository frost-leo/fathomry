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


# Bounded logging data

[Documentation](../../../../README.md) / Public Adapter reference

**Audience:** independent applications, restricted ingress and logging compositions.
**Status:** implemented shared data and diagnostic boundary; not a universal logger.
**Package:** `github.com/frost-leo/fathomry/adapters/logging/v1`.

## Responsibilities

Closed `Value`/`Field` represent null, bool, signed/unsigned integers, distinct
float32/float64, text, binary, byte-string, time, duration, arrays and ordered groups.
The package also owns minimal `Budget`, `Attribution` and output-name/kind
vocabulary. It has no severity enum, Sync/Rotate interface, source owner, queue,
SDK logger, exporter or Runtime. Provider-native semantics remain with providers.

Constructors borrow collection storage until `Freeze`/`FreezeFields`.
Freezing validates and copies recursively; accessors copy their immediate slices.
A zero Value is null. Nil array/group/binary inputs mean present empty containers,
not null. Full `uint64` range is retained; an OTel sink may separately refuse it.
NaN/Inf and invalid UTF-8 text refuse. Time normalizes to UTC without monotonic
clock data. Duplicate/empty keys refuse, keys are at most 128 UTF-8 bytes; concrete
providers impose additional restrictions. No arbitrary reflection or user
formatter/marshaler/LogValuer is invoked.

`Limits` explicitly bounds total fields, nodes, nesting and logical bytes.
`MeasureFields` validates without copying payloads. These are declared storage
charges, not RSS, caller-stack bounds or control over retained foreign error graphs.

## Safe errors and independent facts

`SafeError` recognizes public failure occurrences and i18n presentation, including
bounded transparent wrappers/public-only joins. It preserves safe identity,
location, cause count and localized message/provenance. It never formats private
native causes, invokes unknown Error/Stringer/LogValuer, or invents one primary
owner for heterogeneous aggregates. Nil, unknown leaves, empty wrappers, cycles
and exhausted traversal reject the whole projection. Existing cooperative
Failure/Unwrap accessors remain explicit authority, not forcibly interruptible code.

Safe error projection is not a detector of secrets in caller-authored messages,
attributes or public definitions. Accepted content remains caller-authorized.

Attribution is immutable identity, not a lifecycle snapshot. Logging admission,
per-output filtering/write, telemetry queue admission, export, backend persistence
and independent evidence acknowledgement are different facts.

See [restricted slog](../slog/v1/interface.md), [Zap](../zap/v1/interface.md),
[source](../../../../../adapters/logging/v1) and its focused tests.
