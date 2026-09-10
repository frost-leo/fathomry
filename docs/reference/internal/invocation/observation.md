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

# Optional observation and runtime projections

[Documentation](../../../README.md) / Internal package reference

**Audience:** invocation integration authors.
**Status:** implemented local contract; native guarantees remain integration-specific.
**Package:** `github.com/frost-leo/fathomry/internal/invocation`.

Start with the [invocation interface](interface.md). This is one part of the same
accepted-call responsibility, not another resource owner.

## Observation and data-version boundaries

A nil Observer disables diagnostics. Otherwise a fixed-capacity lossy queue receives
only shape, nested-use flag, elapsed duration, error-presence flags and attempt
counts. No source/name/Run/Item/account labels, arbitrary metadata, data, native
text or error graphs are exported. Results distinguish pending, disabled, queued,
dropped and suppressed observation; queue acceptance is not export or persistence.

`ExportOne` runs on the receiver's stack, with a context suppressing recursive
observation through other controlled calls/Observers. Export failures and panics
are returned separately and never sent back to that exporter. A slow exporter can
block its caller, not operation completion or cleanup. Integration code must
propagate the supplied context rather than replace it with Background.

Logs and telemetry cannot be the sole evidence authority: bounded queues and even
persistent telemetry pipelines have loss modes.
[OpenTelemetry resiliency](https://opentelemetry.io/docs/collector/resiliency/).

These are process-local Go contracts, not a new wire protocol. Runtime calls,
scopes, guards, receipts, outcomes, results, inboxes and delivery records reject
JSON encoding/reconstruction and use restricted ordinary formatting/logging.
The private `fault.Error` guard refuses runtime JSON as well; ordinary standard-library
error wrappers do not inherit that guard or define a wire contract. Deliberate access to
a typed payload or native cause is not automatically sanitized.

Module/API compatibility, error semantic version, configuration format, source
revision, SDK/build/deployment and future data protocols remain distinct.
A future persisted DTO must separately specify schema identity/version, units,
bounds, missing/zero/unknown values and historical compatibility or rejection.
No Temporal converter, Workflow command or payload/history format changed here.

## Executable evidence

[Call tests](../../../../internal/invocation/calls_test.go) and [evidence tests](../../../../internal/invocation/evidence_test.go) exercise the contract. See [SDK integration](../../../development/sdk-integration.md) and [testing](../../../development/testing.md).
