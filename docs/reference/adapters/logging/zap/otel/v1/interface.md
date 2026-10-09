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


# Typed Zap / OpenTelemetry composition

[Documentation](../../../../../../README.md) / Public Adapter reference

**Audience:** applications explicitly selecting local, telemetry or mixed logging.
**Status:** implemented optional acyclic public composition.
**Package:** `github.com/frost-leo/fathomry/adapters/logging/zap/otel/v1`.

New borrows a generation-stable public OTel Client (direct Owner.Client or
Using with a Fixed resource). It refuses a telemetry Follow client: otherwise a
retained old logger would silently switch endpoints behind its frozen policy.
Zap's own Fixed/Follow compositions remain supported; each physical logger
construction supplies its explicitly selected stable telemetry destination.
StableDestination is only a routing fact, never readiness or lifetime ownership.
New copies the supplied client facade before retaining the borrowed capability;
overwriting the caller's wrapper cannot change routing or Runtime identity.
The returned sink, like every trusted structured dependency, must not itself be
overwritten or retargeted while borrowed.
Set Zap Settings.Structured=true and pass this
sink through Dependencies.Structured; select the remote threshold independently
of local outputs. No local output is needed for OTel-only, and local-only need not
import this package. Neither concrete provider imports the other provider.

Write translates the detached closed record directly, without reading local JSON
or invoking arbitrary callbacks. It preserves original message, severity, event
time, trace context, name/caller and correlation attributes. User values are under
attributes; logger facts are under logging. Null, empty maps/arrays and binary are
typed. Integer values do not pass through float64. uint64 beyond int64 and other
unrepresentable local data refuse only this destination; a later independent
legal record is still accepted. Zero original time uses OTLP's unknown timestamp,
not the bridge's wall clock.

A successful Write proves OTel local queue acceptance only. Export, protocol
receiver acknowledgement, backend persistence and evidence custody remain separate.
Logging and telemetry each emit independent results/evidence. Logging Sync is not
Flush, export retry or shutdown. There is no hidden worker, automatic retry or
latency isolation between synchronous outputs.

Zap.Open invokes CheckRuntime before source effects. A telemetry Client bound to
the same Runtime is refused even when its allowance happens to be large enough.
Use independent explicitly budgeted existing Runtime owners; no shared-runtime
child-composition alternative is claimed by this bridge.

## Required shutdown order

1. Stop producers and close/join retained log/slog families and all logging policy
   owners. Telemetry must still be alive and accepting calls during this step.
2. Stop/join the explicit managed export loop, then close telemetry for final
   bounded export and actual source cleanup.
3. Close the operation owners; seal/drain/ack independent evidence to completion.

A resource lease alone preserves existence, not admission after Runtime.Close.
Framework.Close does not infer this dependency order from declaration order.
Separate Framework ownership boundaries or an explicit aggregate owner must
enforce it. Receiver/exporter failures terminate out of band, never reentering
the same logging/evidence chain (synchronously or asynchronously).

[Protocol tests](../../../../../../../adapters/logging/zap/otel/v1/bridge_test.go)
independently decode local JSON and OTLP protobuf, including remote rejection
with local success, subsequent healthy records, exact integer/time/trace facts,
no unused file effects and unsafe shared-runtime refusal.
