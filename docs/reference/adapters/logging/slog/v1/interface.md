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


# Restricted owned slog ingress

[Documentation](../../../../../README.md) / Public Adapter reference

**Audience:** applications using Framework ErrorLog or bounded structured slog.
**Status:** implemented restricted gateway; not unrestricted standard Handler compatibility.
**Package:** `github.com/frost-leo/fathomry/adapters/logging/slog/v1`.

## Construction and ownership

Concrete providers select explicit Limits, exact supported numeric levels, timeout,
message ceiling, concurrent preparation stacks, cumulative view count and retained
bytes. `Binding` supplies trusted bounded concurrent Enabled/Validate/Emit callbacks
and an optional `resource.ReleaseResult` callback for a retained provider view.
These callbacks are assembly authority, never loadable configuration or arbitrary
value formatting. No new operation Runtime, worker, retry queue or global logger
is created.

The original slog context supplies association/values (including trace context),
but its deadline/cancellation does not suppress the attempt. Each synchronous
attempt instead uses the explicit owner lifetime and selected bounded timeout.
Direct provider Log deliberately retains caller-owned cancellation.

`Handler.Close` cancels the family, joins actual ingress/callback stacks and
releases its retained view. All clones share that authority. A timeout is not
release; repeat Close and inspect ShutdownComplete. The gateway never closes a
source, exporter, borrowed output or Runtime. Uncooperative callbacks cannot be
forcibly stopped and keep their real ownership until return.

## Accepted and rejected data

Original Record.Time and return PC are passed unchanged; zero means absent.
WithAttrs fields are frozen at their then-current group path. Later WithGroup
does not move earlier attributes. Native provider filtering is advisory through
Enabled; actual admitted per-output states still belong to provider evidence.

Built-in slog scalar/group data, explicit closed logging.Value and trusted public
failure/i18n diagnostics are admitted within bounds. Groups preserve chronology;
duplicate or colliding keys refuse rather than emit ambiguous JSON. Unknown
dynamic Any/error/LogValuer/Stringer/marshaler inputs refuse without invoking them.
This gateway does not call Resolve to repair unrestricted compatibility.
Empty names/attributes follow the documented standard no-op/inlining cases;
explicit closed empty arrays/maps/null remain representable.

WithAttrs/WithGroup cannot return errors, and slog.Logger discards Handle errors.
Invalid derivations therefore return bounded reusable poisoned handlers, never
valid subsets. `Status` provides shared saturating handled/admitted/refused/failed/
invalid-derivation counters, bounded last error, active work and retained usage.
Unknown severities and invalid/closed views return true from Enabled so Handle
can record refusal. Pre-admission/evidence-capacity failure creates no fabricated
provider receipt. Later success never erases LastError.

Cumulative derivation limits are intentional: keep static derived loggers and use
per-event attributes for unbounded traffic. Closing a family does not revive a
poisoned derivation or reclaim an independently retained clone as another owner.

See [data](../../v1/interface.md), [Zap gateway](../../zap/v1/interface.md) and
[implementation/tests](../../../../../../adapters/logging/slog/v1).

