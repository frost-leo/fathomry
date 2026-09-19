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

# CLI execution interface

[Documentation](../../../../../README.md) / Executable-private package reference

**Audience:** maintainers composing Fathomry CLI commands.
**Status:** implemented private command execution mechanics, not a public CLI SDK or business runtime.
**Package:** `github.com/frost-leo/fathomry/cmd/fathomry/internal/cli`.

## Ownership and call sequence

`Run(ctx, args, in, out, errOut, build)` calls `build` with a fresh
`Invocation`, executes the returned Cobra tree, presents at most one diagnostic
and returns a status. `NewRoot` supplies only the bare root's locale/help and
hidden-completion policy. The [root package](../root/interface.md) explicitly
registers shipped commands; the [version command](../command/version/interface.md)
owns its facts, flags, resources and schema. This package imports neither the
version command nor concrete Providers.

Callers own Context and all three streams. Nil Context, stream or builder is a
local execution failure, not permission to read ambient OS state. Nil and empty
provided argv both mean zero args; `Run` passes an independent, explicitly
non-nil slice to Cobra. Streams are borrowed, not closed. One `Invocation` is
not concurrently reusable; separate `Run` calls have independent flags,
resources, help metadata, streams and locale.

The executable, not this package, owns signal registration and final `os.Exit`.
The runner passes caller Context to Cobra. Commands must finish their own work
and cleanup before returning an error; post-run hooks are not a finally block.
Cancellation does not prove an external effect never occurred, and the runner
cannot forcibly unblock an arbitrary callback or Writer.

## Help, output and errors

`Invocation.Add` attaches a command to Cobra's **actual** tree and associates
only human-resource IDs, not a second routing table. Root help lists registered
visible children; group help lists its registered visible children; `help`
resolves nested targets in that tree. The root prepares only root resources,
and each child summary/help uses that command's own catalog. It does not
aggregate every command's resources into one `i18n.Prepare` call; the public
catalog's 64-file limit therefore does not turn a growing command tree into
an unrelated help/version failure. English and
Simplified Chinese headings, summaries, flags, hints and diagnostics come from
external i18n resources. A valid unsupported locale falls back to English.
The locale grammar is checked without preparing a human catalog on successful
machine output. No ambient locale is used.

The runner's checked stdout Writer turns both explicit errors and short writes
without errors into a nonzero outcome, even if a command ignores the returned
write error. Diagnostics are written to stderr only; a failed diagnostic write
does not recursively report or erase the primary status. Parser failures use
safe generic wording rather than echoing raw args or native English errors.
Unknown commands followed by `--help` remain usage failures, not root-help
success. Unclassified run/pre/post-hook errors are execution failures rather
than misreported usage; command-owned semantic usage must explicitly return
`Usage`. A command-specific failure without an available translation uses the
generic resource-backed execution diagnostic, not raw native text or a
machine-code string in the human channel.
At most 128 argv elements and 64 KiB aggregate valid UTF-8 argument bytes are
accepted; ASCII controls are refused. The diagnostic limit is 4 KiB. These
limits start after the OS has supplied argv, not before allocation.

The shared runner does **not** cap all stdout to version's 16 KiB or impose one
JSON document on later query commands. `WriteFinite` is an opt-in bounded
document helper; future streaming commands own their schema, bounds and
completeness protocol. A failed pipe can contain a partial prefix, so consumers
must check status and the command-specific machine record.

Initial statuses are 0 success, 2 usage, 1 execution/render/output/setup and
130 canceled Context. The [process shell](../../../../../../cmd/fathomry)
maps supported Unix SIGTERM to 143 and explicitly handles SIGPIPE as an output
error. After the first SIGINT/SIGTERM cancels Context, its signal registration
is restored so a subsequent signal can terminate an uncooperative blocked
Writer/command by native process behavior; that forced exit does not promise
ordinary cleanup. These numbers do not define future business/query effect
semantics.

Source: [`cmd/fathomry/internal/cli`](../../../../../../cmd/fathomry/internal/cli).
