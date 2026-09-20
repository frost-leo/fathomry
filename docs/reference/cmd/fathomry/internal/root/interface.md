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

# CLI root composition interface

[Documentation](../../../../../README.md) / Executable-private package reference

**Audience:** maintainers adding approved Fathomry executable commands.
**Status:** implemented root/help/version/new composition; no query, control or application runtime.
**Package:** `github.com/frost-leo/fathomry/cmd/fathomry/internal/root`.

`Run` delegates process-independent execution to [CLI mechanics](../cli/interface.md).
`New` constructs a fresh root with explicit `--lang`, root-local `--version` and
version-only `--output`, then registers the real
[version command package](../command/version/interface.md) and
[project creation command](../command/project/interface.md). Construction opens no
configuration, network connection or Provider. `main` owns OS streams/signals
and final exit. The dependency direction is `main → root → cli + command packages`;
the common CLI mechanics package never imports a concrete command. A separate,
test-only nested command package proves independent registration, help,
execution-time acquisition and cleanup without shipping a placeholder query.

The root with no args and `-h`/`--help`, `help`, `help version` and
`version --help` show resource-backed help on stdout without version inspection.
`version` and root `--version` call one report operation. Root `--output` without
a true version request is usage error. Cobra's visible completion generator and
hidden `__complete`/`__completeNoDesc` paths are excluded; `-v` is unassigned.
Syntax errors precede help; for a resolved command, explicit help precedes
semantic execution validation. Repeated scalar flags use native last-value
semantics, and `--` ends option parsing. Unknown root or group command names
remain usage errors even when followed by `--help`; visible command groups
derive their child list from the registered tree.

When adding a later runtime command family, first establish its public framework
operation and evidence contract in its own approved issue. Creation is command-owned
tooling, not a public framework operation. Give each family a
cohesive private package that owns typed options, help resources, output schema
and lazy dependency/cleanup behavior; register it explicitly in `New`. Do not
put its business/query semantics in `cli`, create a global service locator or
claim a uniform query protocol because the root can parse a subcommand.

See the [checkout build guide](../../../../../development/build-cli.md) for
the shipped program and verification limits. Source:
[`cmd/fathomry/internal/root`](../../../../../../cmd/fathomry/internal/root).
