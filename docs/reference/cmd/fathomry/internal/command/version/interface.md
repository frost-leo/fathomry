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

# Offline version command interface

[Documentation](../../../../../../README.md) / Executable-private command reference

**Audience:** CLI users, script authors and maintainers of the first command family.
**Status:** implemented local software-version report; no workflow/mode version, release authentication or remote compatibility check.
**Package:** `github.com/frost-leo/fathomry/cmd/fathomry/internal/command/version`.

`New` constructs a fresh Cobra `version` command with its own `--output`
selection, help IDs and embedded English/Simplified Chinese resources. `Report`
is shared by the subcommand and root `--version`; it validates output and locale
before calling public `version.Inspect` **only when executed**. Invalid,
partial or conflicting linker declarations fail the report, not help. An
unstamped build is valid and retains native facts. The report never runs Git,
reads CI variables, discovers host target or uses the current clock to fill
unknowns. A declaration is an application claim, not a signature or official
release certificate.

Text reuses [`version/presentation`](../../../../../version/presentation/interface.md)
for the software/build summary and command-owned resources for native module,
source and declaration details. English is the deterministic default;
`--lang=zh-CN` selects Simplified Chinese, and valid unsupported tags use the
English fallback. Human language does not change the
[JSON profile](version-output.md) or the locale input validity rule.

`--output text|json` belongs only to version surfaces; there is no global
query output promise. The command prepares at most 16 KiB, writes once to the
runner's checked stdout and returns the write failure if one occurs. JSON is
one object plus LF; errors are bounded stderr text with nonzero status. A
failed write may leave a prefix, not an atomic result.

The command package owns only CLI projection and wording; the public
[version contract](../../../../../version/interface.md) owns normalization,
validation and declaration semantics. Later query/control command families
belong in their own packages, not alongside this command's implementation.

Source: [`cmd/fathomry/internal/command/version`](../../../../../../../cmd/fathomry/internal/command/version).
