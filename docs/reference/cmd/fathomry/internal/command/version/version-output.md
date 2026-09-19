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

# CLI version JSON output

[Documentation](../../../../../../README.md) / [Version command](interface.md) / Machine output

**Audience:** scripts consuming `fathomry version --output=json` or root `--version --output=json`.
**Status:** implemented initial transport profile, `fathomry.cli.version/v1`; no release authenticity claim.

The command emits one UTF-8 JSON object followed by one LF on successful
stdout. It does not serialize `version.Build` or its guarded in-process records.
Errors use stderr and a nonzero status, not a JSON error document. A failed
write may leave a prefix; consumers must check both process status and JSON
parse success.

Required top-level members are `schema`, `program` (`fathomry`), `metadata`,
`application`, `framework`, `source`, `build` and `declaration`. `metadata` has
`available` (boolean) and `origin` (`runtime|supplied|unknown`). Ordinary
executable self-inspection uses `runtime` even when metadata is absent.

`application` and `framework` each contain `present` (boolean), `path` and
`version` facts, and `replacement` (null or an object containing `kind`
`local|module`, `path` and `version` facts). `present=false` means selected
metadata was unavailable, not proof the module was absent from source. A local
replacement path is always redacted. Original selected module version and
replacement version are different facts.

A fact has exactly `state` and `value`. `reported` and `requested` carry a
nonempty string; `unknown`, `redacted` and `development` carry JSON null.
`requested` identifies an unavailable selected module path. `source` contains
native main-source `vcs` and `revision` facts, `tree`
(`unknown|clean|dirty`) and `commit_time` (UTC whole-second timestamp or null).
`build` contains `go` and `target.os`/`target.arch` facts; missing target data
is not supplied from the reporter's host.

`declaration` is null when no linker declaration exists. Otherwise it has
`origin` (`linker|caller`), `release`, `revision`, `tree` (`clean|dirty`) and
`build_time` (UTC whole-second timestamp). The actual executable uses `linker`
origin. All four declared values must be present and valid; partial, invalid
or conflicting declarations fail before emitting success JSON. A declaration
does not replace native module/source facts or authenticate a release.

The initial report emits no dependency inventory, raw build flags, local paths,
environment, hostname, working directory, current time or update check.
Language selection changes no JSON field or value. Object member order and
whitespace are not a reader contract. Readers must check `schema`, require all
listed members, reject unknown fact states as unknown rather than success, and
may ignore additive unknown members. A changed meaning, type or required field
needs a new profile or an explicit compatibility decision. This schema versions
only the CLI projection, not workflow or framework release formats.

See the public [version contract](../../../../../version/interface.md) for
the underlying fact and declaration semantics.
