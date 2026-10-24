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

# PostgreSQL schema assets interface

[Documentation](../../../../../README.md) / Public package reference

**Audience:** schema-command and catalog implementers.
**Status:** immutable asset access and pure inspection helpers are implemented;
execution belongs to their consumers, not this package.
**Package:** `github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1`.

## Capabilities and ownership

`Assets()` returns a read-only, concurrently usable standard-library filesystem.
It contains `manifest.json`, the technical journal under `bootstrap/`, and the
PostgreSQL initial revision plus derived structure snapshot under `0001/`.
Every read returns detached bytes; modifying a returned slice cannot modify later
reads. Imports have no database, configuration, logging or process-global effects.

`InspectionQueries()` returns detached bounded SELECT statements and their object
kinds, tied to the embedded structure profile. Each binds the target name as `$1`.
`NormalizeInspection` converts only qualified target identifiers in inspected SQL
to the snapshot placeholder; quoted values, aliases and function bodies are retained.
These helpers share dialect facts between schema commands and catalog admission.
They neither execute SQL nor establish readiness, locks or a compatible session.

The [asset contract](../../../../../architecture/control-schema-assets.md) owns
manifest/version semantics, file naming, target validation, checksums and the
complete fixed English/Chinese table/column documentation rule.

## Caller obligations and limits

The current profile is PostgreSQL 18 with UTF-8. A table file is one static DO
statement containing table creation and comments; do not split it at semicolons.
Each remaining file is one reviewed complete statement. `{{target}}` requires
a validated, quoted existing schema identifier; reject control bytes and
`$catalog_table$` before substitution. There is no renderer, pool or SQL executor
in this package.

Consumers must verify exact original-file checksums and ordered steps, then own
bounded execution, transaction-scoped migration exclusion and durable history.
No schema status, automatic repair, rollback, compatible upgrade or authenticity
claim follows from filesystem access.

## Results and verification

Read errors follow `io/fs`; inspection construction can also report embedded JSON
decoding errors. There are no package-specific runtime errors.
The [cross-dialect tests](../../../../../../schema/fathomry/assets_test.go) verify
asset integrity, isolation and complete comment parity. Opt-in isolated SQL tests
exercise the assets through the public PostgreSQL Adapter, not a migration runner.
