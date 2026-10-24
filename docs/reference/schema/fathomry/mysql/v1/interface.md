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

# MySQL schema assets interface

[Documentation](../../../../../README.md) / Public package reference

**Audience:** schema-command and catalog implementers.
**Status:** immutable asset access and pure inspection helpers are implemented;
execution belongs to their consumers, not this package.
**Package:** `github.com/frost-leo/fathomry/schema/fathomry/mysql/v1`.

## Capabilities and ownership

`Assets()` returns a read-only, concurrently usable standard-library filesystem
containing `manifest.json`, technical journal SQL under `bootstrap/`, and the
initial revision plus derived structure snapshot under `0001/`.
Reads return detached bytes. Importing the package or reading its assets performs
no configuration, database or other service work.

`InspectionQueries()` returns detached bounded metadata SELECT statements and
their object kinds; each binds the target database as its sole `?` argument.
Consumers additionally inspect the discovered owned base tables with SHOW CREATE.
`NormalizeInspection` removes only the mutable table AUTO_INCREMENT counter.
Use NO_BACKSLASH_ESCAPES or reject inputs containing backslashes before normalization;
quoted counter-like text is not rewritten. No helper executes SQL, changes session
settings, owns a lock or independently proves compatibility.

The [asset contract](../../../../../architecture/control-schema-assets.md) owns
manifest/version semantics, file naming, checksums, explicit target binding and
complete fixed English/Chinese table/column documentation.

## Caller obligations and limits

The current profile is MySQL 8.4/InnoDB, 16 KiB pages, DYNAMIC row format and strict
SQL mode. Writers establish UTC. Machine keys use byte-exact storage; human text
uses UTF-8. One table file is one CREATE TABLE statement with inline comments.
Trigger bodies are complete server statements without SQL-client DELIMITER
directives. Never split SQL at semicolons.

`{{target}}` requires a validated, quoted existing database identifier. The package
has no template renderer, native handle, pool, session or execution authority.
Consumers verify original bytes/checksums and own bounded physical-session locking,
journal state, independent evidence and cleanup. MySQL implicit-commit DDL means
that a failed initial installation can leave partial effects. These assets do not
authorize automatic resume, adoption, force repair or destructive rollback.

## Results and verification

Filesystem errors follow `io/fs`; inspection construction can also report embedded
JSON decoding errors. No new runtime error facility is introduced.
[Cross-dialect asset tests](../../../../../../schema/fathomry/assets_test.go) verify
integrity and comment parity. Opt-in isolated tests execute through the public
MySQL Adapter and read back every database comment. They do not implement or
qualify retained-session migration coordination or full catalog registration.
