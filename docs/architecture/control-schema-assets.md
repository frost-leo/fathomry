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

# Control-schema assets

[Documentation](../README.md) / Architecture

**Audience:** schema maintainers and Framework catalog implementers.
**Status:** implemented read-only asset boundary and isolated SQL verification;
the private schema/creation commands use the existing Resource and Adapter path.
Workflow catalog registration, bounded reads and selected comparison reuse these
assets without moving source ownership or installation into the catalog.

## Ownership and versions

The Fathomry control model has separate
[PostgreSQL](../reference/schema/fathomry/postgresql/v1/interface.md) and
[MySQL](../reference/schema/fathomry/mysql/v1/interface.md) asset packages.
Neither imports a database provider or performs service/configuration I/O.
Both expose `Assets() fs.FS` and pure read-only inspection SQL/normalization helpers.
The schema command and catalog reuse those dialect facts without importing each
other. There is no raw client, connection ownership, automatic migration,
schema-readiness authority or command dispatch in the schema tree.

Each dialect owns its exact SQL. The Go package `v1` compatibility line,
`fathomry.schema-manifest/v1` encoding, logical migration revision, database server
version, workflow label, mode revision and row revision are different axes.
The initial logical revision is `1` in both dialects; physical statement steps
and checksums differ. Once an asset is released/applied, corrections require a
forward migration rather than rewriting its history.

## Files and manifest

`manifest.json` identifies format, `fathomry.control` family, dialect, qualified
service profile, documentation locales and ordered migrations. Each migration
has a positive revision, name, transaction contract and ordered steps. Each step
has a stable ID, relative path and lowercase SHA-256 of the complete original file.
Hashes cover exact UTF-8 bytes, including comments and line endings, before
target substitution. Manifest order is authoritative; directory scans are not.

The manifest also names one technical `bootstrap/fathomry_schema_history.sql`
statement and a derived `0001/structure.json` snapshot, each checksummed. The
snapshot binds the original SQL source identities/digests and is verified against
fresh actual installation; it is not another maintained DDL definition. The
journal is technical history, separate from the nine business tables.

Each `0001/<table>.sql` owns one table definition and all its database comments.
PostgreSQL uses one static `DO $catalog_table$` block with CREATE TABLE and COMMENT
statements. MySQL uses one CREATE TABLE with inline COMMENT clauses. Both therefore
execute as one complete server statement without a client SQL parser.
Three circular default FKs are explicit later `defaults/<table>.sql` steps.
Immutable guards, PostgreSQL's supporting function/index and English seed are
separate named statements, not duplicated table definitions.

Every table and column has a fixed combined `en: ...; zh-CN: ...` comment.
`documentation_locales` records these documentation languages. These are
versioned database metadata, not runtime locale selection or editable
`i18n_resource`/`i18n_text` rows. A future documentation language needs a
reviewed forward schema change; no process rewrites historical comments at startup.

## Target binding and execution obligations

The only substitution token is `{{target}}`, representing an existing PostgreSQL
schema or MySQL database. The token is an identifier, never an SQL value. No
database provisioning, SQL-client DELIMITER, untrusted external SQL, IF NOT EXISTS
adoption or semicolon splitting is supported.

Consumers must validate identifier length/encoding, reject NUL/control bytes,
then use the dialect's identifier quoting. PostgreSQL targets must additionally
exclude `$catalog_table$`: ordinary identifier quoting cannot protect the
surrounding dollar-quoted table block. A string replacement without this lexical
validation is unsafe. Portable workflow Namespace rules are a different boundary.
The assets do not supply a general-purpose template renderer.

PostgreSQL's initial steps are transaction-compatible. MySQL DDL implicitly
commits, and its comments/guards do not make the entire revision atomic.
The [restricted command helpers](../reference/cmd/fathomry/internal/command/schema/interface.md)
owns the actual retained target/source generation, cross-process
lock, checksum/history checks, bounded native work, required evidence and truthful
unknown outcomes. Its caller still owns source/runtime/reception and scratch/copy
capacity; the [CLI entry](../reference/cmd/fathomry/v1/interface.md)
supplies that composition through Resource bindings, Adapter Using clients and
the existing Framework runtime/receivers. No separate database manager or public
Framework schema package is introduced.
Reading or executing these files alone does not establish
`current` schema status or authorize registration. Unmanaged/incomplete state must
not be silently adopted. A structural checksum is not origin attestation.

## SQL invariants and remaining application responsibilities

The nine tables implement definition → version → version-owned mode → revision,
with composite FKs for same-owner default/category choices. NULL defaults are
unconfigured, not latest. New execution-related entities are disabled; categories
initially permit new associations. Exact admitted keys preserve case/trailing bytes.
Locale uniqueness is case-insensitive; English cannot be disabled/deleted.

Execution/control documents use exact binary bytes with 4 MiB / 1 MiB CHECK bounds;
digests require exactly 32 bytes and are not unique identities. Immutable-update
guards cover ownership, labels, creation facts and document identity/content.
Mutable management revision/time fields are not automatically advanced by DDL.

SQL constraints do not replace the declaration validator, canonical/digest
verification, resource ownership, required English completeness, reserved tag rules,
64-tag aggregate serialization, publisher authorization or management CAS.
MySQL writers require strict SQL mode and UTC sessions. Minimum privileges and
supported API paths are necessary; a privileged administrator can change guards,
delete data or fabricate history. No runtime, Run acceptance or Worker readiness
is implemented by these tables.

## Executable evidence

[Asset tests](../../schema/fathomry/assets_test.go) check manifest checksums,
inventory/parity, pure dependencies, detached concurrent reads and exact comment
coverage. [Opt-in SQL tests](../../schema/fathomry/sql_test.go) and their
[semantic cases](../../schema/fathomry/sql_cases_test.go) use public database
Adapters and Framework evidence reception against isolated PostgreSQL 18.6 and
MySQL 8.4.11. They verify all 9 table and 74 column comments by database read-back,
ownership/default constraints, immutable guards, bounds and explicit ablations.

`go test -race -count=1 ./schema/...` needs no services.
The `schema_service` build tag deliberately requires explicit
`FATHOMRY_SCHEMA_ALLOW_DDL=1` and private, mode-0600 public-Settings JSON fixtures
via `FATHOMRY_SCHEMA_POSTGRES_CONFIG` and `FATHOMRY_SCHEMA_MYSQL_CONFIG`.
Only loopback plaintext `catalog_test` databases are admitted. Tests create/drop
unique test-owned schemas/databases; never use existing business data.

Server-generated large BLOB size controls prove SQL CHECK bounds, not transport.
Separate [document fixtures](../../schema/fathomry/document_test.go) and
[transport tests](../../schema/fathomry/transport_test.go) compile and reconstruct
an exactly 4 MiB valid specification and the current maximum reachable mode:
4,096 used controls, 64-byte keys and 340,033 canonical bytes. The current grammar
cannot express a 1 MiB valid mode; its declared admission cap remains unchanged.
A 45-mode aggregate occupies 16,575,179 bundle bytes; adding another full-size mode
is rejected. Both isolated engines pass per-document SQL, acknowledged aggregate
commit, exact read-back and shared declaration reconstruction with incremental
Framework evidence reception. Existing database defaults remain unchanged; these
tests opt into explicit larger argument/protocol/result budgets.

[Retained-session tests](../../schema/fathomry/session_test.go) verify actual MySQL
connection/lock ownership across implicit-commit DDL, cancellation and server-side
connection loss without automatic replacement. [PostgreSQL transaction tests](../../schema/fathomry/transaction_test.go)
roll back all 25 initial statements together, then commit them while retaining the
transaction-scoped lock. They also demonstrate that acquiring an advisory lock
does not refresh an existing repeatable-read snapshot: installers must recheck
state using a visibility strategy that actually observes the preceding owner.
These are bounded prerequisites. Separate runner tests now qualify initial
installation, read-only status/plan, history/structure refusal and actual lost-reply
outcomes. They do not establish public catalog APIs, aggregate metadata/output
policy, measured RSS or independent-project CLI acceptance; those gates remain open.
