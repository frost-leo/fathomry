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

# Explicit control-database creation

[Documentation](../../../../../../../README.md) / Private command reference

**Audience:** project operators and CLI maintainers.
**Status:** implemented bounded creation through Resource-backed public Adapters.
Account creation, grants, repair and workflow catalog operations are not included.
**Package:** `github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database/create`.

## Invocation and ownership

Run `./project database create --admin-config /protected/admin.yaml`.
The project's runtime configuration supplies the target database, dialect, transport
and trust settings. The explicit admin file supplies only `user`, `password` and
`maintenance_database`; JSON, YAML and TOML are strict, closed inputs. It cannot
redirect the endpoint or change transport policy. PostgreSQL requires an existing
maintenance database distinct from the target. MySQL uses schema-less admin access.

The [shared CLI entry](../../../../v1/interface.md) loads the same application's
configuration, registers fixed target/admin Handles in the existing Resource scope,
and supplies Adapter Using clients. Neither the command nor its SQL executor owns
a pool or selects another global configuration. Construction is local and lazy.
Creation does not run on ordinary startup, help, vet or build.

Shared control-database admission/diagnostic contracts live in the parent database
command package, not in the schema executor. The CLI entry assembles the create
leaf, keeping the parent independent of this executor. Creation failures retain
the `database.create` operation context and the existing stable error identities.

A Creator borrows the two clients for at most one attempt. Its Close joins retained
SQL only; Resource owns source cleanup. Copies share the one-attempt state. Timed-out
cleanup retains the same handle; cancellation does not prove absence of effects.

## Supported behavior

Only the separately checksummed provisioning assets under the dialect asset roots
are accepted. PostgreSQL 18 uses template0, UTF8 and builtin C.UTF-8 locale, assigns
the selected runtime role as owner, then verifies target access and creates/verifies
the selected schema. MySQL 8.4 verifies its writable 16 KiB-page profile and creates
the target with utf8mb4/utf8mb4_0900_bin, then checks access using target credentials.

Existing databases are refused, not adopted. PostgreSQL duplicate classification
includes SQLSTATE 42P04 and only the specific pg_database name-index 23505 case;
unrelated unique-constraint failures are not duplicates. No IF NOT EXISTS,
automatic replay, account/grant changes, drop, repair or resume is performed.

Zero operation/cleanup durations mean 45 seconds/5 seconds; explicit values accept
1 millisecond through 1 minute. SQL calls, metadata rows/bytes, asset size and
operation work remain bounded. The CLI entry reserves existing Adapter capacity
and the shared workspace; this is not an RSS or server-wide quota.

## Results and failures

The report retains these independent facts alongside an error:

- `effect`: physical database creation, one of not_started/unchanged/created/unknown.
- `schema_effect`: selected schema creation, also allowing not_applicable for MySQL.
- `stage`: admission/preflight/database/existing/target/schema/ready.
- `ready`: target access/profile established, not installed tables or Worker readiness.
- `cleanup_complete`: retained SQL settled, not full invocation cleanup or output delivery.

An acknowledged CREATE survives later target-authentication, schema, cleanup or
output failure. A schema COMMIT acknowledgement learned from the original retained
receipt during cleanup also preserves schema effect/readiness after caller
cancellation; the cancellation error remains. A lost reply is unknown; no subsequent
automatic probe rewrites
the original acknowledgement. Known effects and the original failure are retained
by the CLI's ExecutionError and mutation-report path.

Run `schema status`, `schema plan`, and explicit `schema apply` separately after
successful creation. See the [restricted schema commands](../../schema/interface.md).

## Evidence and limits

[Creation tests](../../../../../../../../cmd/fathomry/internal/command/database/create/creation_test.go)
exercise identifier quoting, non-recursive rendering, protected targets and narrow
duplicate classification. The CLI's tagged
[service tests](../../../../../../../../cmd/fathomry/v1/creation_service_test.go) and
[lost-reply tests](../../../../../../../../cmd/fathomry/v1/creation_fault_service_test.go)
cover both engines, duplicate/concurrent creators, target-auth failure and lost
database/schema-commit acknowledgements, plus delayed COMMIT receipt transfer under
cancellation for both selected and default PostgreSQL schemas. Fixtures require
explicit isolated DDL
authority. These are not qualification of Kubernetes, production TLS or a Worker host.
