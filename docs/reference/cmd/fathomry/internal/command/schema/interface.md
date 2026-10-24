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

# Restricted schema runner interface

[Documentation](../../../../../../README.md) / Private command reference

**Audience:** CLI maintainers and schema qualification authors.
**Status:** implemented initial installation and bounded state inspection through
Resource-backed public Adapter clients. Workflow catalog registration and queries
are separate business capabilities and are not implemented here.
**Package:** `github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema`
(private to the CLI; independent projects use their generated executable).

## Responsibility and call sequence

This runner executes only the reviewed
[control-schema assets](../../../../../../architecture/control-schema-assets.md).
It accepts no arbitrary SQL, filesystem, callback, migration directory or native
connection. Constructors `PostgreSQL` and `MySQL` borrow a public client and an
explicit existing physical target. They validate embedded assets and identifier
spelling without service I/O or schema creation.

Common control-operation bounds and physical-target admission are supplied by the
private command/database contracts, which also own stable database-control failures
and their English/zh-CN resources. This schema package owns only its execution and
presentation; creation does not import it to obtain those common contracts.

The CLI entry binds source Handles in Resource, constructs Adapter Using clients,
receives released evidence through the existing Framework receiver, and reserves the
runner's scratch/copy budget. It can then call Status, Plan or Apply. Close joins
only runner-owned retained work; it cannot close the borrowed client, source,
receiver or runtime. There is no automatic application on construction or reads.

A completed Status is not a reusable business-write admission token. The catalog
writer verifies current state against its own retained generation and
physical target, not cache this result across a Follow replacement or later call.

Each operation pins one original source generation and connection. PostgreSQL
uses a read-committed transaction and transaction-scoped advisory lock; MySQL uses
the bounded retained Session, not a transaction ended by DDL. Lock identity uses
the stable family plus actual database and selected target, never source alias,
binary version or checksum. A competing installer causes immediate changing/refusal.
State is rechecked under exclusion; acquiring a lock does not refresh an older
repeatable-read snapshot.

Status/Plan make no **persistent** changes, including on an empty target. They do
establish transaction/session settings and acquire transient exclusion. Plan is
an observation, not a reservation for a later Apply. Apply installs only positively
empty state or returns a verified current no-op. No force, baseline, automatic
resume, repair, downgrade or arbitrary future upgrade is supplied.

## State and history

| State | Meaning |
| --- | --- |
| `not_installed` | No owned objects or journal were observed |
| `current` | Terminal matching history, English seed and all bounded owned-invariant checks agree |
| `incompatible` | Unsupported server/target profile or unknown family/revision/step |
| `drifted` | Proven history disagrees with manifest/step hashes or owned structure |
| `incomplete` | Empty, missing, inconsistent or nonterminal journal rows |
| `unmanaged` | Same-name objects lack proven history, or journal structure is unproven |
| `changing` | Exclusion is busy or repeated structural observations disagree |
| `indeterminate` | An observation could not be completed; not proof of an empty database |

The journal is the separate technical table `fathomry_schema_history`, with a
table-named bootstrap SQL file and full en/zh-CN comments. Its key is logical
migration revision plus ordinal. Ordinal zero records migration intent/completion;
positive ordinals record statement intent/acknowledgement. Family, manifest hash,
step identity/hash and timestamps are explicit. Repetition changes no rows or
timestamps. PostgreSQL keeps journal and initial DDL in one transaction; MySQL
records intent before each statement but cannot make that statement atomic with
the journal update. Bootstrap itself can lose its acknowledgement.

The current line implements revision 1 only. A valid-looking partial MySQL table
set is not current and is never resumed or adopted. Read-only diagnosis preserves
evidence; restoration/repair needs separate approval and tooling. No privileged
origin attestation is promised: an administrator can fabricate consistent history
and objects.

## What structure checking covers

The manifest binds a derived `0001/structure.json` snapshot to exact bootstrap
and migration SQL source digests. Fresh-install tests compare actual structures
to it before terminal history is recorded. It is generated evidence, not a second
hand-maintained DDL source.

- PostgreSQL: relation kind/persistence, RLS and partition properties; column
  type/order/default/collation/identity/generated/nullability/comments; constraint
  validation/enforcement/definition; index readiness/validity/uniqueness; identity
  sequence ownership/options; user guards and internal FK-trigger enablement;
  guard-function definition; unexpected inheritance and rewrite rules.
- MySQL: owned table definitions including columns, keys, checks, FKs, comments,
  engine/row format and table options; constraint enforcement/name collisions;
  trigger attachment/order/body, stored SQL mode and connection character settings.
- Same-name reserved objects on other owners also refuse admission. Unrelated
  differently named objects are not a general schema-diff target.

PostgreSQL target normalization applies only at reconstructed qualified-identifier
positions, not string/dollar literals or OLD/NEW row references. MySQL comparison
ignores the mutable AUTO_INCREMENT counter, not quoted comment values. Account
definer identities, physical placement, external permissions and administrative
origin are not authenticated by these checks. This is a conservative known-manifest
check; arbitrary compatible schema customizations are not silently adopted.

## Qualified profiles and privileges

Real-service qualification uses PostgreSQL 18.6 and MySQL 8.4.11 on isolated
plaintext loopback fixtures, not production, TLS service DDL, HA or failover.
The supported target is one writable database; provisioning is outside the runner.

PostgreSQL requires UTF-8, primary/writable service and normal replication-role
enforcement. The selected existing schema needs USAGE; initial application also
needs CREATE and PL/pgSQL USAGE. Reads of installed journal/seed data require SELECT.
Owned transactions establish pg_catalog search path, UTC and standard string
semantics. The database capability resets returned connections before reuse.

MySQL requires 8.4/InnoDB, 16 KiB pages, lower_case_table_names=0, autocommit and
foreign-key/unique checks enabled, and a writable server. DATABASE() must match the
explicit selected database. The retained session establishes UTC, utf8mb4 binary
connection collation, quoted/verbose structure rendering and a fixed strict,
no-backslash-escape SQL mode. It is physically retired rather than pooled afterward.

MySQL metadata visibility itself needs TRIGGER privilege. The runner proves
direct global/schema SELECT and TRIGGER grants before trusting an empty result.
Role-only, wildcard matching and partial-revoke inference are not guessed.
Initial Apply additionally requires CREATE, ALTER, INDEX, REFERENCES, INSERT and
UPDATE, plus the binary-log trigger-creation prerequisites where applicable.
It never creates accounts, changes grants or alters global server settings.
A read-only current no-op does not require new DDL rights. This operator path does
not qualify a least-privilege business reader lacking trigger visibility.

## Ownership, budgets and failure facts

Operations refuse overlap on one runner. Default operation lifetime is 45 seconds;
default cleanup wait is 5 seconds, each admitting 1 ms–1 minute. A waiting deadline
does not certify native release. Incomplete cleanup stays reachable through Close
on the same runner; new work is refused while it is pending. Close seals new work
and refuses an overlapping active method rather than silently aborting its SQL.
Actual independent cleanup failures remain observable on repeated Close.

The current closed profile admits at most 48 statement assets, 1 MiB total SQL,
256 public SQL calls, and 512 objects / 1 MiB metadata per structural observation.
The statement cap leaves room for intent/acknowledgement, profile and inspection
calls before any mutation. SQL source files remain at most 64 KiB each.
These are not wire-packet counts: native preparation/framing/cleanup have their
existing provider-owned budgets.

The composition owner reserves `WorkBytes` (8 MiB) plus two selected-source
evidence envelopes for detached native-row conversion, including oversized data
rejected here, and its final report/output custody. Existing database budgets,
source generations and required evidence remain separately owned. These are
declared bounds, not a measured RSS/throughput claim. Schema operations reserve
their own workspace without acquiring a workflow Catalog or its aggregate-document
capacity.

Change reports preserve not_started, unchanged, applied, acknowledged rolled_back
and unknown separately. A successful per-step reply is not final application.
After a lost terminal reply, a later Status may report current but does not
acknowledge the original attempt. Acknowledged application remains available even
when subsequent observation or cleanup fails, including acknowledgement learned
from the original retained receipt during cleanup after caller cancellation. The
cancellation error remains observable. CleanupComplete is **local** release,
not instantaneous remote lock release or proof of absent effects.

Returned status/plan/change values are detached. Reasons/step IDs are bounded
machine data, not raw SQL errors, credentials, target inventories or payload
differences. Shared control-command failure codes preserve inspectable lower causes and
compose with the CLI failure/i18n catalogs. Runtime handles refuse serialization.

## Executable evidence

- [Unit/fuzz tests](../../../../../../../cmd/fathomry/internal/command/schema/schema_test.go):
  asset integrity, identifier/literal boundaries, privilege proof, finite budgets,
  copying/local ownership and redacted failures.
- [State/service tests](../../../../../../../cmd/fathomry/internal/command/schema/state_service_test.go):
  empty read-only status/plan, repeat/no-op history, comments, stale/future/unmanaged
  states, removed keys/checks/guards, disabled internal FK enforcement, inheritance/
  rule ablations, competing aliases, target/profile refusal and actual restricted
  MySQL metadata visibility.
- [Reply-loss tests](../../../../../../../cmd/fathomry/internal/command/schema/fault_service_test.go):
  every MySQL bootstrap/asset acknowledgement dropped after actual server success;
  PostgreSQL lost DDL/commit replies; MySQL lost terminal journal reply; acknowledged
  commit plus failed cleanup reply with persistent cleanup evidence; actual commit
  acknowledgement learned during cleanup after caller cancellation.
- [Fixture and explicit snapshot generator](../../../../../../../cmd/fathomry/internal/command/schema/service_test.go).
  Generation writes only to an explicitly selected disposable build directory;
  reviewed snapshots enter source separately. No startup regeneration.

Run `go test -race -count=1 ./cmd/fathomry/internal/command/schema` offline.
The `schema_service` tag, `-run '^TestSchema'`, explicit
`FATHOMRY_SCHEMA_ALLOW_DDL=1` and private fixture files opt into actual service
acceptance. Raw source-bound logs and pinned fixture images live in the sibling
#155 reference workspace. These checks do not qualify workflow catalog registration or a Worker host.
