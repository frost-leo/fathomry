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

# MySQL Adapter interface

[Documentation](../../../../../README.md) / Public package reference

**Audience:** independent Go applications and Framework resource composition.
**Status:** implemented bounded public capability; protocol, external-consumer
and isolated MySQL 8.4.11/InnoDB verified-TLS Unix-socket tests exist. This is not
production, HA, nontransactional-engine or migration qualification.
**Package:** `github.com/frost-leo/fathomry/adapters/database/mysql/v1`.

## Responsibilities and construction

This Adapter exposes the supported private go-sql-driver/mysql profile using its
existing database/sql pool. It does not return a raw DB, Conn, Tx, driver object,
Assembly or producer authority. Direct applications and Framework use the same
capability. Selecting it does not import PostgreSQL. Shared metadata/budgets are
in [database/v1](../../v1/interface.md), and operation/evidence mechanisms remain
in [adapters/v1](../../../v1/interface.md).

Load application-owned Settings with existing strict preparation and validation;
call Recommend to derive explicit Runtime and Inbox[Result] policies. Dependencies
borrows both owners plus an optional bounded/lossy Observer. Open owns one local
pool for its full context lifetime, not service readiness. Retain and close any
non-nil Owner even when Open reports an error. Owner.Client grants operation
access, not Close authority; Ping explicitly checks a connection.

Receive evidence incrementally through NextReleased or the existing Framework
receiver. Live source/transaction records must not block finite children. Required
records remain charged until Ack; Retry repeats facts, never SQL. Finalize
retained handles, close the Owner and join the caller-owned Runtime. Continue
incomplete cleanup on the same owner with a fresh cleanup context.
ShutdownComplete confirms local release separately from cleanup error history.

The [external public-only consumer](../../../../../../adapters/database/mysql/v1/testdata/consumer/main.go)
and its [runner](../../../../../../adapters/database/mysql/v1/integration_test.go)
provide an executable complete local recipe. Their protocol peer is not MySQL.

## Complete supported settings

The public [Settings declaration](../../../../../../adapters/database/mysql/v1/options.go)
has explicit JSON/mapstructure names and no runtime/DSN alias. Strict preparation
rejects unknown, null, duplicate, coerced and overflowing values. Durations are
integer **nanoseconds**. Typed zero chooses bootstrap defaults, not the private
raw-overlay explicit-zero behavior.

| Fields | Contract |
| --- | --- |
| Name | Required non-secret 1–64-byte lowercase ASCII letters, digits, dot, underscore or hyphen |
| Network, Address, Port | Network defaults to tcp: one non-unspecified literal IP/nonzero uint16 port; unix requires an absolute socket path and port zero |
| Database, User, Password | Database may be empty; User is required; Password may be empty; UTF-8/no NUL, at most 256 / 256 / 4096 bytes |
| RootCAPEM, ServerName | Explicit trust up to 64 KiB and verified identity up to 256 bytes; no implicit system roots |
| ServerCertificateSHA256 | Alternative to ServerName: exact leaf DER SHA-256 **plus** normal chain/server-auth/time verification |
| Plaintext | Isolated-test opt-in; incompatible with TLS fields, not a trust-all mode |
| Authentication | Defaults to caching_sha2_password only; mysql_native_password explicitly additionally permits that legacy greeting/auth switch |
| ParseTime, ClientFoundRows, ColumnsWithAlias | Native options, default false |
| MaxConnections, QueuedCalls | Zero defaults to 4 connections/no queue; allowed 1–32 and 0–64 |
| MaxIdleConnections | Zero defaults to MaxConnections; -1 disables idle retention; positive at most MaxConnections |
| MaxIdleTime, MaxLifetime | Zero disables expiration; positive at most 24 h; native pool scheduling applies |
| Timeout, ReadTimeout, WriteTimeout | Timeout defaults to 10 s; read/write default to Timeout; each 1 ms–1 min |
| CloseTimeout, TransactionTimeout | Defaults 5 s and 1 min; each 1 ms–1 min |
| MaxRows | Zero defaults to 1024; allowed 1–65536 |
| MaxResultBytes | Zero defaults to 4 MiB; allowed 1 KiB–16 MiB |
| MaxPacketBytes | Zero defaults to 1 MiB; allowed 1 KiB–4 MiB incoming packet payload before SDK allocation |
| MaxResponseBytes | Zero defaults to 8 MiB; allowed 1 KiB–32 MiB all incoming frames/metadata/drain per native command |

Validate performs no pool construction or service I/O. Native bootstrap sets
autocommit=1, UTC session time_zone and utf8mb4/utf8mb4_0900_ai_ci without reading
ambient DSNs, option files or TLS registries. These are session settings, not
server configuration changes. SQL permissions and later session changes remain
the caller's responsibility.

Timeout bounds native phases; it does **not** impose a deadline on earlier public
Runtime queueing. Supply a caller admission deadline. Read/write deadlines are
clipped by native operation ownership; cancellation is not proof of no effects.

### Budget composition

The default root reservation is 79,003,648 declared work bytes: the native
78,938,112-byte envelope plus 64 KiB public attribution. It exceeds the public
Runtime's default 64 MiB ceiling. Do not combine defaults blindly: use Recommend
or an explicitly sufficient caller-owned policy.

The recommendation includes a source slot (1 MiB work / 64 KiB evidence), all
active families, and evidence for queued roots. One modeled maximal family is a
transaction, eight preparations and one finite call. Children share root work
bytes but reserve independent evidence. Public tree/hold bounds still apply;
acknowledge finite evidence incrementally. Multiple sources and overlapping
Follow generations need deliberately combined policies.

These are declared accounting envelopes, not upfront allocation, measured RSS or
a native/idle-pool heap limit. The source slot covers ownership bookkeeping; TLS,
sockets, caller copies and native idle connection limits remain distinct. Using
rejects a larger generation than its declared Budget before native dispatch,
rather than silently increasing a supplied Runtime.

## SQL routes and native differences

Client exposes Ping, Query, Exec, Prepare and Begin. Transactions expose Query,
Exec, Prepare, Commit and Rollback. Preparations expose Query, Exec, Ready,
Receipt and Close. Stats returns detached sql.DBStats; Profile returns non-secret
effective settings without service I/O or a new SQL admission slot.

A standalone preparation pins one connection. A transaction retains one connection
and source generation, with at most eight live preparations. Client supports
independent concurrent roots; retained-family overlap/reentrancy is refused rather
than queued or raced. Copies share the same authority. No operation creates a
second pool and no SQL mutation is automatically retried or reprepared.

**MySQL Begin's context owns its full transaction lifetime**, including
database/sql automatic rollback, further bounded by TransactionTimeout. In
contrast, preparation's context governs only admission/setup; its retained
lifetime belongs to Client or the parent transaction. The explicit Using lifetime
fences all calls and retained work, and children observe their parent. This is
intentionally different from PostgreSQL Begin's setup-only context.
TxOptions accepts sql.LevelDefault or the four supported MySQL isolation levels
(ReadUncommitted, ReadCommitted, RepeatableRead, Serializable). ReadOnly defaults
false; LevelDefault preserves the server session setting.

Commit, Rollback and Statement.Close use existing reservations, including under
work/evidence saturation. A fresh cleanup context cannot authorize new business
operations on a canceled owner. Actual release is joined independently of caller
wait timeouts. Repeated statement Close returns original final evidence; repeated
transaction finalization preserves native sql.ErrTxDone behavior.

Ready is immutable initial preparation metadata, not final evidence. Receipt
resolves only after cleanup facts are known. Parent finalization also closes its
remaining preparations. Source/facade/runtime cancellation initiates owned cleanup;
a cancellation signal alone never confirms native release.

All accessed tables/triggers must meet the declared InnoDB profile. A statement
error need not roll back the transaction. Native status, including an owned Ping
after an error lacking status, detects an actually ended transaction. DDL and
other implicit commits can preserve earlier changes even if later local Rollback
is acknowledged. Lost finalization replies remain FinalizationUnknown, never an
automatic retry decision.

No controlled MySQL savepoint handle is invented for symmetry; MySQL SQL
savepoints still exist. Raw transaction-control SQL can end or invalidate the
retained scope. Ordinary pool calls do not guarantee session affinity:
database/sql ResetSession is not a PostgreSQL DISCARD ALL equivalent.

SQL is at most 64 KiB, with at most 128 arguments / 1 MiB aggregate reservation
and 64 columns. Basic scalar Go values, finite floats, strings, byte slices,
json.RawMessage and time.Time are accepted. Nonzero times must have a UTC year
1–9999. Native callbacks, driver.Valuer, arbitrary structs/pointers, bulk/streaming
paths and SDK handles are refused. LOCAL INFILE is denied before global
file/reader lookup, including over TLS. These boundaries are not an SQL permission
sandbox. See the [native profile](../../../../internal/database/mysql/v1/interface.md)
for framing, session and prepared-protocol constraints.

## Results, semantic errors and evidence

Results retain positional native column names/types, nullability-known and
precision-known metadata. Duplicate names are not collapsed. RowsCopy is nil
when rows were not retained (including Exec); complete empty Query returns an
empty non-nil slice. ValuesCopy distinguishes SQL NULL (nil) from empty bytes.
First requires complete consumption, preserves sql.ErrNoRows for empty success,
and does not imply uniqueness. Partial rows and RowsRead, including a first
over-limit witness, remain available with errors.

Unsigned integers, decimals and JSON retain exact encodings rather than float64
coercion. Temporal results default to native text, including zero dates; ParseTime
uses native UTC time.Time conversion and RFC3339Nano encoding (zero dates become
Go's zero time). Floats follow native conversion, not arbitrary decimal arithmetic.

RowsAffected and LastInsertID preserve native signed-int64 semantics with explicit
presence booleans. ClientFoundRows changes matched/changed-row meaning; these
methods do not widen native insert-ID conversion. HasData means native result
presence, not acceptance/no effect. Complete means consumed statement/command
acknowledgement and can coexist with cleanup failure; it is not a commit,
durability, replica visibility or business guarantee.

Accepted failures are returned directly as semantic errors, not hidden behind a
nil native setup error. Lifetime Receipts preserve independent Primary/Cleanup.
Sole direct occurrences keep their identity; combined phases/waits remain
inspectable occurrences with exact original causes and typed wait details.
Translation keeps the nearest native classification and recognized nested public
codes for errors.Is without replacing the original graph. InspectError returns
the borrowed sensitive *mysql.MySQLError; errors.Is/As also preserve native and
context causes. Do not mutate, dump or serialize native error/configuration graphs.

Native source-capacity refusal also maps to public `ErrLimit`, including when
different public runtimes share one bounded pool. The private capacity cause and
independent public evidence remain intact; the identity is not a retry policy.

Definitions and English/Chinese Resources compose offline under facility 0x081,
component database_mysql. Existing CLI catalogs include them without constructing
a source. Runtime results/handles are not durable DTOs; normal fmt/slog presentation
is restricted, including nil handles. WithID retains optional opaque UTF-8
correlation up to 256 bytes without ASCII C0/DEL controls, never a cast to private
ASCII correlation.

`Settings` remains ordinary loadable data. Direct logging of a Settings value
or a non-nil pointer is automatically redacted in both slog text and JSON handlers.
A typed-nil `*Settings` is not a valid value-method receiver and can emit slog's
recovered-panic diagnostic. Normalize optional nil pointers to untyped nil (or
omit the attribute) before logging:

```go
var optional *Settings
var logged any
if optional != nil {
    logged = optional
}
logger.Info("database configuration", "settings", logged)
```

The value-receiver LogValue is intentional: a pointer-only method would leave
populated Settings values unprotected in a standard JSON handler. Go does not
allow separate value and pointer methods with the same name; see the
[method-set rules](https://go.dev/ref/spec#Method_sets). No serializer hooks,
configuration shape changes or new logging API are introduced. Explicit JSON
encoding is still configuration serialization, not redacted log output.

## Public resource and Framework use

A public resource.Instance holds Owner.Handle and delegates cleanup to
Owner.Release. Using(lifetime, ref, budget, dependencies) borrows one generation
per root. Fixed retains its original instance; Follow adopts only later roots.
Live transactions/preparations and all descendants keep their original pool,
generation and attribution. Failed replacement preserves last-good ownership;
a larger-than-declared generation is rejected before SQL.

Framework consumes these same public APIs, runtime, resource scope and released
evidence. There is no separate Framework database facade or database migration
engine. [Composition tests](../../../../../../adapters/database/mysql/v1/integration_test.go)
exercise Fixed/Follow, partial failed candidates, cleanup, budget mismatch and
localized native-error presentation.

## Executable qualification and non-goals

[Client tests](../../../../../../adapters/database/mysql/v1/client_test.go),
[semantic controls](../../../../../../adapters/database/mysql/v1/result_test.go),
[lifecycle tests](../../../../../../adapters/database/mysql/v1/source_test.go)
and independent consumers verify bounded supported paths. Protocol peers are not
InnoDB oracles and cannot certify implicit DDL commits or server durability.

The separately authorized [service gate](../../../../../../adapters/database/mysql/v1/integration_service_test.go)
uses tag mysql_service and private FATHOMRY_MYSQL_ADAPTER_CONFIG. The write fixture
requires explicit creation authority, expected version, recovery path and verified
TLS over an existing Unix socket. It creates only an unpredictable isolated
database; exercises public readiness, exact/parameterized SQL, preparation,
commit/rollback, statement-only failure, real DDL implicit commit and lifetime
rollback; independently reads effects; and verifies fixture absence after cleanup.
Unconfirmed creation/cleanup retains recovery responsibility. Normal tests do not
run it. This does not qualify MySQL TCP service writes.

No ORM/Ent, provisioning API, migration, generic retained session, distributed
quota, cross-database transaction, automatic mutation retry, HA, arbitrary engine
or Workflow execution is added. Implementation lineage is
[#102](https://github.com/frost-leo/fathomry/issues/102); local qualification does
not establish merged/released status.

## Error boundary organization

Maintenance follows the shared [public Adapter contract](../../../../../development/public-adapters.md):
configuration and policy are separate, native budgets are authoritative, and
this provider's readiness, context and result semantics remain distinct.

Stable declarations, runtime mapping, diagnostics and locale embedding follow
[the public adapter error boundary](../../../../../development/adapter-errors.md).
Transparent public-error wrappers retain their existing core and original causes
without exposing wrapper text; explicit native semantic frames remain provider-owned.
This changes neither public APIs, numeric identities nor locale resources.
