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

# MySQL v1 interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** in-module composition and future framework adapters.
**Status:** implemented bounded MySQL 8 profile; isolated MySQL 8.4.11/InnoDB
acceptance and separate verified-TLS reads, not production/HA/vendor-wide support.
**Package:** `github.com/frost-leo/fathomry/internal/database/mysql/v1`.

## Responsibilities and native boundary

The package supplies native MySQL pooling, Ping, ordinary/parameterized Query and
Exec, reusable prepared statements, copied metadata/results and single-connection
transactions. It uses unmodified `go-sql-driver/mysql v1.10.1` with
`database/sql.DB`, not pgxpool or an integration-owned replacement pool.

A per-connection framing transport enforces incoming bounds and LOCAL INFILE
denial before native dispatch/allocation, including above owned TLS. Controlled
native calls execute under a pinned `sql.Conn.Raw` lock; no native value, callback,
DSN, pool authority or streaming Rows handle escapes. The package depends on
`resource`, `invocation`, `fault` and `compatibility`; `conformance` is test-only.

There is no public API, workflow schema, ORM, migration system or common
PostgreSQL/MySQL interface. Multi-statements, stored-procedure result sets, LOAD
DATA, compression, binlog/replication, XA, failover and business retries are not
provided. SQL keywords are not an authorization boundary: authorized native SQL
can run, but SQL permissions and the transactional profile remain caller duties.

## Call sequence and ownership

1. `Select(OptionsV1, layers...)` freezes typed bootstrap and strict resource
   overlays. It returns an opaque `Source` selection, without dialing/readiness.
2. Attach `LimitsV1(options)` or a matching effective resource policy and assemble
   ownership. Overrides changing bounds require corresponding explicit limits.
3. `Bind` attaches the source's original `resource.Access`, a separately owned
   `invocation.Inbox[Result]` and optional bounded observer.
4. Call `Ping`, `Query`, `Exec`, `Prepare` or `Begin` with an explicit context
   and valid correlation. Inspect both the direct receipt and independent delivery.
5. Close retained statements/finalize transactions, release received evidence,
   then close the owning assembly. An incomplete assembly close retains its
   continuation and dependencies; retry only its recorded cleanup continuation.

Independent selections/assemblies own separate native pools, settings and trust.
Borrowed aliases preserve original source identity, limits and lifecycle.
`Database` copies share that authority and are concurrently usable. `Stats()`
returns native `sql.DBStats`; counters do not prove readiness or resource release.

A standalone `Statement` pins one connection/root admission until Close. Its
preparation context governs preparation only. It does not inherit a pool-wide
sql.Stmt's reprepare/retry behavior. A transaction statement borrows its parent's
connection and closes automatically with it; at most eight can be retained.
Transaction/statement copies share state. Concurrent methods on the same retained
owner are refused, not multiplexed. Cleanup needs no new admission/evidence slot.

The Begin context owns the entire transaction lifetime, bounded additionally by
`TransactionTimeout`. Cancellation schedules automatic rollback. The integration
retains the original native finalizer result even when `database/sql` returns
`sql.ErrTxDone` before that finalizer completes. One retained guard joins the owned
lifetime worker. A later finalizer cannot replace the original receipt.

## Configuration and limits

Options are process-local bootstrap, not durable DTOs. Caller-owned byte slices
and mutable overlays are borrowed only during Select; SQL arguments are borrowed
synchronously for the call and must not be mutated concurrently. There is no
caller `mysql.Config`, Params map, TLS object or callback to alias. Every native
connector gets fresh settings, Params, explicit roots and per-connection TLS state;
no lossy DSN conversion or process-global SDK configuration is used.

| Fields | Meaning and defaults |
| --- | --- |
| `Name` | Valid resource identity, not a database identifier |
| `Network`, `Address`, `Port` | Default tcp: literal non-unspecified IP and nonzero port; unix: absolute socket path, port zero; no DNS discovery |
| `Database`, `User`, `Password` | Explicit; database/password may be empty; user required; bounded UTF-8 without NUL |
| `RootCAPEM`, `ServerName` | Explicit trust, at most 64 KiB PEM, verified server DNS/IP identity; TLS minimum 1.2 |
| `ServerCertificateSHA256` | Alternative to ServerName: exact leaf DER SHA-256 **and** normal chain/server-auth/time verification; never trust-all |
| `Plaintext` | Explicit isolated/test mode; incompatible with TLS fields |
| `Authentication` | Default caching_sha2_password only; explicitly selecting mysql_native_password additionally allows its greeting/auth switch, not every plugin |
| `ParseTime`, `ClientFoundRows`, `ColumnsWithAlias` | Native options, default false |
| `MaxConnections`, `QueuedCalls` | Default 4 and 0; ranges 1–32 and 0–64 |
| `MaxIdleConnections` | Typed zero defaults to MaxConnections; -1 disables idle retention; explicit raw zero also disables it; at most MaxConnections |
| `MaxIdleTime`, `MaxLifetime` | Default zero disables expiration; positive values at most 24 hours; native pool scheduling applies |
| `Timeout`, `ReadTimeout`, `WriteTimeout` | Default 10 seconds; read/write default to Timeout; native I/O deadlines clipped by the owning phase |
| `CloseTimeout`, `TransactionTimeout` | Default 5 seconds and 1 minute; operation/close/transaction durations each 1 ms–1 min |
| `MaxRows` | Default 1024, range 1–65536 |
| `MaxResultBytes` | Default 4 MiB, range 1 KiB–16 MiB; copied cell encodings and column names/type strings |
| `MaxPacketBytes` | Default 1 MiB, range 1 KiB–4 MiB; incoming packet payload before native allocation |
| `MaxResponseBytes` | Default 8 MiB, range 1 KiB–32 MiB; all incoming frames including headers, metadata and drainage, per native command |

Strict overlays use the JSON field names in
[options.go](../../../../../../internal/database/mysql/v1/options.go), with
nanosecond duration fields such as `timeout_ns`. Unknown/null/coerced fields are
rejected by resource preparation. Typed zero defaulting precedes explicit overlays.

Native bootstrap sets autocommit=1, UTC session time_zone and utf8mb4 with
utf8mb4_0900_ai_ci; it does not read ambient DSNs, option files or TLS registries.
No server configuration is changed. Composition must authorize these session
settings and prevent later SQL, triggers, implicit commits or nontransactional
engines from invalidating its transaction requirements. Pool-wide session mutation
is not pinned-session affinity; use retained scopes where affinity is required.

### What the bounds prove

SQL is at most 64 KiB, with at most 128 scalar arguments and a total argument
envelope of 1 MiB. Results/preparation have at most 64 columns and eight retained
transaction statements. The native outgoing `MaxAllowedPacket=2 MiB` is **not**
the incoming or result limit.

The transport reads and validates the four-byte MySQL header before allocating
its bounded frame or exposing that header to the SDK. Its packet maximum is less
than a physical 16 MiB continuation packet, so oversized/continued logical packets
are rejected before native concatenation. Metadata counts, text/binary row layouts,
row count and response bytes are checked even during Rows.Close drainage. A server
LOCAL INFILE request is rejected before global file/reader lookup, even after
SELECT and even if another component registered a file/reader.

These are concrete MySQL packet/native-buffer and retained-data bounds, not total
process RSS or a general hostile-server sandbox. TLS handshake/certificate memory,
OS sockets, Go runtime allocations, arbitrary caller error graphs and explicit
caller copies are separate. Admission reserves a conservative per-root envelope
including all eight potential native prepared-metadata caches. Evidence has an
independent byte/count reservation before work; neither accounting is an upfront
allocation or a durable ledger. Composition budgets aggregate concurrent sources,
retained evidence, cleanup history and downstream copies.

## Results, errors and transaction outcomes

Before admission, invalid input/identity/limits or saturated admission/evidence
return an error without a receipt. After admission, failures are delivered in the
receipt (the direct setup error can be nil). A failed Prepare/Begin can therefore
return a nil handle with a non-nil receipt. Do not equate nil setup error with
successful native execution.

Query returns immutable, synchronously consumed rows, not a streaming cursor.
`RowsCopy` copies the outer rows; each row's `ValuesCopy` makes independent bytes.
NULL is nil bytes; an empty non-NULL value is non-nil zero-length bytes. A complete
empty query has a non-nil empty row slice. `First` returns `sql.ErrNoRows` only for
a complete retained empty query, not a partial result or Exec. ColumnsCopy exposes
copied native name/type/nullability/precision/scale metadata.

Unsigned integers and decimal/JSON text retain exact representations. Temporal
values default to native MySQL text, including zero dates; ParseTime uses native
UTC time conversion and encodes time.Time as RFC3339Nano (zero dates become the Go
zero time). Floats follow native textual conversion, not arbitrary decimal coercion.
Parameters accept basic scalar Go types, time.Time, byte slices and json.RawMessage
(including typed nil), not driver.Valuer, arbitrary structs/pointers or callbacks.

Exec's RowsAffected and LastInsertID expose native **int64** values with explicit
presence; ClientFoundRows changes matched/changed-row semantics. They do not
strengthen native unsigned insert-ID conversion or imply transaction commit.
`RowsRead` can exceed retained rows and include the first rejected over-limit row.
`Complete` means consumption/command acknowledgement, not durability or complete
business effect. ServerVersion is the observed greeting, not a support certificate.

Native MySQLError number/SQLState and context causes remain inspectable through
errors.Is/As. Ordinary fmt/slog and runtime JSON handling are restricted; deliberately
inspecting native causes, columns or values exposes sensitive SQL data. Primary
execution and cleanup errors remain separate in both direct and inbox evidence.
Errors are facts, not a retryability or Item/Run disposition policy.

No mutation is silently replayed. Direct Query/Exec native dispatch may return
ErrSkip before sending parameterized SQL; native preparation and execution then
occur once. Pinned Raw callbacks retain native errors separately to avoid both
sql.Conn implicit release and sql.Stmt retry loops (including inside sql.Tx).
Acquisition's private ErrBadConn retry marker is hidden from database/sql only,
then the original cause is restored for the caller. Attempt counts are observed
native-call lower bounds, not exact wire packet/retry/constructor counts.

All four MySQL isolation levels plus LevelDefault and ReadOnly are supported.
Statement failure does not universally abort an InnoDB transaction: after an
error, a bounded native Ping reads actual transaction status. A live transaction
may continue after statement-only errors; a terminated/unknown transaction cannot
accept more work or commit. Deadlocks, lock timeouts, implicit commits and triggers
retain MySQL semantics, not a copied PostgreSQL fail-stop policy.

CommitAcknowledged/RollbackAcknowledged describe the original native finalizer's
reply. Missing/lost replies remain FinalizationUnknown; cancellation does not prove
no mutation occurred. Finalization never replays a commit. The independent inbox
retains original errors, partial data and finalization evidence even after a caller
handles the direct error; it is not crash recovery or durable acknowledgement.

## Cancellation and shutdown

Owned phase deadlines cover native I/O, including context-less Commit/Rollback,
statement close and Rows.Close drainage. Cancellation closes the owned socket and
the phase joins its entered cancellation callback. A poller timeout at the owning
deadline preserves the actual context's cancellation cause. A late cancellation
does not overwrite an already observed successful native acknowledgement.

Returned Rows are consumed and closed synchronously, with drainage errors recorded
independently. Standalone queries returning unexpected transaction state retire
their connection rather than returning an unowned transaction to the pool.
Retained statements and transactions hold admission until their explicit/automatic
cleanup; source close does not revoke active Go handles or deadlock reacquiring
their allowance.

Source close seals acquisition, invokes native DB.Close once and joins constructors
and every managed connection retirement, including idle-expiration cleanup already
removed from native statistics. Independent socket-close errors survive native
logging, cause the source to stop replacing connections, and remain in monotonic
source cleanup evidence. A caller close deadline reports incomplete work with a
continuation, never a later no-op success that erases earlier errors.

SDK cancellation watchers and database/sql opener/cleaner/transaction helpers have
no exported join handles. The integration fences their I/O and joins its owned
workers/native close operations; private helpers may finish non-I/O bookkeeping
afterward. Local release does not certify immediate remote query cancellation,
external rollback, production durability or absence of server-side effects.

## Verification and upgrade obligations

[Core and adjacent tests](../../../../../../internal/database/mysql/v1),
[composition/consumer checks](../../../../../../internal/database/mysql/v1/integration_test.go)
and [service gates](../../../../../../internal/database/mysql/v1/service_test.go)
are maintained separately. Run them using the [testing guide](../../../../../development/testing.md).
The consumer executes local construction/closure and reports its own selected
SDK/transitive modules; it explicitly does not claim a server query.

Isolated service evidence covers MySQL 8.4.11 (Ubuntu), InnoDB, autocommit=1,
REPEATABLE-READ default, UTC, strict sql_mode and rollback_on_timeout=0. Authorized
write/transaction/cleanup tests used a fresh random database over a Unix socket
with auth_socket. A separate read-only TCP profile verified TLS 1.3
TLS_AES_128_GCM_SHA256 and native caching_sha2 full authentication using explicit
chain plus certificate pin (the server certificate has no SAN). This is not
TLS-write, MariaDB, proxy/HA, replication or production acceptance. No server
configuration, account or business database was changed.

The framing and TLS upgrade depend on the pinned SDK's handshake/write sequence.
An owned BeforeConnect hook updates only that connection's native TLS state after
the controlled TLS upgrade, so native caching_sha2 full authentication chooses
its encrypted-password branch. It does not mutate caller/global state or fork the
SDK. Upgrades must re-run real TLS full authentication, allocation and LOCAL
negative controls, sql retries, automatic finalizer/drain/idle-retirement races,
consumer module inspection and service read-back/cleanup. The local preparation
benchmark compares identical transactional queries with and without preparation
reuse; it is not a claim about native MySQL or production throughput.

Rationale, failed controls and raw measurements remain with
[Issue #25](https://github.com/frost-leo/fathomry/issues/25) and its sibling reference
materials, not as build dependencies.
