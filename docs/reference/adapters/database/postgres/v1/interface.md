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

# PostgreSQL Adapter interface

[Documentation](../../../../../README.md) / Public package reference

**Audience:** independent Go applications and Framework resource composition.
**Status:** implemented bounded public capability; protocol, external-consumer
and isolated PostgreSQL 18.6 verified-TLS tests exist. This is not production,
HA, migration or arbitrary extension qualification.
**Package:** `github.com/frost-leo/fathomry/adapters/database/postgres/v1`.

## Responsibilities and construction

The Adapter exposes the supported private pgx v5/puddle profile without exposing
an Assembly, native receipt, connection, pool or producer authority. Direct
applications and Framework use the same capability. Selecting it does not import
the MySQL implementation. Shared metadata and budget types live in
[database/v1](../../v1/interface.md); admission and evidence remain in
[adapters/v1](../../../v1/interface.md).

1. Load application-owned `Settings` through existing strict configuration
   preparation, and call `Validate` or use it as the schema validator.
2. Obtain `Recommend(settings)`. Construct a caller-owned public Runtime and
   `Inbox[Result]` from its Policy, or explicitly supply sufficient aggregate
   limits. Pass them in Dependencies; optional Observer remains bounded/lossy.
3. `Open(lifetime, settings, dependencies)` owns one local native pool, not a
   readiness check. Always retain and close a non-nil Owner, including on error.
4. Use `Owner.Client()` for non-owning operations. `Ping` explicitly checks a
   connection. Construction, Ping, configuration acceptance and instance adoption
   are separate observations.
5. Receive required evidence incrementally using `NextReleased` or the existing
   Framework receiver. Do not wait for the live source/transaction record at the
   front of a FIFO. Ack only after required handling; Retry redelivers facts and
   never reruns SQL.
6. Finalize retained handles, close the Owner, and join the caller-owned runtime.
   Continue incomplete cleanup on the same owner with a fresh cleanup context.
   `ShutdownComplete` reports positive local release independently of error history.

The [external consumer](../../../../../../adapters/database/postgres/v1/testdata/consumer/main.go)
is an executable public-only construction, SQL, preparation and transaction
recipe. Its [runner](../../../../../../adapters/database/postgres/v1/integration_test.go)
supplies an isolated protocol peer, not a PostgreSQL server.

## Complete supported settings

Settings is ordinary loadable data, not the private runtime Options type.
Its [JSON/mapstructure names](../../../../../../adapters/database/postgres/v1/option.go)
are explicit. Durations are integer **nanoseconds**, not strings or seconds.
After strict preparation, typed zero uses the defaults below; this public route
does not forward private raw overlays with different explicit-zero semantics.

| Fields | Contract |
| --- | --- |
| Name | Required non-secret 1–64-byte lowercase ASCII identity: letters, digits, dot, underscore or hyphen |
| Address, Port | One explicit non-unspecified literal IP and nonzero uint16 port; no DNS, Unix socket, multi-host or failover |
| Database, User, Password | Required UTF-8 without NUL; at most 256 / 256 / 4096 bytes |
| RootCAPEM, ServerName, Plaintext | Explicit PEM trust up to 64 KiB and verified identity up to 256 bytes; plaintext is isolated-test opt-in and forbids TLS inputs |
| ParserHome | Explicit authorized home metadata-probe base matching the process home; see environment constraints below |
| MaxConnections | Zero selects 4; allowed 1–32 |
| MaxIdleTime, MaxLifetime | Zero disables expiration; positive 1 ms–24 h; maintenance does not interrupt held connections |
| QueuedCalls | Zero disables waiting; allowed 0–64 |
| Timeout | Zero selects 10 s; 1 ms–1 min, independently bounding native admission/setup/execution and transaction Commit/Rollback phases |
| CloseTimeout | Zero selects 5 s; 1 ms–30 s for statement/savepoint/connection cleanup, not a proof of completed release |
| MaxRows | Zero selects 1024; allowed 1–65536 |
| MaxResultBytes | Zero selects 4 MiB; allowed 1 KiB–16 MiB retained cell/name bytes |
| MaxMessageBytes | Zero selects 1 MiB; allowed 1 KiB–4 MiB per incoming protocol message body |

`Validate` checks settings without pool construction or service I/O. `Open`
also checks the current pgx parser environment: nonempty PG-prefixed variables
are refused; ParserHome must match the authorized process home. The environment
must remain static during construction. The adapter reads no ambient database
credentials and never temporarily rewrites environment variables. Parser metadata
probes are not a zero-filesystem-access claim. Windows/Plan 9 parser execution is
unsupported, even though compilation is possible.

Protocol 3.0, required SCRAM-SHA-256, channel-binding preference, UTF-8, ISO dates,
UTC, disabled implicit preparation caches and bounded DISCARD ALL on root return
remain the [native profile](../../../../internal/database/pgx/v5/interface.md).
Public Timeout does **not** add a deadline to earlier public Runtime queueing:
the caller supplies that admission/setup deadline. It is not a whole-call or
transaction-lifetime timeout.

### Budget composition

Each root family reserves 50,626,560 declared work bytes with default settings:
the native 50,561,024-byte envelope plus 64 KiB of public attribution. Children
share that root reservation and reserve their own evidence. This is not upfront
allocation, measured RSS, or an idle-pool/native-heap ceiling.

Recommend includes one source-owner slot (1 MiB work, 64 KiB evidence), all
configured active families and queued-root evidence. A maximal family comprises
one transaction, eight preparations, eight savepoints and one finite operation.
The public runtime's own tree/hold limits also apply. The source slot accounts
for ownership bookkeeping, not all memory retained by idle native connections.
Caller-held result copies, TLS, sockets, Go runtime overhead and native pool
limits remain separate.

Use explicit aggregate policies for multiple sources and overlapping Follow
generations. No recommendation silently raises a supplied runtime's limits.
A larger adopted generation than a resource-backed Client's declared Budget is
rejected before native dispatch. Required evidence left unacknowledged consumes
count/bytes even after native work releases.

## SQL and retained ownership

| Public route | Meaning |
| --- | --- |
| Client.Ping / Query / Exec | One finite operation on the existing pool; synchronous bounded consumption, no mutation retry |
| Client.Prepare | One standalone preparation pins one connection until Close or owner/facade lifetime cancellation |
| Client.Begin | One transaction pins one connection and one source generation |
| Transaction.Query / Exec / Prepare | Use the original connection; at most eight live transaction preparations |
| Transaction.Savepoint | At most eight code-named LIFO scopes; queries remain on Transaction |
| Commit / Rollback / Statement.Close / Savepoint.Release / Rollback | Use existing ownership/evidence, including at saturation |
| Client.Stats / Profile | Read-only detached local snapshots without SQL, Ping or a new operation slot |

Begin and preparation use their method context for **admission/setup only**.
TxOptions requires explicit ReadCommitted, RepeatableRead or Serializable
isolation and ReadOnly or ReadWrite access. Deferrable is valid only with
Serializable/ReadOnly; there is no implicit isolation/access default.
After successful setup, Client lifetime owns retained work. Canceling a short
setup context does not revoke a valid transaction/preparation. Child operations
also observe the parent. Source/facade/runtime cancellation requests retained
cleanup; actual native release is still joined. Explicit finalization accepts a
fresh cleanup context and does not acquire a new admission/evidence slot.

Client methods support concurrent independent roots. Transactions, preparations
and savepoints share their actual family gate; overlapping/reentrant use is
refused, not queued or silently raced. After a finalizer confirms release,
sequential calls cannot race its reporter; a wait-timeout return does not confirm
release. Copies of retained handles share one
authority. Repeated statement/savepoint cleanup returns original final evidence;
repeated transaction finalization retains native closed-transaction semantics.

`Statement.Ready()` is immutable initial metadata. Its public Receipt remains
unresolved until final cleanup facts are available; late deallocation/reset
failure cannot disappear behind initial success. Parent finalization closes its
preparations and settles outstanding savepoints. A savepoint release does not
commit its parent. Lost commit response remains FinalizationUnknown; an aborted
PostgreSQL COMMIT can instead be CommitRolledBack, preserving
`pgx.ErrTxCommitRollback`.

SQL is at most 64 KiB, at most 128 parameters with a 1 MiB encoding reservation,
and at most 64 columns. Basic scalar Go values, finite floats, `time.Time`,
strings and byte slices are supported; bytea expansion counts toward the bound.
Arbitrary pointers/structs, codecs, Valuer callbacks and raw SDK options are not
admitted. Streaming COPY responses are refused after dispatch; this does not
prove SQL was never sent or sandbox server-file/program COPY or external effects.
PostgreSQL json.RawMessage is not silently treated as its
underlying unnamed byte slice. This is not a SQL authorization sandbox: roles
and composition authorize statements, DDL, extensions and external effects.

## Results, errors and evidence

Rows retain positional PostgreSQL text bytes and duplicate column names/OIDs.
`ValuesCopy` uses nil for SQL NULL and a non-nil empty slice for an empty value.
`RowsCopy` returns nil for no retained query result (including Exec), but a
non-nil empty slice for a complete empty Query. First requires complete
consumption and retains `pgx.ErrNoRows` for empty success; it does not assert
uniqueness. Partial rows, RowsRead (including a first over-limit witness), command
tag, affected rows, server text and distinct transaction/savepoint outcomes
remain inspectable. Numeric values are not coerced through float64.

HasData describes native result presence, not acceptance or absence of effects.
Complete concerns native statement consumption and can coexist with cleanup
failure. A command acknowledgement is not transaction commit, crash durability,
replica visibility or business success. Result copies share only immutable
storage; getters return caller-owned metadata/bytes.

Query/Exec/Begin/Prepare return semantic failures even when native admission had
succeeded. Lifetime Receipts retain separate Primary and Cleanup fields. Direct
errors remain inspectable public occurrences, including real combined phases and
wait errors; waiting timeout never rewrites the observed outcome. Translation
preserves the nearest native classification, recognized nested public identities
for errors.Is, and the original native graph. InspectError deliberately returns
the borrowed sensitive `*pgconn.PgError`; other native/context causes remain
available through errors.Is/As. Do not mutate, dump or serialize those graphs.

Native source-capacity refusal also maps to public `ErrLimit`, including when
different public runtimes share one bounded pool. The private capacity cause and
independent public evidence remain intact; the identity is not a retry policy.

Definitions and English/Chinese Resources compose offline under facility 0x080,
component `database_postgres`. The existing CLI catalogs include them without
source construction. Runtime results/handles are not durable DTOs; ordinary
formatting/logging is restricted, including nil handles. WithID accepts an
optional opaque UTF-8 correlation up to 256 bytes without ASCII C0/DEL controls.
Code-owned private ASCII IDs never reinterpret that value.

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

## Resource and Framework composition

Put `Owner.Handle()` in a public resource.Instance and use `Owner.Release` for
cleanup. `Using(lifetime, ref, budget, dependencies)` borrows once per root.
Fixed keeps its original instance; Follow may adopt future generations. A live
transaction/preparation and all descendants keep their original lease, pool and
metadata through final release. Failed replacement does not erase last-good
ownership. This does not migrate a database or certify readiness.

Framework uses these same public APIs, its operation runtime, resource scope and
released-evidence receiver; it adds no parallel database facade. See the
[composition tests](../../../../../../adapters/database/postgres/v1/integration_test.go)
for Fixed/Follow, failed candidate cleanup, oversized-generation refusal and
localized error presentation.

## Verification and explicit limits

[Client tests](../../../../../../adapters/database/postgres/v1/client_test.go),
[semantic controls](../../../../../../adapters/database/postgres/v1/result_test.go),
[lifecycle tests](../../../../../../adapters/database/postgres/v1/source_test.go)
and external consumers exercise cancellation, saturation, partial/late errors,
retained cleanup and authority/dependency boundaries. Protocol peers are not
PostgreSQL atomicity or durability oracles.

The separately authorized [service gate](../../../../../../adapters/database/postgres/v1/integration_service_test.go)
uses build tag `postgres_service` and private
`FATHOMRY_POSTGRES_ADAPTER_CONFIG`. It requires explicit TLS, creation authority,
expected version and a private recovery path. It creates only an unpredictable
isolated database, verifies public reads/writes/preparations/transactions/savepoints
with independent connection read-back, and verifies absence after cleanup. Unknown
creation/cleanup preserves recovery responsibility instead of adopting or dropping
an unproved fixture. It does not run in normal tests.

No ORM/Ent, provisioning API, migrations, generic retained sessions, bulk/streaming
SQL, distributed quota, cross-database transaction, HA, automatic mutation retry
or Workflow execution is implemented. The issue's implementation lineage is
[#102](https://github.com/frost-leo/fathomry/issues/102); local qualification does
not mean a PR has been merged.
