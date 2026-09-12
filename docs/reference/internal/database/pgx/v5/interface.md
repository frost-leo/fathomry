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

# PostgreSQL pgx v5 interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** framework composition and in-module data/control adapters.
**Status:** implemented bounded internal profile; isolated PostgreSQL 18.6 with
verified TLS and required SCRAM has passed the explicit service gate. No production,
HA or general proxy certification.
**Package:** `github.com/frost-leo/fathomry/internal/database/pgx/v5` (Go name `pgx`).

## Responsibility and selected native profile

This package provides PostgreSQL pooling/Ping/statistics/expiration, ordinary and
parameterized SQL, reusable preparation, bounded raw text results, explicit
single-connection transactions and controlled savepoints. It does not define
workflow/control schemas, public capabilities, CLI commands, migrations, an ORM,
a registry, business outcomes, distributed transactions or MySQL support.

The selected modules are pgx v5.11.0 and puddle/v2 v2.2.2. pgxpool was the initial
candidate, not a requirement to hide its limitations: its destructor discards
native Close errors and bounds its own CleanupDone wait; its Acquire can return
while construction continues. The [native counterexample](../../../../../../internal/database/pgx/v5/pool_test.go)
records an injected socket Close error that pgxpool.Close does not return.

The implementation reuses **puddle**, not a newly written pool. A context-aware
gate serializes only acquisition and synchronous `CreateResource`; existing
connections execute concurrently. `AcquireAllIdle` obtains available resources,
and surplus idle resources are returned unchanged. Active resource admission is
never larger than the native pool ceiling. This avoids detached Acquire
construction and retains the actual constructor error on the requesting stack.

Explicit Ping and native statistics are available; construction and expiration
perform no hidden readiness query. Optional idle/lifetime expiration uses one
source-owned worker, coordinated through the acquisition gate and joined before
native pool close. It inspects once per second, never interrupts held resources,
preserves untouched idle age and closes/joins before removing native capacity.
Fresh connections get their first use even when handshake duration exceeds TTL;
expired held resources retire on return. Failed background retirement seals new
acquisition and retains bounded original cleanup history from the existing pool.

Stats returns independent native puddle snapshots. Maintenance can count as
acquired resources; AcquireAllIdle does not increment native acquire/wait metrics,
so those are not Fathomry call/admission counters. Zero resources is not proof that
cleanup callbacks completed. No periodic warmup/refill, implicit statement cache,
native hook surface, retry or transparent reprepare is supplied.

## Composition and call sequence

1. Supply `OptionsV1` to `Select`, optionally with explicit raw resource layers.
   Existing `resource.Prepare` owns structural checks, overlays and freezing.
   Duplicate names/invalid selected resource policies are checked by Assemble.
2. Attach an explicit `resource.WithLimits` policy. `LimitsV1(options)` recommends
   one for defaulted, unoverridden options; if layers change bounds, choose a
   matching policy. Applied limits remain distinct from source revision.
3. Call `resource.Assemble` with separate initialization and cleanup contexts.
   Native pool construction does not open a connection or prove server readiness.
4. Create a separate `invocation.Inbox[Result]` and call `Bind`. Bind checks that
   active permits cannot exceed native connections and that the required byte
   envelope and child lease capacity fit. An insufficient inbox rejects calls.
5. Execute explicit `Ping`, authorized `Query`/`Exec`, retain `Prepare`, or `Begin`
   a transaction and optionally establish controlled `Savepoint` scopes.
   A readiness query is separate, explicit work, not implicit schema provisioning.
6. Inspect direct receipts and independently receive/handle/release their inbox
   deliveries. Close the assembly after all transactions and calls finish;
   retain incomplete responsibility and explicitly continue Close when needed.

`resource.Borrow` aliases retain the original pool, preparation identity and
allowance. Closing a borrower cannot close its owner. Control persistence and
business SQL require separately authorized source selection; equal endpoints do
not imply shared pools, privileges or cross-source transactions.

Setup/admission errors return no receipt. Once a statement is accepted, its
technical failure is in `receipt.Result().Err()`, **not** the setup error return.
Begin returns either a live Transaction and unresolved finalization receipt, or
a nil transaction and completed failure receipt. Handle both cases.

## Options, preparation and implicit-input refusal

OptionsV1 is a process-local contract, separate from SDK major v5 and the private
resource schema format 1. Unknown raw fields/types and invalid effective options
are rejected. Typed zero timeout/size values request bootstrap defaults; a raw
overlay's zero is explicit and does not reapply those defaults.

| Option | Meaning |
| --- | --- |
| Name | Required resource identity under resource's existing safe-label rules; not overridable in raw settings |
| Address / Port | One explicit, non-unspecified literal IP and nonzero port; no DNS, sockets, multi-host, proxy or HA discovery |
| Database / User / Password | Required UTF-8, no NUL; maximum 256 / 256 / 4096 bytes |
| RootCAPEM / ServerName | Explicit trust set up to 64 KiB and verified server name up to 256 bytes; no system-root or certificate-file discovery |
| Plaintext | False by default; explicit isolated-test plaintext requires empty TLS inputs |
| ParserHome | Explicit authorized home metadata-probe base, matching the process home; empty only when home is unavailable |
| MaxConnections | Default 4; 1–32 native connections; cold construction is serialized |
| MaxIdleTime / MaxLifetime | Default zero disables expiration; positive 1 ms–24 h; one-second idle inspection, no interruption of held resources |
| QueuedCalls | Default 0; 0–64; used by the recommended LimitsV1 policy, not an override of resource's actual policy |
| Timeout | Default 10 s; 1 ms–1 min; separate admission and execution budgets, with acquisition and consumption inside execution |
| CloseTimeout | Default 5 s; 1 ms–30 s; native graceful-close budget, not a hard bound on all native cleanup |
| MaxRows | Default 1024; 1–65536 observed rows; the first over-limit row is a rejection witness |
| MaxResultBytes | Default 4 MiB; 1 KiB–16 MiB retained raw cell/name bytes, excluding Go structural overhead |
| MaxMessageBytes | Default 1 MiB; 1 KiB–4 MiB per incoming protocol message body |

Raw field names are the private schema's explicit JSON tags in
[options.go](../../../../../../internal/database/pgx/v5/options.go); durations use
`timeout_ns` and `close_timeout_ns`, not ambiguous numeric seconds.

pgx requires parser-created configuration. DSN allowlisting does not eliminate
ambient discovery. This implementation rejects nonempty PG-prefixed environment
variables and checks the explicitly supplied ParserHome **before** parsing.
The process environment must remain static during preparation/construction.
It is never rewritten temporarily or used to obtain database credentials.

The fixed parser seed has a socket-shaped host and nonempty synthetic user/password.
It is never connected. The socket branch avoids configTLS altogether: in v5.11.0
a TCP parse with sslmode=disable can still read a discovered root CA first.
The native parser still performs its documented home certificate metadata and
standard socket-directory stat probes. ParserHome explicitly authorizes this
residual metadata access; the integration does not claim zero filesystem access.
Windows/Plan 9 parsing is unsupported. Inputs, credentials, roots, runtime
parameters and the actual TCP destination are subsequently set explicitly, with
no stale fallback. Original source snapshots are not modified.

Only required SCRAM-SHA-256, protocol 3.0, channel-binding preference, UTF-8,
ISO dates and UTC session time zone are selected. Custom authentication and
credential rotation are outside this profile. Native ConnectError identity and
causes are retained, but its Config is an independent snapshot: native failed-startup
cleanup may still reference its private config. Callbacks are sealed, and trusted
roots are reconstructed from immutable PEM for each connection and error snapshot.
CertPool.Clone alone would share mutable parsed certificates exposed by x509 errors.

## Statement and result boundaries

Structurally valid ordinary SQL is accepted without a keyword allowlist, including
leading comments, VALUES/TABLE/MERGE/EXPLAIN and authorized DDL. The native extended
protocol rejects multiple statements; this is **not a SQL authorization sandbox**.
Roles and composition own SQL/external-effect permission. COPY stream responses
are refused by a code-owned frontend reader above the selected TLS stream before
native ordinary-result handling can discard them and certify empty success.
Server-file/program COPY and arbitrary external-effect functions are not sandboxed
by that transport fence and remain explicitly outside the selected permission profile.

Separate Database calls have no retained session affinity. Transactions and
standalone preparations own their necessary connection scope; no raw session facade
is exposed. After all child preparations finish and native status is idle/outside
a transaction, root return executes bounded DISCARD ALL before reuse. This removes
temporary tables, prepared state, session locks, cursors and notifications and
restores startup session defaults. Reset failure is independent cleanup evidence
and retires the connection. There is no reset between retained nested calls.

SQL is at most 64 KiB. At most 128 arguments are admitted with a 1 MiB aggregate
encoding reservation (bytea accounts for hexadecimal expansion). Conservative
scalar reservations are 32 bytes for nil/bool/integers, 64 for float32/time.Time,
and 512 for float64; native fixed-point float text is not its Go memory width.
Supported arguments are nil, built-in bool/integer types, finite float32/64, string, []byte
and time.Time. Named/custom values, QueryRewriter, query-mode/format arguments,
driver.Valuer and callbacks are refused before admission. Input slices are
borrowed until the synchronous call returns; do not mutate them concurrently.

QueryExecModeExec is fixed, including zero-argument statements, with both caches
disabled for ordinary execution. Explicit preparations use native text encoding
without described-OID parameter coercion, and explicitly request text results.
Results use native PostgreSQL **text format**, not arbitrary decoded
values. There is no native Rows, Conn, TypeMap, RowScanner or custom-codec escape.

- Query consumes and freezes bounded rows before returning. Rows can coexist with
  failure, but `Complete()` is false for partial/error/limited results.
- Exec uses the same bounded result-consumption path, retaining no row data.
  RETURNING/SELECT output still counts against limits; Exec never uses pgx.Exec's
  unbounded result-collection convenience.
- `RowsCopy` copies the outer immutable-row list. `Row.ValuesCopy` deeply copies
  cell bytes: nil is SQL NULL; non-nil empty bytes are a present empty value.
- `ColumnsCopy` returns owned name/OID metadata, including successful empty
  result descriptions. These getters deliberately expose data, not diagnostics.
- `First` requires complete retained Query data, returns the first row (not a
  uniqueness check), and preserves native pgx/sql ErrNoRows for empty success.
- `RowsRead` includes an over-limit witness, if observed. A partial count is not
  the complete result size.
- CommandTag/RowsAffected are native statement observations, not per-Item,
  whole-transaction, replica-visibility or crash-durability proof.
- The terminal native Rows.Err is always checked. If our own bound fails first,
  a subsequent drain failure is retained separately as cleanup evidence.
- Coincident context cancellation does not replace an observed server error;
  cancellation causes are attached when the native error actually carries that
  cancellation identity.

The raw/name byte limit and maximum 64 columns bound retained content separately
from metadata overhead. Working/evidence reservations also include conservative
cell headers, row-list storage/growth and native-error allowances. Tests independently
measure retained structural storage, including a cell-only negative control.
They are declared envelopes, not measured
heap/RSS or a total network-byte limit. Native frontend buffers, TLS, decoding,
server metadata, caller-retained copies and arbitrary caller causes add costs.
Composition bounds client population, inbox capacities and retained results.

## Reusable preparation

Database.Prepare retains a native connection/root allowance; Transaction.Prepare
borrows its parent and mutex. Preparation context cancellation does not end the
retained lifetime. Query/Exec reuse that exact native statement; Close is required,
idempotent and finalizes the original preparation evidence without new admission.
Copies share state and the original slot. Concurrent calls on one retained owner
are refused. A transaction has at most eight live preparations across its scope.

The package owns PgConn.Prepare/Deallocate descriptions directly, avoiding
Conn.Prepare's private name map and deferred failed-Describe cleanup. Native
ExtendedQueryBuilder uses the existing text-argument contract, ExecStatement
requests text results and RowsFromResultReader reuses bounded consumption.
For example, a non-UTC time.Time retains the ordinary UTC text representation
rather than switching to prepared-OID-directed timestamp wall-clock encoding.
Local argument encoding completes before execution is sent. Invalid arguments
or native statement errors do not silently reprepare/replay a mutation.

Failed preparation distinguishes ParseComplete cleanup responsibility. Any native
deallocation error closes and joins the connection: pgconn can return its error
before ReadyForQuery, and a subsequent finalizer must not consume that stale
message as its own successful acknowledgement. Primary preparation and independent
deallocation/retirement errors remain separate. Root byte reservations include
all eight retained descriptions, bounded native metadata and SQL text; recommended
lease capacity also covers eight savepoints and one finite child.

## Transactions and cleanup

TxOptionsV1 requires read-committed, repeatable-read or serializable isolation
and read-only/read-write access. Deferrable requires serializable/read-only.
There are no custom BEGIN/COMMIT strings or automatic retries.

A Transaction owns one connection and one root invocation. Statements are nested
calls and need a child lease/evidence slot, not another root/native acquisition.
Drain child deliveries incrementally. Commit/Rollback finalize using the root's
existing slot, even when the inbox is full. Concurrent/reentrant transaction
operations are refused rather than racing or silently queueing raw SDK use.
Copies of the facade share the same transaction lock and finalization authority;
copying a Go value cannot create an independent owner of the native connection.

Transaction.Savepoint creates a code-named bounded LIFO scope. Queries/preparation
remain on Transaction and execute inside the current server savepoint stack.
Release retains changes within the outer transaction; terminal Rollback performs
ROLLBACK TO followed by RELEASE so repeated use does not accumulate server scopes.
At most eight points are live. Copies share authority; out-of-order finalization
is refused. Release failure after acknowledged rollback remains distinguishable.
Failed scope cleanup makes the root rollback-only. Parent finalization settles
remaining points without inventing individual savepoint acknowledgements; use
Result.SavepointOutcome, not TransactionOutcome, for these observations.

Native failed status E remains recoverable through a controlled savepoint. Idle
status I/closed connection ends the retained transaction. Successful ordinary
COMMIT/ROLLBACK, including AND CHAIN, also revokes original authority even if a
new server transaction reports T. Raw SAVEPOINT/RELEASE conservatively makes the
root rollback-only; raw ROLLBACK TO is treated as terminal raw control. Use the
owned Savepoint API when recovery/continued use is required. No-op/chained cleanup
does not become an acknowledgement of the original transaction's outcome.

Canceling BEGIN's context never finalizes the transaction. A finalization budget
refused before native entry leaves it live for a later explicit cleanup.
After native finalization, repeated calls retain ErrTxClosed; they cannot replace
the original receipt. Outcomes distinguish commit acknowledgement, explicit rollback
acknowledgement, native ErrTxCommitRollback, and unknown finalization after failure.
A lost COMMIT reply is not permission to retry or proof of rollback.
Acknowledgement never strengthens PostgreSQL's configured durability contract.

Healthy protocol-idle connections return to puddle. Dead/busy/non-idle connections
are closed and joined via CleanupDone before the root allowance is returned;
already-closed handles are removed synchronously without a second destructor.
A failed startup seals its per-connection dial scope, closes owned sockets and
joins dials; retained error callbacks cannot reopen it. This fences I/O authority,
not the inaccessible failed-startup pgconn helper goroutine's remaining instructions.
Context values, including
net/http trace hooks, do not reach native dialing.

Native asynchronous pgconn cleanup has its own approximately 15-second deadline,
and native Close may discard protocol-termination errors. Only observable native
causes are retained; missing server cleanup acknowledgement is not invented.
There is no forced termination guarantee for OS/network primitives.

Assembly.Close cannot release a held transaction or unfinished call. Their explicit
finalization/termination must precede final pool release; nested cleanup remains
usable after owner admission stops. A timed-out final pool release retains exactly
one owned join operation and an explicit continuation. Historical timeout/native
causes survive later completion. Composition still owns aggregate continuation
and retained-cause budgets under the existing resource contract.

## Evidence, versions and verification

Qualified `database.pgx.v5` faults preserve original errors.Is/As without exposing
native SQL, credentials, addresses or error graphs through ordinary fmt/slog.
Runtime options, sources, facades, rows and results refuse JSON reconstruction.
Value formatters protect non-nil value/pointer/container forms, including numeric
verbs; ordinary nil fmt uses its native absence projection and pointer slog is
explicitly nil-safe. Direct nil Formatter invocation and invalid verbs bypassing
Formatter (such as %w on a non-error or %p on a non-pointer) are unsupported.
Intentional native-cause/data inspection is not redaction or a security boundary.

Each accepted call reserves independent in-process evidence before native work.
Handling a direct error does not release its delivery. A nil observer disables
only optional diagnostics. Attempt observations are SDK-entry lower bounds,
not exact wire traffic or retry permissions. No durable evidence/Temporal
serialization format is added, so replay is not this issue's gate.

Profile derives non-sensitive options from the same effective settings as construction.
Actual resource limits/revision, consuming Go/pgx/puddle/transitive selection and
service observations remain separate axes. A self-reported profile or missing
compatibility record cannot certify support.

- [Core options and poison controls](../../../../../../internal/database/pgx/v5/options_test.go).
- [Native pool/TLS/cleanup evidence](../../../../../../internal/database/pgx/v5/pool_test.go).
- [Result and cancellation tests](../../../../../../internal/database/pgx/v5/query_test.go).
- [Transaction and saturation tests](../../../../../../internal/database/pgx/v5/transaction_test.go).
- [Preparation and deallocation regressions](../../../../../../internal/database/pgx/v5/statement_test.go).
- [Savepoint recovery/removal](../../../../../../internal/database/pgx/v5/savepoint_test.go).
- [Native idle/lifetime and cleanup continuation](../../../../../../internal/database/pgx/v5/pool_lifecycle_test.go).
- [COPY stream refusal and session reset](../../../../../../internal/database/pgx/v5/protocol_test.go).
- [Composition, independent reception and rejecting controls](../../../../../../internal/database/pgx/v5/integration_test.go).
- [Consuming executable](../../../../../../internal/database/pgx/v5/testdata/consumer/main.go):
  executes configuration/ownership, explicitly not a server-query certificate.
- [Shared-core versus facade benchmark](../../../../../../internal/database/pgx/v5/query_bench_test.go):
  both paths use the same validation, admission, budgets, immutable results, cleanup
  and independent evidence. It measures useful-call cost and small dispatch overhead,
  not an independent native-driver/pooling comparison or production throughput.
- [Preparation reuse benchmark](../../../../../../internal/database/pgx/v5/query_prepared_bench_test.go):
  both modes share the same retained transaction, text values, copies, bounds,
  independent evidence and cleanup. Large-payload ranges may overlap; no universal
  latency/memory benefit or real-service throughput claim follows.
- [Opt-in real-service gate](../../../../../../internal/database/pgx/v5/service_test.go):
  creates/drops only its random dedicated test database after explicit authorization;
  independent reads and bounded real-server COMMIT/CREATE-response drops are separate
  from protocol-peer tests. Fresh maintenance connections reconcile only the owned
  database even if the creating connection lost its response. Cancellation observes
  PostgreSQL query entry and backend exit independently; transaction visibility,
  aborted COMMIT, empty/no rows and Query/Exec bounds have real controls.
  Compilation or omission is not a service pass.

Applicable architecture: [S01–S12](../../../../../architecture/internal-sdk-integration.md).
Scope and outstanding acceptance: [Issue #23](https://github.com/frost-leo/fathomry/issues/23).

Core completion and mechanism-specific counterexamples:
[Issue #28](https://github.com/frost-leo/fathomry/issues/28).

Native behavior references: [pgx v5.11.0](https://github.com/jackc/pgx/tree/5e583fa7aabfa88b796292f849fc9d7d75ac159d),
[puddle v2.2.2](https://github.com/jackc/puddle/blob/v2.2.2/pool.go),
[PostgreSQL cancellation](https://www.postgresql.org/docs/18/protocol-flow.html#PROTOCOL-FLOW-CANCELING-REQUESTS),
[transaction isolation](https://www.postgresql.org/docs/18/transaction-iso.html).
