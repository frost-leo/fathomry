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

# Doris interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** trusted composition and adapter maintainers.
**Status:** implemented with local actual-driver/protocol tests and limited
on-demand #111 cursor/public-Adapter service verification. Historical #42
finite-operation Doris 4.1.4 evidence remains separate; neither qualifies
production or the whole engine.
**Package:** `github.com/frost-leo/fathomry/internal/sqlengine/doris/v1`.

## Responsibilities and selected clients

The principal SDK is the existing `go-sql-driver/mysql v1.10.1`; `v1` in this
package path denotes that driver major. Native Stream Load uses standard Go HTTP.
OptionsV1, Go build, Doris FE/BE versions, deployment, catalog, Iceberg format and
wire modes are independent axes.

The [official ingestion SDK](https://github.com/apache/doris-sdk/tree/5f10fab27974343612bd25e98658fc6cd12daffa/stream-load-sdks/doris_go_stream_load)
remains a major-0 pseudo-version, not its README's
[unresolvable v1.0.0](https://proxy.golang.org/github.com/apache/doris-sdk/stream-load-sdks/doris_go_stream_load/@v/v1.0.0.info).
Its batching/retry/redirect/lifetime behavior is not used.
This package does not import the existing MySQL integration: MySQL8/InnoDB
qualification and its authentication, session and collation defaults cannot
certify Doris. The selected protocol instead uses `mysql_native_password`,
`utf8mb4_general_ci`, one result set, and one fresh connection per SQL command.
There are no automatic session SET statements or native configuration callbacks.

The boundary implements:

- `StreamLoad`: one native-table JSON physical batch, with row-quality and
  transaction/visibility evidence, never an external Iceberg commit.
- `InspectLabel`: one retained-label observation, without polling or resubmission.
- `Query`: one bounded, fully retained SQL result.
- `QueryCursor`: one owned incrementally consumed SQL result, without SQL
  pagination or a whole-result buffer.
- `Exec`: one authorized SQL text command with protocol acknowledgement and
  affected-row evidence, not an inferred commit/visibility certificate.
- `Profile`: copied effective declarations for `compatibility`, not readiness
  or service-version discovery.

The [capability classification](capabilities.md) includes DDL, CTAS, all requested
DML, catalogs, transactions and maintenance. Classification does not grant SQL
permissions or implement a universal engine facade.

The integration ends at Doris SQL and HTTP endpoints. A preconfigured external
catalog is addressed through fully qualified Doris SQL names, not another
client path. The package and its tests do not import an Iceberg or object-store
SDK, run Java, manage a REST catalog, or inspect/delete backing storage objects.
Backend-specific restrictions still matter to what Doris can execute; they do
not create another engine or storage implementation inside this package.

## Composition and calling sequence

1. Supply explicit `OptionsV1` to `PrepareV1`, with any strict `resource.Layer`
   overlays. Its `Selection` and `Reservation` describe the same validated
   resolution. `OptionsCopy` is deliberate sensitive access to a detached copy.
   Preparation performs no network I/O. `Select` remains a convenience wrapper.
2. Attach `resource.WithLimits` using that preparation's `Reservation().Limits`.
   The reservation includes source, work and independent evidence bytes. Legacy
   `LimitsV1` describes typed defaults only, not a differently overlaid resolution.
3. `resource.Assemble` owns the selected source. Separate assemblies construct
   independent owners; `resource.Borrow` or `Delegate` explicitly shares/transfers
   the original source and its admission allowance.
4. `Bind` joins the exact selection, mandatory independently owned
   `invocation.Inbox[Result]`, and optional diagnostic observer.
5. Call methods with a caller context and non-secret `fault.Correlation`.
   A direct error with no receipt is setup/admission rejection. After acceptance,
   inspect the receipt's primary error, cleanup error and value together.
6. The independent receiver consumes and explicitly releases each inbox delivery.
   Receiving without releasing does not free evidence capacity. Close the owning
   assembly after live calls and borrowers have ended.

Finite calls and cursor setup/pages are synchronous; there is no asynchronous uploader, user callback,
untracked queue, polling worker, or retained transaction/statement handle.
Source admission is the only queue and bounds both active and queued calls.
The source owns immutable settings; each admitted call owns its sockets and
transport. Cleanup needs no additional admission/evidence slot.

Assembly close does not cancel accepted callers. A close deadline with live users
is incomplete, not release or rollback; callers retain the finite operation
context and the owner can continue cleanup through `resource`.

### Incremental SQL lifecycle

`QueryCursor(setup, lifetime, ...)` admits one root. `Timeout` bounds admission
and setup separately; `CursorTimeout` bounds the full accepted lifetime.
The MySQL driver's `QueryContext` retains the owning lifetime, **not** setup.
Successful setup cancellation cannot invalidate accepted Rows. Failed setup
retires the connection and reports through the accepted root receipt.

`Next(ctx, childCorrelation)` reserves independent evidence within the root's
existing lease; its correlation names that root as Parent. Each page is bounded
by `MaxPageRows` and `MaxPageBytes` (including metadata). One copied lookahead row
recognizes exact-bound EOF without dropping/reordering data. A too-large row
fails rather than being truncated. Whole-response `MaxCursorRows` and
`MaxResponseBytes` are never reset between pages. Finite Query and JSON batches
still use `MaxRows`; finite retention still uses `MaxResultBytes`.

Only positive framed EOF sets Complete on the last page and terminal root.
Intermediate pages are not complete business outputs. Root evidence contains
metadata/cumulative counters but no accumulated rows. RowsRead counts decoded
rows including lookahead; BytesRead counts their copied value bytes plus one
metadata copy. ResponseBytes includes protocol framing and authentication.

Next is serial; overlapping use is refused. Every admitted page has a fresh
Timeout bounded by its caller context; cancellation terminates the cursor.
Admission refusal leaves it usable. Close can cancel an in-flight page, joins
local native use, and uses the existing root reservation even at saturation.
Its context and Timeout bound waiting, not ownership; repeated Close observes
the same terminal facts. One bounded cursor worker performs automatic lifetime
finalization. The socket is retired before Rows.Close can drain unread results.
Released is local evidence, not remote SQL termination or rollback.

Claim the live root's inbox delivery before receiving pages, then release each
page incrementally and the root after actual release. Recommended MaxLeases=2
supports the root plus one synchronous page; smaller trees refuse cursor setup.

## Explicit transport, input and memory limits

SQLAddress is a non-unspecified literal IP plus port. HTTPOrigins contain at most
16 exact `scheme://host:port` authorities, without paths, query, userinfo, fragments
or escaped authorities. The first is the initial FE/BE; the rest are explicitly
trusted redirects **in the same authorized Doris deployment**. Membership alone
does not prove common cluster identity.

Default mode requires explicit PEM roots, certificate-chain verification and
TLS 1.2 or newer. SQLServerName verifies SQL identity; HTTP uses each origin's
hostname. Plaintext is a separate isolated-test mode requiring HTTP origins and
no TLS settings, never an opportunistic fallback. There is no ambient proxy,
global driver registry, caller transport, LOCAL INFILE callback or raw SDK escape.

Only 307/308 redirects are eligible. At most two hops may preserve the exact
method, native path, query, label and replayable bounded body. Each target must
match an authorized origin; credentials never follow a path/scheme/authority
escape. A redirect is not a mutation retry or proof that an earlier hop had no
effect. Native ErrorURL/message fields are never fetched or exposed as diagnostics.

Doris FE can echo Basic credentials in redirect userinfo. Only an exact match
to the configured username/password is accepted, and userinfo is removed before
constructing the next request URL. Authentication still comes from configuration;
this exception does not relax origin, scheme, path, query or hop validation.

| Setting | Default | Allowed bound |
| --- | --- | --- |
| Active / Queued calls | 4 / 0 | 1–32 / 0–64 |
| Timeout | 10 seconds | 1 millisecond–1 minute |
| JSON batch bytes | 1 MiB | 1 KiB–8 MiB |
| Rows per batch or SQL result | 1024 | 1–65536 |
| Retained SQL cell/name/type bytes | 4 MiB | 1 KiB–16 MiB |
| MySQL packet payload | 1 MiB | 1 KiB–4 MiB |
| MySQL response including handshake/framing | 8 MiB | 1 KiB–32 MiB |
| HTTP response body | 64 KiB | 1 KiB–1 MiB |
| Cursor lifetime | 1 minute | 1 millisecond–1 hour |
| Rows per incremental page | 128 | 1–65536 |
| Page cell/name/type bytes | 256 KiB | 1 KiB–16 MiB |
| Rows across a complete cursor response | 65536 | 1–1048576 |
| SQL text / SQL columns | 64 KiB / 64 | Fixed ceilings |
| HTTP response headers | 32 KiB | Fixed transport ceiling |

Timeout applies separately to admission and execution. A caller deadline can
bound their combined lifetime; connection, response consumption and local I/O
cleanup use the same finite execution deadline rather than restarting per hop.

Batch.JSON is borrowed without concurrent mutation until the synchronous call
returns; copying and JSON validation happen after admission. The batch must be a
nonempty UTF-8 JSON array of objects, at most 128 fields per object, with no
duplicate top-level object keys. Database/table names use 1–128 ASCII
letters/digits/underscores. Labels additionally permit hyphens.

Stream Load fixes JSON array decoding, single-phase loading, strict mode,
zero filtering tolerance and Group Commit off. It adds no staging table, JSON
conversion through float64, data transformation, merge/delete header or hidden
batch coalescing. Native table models can still aggregate or upsert rows.

Packet length, column metadata, row framing/count and total SQL response bytes
are checked before the driver can allocate from unchecked packet/count fields.
Malformed EOF-like rows are rejected before the native parser can certify an
empty result. On early termination the single-use socket is retired, never
drained for pool reuse. Driver closure still releases its context watcher.
Reserved greeting bytes must be zero: Doris need not advertise the legacy
CLIENT_LONG_PASSWORD bit, but unowned MariaDB extension modes must not reach
the native driver's result parser.

Root working and independent evidence reservations also cover copies, row/cell
objects, metadata, bounded replies and error material. They are accounting
envelopes, **not an RSS ceiling**: TLS/Go runtime/OS memory, caller input and
deliberately retained result copies remain separate. There is no latency,
throughput or stable-memory superiority claim.

## Values and effect evidence

`Result` and `Row` keep immutable private storage; copy methods return caller-owned
slices/bytes. A nil cell is SQL NULL, while a non-nil zero-length cell is empty.
DECIMAL, dates/times, JSON/nested representations and binary values retain native
text/bytes; signed and unsigned driver integers are formatted exactly.
Floating-point values use the driver's float32 (FLOAT) or float64 (DOUBLE)
representation and shortest round-trip text at that precision, not preservation
of the original lexical spelling.
Names, type metadata, decimal precision/scale and nullable presence are copied.
Server-side type/timezone/precision conversion cannot be repaired by copying;
each deployment/catalog must qualify the type semantics it relies on.

SQL is structurally bounded, **not a SQL sandbox or read-only enforcement**.
Even a Query can submit SQL with effects. Composition's roles/permissions,
target/catalog choices and statement semantics remain authoritative.
Preparation/parameters, persistent SET state, multi-statements, transaction
sequencing, cross-catalog transactions and pooling are not supplied.

`Dispatched` means client entry was attempted, not observed server receipt.
`SQLAcknowledged` means the server returned a native command acknowledgement.
It does not promote affected rows, SQL COMMITTED status, external file existence,
or the MySQL greeting to durable/visible data or an attested Doris build.
`ServerGreeting` is only the bounded protocol string.

Native load evidence separates these facts:

| Evidence | Meaning |
| --- | --- |
| NotDispatched | This accepted call did not attempt an HTTP dispatch; not proof that its label never existed |
| Unknown | No qualified effect conclusion, including lost replies, HTTP errors and UNKNOWN label observations |
| Pending | Retained label is PREPARE/PRECOMMITTED; no completion certificate |
| Committed | Publish Timeout or COMMITTED label; visibility remains unconfirmed |
| Visible | Valid native Success identity/transaction evidence or a VISIBLE retained-label observation |
| Rejected / Aborted | Native Fail response / ABORTED label observation; not a client-derived rollback inference |
| TransactionKnown / RowsKnown | Independent presence flags; absent/null/malformed counters are not zero |
| Duplicate | Label already exists; even FINISHED cannot match the prior payload to this caller's payload |
| HTTPStatus | Last observed response status; zero means unobserved |

`StreamLoad` is Complete only with a valid Success transaction/label, all
consistent row counters, the expected total, zero filtered/unselected rows, and
visible acknowledgement. A filtered Success keeps its commit/visibility evidence
**and** ErrRowQuality. Missing counters do not erase an otherwise observed
transaction, but prohibit completion. Counts accompanying Fail are diagnostics,
not evidence of persisted rows. `InspectLabel` never invents table, payload,
transaction or row counters; its Complete flag only denotes its successful
VISIBLE observation.

Labels have database scope and finite retention. The inspected server distinguishes
12-hour streaming-label and three-day general defaults, with additional
count-based cleanup; the actual deployment policy is unknown. UNKNOWN cannot
justify fresh-label replay. Callers own label/payload reconciliation and any
durable ledger; this package never promises exactly-once storage.
[Retention selection source](https://github.com/apache/doris/blob/ad35a140c7fd0b842f18c23300bac581f7d04326/fe/fe-core/src/main/java/org/apache/doris/transaction/TransactionState.java#L665-L682).

Cancellation or timeout terminates local waiting/I/O under the call budget, not
necessarily remote SQL, load, lake file creation or commit. Cleanup joins tracked
HTTP dials and retires every owned socket; private SDK/standard-library goroutines
may finish non-I/O bookkeeping after their handles close. Released concerns local
ownership, never confirmed server cancellation or remote orphan cleanup.

## Errors, privacy and executable evidence

Provider error kinds adapt to `fault`. Native SQL errors remain available through
`errors.As`, with safe ordinary formatting; deliberate native-cause inspection
is borrowed read-only and can expose server text. Do not log SQL, credentials,
rows, label/target identities, payload hashes, URLs or native messages.
Runtime values reject accidental JSON reconstruction/serialization. Higher layers
must define any durable DTO separately.

Attempt observations count intercepted connection/command or HTTP-hop entry,
not logical commits. They are intentionally inexact, never inferred item counts.
The required inbox is independent of optional bounded, lossy diagnostics.

Tests use real driver/standard-library execution against local protocol peers;
those peers are not Doris or Iceberg. See
[composition](../../../../../../internal/sqlengine/doris/v1/integration_test.go),
[connection](../../../../../../internal/sqlengine/doris/v1/connection_test.go),
[SQL](../../../../../../internal/sqlengine/doris/v1/query_test.go),
[load](../../../../../../internal/sqlengine/doris/v1/load_test.go),
[redirect/TLS](../../../../../../internal/sqlengine/doris/v1/http_test.go), and
[explicit native service gate](../../../../../../internal/sqlengine/doris/v1/service_test.go).
The consuming executable test verifies actual contributing driver modules.
Fuzz tests cover native framing and JSON response classification.

The native service tests are opt-in via `FATHOMRY_DORIS_NATIVE_TEST_CONFIG`, an
owner-supplied private JSON file matching `nativeServiceConfig`. It requires
explicit fixture authorization, existing SQL/HTTP endpoints, an existing
`gh42_` database and an explicit replication setting. Each test creates its own
random `gh42_<run>` table; native loads use distinct `gh42-<run>` labels. Unknown
mutation outcomes retain the table for owner reconciliation rather than racing
an unresolved remote writer with DROP. Cleanup drops only the exact test-owned
table and checks exact Doris metadata absence, not physical storage purge.

The historical #42 qualified isolated profile was one FE and one BE from the verified Apache
4.1.4 distribution, reporting `doris-4.1.4-rc04-ad35a140c7f`, with
`mysql_native_password`, explicit plaintext endpoints over an owner-controlled
SSH tunnel, replication 1, DUPLICATE KEY and UNIQUE KEY tables. Tests cover:

- 256-row strict JSON load through FE-to-BE redirect, fresh exact SQL reads,
  DECIMAL(38,5) values/metadata, LARGEINT, FLOAT, DATETIME(6), Unicode and NULL/empty.
- Visible and UNKNOWN label observations, repeated-label rejection, invalid-row
  rejection with unchanged read-back, and bounded partial query results.
- 64-row SQL INSERT, UPDATE, DELETE, MERGE, overwrite and truncate with complete
  expected-rowset checks after each operation.

TLS trust/rejection and malformed/canceled/overloaded protocol behavior are
tested with local peers, not certified against a production deployment. External
catalog persistence/interoperability is not claimed by these native-table tests;
it remains the deployment/catalog owner's qualification responsibility. Neither
direct storage inspection nor an independent Iceberg reader is a client dependency
or a prerequisite for the ordinary Doris test suite.

[Issue #42](https://github.com/frost-leo/fathomry/issues/42) records the selected
scope, corrective review and qualification limits.

[Issue #111](https://github.com/frost-leo/fathomry/issues/111) adds resolved
preparation and incremental consumption, with [cursor tests](../../../../../../internal/sqlengine/doris/v1/cursor_test.go)
and a [public Adapter](../../../../adapters/sqlengine/doris/v1/interface.md).
The owner later authorized a limited on-demand run against the existing isolated
single-FE/single-BE fixture. An external public-only consumer passed race-enabled
finite/incremental, strict-load/label and Framework Fixed/Follow tests on small
DUPLICATE KEY tables, with exact cleanup. See the
[public qualification boundary](../../../../adapters/sqlengine/doris/v1/interface.md#capability-coverage-and-qualification).
This does not qualify other table models, external catalogs, deployment TLS,
failover, service performance or production for the changed Adapter.
