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

# Trino interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** internal composition, adapter and integration maintainers.
**Status:** implemented bounded direct profile; verification scope and exclusions
are explicit below. Local checks and the isolated Trino 482/Iceberg v2 fixture
passed at the issue's recorded worktree manifest. This is not a public workflow
API or connector certificate.
**Package:** `github.com/frost-leo/fathomry/internal/sqlengine/trino/v0`.

## Responsibilities and call sequence

The package integrates the official `trino-go-client v0.333.0` without importing
other engines or the Iceberg Go integration. One SQL statement is one physical
operation. Client-side multi-row VALUES and server-side set operations are both
available; there is no bulk-upload protocol, automatic chunking or transaction
emulation.

1. Supply explicit `OptionsV1` and optional authorized resource layers to `Select`.
   Version zero means configuration format 1, independently of SDK major 0,
   Trino server version, table format and deployment revision.
2. Attach `resource.WithLimits(selection, LimitsV1(options))`. Overlays changing
   budgets require matching **effective** limits, not the original defaults.
3. Assemble with caller-owned initialization and cleanup contexts. Readiness
   executes `SELECT version()` through terminal protocol completion. Native
   `database/sql.Ping` performs no network request and is deliberately not used.
   Readiness does not establish Catalog access or write permissions.
4. Composition owns an `invocation.Inbox[Result]` with sufficient count and bytes,
   then calls `Bind`. `EvidenceBytes` reports the per-receipt reservation.
5. Call `Query`, `Execute` or `Insert` with work and separate cleanup contexts,
   a non-secret correlation ID and authorized inputs. These calls are synchronous.
   A nil method error means invocation setup succeeded, **not SQL success**.
   Inspect the returned receipt's result and `Result.Err()`.
6. Independently receive, inspect and release the evidence delivery. Handling a
   caller-visible error does not free or discard this obligation.
7. Close the owning assembly after its borrowers and operations end. Retained
   capabilities have no raw client, transaction, stream or shutdown escape.

See [Go contracts](../../../../../../internal/sqlengine/trino/v0/doc.go) and
[composition tests](../../../../../../internal/sqlengine/trino/v0/integration_test.go).
The [shared integration standard](../../../../../architecture/sdk-integration.md)
owns resource, invocation, fault and compatibility responsibilities.
`internal/conformance` is used only by tests.

## SQL and argument boundaries

`Query` returns bounded direct JSON rows for SELECT/VALUES/TABLE, ordinary WITH,
SHOW/DESCRIBE and EXPLAIN without ANALYZE. `Execute` drains ordinary DDL, CTAS
and DML, returning protocol evidence and optional aggregate update count; it
validates but discards any returned data rows. `Insert` quotes the selected
catalog/schema and supplied table/columns, building exactly one multi-row INSERT.
Empty/ragged batches, duplicate columns and excess rows/parameters/bytes reject.

The statement-family/single-statement scanner is **not a SQL parser, privilege
system or sandbox**. Trusted composition/adapters authorize SQL and tables; server
privileges remain necessary. `Writes` defaults false. `Maintenance` additionally
gates CALL, ANALYZE, REFRESH and ALTER TABLE EXECUTE. Both gates must be enabled for
maintenance. The selected grammar has non-nested block comments and CR/LF line
comment termination. Comments/quoted semicolons are understood; multiple statements,
transaction/session control, SQL preparation control, GRANT/REVOKE, EXPLAIN
ANALYZE and WITH SESSION overrides are excluded. Session overrides cannot silently
replace the source's server retry policy.

Positional parameters admit nil, bool, selected signed integers, unsigned values
within int64 range (not uint8), UTF-8 strings without NUL, byte slices,
strict decimal/scientific `native.Numeric` text, bounded time.Time values and
non-nil recursively bounded slices. Nil byte slices mean SQL NULL; empty bytes
mean empty VARBINARY. Native float arguments, maps, arbitrary pointers,
driver.Valuer, named options, access tokens and progress callbacks are rejected.
Use SQL constructors and CAST with string/positional parameters where necessary.
All inputs remain borrowed and must not be mutated until the call returns.

Parameters use native EXECUTE IMMEDIATE serialization, not header-based prepared
statements or guaranteed server plan caching. Input and expanded SQL have byte
bounds; no native number or timestamp conversion is treated as a bulk-write API.

## Result meanings and exact representations

The immutable result exposes copies of JSON rows and column/type-signature
metadata. Numeric tokens remain exact JSON numbers; DECIMAL and temporal values
remain strings; VARBINARY remains base64; arrays, ROW, MAP and nulls preserve
direct-protocol shape. Decoding into `float64` is the consumer's choice; use
`json.Decoder.UseNumber` for exact integers. No implicit local-timezone conversion
or nanosecond truncation is applied. Scalar/nested signatures are allowlisted and
bounded to depth 8; unknown non-null values, NUMBER/VARIANT/geospatial extensions,
malformed arity and invalid selected scalar encodings reject. Integer ranges,
REAL/DOUBLE overflow, DECIMAL precision/scale, base64 and scalar MAP key/value
encodings are checked. Date/time, interval, UUID, IP and JSON strings remain
unparsed text rather than a second SQL type system. Structural case-aliased
duplicate JSON fields reject, without folding case-sensitive user MAP keys.

The official driver drains pages via native ExecContext for both operations.
The owned transport captures bounded direct data **before** native decoding.
This preserves data returned in the initial POST, and avoids native Rows'
datetime conversion. No live Rows reaches a consumer: an early limit/cancellation
ends native work, while a slow consumer after return holds only its own copies.

| Fact | What it establishes |
| --- | --- |
| `Submissions` | Zero/one entry into the owned statement transport, not committed writes. |
| `QueryID` | Observed private correlation, not deduplication or durable resumption. |
| `Terminal` | Valid identified reply without nextUri; can coexist with server/local errors. |
| `Succeeded` | Terminal reply without a server error; not result validation or cleanup. |
| `Complete` | Successful terminal evidence plus error-free native draining and data validation. |
| `UpdateCount` | Optional final aggregate count; absent and zero remain distinct, never per-row attribution. |
| `Effect` | NotSubmitted, ReadOnly, Acknowledged or Unknown; failed attempted mutations remain Unknown. |
| Cancellation acknowledgement | HTTP 204 only; never observed remote termination, rollback or spooled-data deletion. |

Validated rows can form a partial prefix alongside failure. They are not a
complete batch unless `Complete` is true. Cleanup failures remain independent of
data completeness. A later read can reconcile a particular effect, but the
package does not invent generalized recovery, Item/Run disposition or durability.

## Ownership, bounds, retries and authentication

Each operation owns a fresh native logical connection, statement and transport.
A random private HTTP registration is resolved synchronously and deregistered
immediately; no lazy database/sql pool or mutable session is shared. Server-set
session/role/catalog/preparation/transaction state is rejected. Borrowing reuses
one authoritative resource allowance, not an additional pool or quota.

Requests and nextUri are confined to the exact configured origin and the selected
`/v1/statement/{queued|executing}/{queryId}/{slug}/{token}` paths. Continuations
must belong to the originally observed query ID before any GET or DELETE.
Redirects, ambient proxies, compressed response bodies and
spooling are excluded. The driver spooling advertisement is removed, and
object-shaped spooled data is rejected before native segment workers start.
There are no segment downloads, acknowledgements or retention guarantees.

The owned transport pre-reads bounded response bodies and closes the actual
network body even on failure, before the SDK receives a memory-backed copy.
On exit, work is canceled and native statement I/O is joined; incomplete queries
with a usable observed ID/continuation receive at most one explicit DELETE using
the caller's cleanup context. The SDK's implicit global-timeout DELETE path cannot
send requests. Owned dials/sockets are closed before releasing the invocation.
No query ID means remote cancellation cannot be targeted; effects stay unknown.

Readiness uses the same local shutdown rules but defers its first remote DELETE
to `Resource.Release`, which receives Assembly's separate cleanup context. If
cleanup has not entered HTTP, the assembly retains an explicit continuation. An
attempted DELETE is never replayed on repeated Close, even after a lost reply.
Only a bounded cancellation target survives failed readiness, not native sessions
or sockets. Earlier cleanup errors remain inspectable after eventual release.

HTTP retry statuses are returned as failures, not replayed. The native server
session specifies `retry_policy=NONE`. Server-side connector commit retries and
external infrastructure remain outside the client's observation; invocation
attempts are explicitly inexact lower bounds over intercepted transport entries.
Do not add outer retries to ambiguous writes merely because the method failed.

| Dimension | Zero-value default | Allowed configuration |
| --- | --- | --- |
| Active operations / waiting queue | 2 / 0 | 1–8 active; no waiting queue |
| SQL bytes / positional parameters | 1 MiB / 65,536 | 128 B–4 MiB / 1–65,536 |
| Query/batch rows / columns | 65,536 / 256 | 1–1,048,576 / 1–1,024 |
| Response page body / retained result bytes | 1 MiB / 8 MiB | 128 B–8 MiB / 128 B–32 MiB |
| Coordinator pages / cumulative work-response body bytes | 1,024 / 64 MiB | 1–4,096 / one page–256 MiB |
| Work / cleanup timeout | 1 minute / 5 seconds | 1 ms–30 minutes / 1 ms–1 minute |

`MaxResultBytes` covers retained rows and metadata. `MaxWireBytes` and `WireBytes`
refer to response **body** bytes, excluding HTTP/TLS headers; the separately bounded
cleanup body and one oversize-detection byte may increase the final counter.
Header size is capped at 32 KiB. JSON depth is capped at 32 with bounded nodes.
`LimitsV1` reserves a conservative working envelope and the inbox reserves retained
evidence before work. These controls are not a measured RSS/GC bound, server
memory limit or throughput guarantee. Caller-held inputs/copies and arbitrary
caller context-cause graphs remain caller-owned.

HTTPS verifies server identity with TLS >=1.2, using system roots or explicit PEM
roots. Basic password and Bearer token modes are exclusive; neither can be sent
over plaintext. Explicit plaintext admits only unauthenticated trusted-network
HTTP. No credential discovery, refresh worker, insecure TLS flag, custom client
escape, per-statement auth, Kerberos or mTLS profile is supplied. Native causes
remain available through errors.Is/As; ordinary formatting/logging/serialization
does not expose SQL, values, endpoints, credentials or native messages.

## Native capability classification

This classification concerns the bounded Go integration and the selected Trino
482 Iceberg REST / format-2 Parquet profile, not all connectors or formats.
The service fixture independently verifies its connector and table properties.
Catalog backend/deployment versions are separate evidence, not inferred from
`Client.Profile` or Ping.

| Capability | Classification and limits |
| --- | --- |
| Finite queries, metadata and snapshot time travel | Supported bounded direct SQL; explicit ordering and snapshot selectors belong to the caller. |
| Multi-row INSERT and INSERT SELECT | Supported single-statement batch shapes; aggregate counts only, no binary upload or automatic chunks. |
| DDL, CTAS, table/schema/column evolution and comments | Restricted by selected connector, properties and privileges; representative operations have service fixtures. |
| UPDATE, DELETE, MERGE and TRUNCATE | Restricted native DML, not append-only. Format 2 is needed for the qualified row-level mutation profile. |
| Table commit atomicity | Connector-defined single-table metadata publication; not cross-statement/table/chunk atomicity. |
| Managed transactions, raw START/COMMIT/ROLLBACK and savepoints | Unsupported. Missing sql.Tx does not negate a single Iceberg table commit. |
| ANALYZE, optimize and optimize_manifests | Restricted maintenance SQL behind explicit gates; no scheduler or blanket maintenance qualification. |
| expire_snapshots, remove_orphan_files, rollback_to_snapshot | Restricted/destructive native operations; classification is not target-specific authorization. Not run by the default service fixture. |
| Branch creation, branch-targeted DML, WAP/promotion | Unsupported integration claims; snapshot/reference reads do not establish write/promotion support. |
| Spooling, Iceberg v3 and unselected connectors | Unsupported/unqualified by this profile; v3 must not be inferred from Trino version. |

Upstream basis: [driver v0.333.0](https://github.com/trinodb/trino-go-client/blob/v0.333.0/trino/trino.go),
[Iceberg connector 482](https://github.com/trinodb/trino/blob/482/docs/src/main/sphinx/connector/iceberg.md)
and [Trino protocol 482](https://github.com/trinodb/trino/blob/482/docs/src/main/sphinx/develop/client-protocol.md).
Atomic table metadata replacement does not make an unacknowledged mutation safe to retry.

## Executable evidence and remaining qualification

[Focused tests](../../../../../../internal/sqlengine/trino/v0/transport_test.go)
cover replay refusal, lost replies, late errors, cancellation acknowledgement
versus completion, routing/session/spooling rejection and malformed results.
[Bounds/types tests](../../../../../../internal/sqlengine/trino/v0/protocol_test.go)
include a fuzz target. [Options/TLS tests](../../../../../../internal/sqlengine/trino/v0/options_test.go)
exercise trust/auth conflict rejection. Composition covers independent evidence,
saturation, aliases, source isolation and compatibility that does not certify mocks.

The opt-in [service tests](../../../../../../internal/sqlengine/trino/v0/service_test.go)
use an explicitly supplied local YAML file, never ambient discovery. The fixture
reads `trino.direct_host`, `direct_port`, `auth`, `username_hint`,
`catalogs`, `iceberg_default_schema` and `iceberg.trino_catalog`. Its selected
service profile requires Trino 482 and unauthenticated HTTP; TLS/auth qualification
above is loopback-only. Unrelated deployment fields are not used.

From the dedicated worktree, after explicit service authorization:

```sh
go test -race -count=10 -timeout=2m ./internal/sqlengine/trino/v0
go test -run '^$' -fuzz '^FuzzDirectPage$' -fuzztime=10s -parallel=2 ./internal/sqlengine/trino/v0
go test -run '^$' -fuzz '^FuzzSingleStatementGate$' -fuzztime=10s -parallel=2 ./internal/sqlengine/trino/v0
FATHOMRY_TRINO_SERVICE_CONFIG=/authorized/connection-info.local.yaml \
  go test -race -run '^TestTrino(ServiceProfile|DirectService)$' ./internal/sqlengine/trino/v0
FATHOMRY_TRINO_SERVICE_CONFIG=/authorized/connection-info.local.yaml \
FATHOMRY_TRINO_SERVICE_WRITES=1 \
  go test -race -run '^TestTrinoIcebergV2Service$' -timeout=6m ./internal/sqlengine/trino/v0
```

The write fixture uses a new `gh41_<random>` namespace, never an existing table.
Cleanup targets only its two known tables, then drops the empty namespace without
CASCADE and independently observes their catalog absence. This does not prove
physical object purge, retention cleanup or production failure recovery.

Real-service outcomes are recorded at the tested worktree manifest for
[Issue #41](https://github.com/frost-leo/fathomry/issues/41), separately from historical
483 preparation. No performance certification, cross-table recovery, production
auth, all-connector support or Temporal replay is claimed. No Temporal commands or
durable DTOs are changed by this process-local integration.
