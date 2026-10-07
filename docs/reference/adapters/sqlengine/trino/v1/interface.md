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

# Trino Adapter interface

[Documentation](../../../../../README.md) / Public package reference

**Audience:** independent Go applications and Framework resource composition.
**Status:** implemented bounded direct-JSON public capability with controlled
native-client protocol, race and independent-consumer tests, plus fresh Trino 482 /
Iceberg format-2 Parquet / direct JSON / explicit unauthenticated HTTP acceptance.
The live fixture verifies exact public read/write/evolution and independent cleanup;
this does not qualify arbitrary connectors, catalog backends or production.
**Package:** `github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1`.

## Responsibilities and construction

The Adapter exposes the supported Trino integration through independent public
[operation and evidence ownership](../../../v1/interface.md). It supports finite
Query, full admitted Execute, one-physical-statement Insert and owned incremental
Read. It does not return a driver, SQL connection, private assembly, arbitrary
transport or producer authority. The same capability works directly and through
[Framework](../../../../framework/v1/interface.md) Fixed/Follow bindings.
Its own [SQL-engine metadata and budgets](../../v1/interface.md) are data contracts,
not a universal SQL or transaction API.

1. Declare `Settings`. Use `Configuration(defaults)` with public strict
   `configsource.Prepare` for loadable layers; use `Validate` for a pure direct
   settings check. Neither performs readiness SQL or reads an ambient DSN.
2. Obtain `Recommend` and construct caller-owned `adapters.Runtime` and
   `adapters.Inbox[Result]`. Supply both through `Dependencies`; an Observer is
   optional bounded diagnostics, never a replacement for required evidence.
3. Call `Open`. Unlike a lazy SQL pool constructor, this executes bounded
   `SELECT version()` readiness. Retain and close **every non-nil Owner**, even
   if acquisition fails. Its context owns the source lifetime.
4. Use `Owner.Client()` for operations. `Client` and `Handle` do not grant
   shutdown or raw native access. Receive released evidence incrementally.
5. Close readers, then the source Owner, then the caller-owned operation Runtime.
   Continue a timed-out close on the same owner. A new wait context does not
   authorize new business SQL on an already canceled source.

`Owner.Info`, `Handle.Info` and result `Source` return detached preparation
identity/provenance. They are not resource-generation or readiness claims.
`Client.Profile` returns detached effective settings and the observed coordinator
version without executing another query. That observation proves neither
connector permissions nor a table format. `Build` inspects only the selected SDK
in the executing binary, preserving unknown, replaced and redacted facts rather
than manufacturing a version from `go.mod`. It does not disclose local module
replacement paths or enumerate unrelated dependencies.

## Settings, defaults and budget composition

All supported [settings](../../../../../../adapters/sqlengine/trino/v1/options.go)
have explicit JSON/mapstructure names. Durations are integer **nanoseconds**;
byte limits count bytes, not characters. Direct typed zero positive limits choose
native defaults. `Version=0` selects options format 1, independent of public API
v1, SDK v0.333.0, coordinator or business-schema versions.

`Configuration` resolves those defaults once before strict layered preparation.
Absent fields inherit; an explicit zero positive bound is rejected, not silently
defaulted again. False permission flags remain false. Unknown/duplicate fields,
null scalar values, coercions and overflowing numbers reject. Intentional
configuration JSON serialization includes credentials; ordinary formatting and
structured logging redact the settings.

All preparation routes reject text defaults whose aggregate raw size already
exceeds the 1 MiB configuration-document ceiling before copying those payloads.
Original identity/version/UTF-8 ordering, escaped-size checks and valid layer
repairs remain unchanged. This does not raise any source, result or reader bound.

| Fields | Meaning and bounds |
| --- | --- |
| Name, Version | Native preparation identity and format; format 1 is supported |
| Endpoint, User | Explicit HTTP(S) origin and bounded user, not a DSN, proxy or arbitrary continuation endpoint |
| Password, BearerToken | Mutually exclusive explicit credentials; HTTPS only; no discovery or refresh worker |
| RootCAPEM, Plaintext | HTTPS system/explicit PEM roots with identity verification; explicit plaintext permits only unauthenticated HTTP, not insecure TLS |
| Catalog, Schema | Optional bounded identifiers; Schema requires Catalog; Insert requires both |
| Writes, Maintenance | False by default; Maintenance requires Writes; neither certifies connector support or privilege |
| MaxActive | Default 2, supported 1–8 native statements/readers per source; no native waiting queue |
| MaxSQLBytes, MaxParameters | Defaults 1 MiB / 65,536; supported 128 B–4 MiB / 1–65,536 |
| MaxRows, MaxColumns | Finite query/batch defaults 65,536 / 256; supported 1–1,048,576 / 1–1,024 |
| MaxPageBytes, MaxResultBytes | Default 1 MiB response page / 8 MiB retained finite result; supported 128 B–8 MiB / 128 B–32 MiB |
| MaxPages, MaxWireBytes | Finite defaults 1,024 pages / 64 MiB response bodies; supported 1–4,096 / one page–256 MiB |
| Timeout, CleanupTimeout | Defaults 1 minute / 5 seconds; supported 1 ms–30 minutes / 1 ms–1 minute |
| MaxReadRows, MaxReadPages | Incremental defaults 1,048,576 rows / 4,096 pages; supported 1–16,777,216 / 1–65,536 |
| MaxReadWireBytes, ReadTimeout | Incremental defaults 256 MiB / 1 minute; supported one page–4 GiB / 1 ms–30 minutes |

See the [native contract](../../../../internal/sqlengine/trino/v0/interface.md)
for precise endpoint, identifier, trust, JSON-shape and transport constraints.
Read shares the page/type/SQL bounds but uses separate total read rows, pages,
wire bytes and lifetime; increasing only finite result caps is not incremental
consumption. Its metadata remains bounded by the retained-result allowance.

`Recommend` derives source, native-work, page/copy overlap and evidence costs
into `sqlengine.Policy`; `Using` accepts `sqlengine.Budget` and metadata getters
return SQL-engine-owned types, without database-layer aliases. SourceWorkBytes
and SourceEvidenceBytes expose exactly the charges already included in totals.
The recommendation derives those charges
from the same pure resolved native preparation used by Open. The source
reservation includes readiness work. Failed readiness can retain a full bounded
native error graph: its source evidence remains charged after native release
until acknowledgement, rather than shrinking to a metadata-only allowance.
Evidence and actual-work reservations have
independent lifetimes. These are conservative declared accounting envelopes,
not upfront allocations, measured RSS, server memory or throughput guarantees.
Caller-retained input and output copies need separate application bounds.

Combine policies deliberately for simultaneous sources and replacement
generations. `Using` checks each selected source against its admitted `Budget`
before native application dispatch. It does not silently enlarge a supplied
Runtime for a larger replacement. Caller contexts bound public admission waits;
native phase timeouts do not replace an admission deadline.

## Finite SQL and exact input values

`Query(ctx, cleanupCtx, Statement)` drains all pages and retains a bounded exact
JSON row array. It admits the supported SELECT/VALUES/TABLE/SHOW/DESCRIBE and
non-ANALYZE EXPLAIN read families. This family gate is not a SQL authorization
sandbox or a substitute for server-side privileges.

`Execute` drains admitted DDL, CTAS, INSERT SELECT, UPDATE, DELETE, MERGE and
TRUNCATE, plus gated maintenance families. It validates but does not retain
result rows. `Insert` quotes bounded table/column identifiers and constructs one
rectangular multi-row VALUES statement. Empty/ragged/duplicate-column batches
reject; it never splits, commits or automatically retries a batch.

`Statement.Args` accepts the supported bool, signed integer, bounded unsigned
integer, string, binary, time.Time, nil and bounded non-nil slice values.
Public `Numeric` preserves exact validated decimal/exponent text without an SDK
import or floating conversion. Scalar uint8, floats, maps, arbitrary pointers,
driver.Valuer and native option/credential escapes reject. Use exact strings
with explicit SQL CAST for other decimal, temporal and nested values. Finite
inputs are borrowed until return and must not be concurrently mutated. Read
freezes admitted input storage before asynchronous use.

Every operation uses a fresh native statement/connection/HTTP transport; there is
no managed SQL session or reusable preparation. The selected SDK does not provide
the required transaction operation; transaction/session mutations reject. There
is no universal connector atomicity, per-row receipt, automatic SQL partitioning,
hidden retry or durable cursor recovery.

## Incremental Read and complete-output boundaries

`Read(ctx, cleanupCtx, Statement)` returns a Reader owning one query and its
original generation. The setup context governs admission/setup only; canceling
it after return does not revoke the accepted reader. The Client/source lifetime
and `ReadTimeout` bound retained work. Keep `cleanupCtx` valid for the separate
bounded DELETE authority. An absent consumer is still terminated by the reader
lifetime rather than kept alive indefinitely.

Call `Next(ctx)` to transfer one exact validated protocol page. Initial response
rows and empty progress pages are preserved. At most one untransferred page is
prefetched; the producer cannot accumulate an unbounded page queue. Each Next
reserves required public evidence before receiving data. A full Inbox refuses
another page, so acknowledge already handled released page records incrementally.
Next's context bounds that wait only; canceling it does not cancel the query.
Overlapping Next/Close calls are refused rather than raced.

Every page is **provisional**, including a terminal protocol page. Its
`Complete()` is false. `ReadProgress` gives a one-based page sequence and the
number of rows transferred before it; neither is a restart token. The page's
public attribution identifies its root and the originally borrowed generation.
Do not publish provisional pages as completed business output. Stage them under
application-owned bounds until successful terminal evidence; Framework owns
target/attempt attribution and completed-range publication policy. This Adapter
adds neither a durable output store nor a workflow publication engine.

`Next` returns `io.EOF` only after successful complete consumption and native
cleanup. A late error, limit, cancellation or cleanup failure returns retained
failure instead, including after the terminal root has released. `Result(ctx)`
waits for compact terminal evidence: cumulative transferred rows, protocol and
effect facts, metadata and an empty JSON row array, not another copy of all pages.
`Receipt()` observes the independently retained terminal record. Successful zero
rows has explicit complete terminal evidence; it is not absence of data.

`Close(ctx)` cancels the reader and joins actual release using existing
reservations, even when evidence is full/sealed. An early close is not successful
exhaustion. If a close wait expires, the same Reader/source still owns cleanup;
retry Close to join later. Ignoring the Reader or its return does not remove the
required terminal evidence obligation. Copies retained by callers remain their
own storage responsibility after producer release.

## Result facts, failures and custody

`DataCopy` and `ColumnsCopy` provide independent storage. Direct JSON numeric
tokens stay exact; decimal/temporal strings, binary base64 and null/nested shapes
do not pass through float64 or local-timezone conversion. Raw type text and
signature JSON are preserved. Contradictory precision, scale, zone, nested or row
metadata reject; supported canonical equivalents remain valid. Changed columns
and malformed later pages cannot certify an earlier valid prefix as complete.
Repeated signatures are validated before their rows are captured or transferred,
and integer signature parameters are compared without float64 rounding. This
applies to finite Query/Execute/Insert and provisional Reader pages alike; a
later SDK rejection is not a substitute for validating a page before handoff.

`Terminal` means an identified final protocol reply was observed. `Succeeded`
means that reply contained no server error. `Complete` additionally requires
successful draining and result validation; cleanup facts remain separately
inspectable. A finite validated prefix may be returned with failure. `HasData`
means an outcome value is present, not successful or nonempty rows.

Effect is separately NotSubmitted, Unknown, Acknowledged or ReadOnly. A mutation's
lost response remains Unknown, never permission to retry. Acknowledged is not
independent connector durability, per-item success or cross-table atomicity.
`UpdateCount` distinguishes absent from aggregate zero. Query row counts are
not affected-row counts. Query IDs are sensitive correlation only.

`Submissions` counts the sole possible statement POST entry; `Pages` counts
buffered coordinator responses, including progress/error responses.
`WireBytes` counts response-body bytes, including bounded cleanup and a possible
oversize detection byte, not HTTP/TLS headers. `Attempts` preserves an inexact
lower bound of intercepted native transport entries. Cancellation attempted and
HTTP 204 acknowledged are distinct; neither proves rollback or remote termination.

Public admission reserves work and required evidence before dispatch. Native
results, source/attempt attribution and primary/cleanup causes enter public
custody before private evidence is released. Receive via Inbox.NextReleased or
the Framework receiver; failed sink delivery requeues the same facts without
replaying SQL. Ack declares local evidence handling only after actual work is
released; it is not automatic durable storage.

Stable failures belong to Database facility `0x083`, component
`database_trino`. English/zh-CN resources and code explanations work offline.
Errors.Is/As preserve cancellation/deadline and original native causes; deliberate
native inspection can expose sensitive text and must not mutate borrowed causes.
Default result/input-handle/error presentation is restricted and runtime JSON
serialization rejects. Localization changes presentation, not effects, cause
identity or retry policy.

The shared [error boundary](../../../../../development/adapter-errors.md) preserves
already-public occurrences through neutral wrappers and keeps heterogeneous
aggregates without inventing one provider owner. Native semantic frames retain
their classification; bounded inspection does not reinterpret private causes of
an already-classified occurrence. Cleanup composition retains the primary core's
known details and preserves unknown detail schemas as opaque inspection data.

The [Adapter tree map](../../../../../../adapters/README.md) owns file roles:
configuration, policy, definitions, error translation, diagnostics and resources
remain separately reviewable without changing signatures, codes or effect facts.

## Framework composition and executable evidence

Place Settings in an application-owned typed snapshot. Bind a public Handle
through `resource.Instance`, retain every non-nil Owner on failed Build, and
delegate Release to that Owner. Construct `Using` with an explicit lifetime,
budget and the same public Dependencies. Follow redirects new root operations
only: existing readers retain their original generation until actual cleanup.
Fixed does not follow. Failed readiness retains the last good instance; an
obsolete candidate cannot supersede a newer desired configuration.

- [Independent direct consumer](../../../../../../adapters/sqlengine/trino/v1/testdata/consumer/main.go)
  and [separate-module runner](../../../../../../adapters/sqlengine/trino/v1/consumer_test.go)
  exercise every public capability and enforce import/ownership boundaries.
- [Framework consumer](../../../../../../adapters/sqlengine/trino/v1/framework_test.go)
  runs both locally and in a separate public-only Go module, including failed and
  obsolete candidates, retained readers and budget refusal.
- [Protocol tests](../../../../../../adapters/sqlengine/trino/v1/protocol_test.go)
  cover metadata consistency, limits, authority, lost mutation replies and cleanup.
- [Reader tests](../../../../../../adapters/sqlengine/trino/v1/reader_test.go)
  cover provisional pages, backpressure, input snapshots, terminal errors and
  timed-out cleanup without loss of ownership or evidence.
- [Opt-in service fixture](../../../../../../adapters/sqlengine/trino/v1/integration_service_test.go)
  qualifies the stated Trino 482 / Iceberg format-2 Parquet profile with exact
  physical batch/evolution/readback oracles and independent test-table cleanup.
- [Lost-response service control](../../../../../../adapters/sqlengine/trino/v1/integration_fault_service_test.go)
  deliberately drops one mutation acknowledgement after backend terminal success;
  public evidence stays Unknown and a direct read independently finds one effect,
  without retrying the mutation. The test-owned table is then independently removed.

These controlled peers exercise the selected real SDK and owned HTTP path, not
connector semantics. Real-service profiles and measured capacity are separate
qualification evidence; no million-target, long-pause, HA, arbitrary-connector
or production guarantee follows from these tests.
