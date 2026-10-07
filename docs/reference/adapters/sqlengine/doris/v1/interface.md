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

# Doris SQL-engine Adapter

[Documentation](../../../../../README.md) / Public package reference

**Audience:** independent Go applications and explicit Framework composition.
**Status:** implemented with selected-driver SQL/HTTP protocol checks and a
limited, owner-authorized on-demand service run. Qualification is restricted
to the small native-table profile below, not whole-engine or deployment support.
**Package:** `github.com/frost-leo/fathomry/adapters/sqlengine/doris/v1`.

## Responsibilities and calling sequence

This Adapter exposes the complete supported
[Internal Doris profile](../../../../internal/sqlengine/doris/v1/interface.md).
It consumes independently owned [SQL-engine metadata/budgets](../../v1/interface.md),
[public operations](../../../v1/interface.md) and
[public resources](../../../../resource/v1/interface.md), not a universal SQL
client or a second ownership engine. Public major 1, configuration format 1,
go-sql-driver/mysql v1.10.1, the module build and Doris FE/BE versions are separate.
The SQL-engine package location does not change the Database error domain,
`database_doris` component or `database.doris.*` operation identities.

1. Load typed `Settings` using ordinary configuration or the strict public
   configuration-source contract. `Validate` is pure; `Recommend` derives
   resource and evidence budgets from the same validated native preparation.
2. Create caller-owned `adapters.Runtime` and `adapters.Inbox[Result]` from
   the recommendation; supply `Dependencies` explicitly. Compose capacities for
   every simultaneously retained source/generation. Source contributions are
   already included in recommendation totals; if a facade selects a larger family
   budget, reserve its operations at that budget. Follow the
   [common composition contract](../../v1/interface.md#budgets-and-source-generations).
   No global registrations or unused transport clients are installed.
3. `Open(lifetime, settings, dependencies)` constructs local ownership, not
   readiness or SQL authorization. Close every non-nil Owner, even on error.
4. Use `owner.Client()` directly, or bind `owner.Handle()` with
   `owner.Release` in a `resource.Instance`; `Using` supports Fixed/Follow
   references. A larger replacement is refused before dispatch when its
   recommendation exceeds the supplied family budget.
5. Ordinary operations return a receipt and admission error. After acceptance,
   inspect `receipt.WaitReleased(ctx)`: its waiting error, `Snapshot.Primary`,
   `Cleanup` and `ValueCopy` are distinct. A nil admission error is not success.
6. Independently receive evidence with `Inbox.NextReleased` and explicitly Ack,
   or use `DeliverOne`. Sink failures retain the same facts, not another SQL/load
   dispatch. Long sessions must receive pages incrementally.
7. Close the Owner with a cleanup context, then the caller-owned runtime.
   Close cancels and joins operations using existing reservations. A waiting
   timeout retains responsibility through the same Owner/Receipt; it does not
   authorize reconstructing or replaying the operation.

Client, Handle and Using never acquire shutdown authority. `Profile(ctx)`
borrows one current generation for copied effective declarations, without SQL
admission or service I/O. `Info` identifies native preparation; public generation
identity belongs to each captured Result.Attribution, not to a name or revision.
`WithID` freezes optional opaque correlation for future roots/descendants.

## Incremental result ownership

`QueryCursor(setup, lifetime, sql)` returns an owned Cursor and terminal receipt.
End setup only after successful return; its cancellation does not invalidate the
accepted native Rows. The cursor keeps its original connection, source generation
and full work envelope until actual local release.

Call serial `Next(pageContext)`, wait for that page's public receipt release,
consume copies and receive independent evidence. Overlapping use is refused.
One bounded lookahead row recognizes a full final page's EOF. The root does not
accumulate pages. Its terminal summary retains cumulative RowsRead, BytesRead and
ResponseBytes; counters include lookahead. Empty, partial, truncated, canceled
and abandoned results are not equivalent.

An intermediate page has Complete=false. The final page and terminal root have
Complete=true only after framed EOF. Even completed technical pages are not
automatically valid downstream business batches: the caller's declared
publication/completeness gate remains separate. This package supplies neither
durable staging nor cross-service commit.

Close abandoned cursors, including after a page failure. Close may cancel an
active Next and never acquires a new result slot. Its caller context and native
Timeout bound waiting for native and public release. Cancellation does not prove
remote SQL termination, rollback, label absence or no mutation.

## Settings, bytes and phases

All supported fields are exposed, including explicit source name, SQL IP:port and
certificate name, ordered authorized HTTP origins, database, user, password, PEM
roots and plaintext test mode. No DSN/environment/global trust is discovered.
HTTP redirects retain the strict native origin/path/credential/hop restrictions.
Settings is ordinary sensitive configuration JSON; direct formatting/logging is
redacted. Its origin slice is frozen during preparation. Do not mutate borrowed
inputs concurrently. Normalize nil optional Settings pointers before logging.

Durations are integer nanoseconds. Typed zero bounds select native defaults;
Queued=0 disables queueing. The
[native bounds table](../../../../internal/sqlengine/doris/v1/interface.md#explicit-transport-input-and-memory-limits)
is authoritative. Relevant separation:

| Field | Applies to |
| --- | --- |
| Active / Queued | Native root admission shared by all facades for that source |
| Timeout | Native admission and finite execution separately; cursor setup, each admitted page, and cursor cleanup waiting |
| CursorTimeout | Entire accepted cursor lifetime, in addition to caller lifetime |
| MaxBatchBytes / MaxRows | One strict JSON batch; MaxRows also limits finite Query |
| MaxResultBytes | Finite Query retained cell/name/type bytes |
| MaxPageRows / MaxPageBytes | One incremental page; metadata is charged on every page |
| MaxCursorRows | Whole cursor response row ceiling, never restarted per page |
| MaxPacketBytes | Every native packet payload |
| MaxResponseBytes | Entire SQL wire response including authentication/framing; never restarted per page |
| MaxHTTPResponseBytes | Each bounded HTTP response body |

The earlier public Runtime queue is owned by the caller's admission context,
not the native Timeout. Supply an admission/setup deadline to bound that wait
(including when using QueryCursor); a native timeout is not an end-to-end public
queue/setup deadline. Owner.Close likewise uses its explicit caller cleanup context.

Recommend consumes the native resolved reservation, including source/work/result
budgets, then adds bounded public transfer/attribution custody. It does not
duplicate native default or reservation formulas. Native strict overlays must
use `PrepareV1.Selection/Reservation` together rather than default-only LimitsV1.
Public strict configuration overlays resolve into Settings before validation.
Bytes are declared accounting envelopes, not process RSS or distributed quota.
Caller-retained copies, Go/TLS/OS overhead and application payloads remain separate.
`Policy.SourceWorkBytes` is the exact source-lifetime charge consumed by Open;
`SourceEvidenceBytes` remains reserved until the source terminal record is
acknowledged, including after work release. Both are already included in the
runtime/evidence totals and come from the same native preparation.

## Effect, value and error contracts

- Query is one authorized text command, **not** a read-only sandbox.
  Exec acknowledgement/aggregate counts do not prove commit, durability,
  visibility or individual business-item effects.
- StreamLoad sends one caller-labeled strict JSON array. It fixes strict mode,
  zero filter ratio, single phase and Group Commit off. State, transaction
  presence, row counters, filtered/unselected rows and completion remain separate.
- Publish Timeout preserves Committed plus uncertainty. Filtered Success preserves
  Visible plus ErrRowQuality. Missing counters are unknown, not zero.
- A duplicate label, even FINISHED, cannot verify this payload. InspectLabel's
  visible observation has no table, payload hash, transaction or row-quality
  witness. UNKNOWN/expired labels or a lost reply never trigger fresh-label replay.
- NULL is nil, empty bytes are non-nil, column order/names and metadata presence
  are preserved. Values are native text/bytes; FLOAT/DOUBLE follow the driver's
  respective precision. Getter slices/bytes are detached and caller-owned.
- Results, Rows, Columns, Batch, load evidence and runtime handles refuse JSON.
  Deliberate copy/native-error inspection is sensitive: do not log SQL, data,
  labels, payload hashes, endpoints, credentials or native server messages.
- FacilityDoris `0x084`, component `database_doris`, belongs to DomainDatabase.
  Definitions/Resources support offline English/zh-CN explanation. Translation
  retains the native cause graph and errors.Is/As identities; shared resource and
  operation failures retain their owners. Error identity is not retry policy.
  Public-only wrapper graphs retain their original cores and typed details via
  the shared private error bridge; heterogeneous aggregates do not acquire an
  invented Doris owner. Native semantic frames remain explicit.

## Capability coverage and qualification

The [SDK/Internal/public matrix](capabilities.md) accounts for every supported
operation and option family and explicitly classifies omissions.

Executable evidence: [adjacent tests](../../../../../../adapters/sqlengine/doris/v1),
[public-only independent consumer](../../../../../../adapters/sqlengine/doris/v1/testdata/consumer/main.go),
and [native cursor protocol tests](../../../../../../internal/sqlengine/doris/v1/cursor_test.go).
The consumer exercises direct and Framework Fixed/Follow paths, replacement
while a cursor retains the old generation, partial candidate cleanup and budget
refusal without dispatch. Existing local TLS/framing/redirect/load controls remain.

The owner subsequently authorized on-demand real-service verification on
2026-10-07. An external public-only consumer passed race-enabled tests against
the existing isolated single-FE/single-BE fixture, with explicit plaintext
endpoints over SSH and small DUPLICATE KEY tables. This verifies:

- Strict 129-row load, exact DECIMAL(38,5)/Unicode/NULL/empty readback, finite
  byte-limit refusal and 16-row pages with 128/129-row positive EOF.
- Visible-label observation without payload/row invention, duplicate-label and
  invalid-row rejection, UNKNOWN labels, local cursor abandonment with incomplete
  evidence, SQL acknowledgement followed by visibility readback and native errors.
- Framework Fixed/Follow replacement while an existing 65-row cursor retains
  its original generation, followed by exact owned-table DROP and independently
  observed metadata absence.

Service readiness required a successful read-only metadata query after backend
heartbeats; open ports alone were insufficient. The test tables were removed
and the fixture returned to its stopped state. The external test and lifecycle
records remain issue evidence, not an automatic deployment or cleanup facility.
Historical #42 results remain separate. Other table models, external catalogs,
deployment TLS, failover, service performance and production remain unqualified
by #111. There is no throughput, latency-tail, RSS or platform-superiority claim.

[Issue #111](https://github.com/frost-leo/fathomry/issues/111) defines this scope.
