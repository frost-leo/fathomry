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

# Doris capability coverage

[Documentation](../../../../../README.md) / [Doris Adapter](interface.md)

**Audience:** callers choosing the supported client profile and reviewers checking
SDK-to-Internal-to-public coverage.
**Status:** implemented client coverage with actual-driver local tests and the
limited [on-demand native-table qualification](interface.md#capability-coverage-and-qualification).
Local peers and historical results do not extend that service profile.

The selected implementation is go-sql-driver/mysql v1.10.1 plus standard Go HTTP.
Doris uses that wire protocol but is not the MySQL/InnoDB Adapter. Generic
authorized SQL is not certification of every Doris/table/catalog operation.
See the [native classification](../../../../internal/sqlengine/doris/v1/capabilities.md)
for historical service-specific evidence and explicit restrictions.

| Selected native capability | Internal owner | Public contract | Evidence |
| --- | --- | --- | --- |
| Resolved typed options and strict layers | PrepareV1; one Selection/Reservation; OptionsCopy | Loadable Settings, Validate, Recommend; public configuration resolves layers before Open | Option field/default parity and native strict-overlay budget tests |
| Local source construction/release | Select/Bind/resource assembly | Open, Owner.Close/Release/ShutdownComplete, Client/Handle, Using | Direct consumer, partial Framework candidate and saturated shutdown |
| Profile and preparation metadata | Profile, preparation Description | Client.Profile, Owner/Handle.Info, Result.Source | Detached profile and no-I/O construction tests |
| Native text finite SQL | Query; fresh framed connection | Query | NULL/empty/order, signed/unsigned/decimal/time/binary values, native errors |
| Authorized native command | Exec | Exec | Aggregate acknowledgement and lost-response uncertainty; no SQL filtering/replay |
| Owned incremental result | QueryCursor, Next, Close | QueryCursor, Cursor.Next/Receipt/Close | Real-driver multi-page/EOF, byte pages, aggregate guards, cancellation, old-generation retention |
| Strict JSON-array native-table load | StreamLoad | StreamLoad, Batch, Result.Load | Visible/committed/filtered/missing/duplicate/lost-response controls |
| Retained-label observation | InspectLabel | InspectLabel | UNKNOWN/pending/committed/visible/aborted; no fabricated payload match |
| Copied positional metadata/results | Column, Row, Result | Independent Column/Row/Result and explicit copy getters | Aliasing, exact encodings, attribution and partial evidence tests |
| Independent receipt/inbox custody | invocation root/page records | adapters receipts/inbox; captured SQL-engine Attribution/Attempts | Handled error, failed sink/redelivery, cleanup at saturated capacity |
| Original multi-cause errors | fault and native MySQLError | Stable database_doris codes, InspectError; shared owner preservation | Classification, errors.Is/As, redaction, i18n and combined offline CLI atlas |

## Every supported option family

| Fields | Meaning retained |
| --- | --- |
| Name | Explicit source identity; native preparation revision is not a public generation |
| SQLAddress, SQLServerName | Literal SQL IP:port and explicit TLS certificate identity |
| HTTPOrigins | Ordered initial origin and authorized redirect destinations; copied; strict scheme/authority/path rules |
| Database, User, Password | Explicit endpoint credentials and native database scope; not ambient discovery |
| RootCAPEM, Plaintext | Explicit verified trust or separate plaintext test mode; no TLS fallback |
| Active, Queued | Process-local root capacity, with shared native admission across facades |
| Timeout, CursorTimeout | Separate finite/setup/page/cleanup budgets and accepted cursor lifetime |
| MaxBatchBytes, MaxRows | One strict load and finite Query rows; no per-item success inference |
| MaxResultBytes | Retained finite SQL result, separate from page and wire budgets |
| MaxPageRows, MaxPageBytes, MaxCursorRows | Per-page retention and cursor-wide work; bounded lookahead |
| MaxPacketBytes, MaxResponseBytes | Pre-parser framing; complete-response totals never restart per page |
| MaxHTTPResponseBytes | Bounded HTTP replies; missing/truncated counters never imply success |

Zero/absence, units and limits are defined by the
[interface](interface.md#settings-bytes-and-phases), not guessed from recommendations.
Tests enumerate every exported native option and public mapping. The optional
public observer remains lossy diagnostics; it cannot replace required evidence.

## Explicit omissions and semantic restrictions

| Capability | Classification and reason |
| --- | --- |
| Parameters/prepared statements, retained arbitrary sessions or transactions/savepoints | Not supplied by this selected text-single-use profile; never fabricated from separate Exec calls |
| Additional Stream Load formats/headers, partial updates, merge/delete, 2PC, Group Commit, multi-table ingestion | Not supplied; the strict labeled JSON route is retained rather than silently changing row/effect guarantees |
| Driver Conn/Rows, arbitrary SDK callbacks, global driver registry or DSN callbacks | Private ownership boundary; no unbounded draining/handle escape |
| SQL DDL/DML/CTAS/metadata/external-catalog reads or writes | Explicitly authorized text may be dispatched; acknowledgement is not backend/visibility/atomicity qualification |
| Job submission/export/routine/broker ingestion and administration | No owned job/transfer completion API or automatic execution |
| Arrow/Flight/ADBC/HTTP SQL, other storage/table-format clients | Not selected; no extra SDK or generic execution engine |
| Mutation retries, automatic label replacement/reconciliation, polling/staging/cleanup of remote orphans | Not supplied; unknown effects and finite label retention prohibit inference of safe replay |
| Durable resume, cross-service commit, Item/Run disposition or downstream publication | Higher-level business contracts, not technical pages or aggregate counters |
| Actual FE/BE/table profile | Qualified only for the small single-FE/single-BE DUPLICATE KEY profile linked above; other table models and whole-engine support remain unqualified |
| External catalogs, deployment TLS, failover and service performance | Separate unqualified deployment profiles; generic SQL access is not certification |

These omissions describe this client, not absence of server features. Any new
required capability needs an explicit scope decision and its own evidence.
