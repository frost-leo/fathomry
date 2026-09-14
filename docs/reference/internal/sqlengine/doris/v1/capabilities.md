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

# Doris capability classification

[Documentation](../../../../../README.md) / [Doris interface](interface.md)

**Audience:** maintainers selecting native-table or external-catalog operations.
**Status:** implemented client classification plus an isolated Doris 4.1.4
native-table qualification. External catalogs and whole-engine support are not
certified by that result.

## How to read the matrix

**Supported locally** means implemented with bounded protocol tests.
**Restricted** means SQL can be dispatched, but authorization and exact native
semantics remain outside this client. **Unsupported here** means no corresponding
client execution/lifetime mode is supplied, not that Doris lacks the feature.
**Unverified** means no applicable service evidence. An upstream experimental
feature is never promoted to stable by generic Exec access.

The [qualified profile and executable tests](interface.md#errors-privacy-and-executable-evidence)
use only Doris endpoints. Backend tables, REST catalogs and object stores remain
Doris/deployment responsibilities; this client does not implement their SDKs or
make their independent tooling a product test dependency.

These are coverage decisions, not permission to invoke destructive SQL, grant
roles, deploy services or create catalogs. The client does not keyword-filter SQL.

## Native tables and external Iceberg are different destinations

| Operation | Client boundary | Native Doris table | External Iceberg through Doris |
| --- | --- | --- | --- |
| Batch query | Supported: bounded copied text result | Qualified 4.1.4 value/metadata and partial-result profile | Generic Doris SQL; catalog/cache/snapshot/type profile unverified |
| JSON-array Stream Load | Supported: one labeled strict physical batch | Qualified strict DUPLICATE KEY load; aggregation/upsert is still table-model dependent | Unsupported write path: it does not commit Iceberg |
| CSV/Parquet/ORC Stream Load, partial updates or merge/delete headers | Unsupported here | Additional native formats/modes need independent qualification | Not an external-table writer |
| INSERT VALUES / INSERT SELECT | Restricted single SQL command | 64-row VALUES qualified on UNIQUE KEY; SELECT unverified; Group Commit/strict settings remain server-owned | Candidate external append through Doris; backend persistence unqualified |
| INSERT OVERWRITE | Restricted, destructive SQL | Qualified small UNIQUE KEY overwrite; no automatic retry | Version/partition restrictions; not append |
| CREATE/DROP database/table; TRUNCATE | Restricted DDL | Native table CREATE/DROP and TRUNCATE qualified; database DDL and other implicit effects unverified | Backend/version-specific DDL and destructive effects unverified |
| CTAS | Restricted SQL | Create plus load, not an atomicity promise | Distinct external table creation and write; no default staging |
| ALTER schema, properties, partitions; distribution/key changes | Restricted SQL | Table-model and server limitations apply | Catalog, type, schema/partition evolution and format gates apply |
| UPDATE | Restricted SQL | Single-key UNIQUE KEY update qualified; not an InnoDB update contract | Experimental 4.1+ row-level lake DML; backend interoperability unqualified |
| DELETE | Restricted SQL | Single-key predicate on UNIQUE KEY qualified; USING/other models unverified | Experimental 4.1+ row-level lake DML; backend interoperability unqualified |
| MERGE INTO | Restricted SQL | UNIQUE KEY matched update/unmatched insert qualified; duplicate-match behavior remains restricted | Experimental 4.1+; do not reuse native-table duplicate-match semantics |
| Metadata, schema, snapshot/history/branch queries | Restricted Query | Exact native table metadata absence qualified; no automatic administration | Snapshot/cache freshness and accessible metadata vary by catalog/version |
| Native prepared statements and bound parameters | Unsupported here | Doris FE provides preparation in selected modes | No inferred catalog/type compatibility |
| Persistent session settings, BEGIN/COMMIT/ROLLBACK or savepoints | Unsupported sequencing: every call has a fresh connection | Native transaction restrictions are not MySQL/InnoDB semantics | No cross-catalog or external transaction promise |
| Broker/S3/Routine Load, EXPORT/SELECT INTO OUTFILE | Unsupported job/transfer ownership | Generic SQL may submit jobs; acknowledgement is not job/file completion | Reading lake data or exporting files is not Iceberg commit |
| Group Commit async/sync, 2PC, multi-table ingestion | Unsupported selected runtime modes | StreamLoad explicitly disables Group Commit and 2PC | Not implied by external SQL append |
| Arrow Flight SQL / ADBC / HTTP SQL query | Unsupported transports here | Separate client/protocol/result lifetime | Separate catalog/type qualification if selected |
| Native compaction/repair and lake maintenance | No automatic execution; restricted explicitly authorized SQL | Administrative permissions and server effects unverified | Destructive snapshot/file effects require separately owned evidence and cleanup |

Native UPDATE and MERGE restrictions are documented separately from lake DML:
[UPDATE](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-modification/DML/UPDATE/),
[MERGE INTO](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-modification/DML/MERGE-INTO).
The native MERGE guide permits sequence-dependent/arbitrary duplicate-match
outcomes, unlike the version-qualified Iceberg rule below.

Native SQL INSERT can acknowledge COMMITTED before VISIBLE; strict/filtering and
Group Commit settings matter. The client therefore retains an OK acknowledgement
without declaring visible/fully accepted rows.
[INSERT VALUES](https://doris.apache.org/docs/4.x/data-operate/import/import-way/insert-into-values-manual/).

Doris transaction documentation restricts the database and mixed write modes,
and imposes ordering constraints on some deletes. A series of separate Exec
calls cannot provide that retained-session contract.
[Transactions](https://doris.apache.org/docs/4.x/data-operate/transaction/).

## Catalog, version and experimental lake limits

No catalog is selected merely by enabling Query/Exec.

| Catalog route | Evidence-based classification |
| --- | --- |
| HMS / filesystem | Documented writable candidates; operations, locations, authorization and server build unverified |
| REST | Provider-specific candidate; read success does not qualify writes, credential vending or commit conflict handling |
| Native Glue | Unresolved: the pinned matrix disagrees with later text and server metadata code; neither blanket support nor blanket prohibition |
| Native DLF | Unresolved/unselected; inspected class rejects several DDL operations, not a complete DML qualification |
| JDBC | Documented experimental in 4.1+, not selected |
| S3 Tables or Glue/DLF through REST | Different routes from their native catalog types; independently unverified |

The [pinned Iceberg catalog reference](https://github.com/apache/doris-website/blob/047e5de5093e7ae8eaaa4a1247d6f0ed8acb8e11/versioned_docs/version-4.x/lakehouse/catalogs/iceberg-catalog.mdx)
describes row-level UPDATE/DELETE/MERGE as experimental since 4.1.0 and requiring
format >=2. Partition overwrite, maintenance and type mappings have additional
version gates. Its generic format/timezone statements coexist with newer V3/DV
and TIMESTAMPTZ material; that is not uniform support across 4.x.

The same document's Glue matrix conflicts with later 4.1.4 database-property
text and [metadata code](https://github.com/apache/doris/blob/ad35a140c7fd0b842f18c23300bac581f7d04326/fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergMetadataOps.java#L225-L280).
Preserve that conflict until the exact route/operation is independently tested.

The current Iceberg guide also conditions MERGE duplicate-source validation on
4.1.4 with **all BEs upgraded**. A single FE connection or version string cannot
establish that condition. Concurrent lake writes may conflict and leave files;
no automatic mutation retry or orphan deletion follows.
[Iceberg catalog](https://doris.apache.org/docs/4.x/lakehouse/catalogs/iceberg-catalog/).

Advertised release names, Git tags, binaries and deployment attestations are
different evidence. The preparation inspected the advertised-4.1.4 release's
`4.1.4-rc04` source at `ad35a140c7fd0b842f18c23300bac581f7d04326`, and
4.0.8 source at `bc8ea1bac6d62d92cdebd2e3ac33cc668961ec36`.
Neither source inspection nor moving 4.x documentation alone validates a deployed
build. The native-table tests used the independently signature/checksum-verified
4.1.4 binary; that does not attest other deployments or external catalog routes.

## Maintenance is not automatic cleanup

Explicitly classify data-file compaction/rewrite, manifest rewrite, snapshot
expiration/rollback, branch/tag management and orphan handling separately.
A successful query does not authorize any of them. Some are version-gated SQL
procedures, some need another native tool/catalog path; all are unverified here.

Catalog commit and failed-write file cleanup are separate outcomes. Rollback or
snapshot expiration can remove references used by other readers; orphan deletion
needs an independent ownership/reachability/retention proof. A prefix resembling
a test label is insufficient. This package does not list/delete storage objects,
create a staging table, expire snapshots, or clean another session's resources.

## Limits of qualification

The isolated native tests do not qualify production TLS, multi-node failure,
every native table model/type, catalog route, external commit conflicts or
downstream-reader interoperability. A caller relying on an external catalog must
verify its selected Doris operations and persistence semantics; native-table
success is not an external-write certificate. Such deployment/backend checks
do not add direct Iceberg/S3/Java paths to this package.

There is no measured equal-workload, equal-correctness service baseline, latency
tail, throughput or production stability result. Single-use SQL and HTTP setup
costs are explicit tradeoffs, not performance optimizations. Other engines, Hudi,
cross-engine transactions, infrastructure and framework orchestration remain
outside [Issue #42](https://github.com/frost-leo/fathomry/issues/42).
