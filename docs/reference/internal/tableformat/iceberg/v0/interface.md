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

# Iceberg batch table interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** framework composition and integration maintainers.
**Status:** implemented internal contract for Issue #43; not a public runtime or a release.
**Package:** `github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0`.

## Responsibilities and dependency direction

This package owns bounded single-table batch access through Apache Iceberg Go.
`v0` identifies the SDK major; admitted tables use **Iceberg format 2** and
configuration uses **OptionsV1**. These are independent version axes.

The only first-party dependencies are `resource`, `invocation`, `fault` and
`compatibility`. There is **no dependency on another internal integration**.
A provider-local implementation of Iceberg's native FileIO interfaces uses an
explicit AWS S3 client. MinIO is an independently selected S3-compatible service
in the verification fixture, not an imported implementation or an embedded assembly.

The SDK's native Go CDK FileIO was inspected, not silently adopted: its default
Create permits overwrite, its alternative existence check is not a conditional
PUT, and its construction can consult ambient settings and a global registry.
Here, configuration, path authority, conditional single PUTs, exact reads, request
bounds and transport ownership remain local to this provider. This is not a
general object-store API or a mirror of either SDK.

## Call sequence

1. Supply explicit Catalog URI/prefix/warehouse, namespace, S3 location, endpoint,
   region, static credentials and transport trust in `OptionsV1`.
2. Call `Select`, then attach `resource.WithLimits(selection, LimitsV1(options))`.
   Selection validates/freezes settings without network access. Overlaid limits
   require composition to attach the corresponding effective limits.
3. `resource.Assemble` owns construction and cleanup. Readiness sends a bucket HEAD
   and reads Catalog configuration; it creates no namespace, table or object.
4. Create a separate `invocation.Inbox[Result]` and call `Bind`.
5. Await receipts and independently drain/release inbox deliveries. A handled
   direct error does not consume the required evidence.
6. End borrowing scopes and native work before closing the owning assembly.
   Preserve incomplete cleanup responsibility rather than discarding the assembly.

Clients are concurrent and non-owning; copied or borrowed capabilities share
the authoritative resource allowance. Each call reloads its table and owns its
transaction, buffered files, Arrow references and HTTP work. No native Table,
transaction, iterator, FileIO or shutdown authority escapes to the consumer.

## Capability matrix

“Restricted” means only the stated profile is supplied, not unrestricted native
SDK support. Method presence upstream does not qualify a mode.

| Capability | Integration status and boundary |
| --- | --- |
| REST Catalog | Restricted: one explicit Catalog/prefix/warehouse; no redirects, ambient authentication, credential refresh or multi-table commit |
| SQL, Hive, Glue, Hadoop Catalogs | Unsupported here; implementations exist upstream but are not imported or qualified |
| Namespace create/drop | Restricted to the configured namespace; no implicit adoption, recursive drop or create-if-missing retry |
| Namespace metadata updates / enumerate all namespaces | Unsupported |
| Table create/load/list/drop | Restricted: explicit names, bounded metadata/listing, format 2; Drop removes the Catalog entry only |
| Rename/register/import tables or files | Unsupported; native APIs exist but arbitrary file provenance/ownership is not qualified |
| Table metadata, properties, schemas, specs, snapshots, sort orders | Bounded copied metadata inspection; not mutable SDK handles |
| Arbitrary property updates, sort-order evolution, views | Unsupported |
| Schema evolution | Restricted: optional flat-column addition, rename and removal; staged metadata must remain within the admitted profile before commit |
| Partition evolution | Restricted: add identity/bucket transforms and remove fields; at most 16 fields, bucket count 1–16; historical field identities are checked |
| Batch append | Restricted: bounded coalesced scalar batches; unpartitioned and canonical partition paths; integer bucket partitioning is tested |
| Snapshot reads, projection and predicates | Restricted: schema/names from the selected snapshot; AND of scalar comparisons or null checks; bounded copied IPC results |
| Filtered overwrite/delete | Restricted: unpartitioned, delete-file-free **full-snapshot copy-on-write**; null nonmatches are preserved; explicit all-row authorization |
| Position/equality delete files, RowDelta, MERGE/upsert | Unsupported, including scans with delete files; native capabilities are not certified here |
| Single-table staging/commit/reload | Supported within this profile; independent evidence for each phase, no application retry |
| Snapshot rollback | Restricted to an existing snapshot/main reference; not reversal of external effects |
| Branch/tag administration | Unsupported; existing metadata may be inspected |
| Compaction | Restricted: unpartitioned, delete-free, bounded full rewrite, one final commit; old files remain |
| Expiration, orphan removal, purge and retention management | Unsupported; the test's separately authorized cleanup is not a production maintenance API |
| Formats 1/3 and their advanced features | Unsupported; SDK maximum format support is not a package guarantee |
| Other reader/writer engines | Unverified; no DuckDB, Trino, Doris or Hudi implementation or interoperability qualification |

Flat scalar types are boolean, int32, int64, float32, float64, string and binary,
with explicit nullability. Nested types, dictionaries, decimals, dates/times,
UUIDs, variants and type promotions are not admitted. Arrow field-ID metadata,
when supplied, must agree with the selected table. Non-nullable columns cannot
contain nulls. Partition paths requiring noncanonical URI encodings are refused.

## Ownership, limits and input semantics

`CreateTable` borrows then copies its schema. `Write` borrows Arrow batches during
the method call only: it checks row/buffer bounds and serializes a coalesced copy
before returning. Callers retain/release their own references and must not mutate
them concurrently. Native writes receive one owned record per invocation, avoiding
the pinned SDK's unreleased queued references on writer errors.

Reads use native snapshot planning and unfiltered native record consumption,
followed by owned Arrow filtering/projection. This deliberately does not claim
native predicate pushdown: the release can skip empty filtered records without
releasing them. Overwrite/delete retain rows for which the deletion predicate is
null; they do not apply nullable SQL NOT as a keep-mask.

| Setting | Default | Meaning |
| --- | --- | --- |
| `MaxActive` | 2 | Concurrent root calls, no admission queue; permitted 1–4 |
| `MaxRows` | 65,536 | Input/result rows, per-file read admission and aggregate rewrite rows |
| `MaxBatchBytes` | 8 MiB | Input Arrow buffers / private IPC, retained rewrite buffers and output IPC |
| `MaxObjectBytes` | 16 MiB | One buffered physical file |
| `MaxFileOps` | 128 | File read/upload operations; also caps Catalog requests, planned files and snapshot count |
| `MaxIOBytes` | 128 MiB | Per-call reserved file payload; reads reserve HEAD size before conditional exact GET |
| `MaxMetadataBytes` | 1 MiB | Aggregate Catalog request/response and S3 control bodies; also caps a decoded metadata file |
| `Timeout` | 1 minute | Separate admission/work budgets; caller deadlines still apply |

File I/O is serialized within each root call. S3 requests have an additional
ceiling of `3*MaxFileOps+1`. Uploads use bounded, create-only single PUTs; no
multipart sessions or automatic compensation are created. Metadata supports
bounded plain/gzip/zstd decoding. Counted envelopes do not prove total RSS, bound
arbitrary native cause graphs, or sandbox hostile compressed Parquet/Avro input.
Catalog/S3 endpoint text is limited to 1,024 bytes, the source location to 960
bytes and an individual canonical file URI to 2,048 bytes. Malformed URI aliases
and control characters are rejected rather than normalized into another target.

SDK process globals must not be mutated. The release reads an ambient configuration
file at import time; this package does not use it to select a source or credentials.
A native worker setting other than the inspected default of five is refused.
Native producer work can still use those workers; the provider bounds actual I/O.

`BatchRead.SnapshotID=0` selects the reloaded current snapshot; a positive ID
selects historical metadata/schema. `Limit=0` selects `MaxRows`.
`Complete=false` distinguishes a truncated or failed scan from exhaustion.
A valid IPC prefix can accompany a later failure; a failed oversized record is
not advertised as delivered. Returned metadata/IPC/names/file-effect copy methods
provide independent storage. These are runtime values, not Fathomry durable DTOs.

## Effects, errors and cleanup

- File acknowledgement proves an object request was acknowledged, not a table commit.
- `Staged` records successful local staging, even if a subsequent bound rejects commit.
- `Effect` describes Catalog submission/acknowledgement: missing or inconsistent
  replies remain `Unknown`; a protocol conflict is `Rejected`.
- `CommittedSnapshotID` is validated against the staged intent.
  `SnapshotID` separately reports a reload that may observe a concurrent head.
- `Reloaded` also verifies the named metadata object through the selected S3
  endpoint. Unused server FileIO/credential hints cannot substitute another client.
  ID-addressed collections compare by ID; chronological logs remain ordered.
- Cancellation ends neither effect uncertainty nor resource responsibility by itself.
  Owned dial/TLS work and native iterators are joined before local release.
- Cleanup errors remain separate from primary errors. Unknown commits do not
  authorize deleting their files. Old snapshots/files survive rewrite and rollback.

Errors use provider-local `fault.Kind` values and preserve deliberate
`errors.Is/As` inspection of native causes. Ordinary formatting and logging redact
runtime values; explicit data/cause inspection is private and is not a safe log
projection. SDK attempt counts are not inferred: receipts currently report
inexact/unobserved attempts.

## Evidence and boundaries

See [verification](verification.md), the
[SDK integration architecture](../../../../../architecture/sdk-integration.md),
[source](../../../../../../internal/tableformat/iceberg/v0/),
[composition tests](../../../../../../internal/tableformat/iceberg/v0/integration_test.go)
and [Issue #43](https://github.com/frost-leo/fathomry/issues/43).

No business schema, Item/Run terminal policy, automatic business retry, durable
publication protocol, cross-service transaction, public runtime or Temporal
Workflow I/O is implemented.
