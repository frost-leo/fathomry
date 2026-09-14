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

# Iceberg verification profile

[Documentation](../../../../../README.md) / [Iceberg interface](interface.md)

**Audience:** integration maintainers evaluating this exact profile.
**Status:** bounded local and isolated-service evidence; not a general SDK,
production deployment, retention or cross-engine certificate.

## Version axes

| Axis | Selected or observed evidence |
| --- | --- |
| Implementation evidence baseline | Issue #43 topic based on `977b692`; publication/merge state is tracked in the linked issue |
| Framework Go requirement | 1.26.0; full vet/race/build passed on 1.26.0, 1.26.4 and 1.26.6 |
| Iceberg Go | `v0.6.0`, upstream commit `350ae7270d7c44578638ef55e7e0392227745e8a` |
| Arrow / Parquet implementation | `github.com/apache/arrow-go/v18 v18.6.0` |
| Avro | `github.com/twmb/avro v1.7.2` |
| FileIO | Provider-owned buffered S3 adapter; no internal MinIO or native Go CDK dependency |
| AWS Go SDK core / S3 | `v1.41.7` / `service/s3 v1.101.0`; static credentials, no SDK retry |
| Metadata compression | Bounded none/gzip/zstd decoding; independent of Parquet compression |
| Catalog | Lakekeeper, declared `0.13.1` in owner-supplied configuration; no binary attestation |
| Object service | Owner-authorized MinIO-compatible S3 service; exact server build not attested |
| Table format/features | Format 2, flat scalar schema, no delete files; see the operation matrix |
| Service transport | Explicit trusted-network HTTP; TLS is a separate local fixture |
| Other engines | Not tested or imported; no DuckDB/Trino/Doris/Hudi compatibility conclusion |

An inspected main revision is not the dependency selection. A previous attempted
release/main dependency mixture failed compilation against Avro 1.8.0 and remains
historical evidence, not a result for this graph.

## What the tests establish

- Explicit/frozen source configuration, admission, borrowed-source lifetime,
  independent evidence, malformed metadata/replies, redaction and no dependency
  on another internal integration.
- Exact scalar/null row values after append, filtered reads, overwrite/delete,
  historical reads, evolution, rollback and restricted compaction.
- Concurrent stale-base operations compare complete accepted row sets and retain
  conflict/unknown outcomes; they do not use total row count as the only oracle.
- Empty/inconsistent commit replies, malformed status bodies and modeled lost
  replies cannot certify absent or unobserved mutations. Staged metadata and
  expected commit intent are checked before/after submission respectively.
- Arrow checked-allocator controls cover sparse filtering, retained-prefix reads,
  rewrites, coalesced input and cancellation. The release's original failing
  controls are retained separately.
- File budgets reserve object size before conditional ranged reads. Buffer/close
  failures remain independent; incomplete Parquet buffers are not uploaded.
- Local TLS, exact endpoint settings, no automatic S3 retries, create-only PUTs,
  caller cancellation causes, and native-cause inspection have focused controls.

The synthetic HTTP peer stores real native metadata/Parquet/manifest bytes, but
it is still a **test double**. Native probes and those tests are not substitutes
for an authoritative Catalog and object store.

## Real-service fixture

`TestIsolatedLakekeeperMinIOService` requires all three explicit controls:

```sh
FATHOMRY_ICEBERG_SERVICE_CONFIG=/path/to/private/connection-info.local.yaml \
FATHOMRY_ICEBERG_SERVICE_WRITES=1 \
FATHOMRY_ICEBERG_SERVICE_MANIFEST=/path/to/new-private-fixture-record.json \
go test -count=1 -run '^TestIsolatedLakekeeperMinIOService$' \
  ./internal/tableformat/iceberg/v0
```

The configuration decoder selects the existing Fathomry Catalog/object-store
fields and dedicated credentials. It does not deploy a Catalog, create a bucket,
change policies or enumerate other deployment components. The fixture manifest
is created exclusively with mode 0600 **before mutation** and retains the exact
test-owned namespace/location for failure recovery.

Executed independent-FileIO acceptance covers a 4,097-row write → table commit →
reload → exact read, filtered overwrite/delete, compaction, historical reads,
concurrent append outcomes, schema/partition evolution, partitioned append and
rollback. Cleanup drops the exact table, observes its absence, enumerates and
removes only test-owned keys, observes current-prefix absence, and verifies
namespace absence. This does not establish deletion of historical object versions.

Earlier trials exposed real profile mismatches: unsolicited credentials,
different client/Catalog S3 endpoint names, compressed metadata and reordered
ID-addressed metadata collections. Repeated failures and successful recovery are
retained, not rewritten as passes. One earlier fixture cleanup exceeded its own
request allowance; the three remaining exact objects were independently identified
and removed. Later manifests and cleanup use bounded explicit ownership.

## Upstream defects and local disposition

The original v0.6.0 probes still reproduce empty success acceptance, malformed
error classification loss, invalid format fallback and historical partition-ID
reuse. The provider rejects/guards these cases instead of changing the release.

Independent review additionally reproduced null-to-zero input corruption,
nullable COW data loss, empty-record reference loss, queued writer references,
late byte-budget checks, historical-ID filtering errors and bound-crossing
metadata commits. Correcting these required explicit input checks, staged
validation, owned Arrow filtering, single-record writes and pre-transfer admission,
not suppressing the failing tests.

ID-addressed metadata collections are compared by ID, while snapshot/metadata
logs and schema field order remain ordered. This follows the
[table metadata and history model](https://iceberg.apache.org/spec/#table-metadata);
it is not permission to ignore different snapshot contents.

## Reproduction and remaining limits

```sh
go mod tidy -diff
go vet ./...
go test -race ./...
go build ./...
go test -run '^$' -fuzz '^FuzzMetadataBoundary$' -fuzztime=10s \
  ./internal/tableformat/iceberg/v0
go test -run '^$' -bench '^BenchmarkFreezeBatch$' -benchmem \
  ./internal/tableformat/iceberg/v0
```

The benchmark measures bounded batch preparation/coalescing only. Service timings
are single-run observations, not sustained throughput, latency tails or an RSS
capacity study. No fixed-mechanism performance comparison or universal efficiency
claim is made.

Final local gates also passed CGO-disabled package tests and a ten-second metadata
fuzz run. The final independent-FileIO service run passed on Go 1.26.6 after the
source-bound audit; additional service/cleanup runs passed on 1.26.0 and 1.26.4.

Unverified or unsupported: production TLS/authentication, STS refresh, other
Catalogs/FileIOs/engines, delete-file modes, arbitrary foreign metadata/data
corruption, crash recovery, large sustained workloads, retention/orphan races,
full physical versioned cleanup, multi-table transactions and durable business
publication. No Workflow commands or serialized Workflow contracts change;
Temporal replay is not an acceptance substitute here.

Detailed native failures, review overlays, exact commands/source hashes,
service manifests and current gate outputs stay with
[Issue #43](https://github.com/frost-leo/fathomry/issues/43)'s local handoff.
Private fixture paths and deployment material are not publication artifacts.
