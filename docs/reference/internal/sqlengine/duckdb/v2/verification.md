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

# DuckDB qualification and upgrade obligations

[Documentation](../../../../../README.md) /
[DuckDB interface](interface.md)

**Audience:** integration and release maintainers.
**Status:** bounded native Linux/amd64 profile with explicit qualification limits.
Full repository checks remain an integration/release gate; no unrestricted
external-service or performance certificate is supplied.

## Separate version axes

| Axis | Selected/observed boundary |
| --- | --- |
| Provider import path | `internal/sqlengine/duckdb/v2`, Go driver SDK major |
| Bootstrap | `OptionsV1`, resource configuration format 1 |
| Go driver | `github.com/duckdb/duckdb-go/v2 v2.10505.0`, commit `36040984c722e0f174b880eb7d77e238b3d8bc61` |
| Bindings | `github.com/duckdb/duckdb-go-bindings v0.10505.0` |
| Embedded native core | `v1.5.5`; `Database.Profile` observes the linked library version |
| Native library module | Platform-specific bindings library `v0.10505.0`; runtime evidence here is Linux/amd64 with CGO |
| Arrow | Existing consuming module graph selects `arrow-go/v18 v18.6.0`, not the driver's declared `v18.5.1`. No Arrow API/interop profile is supplied |
| Iceberg extension | Not selected or loaded by the product provider. Separate cached-artifact REST probes do not establish external product API support |
| Durable/Workflow format | Not introduced; replay verification is not applicable to these process-local changes |

Use `compatibility.Inspect` for actual consuming-binary module, replacement, Go
and platform facts, not the framework's dependency file alone. Supply
`Database.Profile()`, the actual `resource.Access`, and reviewed evidence to
`compatibility.Assess`. Missing records remain unknown/untested and default policy
rejects them. This package does not ship a fabricated universal certification
record or treat source review as native execution.

The [official driver release](https://github.com/duckdb/duckdb-go/releases/tag/v2.10505.0)
is a stable selection, not proof that its known defects are fixed.
The [release calendar](https://duckdb.org/release_calendar) had overlapping
maintenance signals when this profile was selected: 1.5's listed end-of-life and
a planned later 1.5 patch. Recheck current maintenance policy before release;
neither an unreleased main checkout nor newer documentation is an approved upgrade.

## Executable boundaries

Run from the dedicated product worktree with CGO and a working C/C++ toolchain:

```sh
go test -count=1 ./internal/sqlengine/duckdb/v2
go test -race -count=1 ./internal/sqlengine/duckdb/v2
go test -tags=debug_bindings -run TestNativeHandleLifecycle -count=1 -v ./internal/sqlengine/duckdb/v2
go vet ./...
go test -race -count=1 -timeout=10m ./...
go build ./...
go mod tidy -diff
```

No service environment variables, credentials or extension installation are
needed. Tests create issue-labeled in-memory tables and unique test-owned
temporary directories/native files. They do not inspect or clean another
session's storage. These are real embedded native-composition tests, **not**
external Catalog/object-store/server acceptance.

| Test area | Evidence and rejecting controls |
| --- | --- |
| Composition | Required resource/invocation evidence, 4,097 exact ordered rows across 2,048-row native chunks, null/blob values, DDL/CTAS/replacement/update/delete/MERGE |
| Preparation | Reuse across parameter batches; semicolon literals succeed; multi-statement preparation rejects without an earlier INSERT effect |
| Type boundaries | Scalar integer/decimal/time extremes, UTF-8/empty/null values, independent snapshots, narrowing/precision rejection, composite/JSON rejection before Scan |
| Effects | Appender flush/close/rollback stages, duplicate-key batches, earlier committed write preserved, failed transaction prefix preserved, DML effect despite unsupported RETURNING data |
| Query bounds | Exact-limit EOF, truncated row/byte prefixes, shared transaction envelopes, empty results, cancellation and healthy subsequent calls |
| Ownership | Persistent checkpoint/close/reopen/exact read, sealed duplicate-owner rejection, isolated memory instances, concurrent calls, borrowed-owner separation, evidence saturation/admission cancellation |
| Cleanup | Canceled work after a successful flush; separate expired cleanup context; no invented rollback acknowledgement; native connection release |
| Native resources | Native OOM rejection with spill disabled and a healthy following query; no claim that the memory setting bounds all RSS |
| Privacy/compatibility | `conformance` checks, native cause identity, safe diagnostics/runtime serialization guards, actual build/profile inspection and rejection of absent compatibility evidence |

For the tagged handle check, verify that tracking is active and the selected
transient handle counters return to zero. The ordinary build runs the lifecycle
actions but explicitly reports that allocation accounting is inactive. Neither
the binding counters nor the RSS smoke observation are a complete C++ allocator
or crash-safety proof.

Owner-requested agent review added rejecting controls for EXPLAIN ANALYZE
transaction escape, nanosecond binding overflow/infinity, TIME 24:00 folding,
mode-specific scalar parameter adaptation and nil progress-pointer logging.
Original failing controls remain in the issue reference workspace; agent review
is not independent human approval.

The selected driver still fails the original native MAP and latency safety
controls. The integration's passing rejection/cooperative-cancellation tests do
not reclassify those upstream controls as passing. In particular:

- [MAP-key fix](https://github.com/duckdb/duckdb-go/commit/c8157e8bce70563904a7cb7d817d6016c004f906)
  is not in the selected stable driver; all composite result decoding is excluded.
- [Interrupt-wait fix](https://github.com/duckdb/duckdb-go/commit/6e4a877b87e6e461d13cd2b19f61a3ced8736f6e)
  is not adopted through a source overlay or main dependency; deadlines remain
  explicitly cooperative.
- Native Appender setters narrow numeric values with Go casts. The provider
  validates target ranges/precision before buffering rather than trusting a
  nil SDK error to establish exact input preservation.
- Native `ALTER ADD COLUMN DEFAULT` expansion and initialization configuration
  ordering have explicit controls. Broader valid SQL is not silently sent through
  an unsafe fallback.

CGO is a dependency prerequisite, not a provider-level skip condition. There
are no `cgo` build tags or pure-Go stubs in this package. With CGO disabled,
building/testing this provider (including `./...`) fails in its native dependency
instead of silently excluding the package. Builds with optional Arrow tags do not qualify Arrow lifetime,
zero-copy ownership, memory or interoperability. Other operating systems,
architectures, dynamic native libraries, sanitizers, hostile inputs, multiprocess
crash/recovery, remote files and external Iceberg remain separately unqualified.

## Upgrade gate

Keep transaction/type/correctness guarantees fixed when comparing mechanisms or
versions. Re-run original native counterexamples and all integration rejection/
effect/cleanup controls before removing a restriction. Record exact driver,
bindings, native library/core, Go/toolchain, build tags and consuming Arrow
selection; an SDK-major directory does not collapse those axes.

Any future Iceberg profile needs explicit artifact/core ABI, REST Catalog,
table-format/delete/partition semantics, independent snapshot/readback evidence
and isolated cleanup authority. Native Parquet output is not catalog publication;
CHECKPOINT/VACUUM are not snapshot expiration, orphan removal or lake compaction.

Raw failed controls, exact commands, worktree manifests and any RSS measurements
belong in the owner-managed Issue #40 reference workspace. They are not copied
into public documentation or presented as benchmark/service results. No throughput,
latency-SLO or production memory claim is made by this implementation.
