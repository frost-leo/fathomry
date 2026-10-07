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

# DuckDB Adapter interface

[Documentation](../../../../../README.md) / Public package reference

**Audience:** independent Go applications and Framework resource composition.
**Status:** implemented local embedded-engine capability with real Linux/amd64
tests and separate-module consumers. This is not production, all-platform,
crash-recovery, external-catalog, native-streaming or hard-RSS qualification.
**Package:** `github.com/frost-leo/fathomry/adapters/sqlengine/duckdb/v1`.

## Responsibilities and construction

Use the concrete DuckDB capability without importing Internal, the driver or its
bindings. Its own [SQL-engine contracts](../../v1/interface.md) hold budgets and
attribution, not a universal SQL client. Admission, actual-work ownership and
required evidence use [adapters/v1](../../../v1/interface.md). No global client,
hidden pool, extension installation, service registry or automatic retry is added.

1. Use `Configuration(defaults)` with existing strict `configsource.Prepare` to
   load application-selected `Settings`. `Validate`, `Configuration`, `Recommend`
   and `Build` do not open an engine, create directories or probe a service.
2. Use `Recommend` to construct a caller-owned operation Runtime and
   `Inbox[Result]`. `Dependencies` borrows them and an optional lossy Observer.
3. `Open` initializes a real embedded engine. Empty `Path` creates an isolated
   in-memory database. A nonempty path authorizes opening/creating that database
   file and its WAL; the caller owns permissions, exclusivity and file removal.
4. Retain every non-nil `Owner`, including one returned with an Open error.
   `Owner.Client` and `Owner.Handle` are non-owning access. Only the Owner grants
   source Close/Release authority.
5. Receive required evidence with `NextReleased`, inspect it, then `Ack` only after
   the application's sink accepts it. Live source/reader records must not block
   finite child evidence. Ignoring a convenience result or dropping optional
   observation never discards required facts.
6. Finish readers, Close the Owner, join the Runtime, and acknowledge remaining
   released evidence. Repeat incomplete Close/Release on the same Owner with a
   fresh wait context. ShutdownComplete means actual local cleanup completion,
   independently of cleanup errors.

The [direct external consumer](../../../../../../adapters/sqlengine/duckdb/v1/testdata/consumer/main.go)
is an executable public-only construction, exact-value, finite-operation, reader
and evidence recipe. It uses the real engine, not a database protocol mock.

## Settings, defaults and reservations

The [Settings declaration](../../../../../../adapters/sqlengine/duckdb/v1/options.go)
defines matching JSON/mapstructure names. Durations are integer **nanoseconds**.
Direct code literals passed to Open/Validate/Recommend use zero for documented
bootstrap defaults. `Configuration` resolves those defaults once: absent fields
inherit; explicitly configured zero bounds are rejected rather than re-defaulted.
`QueuedCalls: 0` and `Path: ""` remain meaningful valid values. Strict preparation
rejects unknown, duplicate, null, coerced and overflowing fields.

All preparation routes reject bootstrap paths already larger than the 1 MiB
configuration-document ceiling before copying them into canonical configuration.
The resolved Path limit remains 4096 bytes; this does not change accepted
settings, native reservations or the documented absence of a hard RSS limit.

| Setting | Default and supported range |
| --- | --- |
| Name | Required 1–64-byte lowercase source identity: letters, digits, dot, underscore or hyphen |
| Path | Empty for isolated memory; otherwise clean absolute path, at most 4096 bytes, without NUL, query/fragment/percent markers or line breaks |
| Connections / QueuedCalls | 1 / 0; ranges 1–8 / 0–64 |
| Threads | 1; range 1–32 |
| MemoryBytes | 256 MiB; range 16 MiB–64 GiB; native engine setting, **not** process memory cap |
| Timeout / CleanupTimeout | 30 s / 5 s; each 1 ms–5 min, cooperative |
| MaxRows / MaxBatchRows | 8192 / 65536; each 1–65536 across the entire finite call |
| InputBytes / ResultBytes | 8 MiB / 4 MiB; each 1 KiB–64 MiB, including scalar/container and relevant SQL/metadata accounting |
| ReaderChunkRows / ReaderChunkBytes | 256 / 1 MiB; ranges 1–8192 / 1 KiB–64 MiB |
| ReaderTotalRows / ReaderTotalBytes | 1,000,000 / 1 GiB; ranges 1–1,000,000,000 / 1 KiB–1 TiB |
| ReaderLifetime | 5 min; range 1 ms–1 h, independent of the setup wait |

`Recommend` returns `sqlengine.Policy`; `Using` accepts `sqlengine.Budget`, and
metadata getters return SQL-engine-owned observations, not database aliases.
SourceWorkBytes and SourceEvidenceBytes expose the exact source reservations
already included in policy totals, with independent work and evidence lifetimes.

`Recommend` and construction consume the same authoritative validated Internal
preparation. It covers the live source, native memory allowance, active root work,
one terminal plus one current chunk evidence record per reader, and queued roots.
Children share root work but own separate evidence. Ack chunks incrementally;
retaining application copies is caller memory, not free Adapter storage.

Combine policies deliberately for multiple sources or overlapping Follow
generations. Every still-live source and reader remains charged through actual
cleanup, even after a wait times out. `Using` rejects an adopted generation whose
root Budget exceeds its declared Budget before native dispatch. It does not
silently resize caller-owned limits. Timeout governs native phases, not earlier
public queueing: provide an explicit caller admission deadline.

Declared budgets are not upfront allocation or measured RSS. The pinned driver
materializes native query results. The Adapter bounds retained Go chunks and
evidence, not native materialization, native allocator retention or every transient
decoder allocation. An oversized string/blob can be allocated by the decoder
before its size becomes inspectable; it is rejected without successful delivery.
Native connect/prepare/destruction are not preemptible Go operations. Cancellation
must still join actual native release and cannot prove absent effects.

## Finite SQL and exact values

`Run` selects one provider-specific `Request`; Execute, Query, ExecuteMany and
Append are its typed convenience routes. `Transaction` accepts an ordered mixed
list, executes it on one connection/generation and commits only after every step
and required cleanup succeeds. Each finite route explicitly accepts operation and
cleanup contexts; cleanup authority is not silently detached from the caller.

| Route | Scope and evidence |
| --- | --- |
| Execute | One permitted DDL, DML or maintenance statement; submitted/execution/changed-row facts |
| Query | One bounded snapshot, including supported DML RETURNING; observed EOF or a retained incomplete prefix |
| ExecuteMany | One preparation, sequential positional parameter rows, one local transaction; partial acknowledged execution count survives failure |
| Append | One native Appender, optional schema/column subset and finite rows, one transaction; accepted, flush and close stages remain distinct from commit |
| Transaction | 1–32 ordered requests, one aggregate input/output envelope and native transaction; partial earlier rows are not committed business output |
| Read | One SELECT-shaped retained result with incremental consumption; not an unbounded transaction or streaming Appender |

SQL is at most 64 KiB, results/arguments at most 64 columns, and transactions at
most 32 steps. Oversized atomic requests fail; they are never implicitly chunked,
reordered or replayed. Empty parameter and Appender batches are supported and
retain native transaction/flush acknowledgement. Standalone CHECKPOINT is
supported, but not inside the owned transaction envelope.

Request containers and values are borrowed until the synchronous call returns;
do not mutate them concurrently. The closed scalar whitelist is SQL NULL (`nil`),
bool, Go signed/unsigned integer widths, float32/float64, string, bytes, bounded
`*big.Int`, constrained `time.Time`, and public-owned `Decimal`, `UUID`, `Interval`.
Arbitrary structs/pointers/callbacks, driver.Valuer and SDK handles are refused.
Decimal preserves width/scale and a copied unscaled integer: magnitude must fit
its declared width. UUID preserves 16 bytes. Interval keeps calendar months,
days and microseconds separate. Exact integers never pass through JSON float64.
UUID/Decimal parameter binding uses canonical text with an explicit SQL target
type; Appender additionally validates exact target type/range/precision.

Temporal values retain qualified UTC, year and native precision boundaries.
Nanosecond overflow/infinity and unsafe narrowing remain refused. Raw JSON,
list/map/struct/composite and TIME result decoding is not supported. Callers can
author explicit casts to VARCHAR; the actual changed schema remains visible.
For example, casting TIME `24:00:00` preserves its exact textual representation.
No hidden cast, flattening or generic nested-value codec is supplied.
Native metadata passes the Internal closed type whitelist before decoding; the
public conversion then enforces its scalar whitelist and result shape. There is
no external native-result loader. Future representation changes require explicit
contract versioning and requalification, not an inferred decoder upgrade.

Multiple parsed statements, EXPLAIN (including ANALYZE), user transaction-control
and unqualified statement categories remain refused. External access, automatic
extension loading and spill are disabled and configuration is locked. Arrow,
UDFs, external files/catalogs, recovery/migration and arbitrary native APIs are not
qualified by this local profile. These limits do not turn arbitrary SELECT into
a read-only security guarantee: sequence advancement can survive rollback.

## Incremental result ownership and EOF

`Read` setup is governed by its caller context. Once accepted, the Reader belongs
to the Client/Using lifetime and ReaderLifetime; canceling a finished setup wait
does not revoke it. One native query is retained, never repeated with LIMIT/OFFSET
or keyset rewrites. A reader holds its result, statement, connection and public
generation through actual cleanup. Client roots may run concurrently; overlapping
Next/Close on one reader is refused, not raced or silently serialized.

Each Next reserves independent public and native evidence **before** advancing.
Full or claimed inbox custody rejects work without consuming hidden rows.
`Progress.Reader` reports chunk number, offset, returned rows/bytes, accumulated
rows/bytes and Closed. Each chunk repeats stable positional schema. Returned
containers/scalars are detached; no history of prior chunks is held by the reader.

`Steps[0].Complete` is true only after observed native EOF, including empty
success. A full chunk is not EOF. At a total bound, bounded one-row lookahead
distinguishes exact EOF from an incomplete limited prefix. Total byte accounting
includes metadata once; per-chunk byte accounting includes repeated metadata.
One byte-bound lookahead row may be held until the next delivery.

Next can return partial rows with an error. Early Close, lifetime cancellation,
oversized values and total limits preserve incomplete/limited progress, never
manufacture EOF. The terminal Receipt is reserved before setup; Close and source
shutdown need no fresh admission/evidence slot, including under a sealed inbox.
`Result(ctx)` waits for terminal public projection and actual release without
draining rows or canceling work merely because that wait expires. The terminal
snapshot contains schema/counters, not previously delivered rows.

Chunks are process-local capability output, not durable range manifests or
framework business-stage commitments. Applications must not publish ordinary
business output as complete until their declared range has the required completion
evidence. Readers cannot be serialized, reconstructed or resumed after a crash.

## Results, effects, errors and versions

`Result.HasData` distinguishes absent progress from an accepted result.
`Snapshot` deeply copies steps, positional columns, row containers, byte slices,
big integers, decimals and reader metadata. Concurrent snapshots are safe to
mutate independently. Native column names/types and duplicate names remain exact;
NULL differs from empty string/blob. Complete empty Query has non-nil empty rows;
non-query/absent rows are not silently represented as successful empty output.

Every native finite fact is retained: Prepared, Submitted, Executions,
RowsChanged/knownness, accepted/flushed counts, flush attempt/result, Appender
close attempt/result, schema/rows, Complete/Limited, transaction Begin,
commit/rollback attempt/acknowledgement and local ConnectionClosed. A committed
mutation can precede unsupported RETURNING decoding. Flush, local rollback,
aggregate counts, error identity, observed EOF and business completion are not
interchangeable proofs. Cancellation/timeout never establishes absent effects.

Source preparation identity/revision, public resource generation, opaque
correlation and observed attempt count remain separate. Unknown attempts stay
unknown. Each matching Internal receipt and inbox record transfers once into
required public custody; failed receiver delivery retries facts, never SQL.
Primary and cleanup errors remain separately inspectable in public receipts.
Recognized classifications retain their nearest meaning and original
`errors.Is`/`errors.As`, including cancellation causes. Inspect native causes
deliberately: they may contain SQL, paths and values.

The shared [error boundary](../../../../../development/adapter-errors.md) preserves
already-public occurrences through neutral wrappers and keeps heterogeneous
aggregates without inventing one provider owner. Native semantic frames retain
their classification; bounded inspection does not reinterpret private causes of
an already-classified occurrence. Cleanup composition retains the primary core's
known details and preserves unknown detail schemas as opaque inspection data.

The [Adapter tree map](../../../../../../adapters/README.md) owns file roles:
configuration, policy, definitions, error translation, diagnostics and resources
remain separately reviewable without changing signatures, codes or effect facts.

Definitions and English/zh-CN Resources use Database facility `0x082`, component
`database_duckdb`. Existing CLI list/explain operations compose them offline,
without initializing an engine. Ordinary fmt/slog output is redacted. Public
runtime results, requests, exact value wrappers and ownership handles reject JSON
serialization/reconstruction. Settings remain explicitly serializable
configuration; direct Settings formatting/logging is restricted.

Public package major `v1`, configuration format `1`, result representation
`ResultRepresentationVersion`, Internal provider `sqlengine.duckdb.v2`, driver
`v2.10505.0`, bindings `v0.10505.0` and core `v1.5.5` are independent version axes.
`Result.RepresentationVersion()` is zero when native data is absent. `Build`
observes selected driver/bindings from the executing binary, preserving missing,
replacement and development facts; it is not artifact attestation. `Profile`
observes the linked core and copies resolved non-secret options, not deployment
or production compatibility certification.

## Fixed/Follow composition and executable checks

A public resource binding returns a non-nil `resource.Instance[Handle]` whenever
Open acquired ownership, including failure. Its Value is Owner.Handle and its
Release delegates to Owner.Release. Fixed retains its first native source. Follow
changes future roots only: each ordered transaction or retained reader pins one
generation for its entire native lifetime. Failed candidates preserve last-good
ownership and cleanup evidence; a larger generation cannot undercharge an
already-declared Using Budget.

Changing memory engines creates different database state. Different files select
different state; there is no file copy or transparent migration. Concurrent owners
of the same persistent path can conflict with native sealed configuration and
must be handled as a failed candidate, not an in-place upgrade.

The [Framework external consumer](../../../../../../adapters/sqlengine/duckdb/v1/testdata/framework/main.go)
executes Fixed/Follow reads/writes, concurrent transaction/reader retention,
last-good/failing and superseded real-engine candidate cleanup, and larger-budget
refusal using only public imports. The [consumer runner](../../../../../../adapters/sqlengine/duckdb/v1/integration_test.go)
prepares independent modules, verifies offline tidy/read-only race builds, checks
pinned driver metadata and proves rejected cross-provider/ownership conversions.

Maintained [finite/effect tests](../../../../../../adapters/sqlengine/duckdb/v1/client_test.go),
[exact-value controls](../../../../../../adapters/sqlengine/duckdb/v1/result_test.go),
[settings tests](../../../../../../adapters/sqlengine/duckdb/v1/option_test.go),
[source ownership tests](../../../../../../adapters/sqlengine/duckdb/v1/source_test.go)
and [reader tests](../../../../../../adapters/sqlengine/duckdb/v1/reader_test.go)
use the actual native engine. Persistent tests use unique test-owned files and
verify reopen/cleanup. The 70,000-row reader test verifies every row and sequence
effect in chunks of at most 257; it is a capacity test, not a speed comparison
against finite Query rejection. Performance comparisons require identical
successfully completed work and correctness/evidence conditions on both routes.

Implementation lineage is [#109](https://github.com/frost-leo/fathomry/issues/109).
Implemented tests do not by themselves establish merged or released status.
