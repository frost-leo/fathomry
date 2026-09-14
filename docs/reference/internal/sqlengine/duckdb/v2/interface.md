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

# DuckDB interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** in-module capability and composition maintainers.
**Status:** implemented bounded native-table profile; not an unrestricted DuckDB
or external Iceberg integration. See [qualification](verification.md).
**Package:** `github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2`.

## Responsibilities

This independently selectable provider owns one native database and finite calls
on its connections. It provides native Appender batches, reusable preparation
within parameter batches, many-row SQL results, and ordered local transactions.
There is no common engine API, public framework facade, deployment service,
background maintenance, automatic retry, or Workflow-side I/O.

The directory's `v2` denotes the **Go driver's SDK major**.
`OptionsV1` and configuration format 1 are separate contracts. The selected
driver is `duckdb-go/v2 v2.10505.0`, bindings are `v0.10505.0`, and the native
core is `v1.5.5`. A version string is not compatibility certification.

## Capabilities and call sequence

1. Call `Select(OptionsV1, layers...)` to freeze explicit settings with
   `resource.Prepare`. No native database is opened during selection.
2. Attach `resource.WithLimits`; `LimitsV1` describes defaulted options only.
   Overridden bounds require a matching explicit policy.
3. `resource.Assemble` opens the native database and seals its configuration.
   The assembly exclusively owns database close. Borrow/delegate through
   `resource`, not by extracting SDK handles or independently opening the same file.
4. Create a separately owned `invocation.Inbox[Result]` and call `Bind`.
   A nil optional observer disables diagnostics, not required evidence.
5. `Run` accepts one `Request`; `Transaction` accepts 1–32 requests on the
   same connection, in order, sharing one total input/result envelope.
6. Inspect the returned receipt's final result **and its `Err()`**. Receive and
   explicitly release the independent inbox delivery even if the caller ignores
   its receipt. Close the assembly with a separately supplied cleanup context.

| Mode | Inputs and behavior |
| --- | --- |
| `Execute` | One `SQL` with positional `Args`; preserves the native changed-row aggregate |
| `Query` | One `SQL` with positional `Args`; consumes one result and returns a bounded positional snapshot |
| `ExecuteMany` | One `SQL`, prepared once; each `Rows` entry is a parameter set. `Run` wraps the entire batch in one local transaction |
| `Append` | Native `Schema`/`Table`, optional selected `Columns`, and `Rows`. `Run` wraps Appender buffering, flush and close in one local transaction |

Unused request fields must be empty. Empty batch input is valid, including
preparation/schema validation and transaction completion; it does not invent
inserted rows. Append without a column list uses all table columns. A selected
column list preserves its order and permits native defaults for omitted columns.

Statements, appenders, result readers and fresh native connections never escape.
There is no exported long-lived prepared-statement/session handle, borrowed raw
SDK connection, callback-owned stream, or implicit `database/sql` pool/retry loop.
Preparation reuse is scoped to `ExecuteMany`. Query-driven batch mutations can
also use SQL directly or a transaction containing staging-table Appender writes
followed by SQL DML; the SDK QueryAppender API is not exposed.

## Operation audit

These classifications concern local native DuckDB tables, not every SQL
variant or attached table format.

| Family | Supported path and boundary |
| --- | --- |
| DDL / CTAS | `Execute`: CREATE TABLE, CTAS, native ALTER/DROP. CREATE, CTAS, column rename and DROP have native tests |
| Whole-table replacement | Native CREATE OR REPLACE TABLE AS; distinct from conflict-key upsert and file overwrite |
| Append / bulk insert | Transactional Appender, selected columns/defaults, INSERT SELECT, and prepared parameter batches |
| UPDATE / DELETE / MERGE | Native SQL, including MERGE update/insert branches; not append-only. Native constraints still apply |
| Queries and RETURNING | One many-row result, including DML results. Query does **not** mean read-only or no effects |
| Transactions | Ordered finite requests; one default-isolation native transaction and connection. Raw transaction SQL, savepoints and alternate isolation/read-only transaction modes are not supplied |
| Maintenance | Native ANALYZE, VACUUM and bare CHECKPOINT. CHECKPOINT is outside transactions; VACUUM is not lake compaction or a general space-reclamation guarantee |
| File import/export | COPY, EXPORT and external file/network reads are rejected by statement policy and/or native external-access restrictions |
| Extensions/catalogs | ATTACH/DETACH, INSTALL/LOAD, raw configuration changes and arbitrary CALL/PRAGMA are not supported. No external Iceberg qualification |

EXPLAIN is rejected, including EXPLAIN ANALYZE: the top-level native statement
type does not expose its inner command, which could COMMIT or ROLLBACK the
provider-owned transaction. There is no textual keyword-filter workaround.

The native `Conn.Prepare` path rejects any input extracted into more than one
statement **before executing those statements**. The provider never falls back
to `PrepareContext`, which can execute preceding statements. This also rejects
some syntactically single statements: the selected core expands
`ALTER TABLE ... ADD COLUMN ... DEFAULT ...` into multiple statements.
Column rename has a passing control. This restriction is explicit, not a claim
that all valid DuckDB DDL is supported.

Preparation itself is native, uncancellable binding/planning; it is not a general
SQL purity guarantee. Inputs must be authorized, trusted SQL. Configuration
restrictions and Go's `internal` boundary are not an untrusted-code sandbox.

## Values and results

Supported scalar transport includes null, booleans, signed/unsigned integers,
FLOAT/DOUBLE, UTF-8 strings, BLOB, UUID, HUGEINT/UHUGEINT, DECIMAL, INTERVAL,
DATE, TIME and timestamp precision variants. ENUM uses native strings.
Arbitrary `driver.Valuer` implementations, handles and recursive Go values are
rejected. `uint` parameters normalize to `uint64`; UUID and Decimal parameters
use canonical exact text, never floating-point conversion. The SQL target column
or an explicit cast supplies their native type; an untyped placeholder therefore
returns their text representation. Temporal parameters are checked against their
inferred target type using the same UTC/precision/range rules as Appender inputs.
Other SQL conversions remain native semantics. Appender validation
rejects narrowing overflow, fractional-to-integer casts, mismatched
decimal width/scale and timestamp precision loss before buffering.

- Query column names and native type names remain positional, including duplicates.
  Only caller-specified `ORDER BY` establishes ordering.
- SDK decimal/`big.Int`, byte slices and result containers are copied.
  `Result.Snapshot()` returns independent storage; mutating one snapshot cannot
  modify the receipt or the independently delivered evidence.
- Null and empty values remain distinct. A complete empty result has observed
  EOF and column metadata; absence of an outcome is a different state.
- All composite result decoding (MAP, LIST, ARRAY, STRUCT, UNION), automatic JSON
  decoding, TIME, TIMETZ, BIT, BIGNUM and unqualified types are rejected before
  `rows.Next`. This prevents the selected driver's BLOB/UUID-key MAP panic
  without suppressing a panic after decoding. Use explicit scalar projections or
  explicit SQL casts when that representation is the caller's chosen contract.
  TIME input remains available, but raw TIME output is not exact in this SDK:
  24:00 and 00:00 both decode to midnight. `CAST(time_column AS VARCHAR)` preserves
  the distinction; the provider does not guess after information has been lost.
- Decimal values retain an integer coefficient and scale, not a float conversion.
  Timestamp inputs must fit the selected precision; native time transport is
  limited to UTC years 1–9999. TIMESTAMP_NS inputs additionally require year 1678
  or later for this SDK, a roundtrippable Unix nanosecond count and a native finite
  value. TIMESTAMP_NS infinity sentinels are rejected on input and typed output,
  even though the SDK can decode them into apparently finite Go times.
  Timezone identifiers/offset provenance and infinite dates are not transported.
- `Complete` requires actual EOF. A row/byte limit returns `ErrLimit`, preserves
  the exact retained prefix and sets `Limited`; it never reports silent success.
  One lookahead row may be decoded to distinguish an exact bound from truncation.

Runtime options, requests, results and handles refuse JSON reconstruction and
redact ordinary formatting/logging. They are not durable DTOs. Callers explicitly
construct their own versioned serializable values/references for durable work.

## Ownership, limits and cancellation

An empty `Path` creates an isolated in-memory instance. A nonempty path must be
an explicit clean absolute local path, not a DSN/URI. Opening/creating the selected
file and its native WAL is authorized by that option; directory and file removal
stay caller-owned. Use one provider owner per persistent file and coordinate
other processes. A second independent provider owner of the same file is rejected
by the tested sealed-configuration path; use resource borrowing instead.
Native file locks are not a distributed writer protocol.

Extension auto-install/autoload, unsigned/community extensions and persistent
secrets are disabled. Spill is disabled before external access is disabled and
configuration locked, with no caller SQL exposed between initialization phases.
The order is necessary: core refuses a temp-directory change once external access
has been disabled, whereas SDK DSN option iteration is unordered.

Defaults are 1 connection, no queued calls, 1 native thread, a 256 MiB native
memory setting, 30s admission/execution budgets, and a 5s appender/rollback cleanup
budget. Settings and their ranges are documented in `OptionsV1`.
Admission and execution have separate budgets; the caller's absolute deadline
can bound their aggregate. Cleanup receives a separately authorized context;
each appender/rollback cleanup phase derives its budget from it. Assembly shutdown
uses its own caller-supplied context.

Bounds are per source and per accepted call:

- At most 64 columns, 64 KiB per SQL statement, 32 transaction steps,
  65,536 input batch rows by default and 8,192 retained output rows by default.
- Defaults: 8 MiB input and 4 MiB retained result accounting. Scalar payloads,
  conservative Go container costs, SQL and result metadata are counted.
- Root reservation: `2*InputBytes + 2*ResultBytes + 256 KiB`.
  Evidence reservation: `InputBytes + ResultBytes + 128 KiB`, retained until the
  independent receiver releases it. Saturation rejects before native execution.
- These are **not hard RSS/native-allocation/error-graph limits**. Native queries
  may materialize results, scalar decoding allocates before the retained-output
  check, and native error text can contain data. The engine's memory setting and
  no-spill policy do not cap every C++ allocation. Deployments requiring a hard
  process-memory or hostile-input boundary are unsupported by this in-process API.
- Caller-retained snapshots and work in other processes/owners are outside this
  resource's accounting. No new cache, connection pool or feedback controller
  is introduced.

Connection creation, preparation, Appender row writes/clear and destruction lack
context interruption. The stable driver's execution cancellation can add about
500 ms after an interrupt; neither cancellation nor a configured timeout proves
native termination. Calls remain synchronous until ownership actually ends.
There are no detached native goroutines or early resource-lease releases.

## Effects, errors and independent evidence

A returned setup/admission error has no accepted receipt. For accepted calls,
`invocation.Outcome` separately retains primary and cleanup errors, with native
`errors.Is/As` causes intact and safe technical diagnostics. Optional observation
does not determine success or hold native cleanup.

SDK-wide attempts remain inexact/unobserved. `Step.Executions` counts successful
explicit statement executions, not all attempted native work. Caller cancellation
causes remain inspectable alongside the native/context error; they do not erase
acknowledged effects.

`Step` preserves preparation, submission, successful native execution count,
changed-row aggregates, Appender accepted/flushed rows, close attempt completion,
and result completeness. `Progress` separately records begin, commit attempt/
acknowledgement, rollback attempt/acknowledgement and connection destruction.
`FlushAttempted` and `Flushed` distinguish a successful zero-row flush from an
unattempted or failed flush. Changed-row counts cover acknowledged executions
only, not any effects of a failed execution. Native sequence advancement is not
undone by a table transaction rollback.

Appender `CloseWithCancel` can write even with an already-canceled context.
On failure the provider clears pending buffers before close, then attempts
rollback with the cleanup context. Clear is not rollback, flush is not commit,
and a failed commit acknowledgement leaves uncertainty. Earlier successful
step progress remains visible after a later failure or rollback.

A query can execute DML successfully and then fail result-type validation:
the autocommit write can remain committed despite the result error. For atomic
write/result handling use the local transaction path. Native disconnect can
discard uncommitted changes when rollback was not attempted, but the result
does not invent a rollback acknowledgement from that fact. Retry and business
reconciliation remain caller decisions.

See [verification and upgrade obligations](verification.md), the
[native composition tests](../../../../../../internal/sqlengine/duckdb/v2/integration_test.go),
and [Issue #40](https://github.com/frost-leo/fathomry/issues/40).
