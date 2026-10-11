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

# Maintained Iceberg Go source

Base: official Apache Iceberg Go `v0.7.0`, commit
`89a9104ec3639d38ab07e4461721cb5270e95571`. This source is maintained for Fathomry
issue #137; it is not an upstream release or a claim of complete service
qualification. The original Apache LICENSE, NOTICE, source headers and all 770
published regular files are preserved. `CLAUDE.md` remains an upstream symlink.
First-party additions carry the project license notice. Modified upstream files
identify their Fathomry changes after the original notice.

The acquisition archive SHA-256 is
`a58269ea6aa8228e8d4d3ff3da2eefad6aac76f11d249d2c6a618b8ea982ee8d`.
Official module sum: `h1:bTD6Pb4uM4sWcMfIx0/cV1haHMc84r+CvPieUqYzD4I=`.
Official go.mod sum: `h1:oGz5MX3/m3GDc6acsrwbdHChj28jZH8MVDWqUrW6WQQ=`.

## Managed integration seams

- REST path construction escapes table/view/function names as opaque components
  and preserves pure-dot namespace levels. Literal percent escapes must not turn
  into a different resource path. Namespace/plan-id encoding remains single-pass;
  request body identifiers are not URL-encoded. Managed and native regression
  controls cover these routes without narrowing the native identifier grammar.

- `rest.WithScanPlanningContext` admits and attaches source-local custody before
  native remote-plan request setup. Initial retries retain that context; the
  caller still owns cancellation/deadlines. Fathomry uses it to reserve bounded
  plan cleanup before I/O, including ignored low-level results. The hook is not
  a replacement planner or a claim that a cancellation acknowledgement joins a
  remote server's workers.

- `rest.WithTableValidator` performs source-local metadata-object validation on
  every native Catalog table load, including refreshes inside commit replay.
  It retains native FileIO, credential and planning configuration and refuses a
  bad object before handing that table to subsequent native work.

- `rest.WithFileIOFactory` supplies per-Catalog FileIO construction after native
  property and vended-credential merging. It covers ordinary table loads,
  create/register/update responses, refresh and remote scan plans without a
  process-global IO registry change. Factory errors never fall back to the
  registry. Effective property maps passed to callbacks are independent copies.
  The custom path caches credential configuration, not context-bound FileIOs:
  each table or plan-reader acquisition gets its own context. The embedding
  source owns physical resources; any returned Close method must release an
  independent lease, not a shared owner. Native default-registry construction
  remains available when this explicit option is absent.
- `rest.WithStorageCredentialValidator` checks every credential entry before
  prefix selection on table loads, credential refresh and remote-plan responses.
  Low-level plan endpoints supply an empty location because their requests have
  no metadata URI. Validation receives deep-copied maps; rejecting a plan response
  attempts bounded native plan cancellation. The caller defines URI canonicality,
  roots, authority, expiry and whether vending is enabled. The native prefix
  matcher is not advertised as an authorization policy.
- `table.WithWorkerLimit` caps each reached worker pool using the operation
  context. Explicit source limits take precedence over ambient worker defaults
  without changing global configuration or table properties. Child contexts
  cannot relax a cap. This is a per-pool worker bound, not a whole-process
  goroutine, allocation or RSS bound. Import-time native `config.EnvConfig`
  loading remains; its worker selection is bypassed by the explicit context cap.
- `table.WithCommitRetryLimit` caps known-conflict retries without modifying
  metadata. Zero means one initial attempt, not zero attempts. Unknown outcomes
  remain nonretryable. Child contexts cannot relax a cap. Without this option
  native table properties determine retries.
- `table.WithOperationLogger` and `rest.WithLogger` borrow operation/source-local
  loggers. Cleanup errors, reporter panics and value-sensitive operation warnings
  no longer require changing a process-global sink. The owner retains handlers
  through producer and dispatcher completion. Contextless format warnings omit
  private paths, schema names, property values and native error details; no data
  or mutation semantics are changed by this presentation adjustment.
- `Table.AllManifests` starts producers only when iterated and synchronously
  cancels, drains and joins on early return. An iterator that is never consumed
  does not retain running work. Its existing materialized snapshot and manifest
  costs are not described as constant-memory streaming.
- Incremental append/changelog `UseRef` preserves the ordinary native Scan
  branch/tag schema binding while returning a copy, like the existing range
  setters. The provider keeps those returned objects rather than discarding
  native snapshot selections.
- `rest.Catalog.CloseAndWait` joins the actual metrics worker/statistics group
  after native Close's bounded observation. Source owners retain cleanup when
  their own wait expires; a timed-out native Close is not producer-join evidence.

The source also contains the separate nullable copy-on-write
Delete/Overwrite repair in `table/transaction.go` and its focused regression
tests. Retained rows are the rows for which the original predicate is false or
NULL, rather than a nullable SQL-style NOT expression. This avoids dropping
nonmatching NULL rows and preserves existing row-lineage projection behavior.

`Table.ExecuteOrphanCleanup` preserves its planned file set, total size and
acknowledged `DeletedFiles` alongside a deletion error. Previously the native
error branch discarded this partial result even when FileIO returned successful
deletions. Unknown/refused deletions are never added to the acknowledged list.

Schema evolution compares defaults by value rather than Go interface equality,
which panicked for binary/fixed byte slices. Repeated schema edits also keep their
current-schema requirement on the transaction's original base, rather than
asserting a staged schema ID the Catalog has never seen. Native regressions and
managed binary-default mutation controls exercise both corrections.

Compaction now selects fanout when a group's input partition spec differs from
the current write spec. A historically unpartitioned group is not necessarily
clustered after bucket/identity evolution. The managed regression preserves the
exact multiset across schema, partition and sort evolution; unchanged-spec groups
retain the original clustered writer. The failing native path and corrected live
REST/S3 result are retained in issue #137's implementation evidence.

## Verification boundary

Focused controls live in `catalog/rest/file_io_factory_test.go`,
`table/execution_limits_test.go` and `table/transaction_nullable_rewrite_test.go`,
alongside unchanged upstream tests. Exact commands and results belong to the
issue #137 evidence record and must identify the consuming module graph and
worktree state. Source copying, compilation, native test success and live
REST/S3 qualification are separate facts. Do not infer a live service pass from
these files or treat an absent/skipped control as successful verification.

Before upgrading, compare this maintained tree against the next official source,
rerun the causal tests and retain or retire each seam explicitly. A root-only
local replacement is not an independently consumable distribution certificate.
