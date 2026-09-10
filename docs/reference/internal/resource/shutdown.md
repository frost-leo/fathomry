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

# Initialization failure and shutdown

[Documentation](../../../README.md) / Internal package reference

**Audience:** composition and Provider authors.
**Status:** implemented cooperative resource management.
**Package:** `github.com/frost-leo/fathomry/internal/resource`.

See the [resource interface](interface.md) and [lifetime architecture](../../../architecture/resource-lifecycle.md).

## Initialization failure and truthful shutdown

Factories run in declaration order; readiness checks run only after construction
succeeds. Parsing, constructing, checking readiness, and creating remote resources
are different permissions. A `Check` callback authorizes only its documented work.

A factory must return `Acquired: true` whenever it acquires any cleanup responsibility,
including on error, and preserve every required handle through `Release`. A
non-nil release callback is conservatively treated as acquired responsibility.
Reporting no acquisition is a Provider statement about local ownership, not proof
that no remote effect happened. A factory missing its required cleanup callback
leaves an explicitly pending record; the manager cannot invent a cleanup action.

On construction or readiness failure:
- no usable assembly is exposed;
- `Report.Primary` retains the original attributed failure;
- cleanup runs with the separately supplied cleanup context;
- the returned assembly retains unresolved cleanup/return responsibility;
- the returned error preserves primary and cleanup causes through `errors.Is/As`.

Keep that returned assembly until responsibility is discharged or explicitly
handed off to an accountable owner. Do not discard it merely because construction
returned an error.

Framework-owned cancellation checks before construction, before readiness checks,
and after readiness preserve both `ctx.Err()` (`context.Canceled` or
`context.DeadlineExceeded`) and `context.Cause(ctx)`. Both the returned error and
`Report.Primary` retain deliberate `errors.Is/As` inspection, including causes
propagated from a parent context; ordinary diagnostics do not print cause text.
If a constructor or readiness callback returns its own error, that observed error
remains primary: coincident context cancellation neither replaces it nor adds an
inferred cause. Cleanup still uses the caller's separate budget, retains its own
errors and leaves unconfirmed resource/dependency responsibility pending.
Cancellation is not release, rollback or permission to retry cleanup. See the
[cancellation regressions](../../../../internal/resource/resources_cancellation_test.go) for
[Issue #11](https://github.com/frost-leo/fathomry/issues/11).

Dependencies, including callback captures, must precede consumers. Shutdown is
reverse-order and conservative: an incomplete resource retains all earlier
entries, including borrowing leases. This intentionally does not promise precise
cleanup of independent branches or a general dependency graph.

`ReleaseResult` reports three independent kinds of information:
- `Quiescent`: positive evidence that users/background work no longer need dependencies;
- `Released`: positive evidence that all resources owned by this record are released;
- `Err`: cleanup failure, which may coexist with either positive fact.

Both positive facts are required for completion; false means unconfirmed.
Confirmed evidence is monotonic for this non-reloadable lifecycle. These fields do
not represent business data durability or reversal of external effects.

An incomplete result can explicitly return a `Continue` action to resume cleanup
or reconcile completion. The original callback is never blindly retried. Without
a continuation, responsibility remains visible but cannot be completed by this
manager alone. A later nil/no-op result cannot supply missing positive evidence.
Historical cleanup errors remain inspectable even after confirmed completion.

### Who bounds accumulated cleanup history?

Framework composition, as the caller of `Close`, owns the **aggregate** continuation
attempt and retained-native-error budget across calls and resources. Each callback
owns the size/lifetime of the native errors and object graphs it returns. A fresh
deadline on every call limits neither the total number of calls nor the bytes
retained by those causes. Business consumers are not expected to implement this
internal composition policy themselves.

Every explicit action returning a non-nil `Err` adds its attributed error to history.
After N such actions, one record retains O(N) error references plus their native causes; each
`Close`/`Snapshot` copies the current history, so repeated calls can incur O(N²)
cumulative copying. Retaining those returned snapshots/errors has an additional
caller-owned cost. Budget action count, native cause size and retained reports
together, not merely formatted diagnostic length. `errors.Is/As` still reaches
the original causes after eventual completion and later no-op closes.

The manager neither retries automatically nor silently truncates/deduplicates
causes. Exhausting an outer budget does not confirm release or authorize discarding
the assembly: retain unresolved responsibility or explicitly hand it to an
accountable owner. This contract adds no lifecycle controller or retry policy.

### Cooperative execution and snapshots

`Close` is synchronous and serialized. Waiting for another Close observes the
caller's context; invoking an arbitrary callback cannot provide a hard deadline
or forcible cancellation. No goroutine is spawned to hide unfinished work.
Callbacks must cooperate with their declared budgets and must not reenter Close.
They must return errors rather than panic after acquiring resources; arbitrary
Provider panics and process termination are not recoverable ownership guarantees.

Snapshots copy metadata and error slices. They observe each record consistently,
but do not promise a single atomic instant across different owners/resources.
The lifetime of native errors themselves remains their authors' responsibility.

## Executable evidence

[Resource tests](../../../../internal/resource/resources_test.go), including
`TestCleanupHistorySurvivesExplicitContinuationAndCompletion`, and
[cancellation regressions](../../../../internal/resource/resources_cancellation_test.go)
cover this contract. Aggregate-budget clarification: [Issue #17](https://github.com/frost-leo/fathomry/issues/17).
