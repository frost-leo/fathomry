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

# Runtime resource ownership

[Documentation](../../../README.md) / Public package reference

**Audience:** component authors, framework composition and independent Go consumers.
**Status:** implemented public instance ownership; component factories define native
readiness and migration limits. Framework startup and concrete SDK Adapters remain
separate, unfinished work.
**Package:** `github.com/frost-leo/fathomry/resource/v1`.

## Responsibility and dependency direction

Resource owns typed named instances, fixed/following configuration adoption,
generation borrowing and cleanup continuation. It imports only public failure,
settings and the standard library, never Internal mechanisms or concrete SDKs.
It is not a parser, service locator, connection pool, configuration store or
workflow scheduler. Its [package overview](../../../../resource/v1/doc.go) and
[examples](../../../../resource/v1/example_test.go) describe executable usage.

A project supplies its complete settings type. Components supply their selectors,
copy/equality rules and construction/cleanup functions once. Normal Framework
composition can connect these declarations without generating another lifecycle
state machine in every project.

## Declare and apply

1. Create `Scope` with an explicit parent context and `Options`.
2. Use `Bind[C,T]` to declare named component bindings and obtain typed `Ref[T]`.
3. Feed an accepted `settings.View` through `Scope.Apply`, or attach `Scope.Watch`
   to an explicit channel of accepted views.
4. Inspect/wait for the returned `Update` when adoption must be confirmed.
5. Acquire a lease for the complete operation, use its value, then release it.
6. Close the scope and retain it until cleanup is actually confirmed.

Binding names are unique within one scope, case-sensitive non-secret ASCII labels
of 1..64 letters, digits, dots, underscores or hyphens. Different scopes and names
never share instances implicitly. Optional scope Name uses the same grammar.

Apply admits context/view before sealing declarations. Later selection failure
does not reopen declarations. Watch seals declarations when its receiver is created.
Zero views, nil contexts, missing callbacks and invalid limits reject. An empty
scope is valid; no active generation is fabricated for an unconfigured binding.

Select receives one captured view and performs pure selection/validation. Clone
isolates selected mutable configuration; Build receives another copy. Equal borrows
immutable configurations and cannot modify them. Follow requires Equal; Fixed does
not. These owner callbacks must be bounded, synchronous, concurrency-safe and
non-panicking. There is no universal configuration type whitelist or reflected
deep-copy fallback. The framework cannot prove a dishonest clone is correct.

All relevant selections complete before any new target is committed. Apply/Retry
admission is serialized, while construction occurs asynchronously. Parent shutdown
and caller cancellation are checked before commitment. Producers must order their
source observations before applying them; native hashes/timestamps are not clocks.

## Fixed and following instances

- `Fixed` retains its first accepted selected configuration. Later views do not
  invoke its selector or reconstruct it.
- `Follow` reconstructs only when its own selected configuration changes.
  Duplicate/unrelated updates reuse the same target; they do not retry a failed
  constructor implicitly.
- `Ref.Retry` explicitly continues a failed construction or incomplete cleanup.
  It does not duplicate an already running constructor or retry business operations.

A-to-B-to-A changes cancel obsolete B even when active A can be reused. Candidates
cannot publish after their target is superseded or the scope starts closing.
Generation reservations bound active instances, outstanding construction and
retirement together. Retained old borrows or incomplete cleanup can backpressure
new construction; only the latest desired configuration is retained.

A construction failure leaves an earlier active generation available. Availability
does not establish current service health, credential validity or authorization.
The component/framework must enforce its own account, migration and continued-use
rules. A different control database is not an ordinary pool refresh.

`Update.Wait` reports historical target outcomes, not a live readiness query.
An already successful receipt remains successful after later changes or shutdown.
A target superseded before adoption returns `ErrSuperseded`. An unchanged Apply
can return a receipt for the same failed target; explicit Retry creates another
attempt receipt. Different bindings adopt independently: complete data publication
does not imply atomic replacement of all application resources.

## Construction lifetime is not a waiting budget

Build receives a generation-owned context. It is canceled for a superseded
candidate or when an unborrowed generation is being cleaned. Successful Build
return does not cancel it. Canceling Apply/Update.Wait does not cancel an already
accepted construction.

The scope's parent cancellation stops admission and initiates shutdown. It does
not forcibly cancel a borrowed active generation and pretend cleanup is complete.
The generation context retains parent values without inheriting the parent's
deadline/cancellation. Components should create child contexts for bounded setup
or readiness operations and pass the lifetime context only where the SDK actually
retains it. Context values and callbacks remain retained with their owners.

Constructor dispatch checks the scope/parent stop state both before copying and
again before Build, independently of asynchronous parent-shutdown notification.
A call already past that check can still overlap cancellation; its actual acquired
ownership must be returned and joined, not inferred absent from a timeout.

A non-nil `Instance[T]` transfers ownership even if Build also returns an error.
Nil plus nil error rejects as failed construction. Legitimate zero/nil T values
are permitted when the component contract says they are usable. Instance.Value's
concurrent-use guarantees come from the component, not from the generic holder.

Foreign work runs outside state mutexes. It cannot be forcibly terminated if it
ignores cancellation. Callbacks cannot synchronously wait for lifecycle progress
that requires their own return: selection must not re-enter Apply/Retry, a factory
must not wait for a constructor needing its occupied slot, and cleanup must not
wait for Scope.Close. Callbacks must return acquired cleanup responsibility on partial failure;
a panic or secretly discarded acquisition violates the component contract.

## Borrowing and retirement

Selecting the current generation and increasing its borrow count are one atomic
admission step. New borrows select the latest active generation; existing leases
never retarget. A transaction or long session must keep its lease for its entire
declared use, not reacquire separately for each nested operation.

Lease copies share one release authority. Release succeeds once; repeated release
and subsequent Value access return `ErrLease`. Do not call Value concurrently with
Release, or retain/use the returned T after releasing the lease. The holder cannot
revoke arbitrary raw copies retained outside that contract.

Retired generations remain owned until their borrows end and cleanup confirms
completion. Long subscriptions are not automatically migrated to another endpoint;
their typed Adapter supplies explicit resync/rebind/retirement semantics.

## Cleanup, cancellation and failure evidence

Instance.Release receives a fresh context with CleanupTimeout, independently of
the canceled generation lifetime. A nil Release explicitly declares no external
cleanup requirement.

| Release result | Meaning |
| --- | --- |
| Complete true, Err nil | Ownership ended successfully |
| Complete true, Err non-nil | Ownership ended with retained cleanup error evidence |
| Complete false | Ownership remains; a later serialized continuation is required |

An incomplete result suspends automatic attempts instead of spinning. Retry or
another Scope.Close resumes it. Close reports known incomplete cleanup with
`ErrCleanup` and Details.Pending true; waiting interruption returns `ErrWait`.
The same scope remains the continuation handle. Multiple callers never cause
concurrent cleanup callbacks for one binding.

Close stops new input/borrows, cancels candidates and joins the actual receiver,
selectors, construction, borrowers and cleanup. A completed scope can still return
retained completed-cleanup errors. Status.CleanupErr is the last failure occurrence,
not proof that cleanup remains pending now; use PendingCleanup/Closed for current
ownership. Completed cleanup errors and failure counts have bounded retention.

## Watch handoff

Watch consumes a caller-owned channel of already accepted settings views. It
does not close that channel or own the upstream file/Nacos observer. One receiver
can be active per scope; another can be attached after the previous one exits.

Next consumes one bounded latest `Observation`. Sequence counts consumed inputs;
Skipped counts reports coalesced before consumption. Observation.Err describes Apply
admission failure; Observation.Update must be waited/inspected for actual adoption.
This is not a complete change log. A pending report can be consumed after EOF;
subsequent Next returns an error retaining `io.EOF`.

Next cancellation affects only that wait. Watch.Close stops only the local receiver,
leaving existing instances running. Scope.Close also stops and joins its receiver.
Uncooperative selection can delay that join; timed-out Close retains responsibility.

## Limits and diagnostics

| Boundary | Default | Allowed |
| --- | --- | --- |
| Bindings per scope | 64 | 1..256 |
| Concurrent constructors per scope | 4 | 1..64 |
| Generation reservations per binding | 2 | 2..16 |
| Borrowers per binding, including retired generations | 1024 | 1..65536 |
| Cleanup callback budget | 5 seconds | 1 millisecond..1 minute |
| Active local Watch receiver | 1 | Fixed |
| Pending Watch report | 1 | Latest report with skipped count |

Counts are not RSS limits, external quotas or permission to exceed an SDK's own
limits. Configuration/instance sizes and callback work belong to their owners.
Scope sequences, binding targets and construction generations are separate local
uint64 counters; overflow rejects rather than wraps.

Status provides detached per-binding observations. Scope.Inspect preserves
declaration order but does not claim a common-time cross-instance snapshot.
Active means locally adopted, not externally ready. Runtime fmt/slog and JSON
guards do not format configuration, arbitrary T values or native causes. Value,
Status errors and failure cause traversal are explicit potentially sensitive access.

Definitions use the reviewed core-domain facility 0x004. Details identifies scope,
target, generation and occurrence-time pending responsibility. Numeric Code,
Identifier, native causes, retry decisions and localized text remain separate.
Definitions/Resources compose explicitly with i18n; CoreComponents does not
implicitly add resource declarations or construct any instances.

## Verification and remaining boundary

[Feature tests](../../../../resource/v1/) cover policies, stale candidates, copying,
borrow/replace races, limits, partial acquisition and cleanup continuation.
[Public integration](../../../../resource/v1/integration_test.go) uses a real
loopback HTTP client/transport and i18n Presenter in one scope. It proves that
new requests use a replacement while an old request retains its original transport.
The [external consumer](../../../../resource/v1/testdata/consumer/consumer_test.go)
uses no Internal import; compilation controls reject generic tag conversions and
scope shutdown through Ref.

The opt-in [native service composition](../../../../internal/conformance/resource_service_test.go)
separately checks Nacos push through validated settings into fixed/following
PostgreSQL pools. It exercises rejected input, real SQL readiness failure, retained
transactions and session-local state across replacement, and distinct old/new
Nacos subscription lifetimes. Test-only native ownership translation stays in
maintainer conformance, not the public package. See [execution requirements](../../internal/conformance/fixtures.md#public-owner-and-native-service-composition).

These tests do not qualify control-store migration, arbitrary SDK session transfer,
multi-service transactions, generated projects or a completed Framework runtime.
Source reasoning, rejected hypotheses and exact module qualification are retained
under the local #98 reference workspace, not claimed as public deployment evidence.
