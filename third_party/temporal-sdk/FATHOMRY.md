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

# Temporal native ownership compatibility extensions

**Status:** local development replacement for issue #61; not an upstream release
or complete Temporal integration qualification.

## Source and version ownership

- Main module: `go.temporal.io/sdk v1.49.0`.
- Exact origin: `58115b581391037b93451f50ac3628611ea0493b`.
- Original module and go.mod sums, and all 306 original file hashes:
  [UPSTREAM.json](UPSTREAM.json).
- Local revision: `v1-development`.
- Original MIT [LICENSE](LICENSE) and upstream notices remain unchanged.
- No Server, UI or independently released contrib module is vendored here.

Six original files also receive whitespace-only gofmt normalization required by
the consuming repository's formatting check. Their original hashes remain in
UPSTREAM.json. Use the selected Go 1.27 formatter: an ambient Go 1.26 gofmt cannot
parse the upstream Go-1.27 generic-method file.

Four upstream trailing-whitespace lines in README.md, CHANGELOG.md and
.github/workflows/docker/dynamic-config-custom.yaml are also trimmed for the
consuming repository's committed-tree whitespace check. No content, YAML values
or original provenance hashes change as a result of that normalization.

The local .gitignore adds a narrow exception for internal/cmd/tools/doclink/ so
the copied module's two original tool source files enter version control. The
upstream basename rule would otherwise hide this previously tracked directory
when importing the module into a new repository; generated doclink binaries
remain ignored.

The owner authorized this bounded lifecycle repair after original-version
loopback/race counterexamples. Native Worker.Stop, Local Activity timeout and
asynchronous cache purge are not complete join certificates. This does not
establish a general defect in Temporal's durable execution or replay machinery.

## Opt-in extension

Set `worker.Options.FathomryLifecycleV1` before construction. Call native Stop,
then `worker.FathomryWaitStoppedV1(ctx, worker)`. A canceled wait retains owned
work and can be resumed; it cannot authorize dependency release. Stop's native
timeout, task outcomes, retry policies and Workflow commands are unchanged.

The extension joins native base workers and separately accounts for late poll
work, Local Activity function/converter tails, heartbeat batching, local retry
timers, Workflow coroutines and their asynchronous destruction. Owned cache entries
are conditionally removed without purging live peers. Explicit cache ownership
release is idempotent and does not depend on a finalizer.

Eager reservations remain owned until processed or returned. A response arriving
after shutdown releases its reservation instead of being abandoned in a stopped
queue. Shared heartbeat snapshots retain retiring workers through callbacks and
acknowledgements already copied for execution.

Ordinary SDK Workflow functions, aliases and dynamic definitions retain their
native execution path. Unstable internalbindings WorkflowDefinitionFactory
implementations have no equivalent join contract and are explicitly refused in
this opt-in mode; ordinary upstream mode is unchanged.

## Limits and maintenance

Distinguish two reasons for local changes. Fresh-Client construction rollback and
unused eager-reservation cleanup address reproduced resource leaks (including
pre-response visitor errors, panic and Goexit). Complete Worker join, owned cache
retirement, scoped lazy decoding and the eager bridge enforce Fathomry's stronger
composition contract. Native bounded Stop and ordinary native lazy handles are
not themselves SDK bugs merely because they do not provide that contract.

`worker.FathomryNewWithEagerClientV1` connects a managed polling Worker's eager
dispatcher to its source execution Client at construction only. Both must be
native Clients for the same namespace and target, with lifecycle observation
enabled. It does not expose the execution Client to Worker plugins or change
server-side routing, retry or task commands.

The owner separately authorized `client.FathomryScopeWorkflowUpdateHandleV1`
after a callback-scoped completed Update continued user decoding after expiry.
The returned handle snapshots identity and calls an explicit owner guard for every
Get; the SDK's internal completion predicate recognizes the guarded handle without
exposing its native delegate. This preserves the synchronous Nexus Update branch.
Ordinary unguarded handles and all commands, retries and service messages are unchanged.

The owner also authorized native lazy-decoder scopes for known error graphs and
Activity/Workflow/Nexus descriptions. `client.FathomryScopeErrorV1` preserves native
types, cause graphs, original Failure data and intentional error identity. Scope
owners are private runtime capabilities, never payloads or configuration. Foreign
owners can add restrictions, not replace an existing owner's guard; guard chains
are bounded and fail closed. Description copies share the native decode lock.
Original aliases cannot be retroactively revoked. Opaque custom error objects
remain caller-owned; custom errors with borrowed decoders must implement the
documented explicit decoder-scoping hook. This is not a sandbox for arbitrary Go
object graphs, converter callbacks or retained external references.

Worker plugin isolation and lifecycle composition live in the Fathomry Provider,
not this SDK replacement. The Provider invokes plugin Stop around native Stop
plus the full join, avoiding premature post-next teardown without changing native
Stop's timeout. It also handles dynamic registration hooks in its registry view;
no additional SDK plugin implementation is copied or patched for that purpose.

This is cooperative ownership observation, not forced termination, a new tuner,
a hard memory ceiling or a process sandbox. Application/plugin code must own and
join any goroutines it creates outside SDK-managed execution. Misbehaving code may
keep shutdown incomplete indefinitely. Additional extension qualification remains
required; do not infer a universal safe-plugin or full-SDK certificate.

Maintained controls are `TestFathomry*` in `worker` and `internal`. They cover
normal/noncooperative Activities, timed-out Local Activity conversion, workflow
eviction, live peer cache preservation, late eager responses and copied heartbeat
callbacks. Tests use the product's actual selected graph, not dependency-module
replace directives that a consumer would ignore.

The multi-Namespace control uses one test executable, one loopback service and
native Clients sharing a connection. Identical Workflow/Workflow-ID/task-queue
names remain Namespace-bound; closing the first stopped owner preserves the live
peer. This is native compatibility evidence, not a complete framework Worker API.

A broader native internal-package run timed out in
`TestWorkersTestSuite/TestErrorProneSlotSupplier`; the unmodified v1.49.0 module
also timed out in the isolated same test under the consuming graph. This result
is retained as a failed gate, not reported as a full-suite pass and not silently
fixed as part of the lifecycle change. The upstream build-runner module is not
distributed in the main-module ZIP; direct focused Go tests do not claim the full
upstream build/contrib/integration pipeline.

Before upgrading, compare exact upstream sources and rerun rejecting controls,
replay, multi-worker, callbacks, eager, session and authorized service tests under
the consuming graph. Retire this replacement only when the same ownership
guarantees are independently demonstrated by an official release.
