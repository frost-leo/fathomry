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

**Status:** local development replacement introduced for issue #61 and maintained
for issue #134; not an upstream release or complete Temporal qualification.

## Source and version ownership

- Main module: `go.temporal.io/sdk v1.49.0`.
- Exact origin: `58115b581391037b93451f50ac3628611ea0493b`.
- Original module and go.mod sums, and all 306 original file hashes:
  [UPSTREAM.json](UPSTREAM.json).
- Local revision: `v1-development`.
- Maintenance revision and actual replacement-file hashes:
  [FATHOMRY_PATCHES.json](FATHOMRY_PATCHES.json). The original-file hashes in
  UPSTREAM.json remain unchanged.
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
Explicit Stop must follow Start's return; reentrant Stop from a startup tuner or
plugin is not a supported managed profile. Automatic fatal cleanup waits for
native startup publication independently of an observer's canceled startup wait.

The extension joins native base workers and separately accounts for late poll
work, Local Activity function/converter tails, heartbeat batching, local retry
timers, fatal notification callbacks, Workflow coroutines and their asynchronous
destruction. Owned cache entries
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

The #134 maintenance keeps released v1.49.0; it does not adopt all of main or
main-only Workflow LocalVar. The exact merged upstream repair references are:

| Reference | Adapted behavior |
| --- | --- |
| [#2710](https://github.com/temporalio/sdk-go/commit/bd569a57668113c9158c9f22f5dde94162738479) | Default a copied LocalActivity RetryPolicy, not the caller's value. |
| [#2747](https://github.com/temporalio/sdk-go/commit/def71078bba0938270c073bdfe921e82864fb6e5) | Copy Schedule workflow actions and clone mTLS setup with a fresh certificate slice. |
| [#2694](https://github.com/temporalio/sdk-go/commit/8b8ac9edb6e62a3b7f86ffa08d9af4bbcc53b2a2) | Keep a stable cache generation, fence late inserts after release, and condition normal removals on the original workflow context. Native Stop and replay completion release their owner explicitly; managed release still follows full join. |
| [#2731](https://github.com/temporalio/sdk-go/commit/abb808c0a7e88a78c20845aeb9f1e287aeb6fd84) | Share a remote-poll stop flag, recheck after slot/rate waits, move fatal notification off the poller, retain the first cause across interruption, and join one concurrent Stop/plugin cleanup. |

The cache adaptation reuses existing cache handles and conditional deletion;
it does not import upstream's separate lease API or bulk-eviction metric change.
Unreachable-cache finalization remains a fallback, not a substitute for Stop.
Fatal handling additionally waits for startup before automatic Stop and retains
the notification in managed full-join accounting. LocalActivity polling remains
independent so accepted Workflow tasks can finish. Native Stop still does not
join every task tail or fatal callback; the opt-in full join supplies that contract.
Stop plugins must invoke their continuation, not recursively call their own Stop.

Copying preserves effective defaults, not incidental mutation that a Workflow
may have observed in earlier SDK code. Such application-dependent command
changes still require the application's own versioning and replay controls.
Maintenance verification includes positive historical LocalActivity and
serialization-context replay plus incompatible-history rejection; it does not
claim universal replay compatibility, a Schedule CAS guarantee, or new external
retrieval bounds. Original constructor, eager, decoder and stronger join repairs
remain selected.

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

`client.FathomryWithEncodedValueDecoderV1` supplies an opt-in process-side hook
at native Update, standalone Activity and Nexus result consumption. Cached values
retain the hook selected when their result was acquired, while each Get supplies
its current context. A fresh poll selects its own acquiring hook; it cannot
legitimize an expired interceptor alias from another call. Without this hook,
native Get, nil-output and polling/cache behavior remain unchanged.

The Internal integration binds that hook to the exact originating call and
retained Access (and callback task, when applicable). Cooperative custom encoded
wrappers map isolated copies of borrowed children into the current admitted
decode scope. Those copies are sealed and entered decodes joined before returning;
original aliases stay expired. No extra RPC, global registry, source permission,
Workflow command or new admission allowance is introduced.

The owner also authorized native lazy-decoder scopes for known error graphs and
Activity/Workflow/Nexus descriptions. `client.FathomryScopeErrorV1` preserves native
types, cause graphs, original Failure data and intentional error identity. Scope
copies retain bounded private origin ancestry for `errors.Is`, including repeated
scope/finalization copies. That comparison does not inspect arbitrary user
Is/Unwrap callbacks or select a semantic cause. Scope
owners are private runtime capabilities, never payloads or configuration. Foreign
owners can add restrictions, not replace an existing owner's guard; guard chains
are bounded and fail closed. Description copies share the native decode lock.
Original aliases cannot be retroactively revoked. Opaque custom error objects
remain caller-owned; custom errors with borrowed decoders must implement the
documented explicit decoder-scoping hook. This is not a sandbox for arbitrary Go
object graphs, converter callbacks or retained external references.

Native task finalization has a separate opt-in decoder boundary. Private child
scope owners associate callback operations with their task. Preparing a returned
error for finalization marks only that group's existing scopes in a native-shaped
copy; it does not rewrite unknown wrappers/joins or unscoped application errors.
The managed FailureConverter's synchronous conversion window replaces only those
marked guards, preserves foreign restrictions and serialization context, then
seals and joins entered decodes on return, panic or Goexit. A converter-retained
copy cannot decode afterward; original callback aliases never regain authority.
No fresh process operation/evidence admission is required to finish admitted work.

Custom lazy errors can implement `FathomryMapDecoderScopeV1` over an opaque
`FathomryDecoderScopeV1` token. They must return an isolated same-shape copy,
preserve all mapped guards, and call Decode around complete borrowed retrieval.
The mapper cannot be retained and scope tokens refuse serialization. The legacy
guard-only `FathomryScopeDecodersV1` hook retains ordinary-use compatibility but
cannot express owner-preserving native-finalization transfer. Arbitrary custom
code remains cooperative, not an automatically revocable or cancelable sandbox.

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
