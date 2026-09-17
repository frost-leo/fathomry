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

# Temporal SDK interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** framework composition and integration maintainers.
**Status:** implemented, profile-qualified internal integration.
**Package:** `github.com/frost-leo/fathomry/internal/orchestration/temporal/v1`.

## Responsibilities

The package owns native Temporal Clients and managed Workers, and preserves
native Workflow/Activity definitions and Nexus services. It reuses resource preparation,
authoritative admission and borrowing, invocation evidence, fault inspection and
compatibility profiles. It is process-side code, never deterministic Workflow code.

The file layout follows the other internal SDK integrations: configuration and
errors are separate, while native operations and their borrowed callback views
live together under the capability that owns them. Native deterministic primitives
are not copied into a parallel Workflow API.

| Responsibility | Implementation | Tests |
| --- | --- | --- |
| Configuration, failures, acquisition and transport | `options.go`, `errors.go`, `connection.go`, `transport.go` | Matching `*_test.go`; private lifecycle controls in `*_internal_test.go` |
| Workflow, Activity, Nexus, Schedule, Deployment and History | `workflow.go`, `activity.go`, `nexus.go`, `schedule.go`, `deployment.go`, `history.go` | Matching capability `*_test.go` |
| Worker lifecycle | `worker.go` | `worker_test.go`, `worker_internal_test.go` |
| Native extension boundaries | `plugin.go`, `interceptor.go` | `plugin_test.go`, `interceptor_test.go`, `interceptor_internal_test.go` |
| Shared invocation and generated RPC composition | `invocation.go`, `rpc.go` | `integration_test.go` |
| Injected logging, metrics, tracing and serialization | Native options in `options.go`; no concrete upper-layer adapter | `observability_test.go`, `serialization_test.go` |

`service_test.go` contains the common opted-in service fixture and RPC profile.
`service_<capability>_test.go` contains real-service acceptance for that capability,
including replay and owned cleanup. They are separate from the controlled loopback
composition in `integration_test.go`. Missing `FATHOMRY_TEMPORAL_TEST_CONFIG` skips
service tests; a normal package test pass is not a real-service pass. UI fixtures
and namespace-gated Nexus profiles retain their additional explicit prerequisites.

Core execution and native extension entrypoints are implemented. Qualification is
profile-specific: generated RPC reachability does not certify every namespace,
administrative operation, optional contrib backend or deployment combination.
Reviewed local qualification does not replace human review or qualify unselected
service/deployment combinations.

## Client acquisition and shared ownership

`Select` and `SelectWithRuntime` eagerly discover service capabilities by default.
Endpoints accept explicit `host:port`, `dns:///host:port` and
`passthrough:///host:port` targets. DNS targets may specify a resolver authority;
its omitted port uses native port 53, while the Temporal service port is required.
Native DNS resolution and round-robin behavior remain unchanged. Custom resolver
schemes, process-global resolver replacement and opaque gRPC dial-option overrides
are not part of this owned transport profile.
`OptionsV1.Lazy` selects native lazy construction without a service RPC. Capturing
the private transport does not invoke it or invent readiness; the first admitted
native operation discovers capabilities when required by the SDK. `Profile` keeps
the Server version unknown until a successful discovery response is observed.

`SelectFromExisting(options, runtime, parent, parentBytes, layers...)` derives a
native Client over a bound parent's existing transport, including a different
explicit namespace. It holds one parent resource lease and the declared byte
envelope until derived-source cleanup completes. Derived binding limits must fit
that envelope. Namespace and per-source admission remain distinct; the native
transport, authentication and connection-level RPC instrumentation are inherited.
This is not a new account-wide rate limiter or a second connection pool.

Native derivation is always eager, including from a lazy parent. Both direct and
resolved layered `Lazy` settings are rejected for derivation. Transport overrides
are not silently accepted and ignored. Closing the parent reports outstanding
ownership while the derived source remains live; close derived sources first,
then complete parent cleanup. Managed Workers still own their separate polling
connections rather than borrowing another Worker's shutdown authority.

## Native logging boundary

`SelectWithRuntime(options, RuntimeOptions{Logger: logger}, layers...)` accepts the
native `go.temporal.io/sdk/log.Logger`. The same borrowed logger is supplied to the
Client and its managed Workers. Native `WithLogger` and `WithSkipCallers` behavior,
fields, severities and error identities are preserved without conversion. Nil
disables logging; typed nil is rejected. `Select` selects that no-logger mode and
does not implicitly enable the SDK's process output.

This Provider imports no Fathomry logging or telemetry Provider. Concrete Zap,
zerolog and OTel adapters, filtering, queues, formatting, privacy, export and
logging-failure reporting belong above this boundary. It neither closes nor
flushes the logger. Composition must declare logger dependencies before Temporal,
using `resource.Borrow` when their owner is another assembly, and release those
dependencies only after Temporal use ends. The compatibility profile records logger
presence only; it does not attest an adapter's implementation or delivery behavior.

Use native `workflow.GetLogger`, `activity.GetLogger` and
`temporalnexus.GetLogger`. Workflow replay suppression remains in the SDK; its
explicit replay-logging option is not overridden. No process invocation/resource
operation is inserted into application Workflow definitions for logging.

The native logger interface has no context/error return. A blocking logger can
delay execution and shutdown, and a stop-wait timeout does not prove its callback
has returned. The upper adapter must choose and bound any asynchronous delivery;
its background work is not joined by `Worker.Stop`. Optional logs do not replace
the independent invocation/Worker/task evidence. Native messages and field values
reach the explicit logger unchanged and can contain sensitive data; there is no
automatic redaction guarantee.

## Native metrics, tracing and context propagation

`RuntimeOptions.MetricsHandler` accepts `client.MetricsHandler` unchanged. Native
counter, gauge, timer, tag and replay behavior remains in the SDK. Nil selects its
no-op implementation. The upper adapter owns backend selection, cardinality,
aggregation, export, buffering and termination; this package adds no telemetry
Provider dependency or exporter.

`RuntimeOptions.Interceptors` accepts native client interceptors, including
`interceptor.NewTracingInterceptor` and native tracing adapters supplied by
composition. Combined client/worker interceptors also apply to managed Workers.
Their order relative to each other and Worker-specific interceptors is preserved;
the mandatory task guard remains outside that user chain. Do not register the same
interceptor in both lists. Native tracing errors are not suppressed or translated
into successful business execution.

Client interception is installed only on the admitted source client. Worker
polling uses its private connection; Worker halves of combined interceptors are
installed explicitly inside task admission. Short-circuiting an Activity still
retains a task record and callback lifetime. Activity interceptor factories receive
restricted client access even before native Init, without losing native metadata;
an inbound continuation does not expose the concrete SDK's outbound client getter.
Retained process continuations cannot invoke source RPCs after their admitted call
ends, including with a cancellation-stripped context.

`RuntimeOptions.ContextPropagators` preserves the native process/Workflow methods
and selected payload headers. Application context values reach native propagation
and tracing; stale internal authority markers and caller outgoing gRPC metadata do
not carry into a new admitted call. Native SDK and explicitly configured extension
headers then follow native construction. Temporal payload propagators and Nexus
HTTP tracing headers are distinct native mechanisms, not one invented format.

Interceptor and propagator slices are copied, each limited to 32 non-nil entries;
referenced objects remain borrowed and must be concurrency-safe. Workflow-side
methods must obey native determinism. Composition declares all extension resource
dependencies before Temporal, and owns any independent background work. These are
trusted native extensions, not a sandbox for arbitrary application code.

Client interceptor factories have no native error return. A panic or nil result
is retained as an initialization error while enough construction finishes to
recover cleanup ownership. It is not silently treated as a usable skipped
interceptor. Unscoped setup permits capability discovery, not arbitrary mutations.
A captured private Worker connection is closed even if a metrics callback panics
before Client construction returns. Typed nil is rejected before construction.

## Workflow interaction and execution chains

`ExecuteWorkflow`, Signal-with-Start and Update-with-Start use native start options.
For Update-with-Start, create `NewWithStartWorkflowOperation`, then pass it to
`UpdateWithStartWorkflow` with native Update options. Its `Get` independently
observes the start; an Update wait failure does not erase an already acknowledged
start. The intention is source-bound and single-use after native entry. Evidence
admission refusal does not consume it. Options and arguments are borrowed until
the combined call returns. A new intention is not automatically a safe retry:
native IDs and conflict/reuse policies still determine its meaning.

`WorkflowRun` getters are local identity snapshots, never hidden RPCs. A detached
empty RunID remains unknown until an admitted result observation resolves it.
Waits on the same handle are serialized because native execution-chain following
mutates the handle. The serialization wait remains inside resource admission and
the execution deadline; it is not reported as SDK execution time. Cancellation
while waiting for the handle does not enter the native SDK. Different handles
can observe concurrently within source limits. Result destinations are caller-owned
and must not be concurrently shared or mutated. Do not copy handles.

`GetWithOptions` preserves native strict-run versus retry/Continue-As-New following.
After a result observation, RunID is the latest observed run, while the native
first-execution ID remains separate and may be unknown on detached handles.
`WorkflowUpdate.Get` does not cancel the remote Update when its observer cancels.
Workflow/Update IDs remain available in evidence after response loss, allowing an
explicit `GetWorkflowUpdateHandle` observation without resubmitting the mutation.
Missing IDs are assigned before invocation. Evidence captures bounded target IDs
after native interceptor and traffic-controller changes, before the native
transport invocation; successful replies supplement the observed RunID. Stale
wrapper defaults cannot replace that target. This is the latest observed native
attempt, not proof of remote acceptance or a multi-target effects ledger for custom
extensions. Oversized observed identities retain the documented intention-only
fallback with `IdentityOmitted` and `ErrLimit`.

`QueryWorkflowWithOptions` preserves native headers and state-rejection conditions.
It decodes successful results inside admission and returns a caller-owned native
rejection separately. `DescribeWorkflowExecution` returns native metadata without
requiring an unrestricted service view.

## Activity completion, heartbeats and native controls

`CompleteActivity` accepts native token options; `CompleteActivityByID` targets a
Workflow Activity, while `CompleteActivityByActivityID` targets a Standalone
Activity. Native options and success/failure/cancellation conversion are retained.
Namespace fields must match the selected source, including optional serialization
hints, and are checked before conversion. Required native by-ID namespace fields
are not silently defaulted. Optional serialization hints are not asserted as
observed execution identities. Tokens and heartbeat payloads are not retained in
independent evidence.

Task tokens are opaque bearer capabilities. Request namespace checks do not decode
or independently authenticate their embedded identity. The selected Server source
defaults token/request namespace enforcement on, but this is a server policy, not
a client-side token verifier. No qualification is claimed for disabling that policy.
By-ID completion has no attempt parameter; it does not provide attempt fencing.
Prefer an actual attempt token when stale completion must be rejected. Empty run
IDs retain native latest-execution targeting and require deliberate caller policy.

`RecordActivityHeartbeat` and `RecordActivityHeartbeatByID` preserve native SDK
behavior. `HeartbeatAcknowledged` and cancellation/pause/reset flags record the
response independently of a returned error. SDK v1.49.0's token heartbeat converts
all three directives to native errors; its by-ID helper only converts cancellation.
This package does not hide that difference. `CompletionAcknowledged` means an actual
completion/failure/cancellation response, not merely a nil SDK return: passing
`activity.ErrResultPending` can cause no completion request at all. Neither ACK
attests external effects or proves that a cancellation target has stopped.

Native Activity/Nexus callback Clients route these completion/heartbeat families
through nested admission before conversion. Saturated evidence or expired scope
does not enter a marshaler. An already admitted conversion/RPC retains its parent
dependency even if the user callback returns meanwhile; Worker Stop waits for
retained use as well as native termination. Native error identity is returned to
SDK callers while independent evidence remains available. Panic and Goexit paths
also release their call ownership and retain failure evidence; panics are rethrown.

Standalone handles serialize native result-cache access without serializing Cancel
or Terminate behind a result wait. `Describe` returns native metadata fields and
presence methods through `ActivityDescription`; decoder getters are separately
admitted and serialized. The native description getters use background contexts
internally: observation cancellation cannot forcibly interrupt their conversion or
payload visitor, and the lease remains held until return. Raw metadata/payloads
are caller-owned; do not mutate/read them concurrently with decoding. This is not
yet an external-payload/codec qualification.

`CountActivities` preserves native visibility aggregation semantics. Experimental
`Pause`, `Unpause`, `UpdateOptions` and `RestoreOriginalOptions` require their exact
configured operational RPC grants. Native clear/set/no-change, jitter and restore
semantics remain unchanged; a grant does not enable a namespace feature, and
Unimplemented remains an error. No operational feature flag is changed implicitly.
High-level Activity enumeration and remaining operational profiles are still open.

## Nexus handlers and backed operations

Register native Nexus services in `WorkerSpec.NexusServices`; Workflow-side
clients, futures and cancellation remain native deterministic SDK operations.
The mandatory handler boundary owns each Start/Cancel callback, not the entire
remote operation. `TaskResult.AsyncCompletion` identifies a native asynchronous
start result; it does not mean the backend or its completion callback finished.
Service/operation/request IDs, callback presence and input link count are retained
within the bounded task identity envelope. Callback URLs, headers, task tokens,
operation tokens and payloads are not copied into that record.

Workflow-, Query-, Activity- and Update-backed native helpers can use the scoped
`temporalnexus.GetClient`. Workflow/Activity starts, Update, Query and cancellation
reserve nested evidence before interception/conversion, as do the completion and
heartbeat families above. A saturated evidence inbox or an expired callback scope
refuses these methods before conversion. Native returned types/errors are retained:
the SDK distinguishes a completed Update's synchronous Nexus result by its concrete
handle type. Native returned lazy values and description-created native errors
retain their ownership guards. Arbitrary custom Go objects are not sandboxed.

SDK v1.49.0's generic Activity/Update-backed helpers require server callback support.
For Server 1.32.0, namespace settings `activity.enableCallbacks` and
`history.enableUpdateCallbacks` default off. This package does not enable them.
Unsupported service responses remain errors. The Go 1.27 generic helpers' service
qualification used an explicitly enabled test namespace; it does not imply that
experimental standalone Nexus Client operations are available.

## Schedules and native time semantics

`CreateSchedule`, `GetSchedule` and `Schedule` support native creation, description,
update, deletion, trigger, backfill, pause and unpause. Options stay native, including
action metadata/payloads, intervals, calendars, cron, timezone, jitter, catchup and
overlap. Temporal Server evaluates time rules; this package installs no local timer
or cron evaluator. Zero catchup leaves the native server default unset on the wire.
`GetSchedule` and `GetID` perform no I/O and do not prove that a schedule exists.

Each operation owns its native conversions and callbacks until they return.
Describe results are caller-owned data; action arguments retain native payload
representation. `Update` preserves `temporal.ErrSkipScheduleUpdate` and callback
errors. Cancellation cannot forcibly terminate `DoUpdate`, and dependencies remain
owned while it is running. Callers must not mutate borrowed options concurrently.

Schedule mutation `Execution.Accepted` requires an observed native mutation response;
skipping an update does not set it. An ACK still does **not** attest that the scheduler
applied the state. SDK v1.49.0 sends no Schedule conflict token, and two callbacks can
read the same state and overwrite each other's changes. This package supplies neither
compare-and-swap nor a cross-client lock. Read back material changes; do not blindly
repeat a mutation after an uncertain response. Overlap policy controls this schedule,
not a shared business-account quota. `RunningWorkflows` is not a complete census of
all overlapping actions; use the actual execution identities/results.

`WalkSchedules` owns native lazy pagination and synchronous visitor callbacks in one
bounded invocation. No iterator carrying an expired call context escapes. Each RPC
keeps the configured wire bounds; retained entries belong to the visitor, not an
unbounded internal collection. Empty pages with a continuation token are not treated
as exhaustion, despite the selected SDK iterator's early false result. Visitor errors
stop the walk and remain in evidence. A blocked visitor delays release even after
cancellation. Visibility is eventually consistent and cannot prove absence.

## Capabilities and call sequence

1. Supply explicit `OptionsV1`, including endpoint, namespace and exact RPC grants.
   `KnownRPCs` inventories the selected generated service methods; it does not
   certify that a server implements them.
2. Call `Select`, attach `resource.WithLimits`, and assemble the named source.
   Dialing observes GetSystemInfo only; it does not establish execution readiness.
3. Bind native operations with `BindExecutions` and an independent
   `invocation.Inbox[Execution]`. Full native start/control options are passed to
   the selected SDK; result decoding stays inside the admitted operation.
4. `StartWorker` takes explicit native registrations/options, a cancellable
   lifetime context, startup observation context, and independent Worker/Task
   Inboxes. It returns its owned handle even when startup fails or waiting ends.
   Call `Worker.Stop` before closing the assembly; a timed-out stop retains all
   live responsibility. A pending Assembly.Close does not revoke Stop authority.
5. Receive and explicitly release evidence independently of direct error handling.
   Close the assembly after outstanding operations and borrowers finish.

For explicitly authorized low-level operations, `Bind` supplies generated service
views and a separate `Inbox[RPCResult]`. Ordinary `Executions` calls do not require
copying their internal RPC method list into configuration. Advanced/raw and
callback operational RPCs require exact explicit grants.

There are no default RPC grants. An explicit grant covers the entire native
method, including administrative or destructive effects. Namespace-bearing
requests must match the selected namespace. This is not a tenant authorization
platform: methods without a top-level namespace may be cluster-wide, and native
nested targets retain their protocol meaning. Server authentication and privileges
remain necessary.

## Ownership, bounds and cancellation

Requests are borrowed until their synchronous call returns and must not be
mutated concurrently. Each generated response becomes caller-owned; the evidence
Inbox retains only immutable technical facts, not the response or its payloads.
Views contain no exported connection or source-release authority. Retained views
cannot reopen a closed resource. Borrowing aliases retain original identity,
admission limits and transport ownership.

One process/build may select multiple Namespace sources, including sources using
the same service endpoint. Namespace is immutable per source, never process-global.
The named-client isolation test verifies separate results/evidence and refuses
retargeting. The native lifecycle suite also runs identical Workflow and task-queue
names in two Namespaces over a shared connection, then stops one without disrupting
the other. Managed Workers inherit their selected Namespace. Their private polling
connections are separately owned; no shared-connection Worker profile, automatic
account-wide quota or one-Namespace-per-business-domain policy is implied.

The Worker holds one source lease for its lifetime; tasks and their controlled
Client calls borrow inside that reservation. `MaxHandlers` bounds actual
Activity/Local Activity/Nexus callback entries, including timed-out Local Activity
tails. Native tuner/poller options retain their distinct native meanings. Handler
admission occurs after native task receipt; rejection is not a claim that the
server never dispatched work. `TaskResult.HandlerReturned` is not native response
completion. The Worker join covers the later native conversion/response work.

Activity/Nexus GetClient returns a non-owning, callback-scoped native interface.
Close is explicitly refused, raw operational routes require grants, and callback
methods refuse expired scope before conversion. The facade explicitly implements
the selected native Client interface instead of inheriting unchecked methods.
Schedule/Deployment/Activity/Nexus handles and iterators preserve callback ownership.
No source client is handed to
application code. Runtime definitions, services and interceptors are borrowed
under their native concurrency contracts; arbitrary application-created goroutines
remain application-owned.

Native Worker plugins are supplied explicitly in `WorkerSpec.Options.Plugins`
(at most 32). Configuration receives detached option containers; their referenced
extension objects remain borrowed. Plugins cannot replace the plugin list or
remove the final mandatory lifecycle/task guards. Registry hooks retain their
native options and order, including dynamic definitions. Each plugin's registration
view expires when its Start callback returns; entered registration calls are joined.
It exposes registration only, not native Worker ownership. Registration-call
bounds include bootstrap and plugin calls (256 Workflow/Activity and 32 Nexus
service registrations); these are not measured heap limits.

Start/Stop continuations are single-use and callback-scoped. Omitted Start does
not silently start a Worker. Stop omission, panic or Goexit records a failure and
still executes the remaining native cleanup without new evidence admission.
Handled registration/continuation errors remain in the Worker outcome. Native
startup success remains observable even if plugin code subsequently fails.
Managed Stop's continuation returns only after native Stop **and the full native
join**, so plugin dependency teardown belongs after `next`. Plugins must stop
their own producers before `next` if those producers would prevent native work
from ending. Arbitrary plugin-created goroutines remain plugin-owned; a blocked
plugin can keep shutdown incomplete. Plugins own rollback of resources they
allocate before native Worker construction succeeds.

`BackgroundActivityContext` preserves configured values, cancellation and the
earlier configured/Worker-lifetime deadline. Worker-lifetime cancellation also
cancels the combined context. The Provider owns and joins its cancellation
callback; it does not cancel the borrowed parent context.

`RuntimeOptions.Plugins` accepts native Client plugins, including combined
Client/Worker plugins. Configure receives detached option containers, not the
private transport or protected interceptor chain. NewClient continuations are
single-use and expire on return; entered asynchronous continuations are joined.
Post-dial plugin failures retain the actual Client for rollback instead of losing
its ownership. Replacing the forwarded context does not remove the construction
owner or its cancellation bound. Combined Worker plugins run once per Worker,
before Worker-specific plugins; creating the polling Client does not rerun Client
plugin initialization. `FromExisting` supplies a restricted non-owning view, not
authority to close or mutate the parent during construction.

Values returned to a Client interceptor's `next` caller are borrowed for that
invocation. Entered decoding is joined, and retained aliases cannot reopen a
completed invocation. Private same-call transfers preserve SDK cache reuse and
completed Update classification without reactivating old aliases. Custom objects
that retain borrowed values must obey that lifetime; arbitrary private wrapper
graphs cannot be automatically detached or promoted.

Wire limits apply to uncompressed protobuf request and response sizes. Validation
also refuses input cycles or nesting deeper than 64 and bounds traversal work.
The resource byte reservation covers the configured request/response envelope
plus 16 KiB of metadata. It is not an allocator, garbage-collection, user-result
retention, process RSS or distributed quota limit. Caller-owned input objects and
arbitrary native error graphs are not charged as an exact byte count.

Evidence count and declared bytes are reserved before admission. Saturation refuses
work without invoking transport; completion never waits for evidence reception.
The RPC deadline is separate from admission. Cancellation ends local observation,
not the remote Workflow, accepted request or external mutation. No business retry
is added. Observed interceptor entries are lower bounds, not exact wire attempts.
Execution evidence limits each identity to 1,024 bytes and their combined total to
3,072 bytes within its 4,096-byte declared envelope. If a native observation exceeds
that limit, `ErrLimit` and `IdentityOmitted` retain the original intention's IDs
instead; other acknowledged facts are not discarded. The caller-owned native
response/handle is not silently truncated.

Per-call gRPC options may request metadata/peer observations, wait-for-ready, or
smaller message caps. Codec, credential, authority and callback overrides are
refused because they would bypass the selected transport/ownership boundary.
Source configuration supports explicit plaintext or verified TLS, optional mTLS,
static or native runtime credentials, explicit header providers and native
connection options. Opaque dial-chain overrides are refused. Caller/interceptor
gRPC metadata is stripped; the configured header provider cannot replace the
selected namespace. Native traffic-controller request mutations are revalidated.
No proxy, environment configuration, plugin or secret file is discovered implicitly.

TLS ownership covers the complete native handshake and subsequent TLS I/O, including
opaque root-certificate constraints and post-handshake callbacks. Canceling a
handshake closes the raw connection but does not certify callback termination;
release joins it. Native TLS/ALPN/auth-info behavior remains delegated to gRPC.
Credential freezing occurs after SDK mTLS injection, preserving both explicit
certificates and native mTLS credentials. Callback and certificate objects remain
borrowed and must not be mutated concurrently. TLS/service qualification remains
specific to the tested trust/authentication profile.

## Results and failure boundaries

`RPCResult.Invoked` means local transport invocation, not server acceptance.
`Acknowledged` means a successful RPC response, not completed execution, applied
Schedule state, committed external effects or durable framework recording.
`ResponseBytesKnown` distinguishes measured empty responses from unmeasured
response bytes. Native causes remain inspectable with `errors.Is/As`; ordinary
Provider-fault formatting does not expose native messages or raw payloads. This
does not sanitize messages deliberately sent to an explicitly supplied SDK logger.

An SDK-decoded `serviceerror.Unimplemented` remains an error. Protocol presence,
GetSystemInfo success and a server version never turn it into an empty success.
The compatibility profile records actual selected settings and the observed server
version, without claiming namespace capability or production qualification.

`Execution.Accepted` means a successful native request response, not acceptance by
an application Update validator. `UpdateStage` is the observed native protocol
stage: completed can include rejection. `StartAccepted` independently preserves a
combined-start acknowledgement. `ResultObtained` requires successful result
observation, including a caller choosing a nil result destination; these facts
never attest external business effects. Native service cancellation can precede
local context cancellation and is not rewritten as an invented local context error.

## Executable evidence and remaining work

`integration_test.go` exercises actual SDK/gRPC loopback composition, shared alias
saturation, independent evidence after handled errors, response-loss uncertainty,
cancellation, cyclic/oversized messages, option escapes and retained views.
The selected graph is Go 1.27.0, SDK v1.49.0, API v1.63.5, Nexus v0.7.0,
gRPC v1.83.2 and protobuf v1.36.12.

`workflow_test.go` and `worker_test.go` add native-handle lifetime, partial
registration cleanup, handled callback-client misuse, retained client expiry and
stop-timeout/dependency retention controls. Native fatal-poll controls exercise
normal, blocked, panicking and Goexit callbacks without changing the SDK's two-
minute fatal-error grace; they run concurrently and skip only with `-short`.
The blocked callback prevents early dependency release, and its final joined
outcome independently retains the native failure and callback cleanup error.
`service_client_test.go` uses the
formal Client/Worker path against the authorized Server 1.32.0: a Workflow executes
a remote Activity, Local Activity and synchronous Nexus operation; a Standalone
Activity completes separately. Activity/Nexus native GetClient calls remain
controlled, task evidence is independent, and the generated Workflow history
replays with the same registered definition. Test-owned execution/endpoint cleanup
is explicit. This is one actual chain, not all-family service acceptance.

`observability_test.go` checks unchanged native fields/error identity, optional logger
methods, typed-nil/privacy behavior and a blocked logger callback. Stop cannot
claim completion or release the borrowed logging resource while that callback is
live. The actual execution-chain service test observes logs from all five native
execution paths and verifies both default replay suppression and explicit replay
logging. These checks use a test-native logger, not a concrete upper-layer adapter.

`interceptor_test.go` and `connection_internal_test.go` add independent header/transport
controls, expired continuations, short-circuit interception, pre-Init client
refusals, propagation/tracing failures and partial-construction connection cleanup.
The real execution chain checks all five paths' counter/gauge/timer values and
native tracing parent/finish behavior. `service_observability_test.go` stops and
joins one Worker, then starts another for the same running Workflow: two native
Workflow trace entries prove reentry while its initial metric is emitted once,
context headers survive replay and native span idempotency keys remain stable.
The ordinary SDK replayer uses a no-op metrics handler and is not used as proof
of that live metric suppression. No backend delivery or global quota is inferred.

`activity_test.go` cover heartbeat directives,
native routing/error identity, unknown completion acceptance, namespace rejection
before conversion, cached-handle/decoder lifetime, operational grants and option
masks, callback evidence saturation, retained conversion tails and abrupt exits.
`service_activity_test.go` verifies actual asynchronous completion by token and IDs,
heartbeat readback/cancellation, canceled observation without remote cancellation,
retry with rejection of the previous token, native attempt timeout and Local
Activity retry. Workflow and Local Activity histories replay; handled attempt
failures remain in task evidence. Exact test-owned execution deletion is followed
by bounded absence checks. Experimental operator service profiles remain separate.

`workflow_test.go` adds independent response-loss, partial combined-start, Update
failure, single-use/admission and native validation/Unimplemented controls.
`service_workflow_test.go` verifies combined starts, Update deduplication and
validator rejection, continued remote work after observer cancellation, native
Query rejection, child Workflows, timers, SideEffect/GetVersion and Continue-As-New
against Server 1.32.0. Initial, successor and child histories replay with their
original execution identities; the same successor history rejects an incompatible
timer-before-child definition as nondeterministic. The native test environment
separately checks the child/marker result against an independent expected value.
Test-owned execution deletion is followed by bounded absence observation, not
inferred from the delete acknowledgement. These fresh synthetic histories do not
qualify migration of historical business payloads or command versions.

`nexus_test.go` verifies handler lifetime, nested pre-conversion admission, evidence
saturation, scope expiry and privacy. `service_nexus_test.go` checks native async
callbacks, Continue-As-New, forward/back links, cancellation, retry after backend
acceptance, Query backing and both synchronous/asynchronous Update branches. Its
Activity/Update helper test requires the explicit callback-enabled service profile.
Three Workflow-backed and five generic-helper histories replay.

`schedule_test.go` checks native time/options fidelity, skipped/failed callbacks,
canceled callback ownership, pagination and unknown update effects. An empty-page
negative control demonstrated premature native iterator exhaustion; the controlled
walk continues using the actual protocol token. `service_schedule_test.go` verifies
the lifecycle and two action histories, independently reproduces lost concurrent
updates, and compares actual Server DST previews with fixed UTC expectations for
the spring gap and fall fold. Owned deletions are followed by absence observations.

The separately maintained [native lifecycle extension](../../../../../../third_party/temporal-sdk/FATHOMRY.md)
supports managed Worker joins and has its own native controls. The selected
Server 1.32.0 / UI 2.54.1 browser profile verifies Workflow identity, history,
metadata, Query results and codec rendering, with an encoded-payload negative
control. It does not qualify external-storage UI, arbitrary UI authorization or
container security. Historical business migration, unselected backends and a
complete cross-deployment service matrix are not claimed. Local implementation
and machine review do not constitute human approval, merge or issue completion.
