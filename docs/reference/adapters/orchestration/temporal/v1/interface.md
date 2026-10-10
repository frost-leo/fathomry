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

# Temporal Adapter interface

[Documentation](../../../../../README.md) / Public Adapter reference

**Audience:** Go applications composing native Temporal technical capabilities.
**Status:** implemented candidate; local acceptance and service qualification are
tracked separately. This does not claim issue #134 completion or universal Server
compatibility.
**Package:** `github.com/frost-leo/fathomry/adapters/orchestration/temporal/v1`.

The [orchestration category contracts](../../v1/interface.md) own shared Budget,
Policy and attribution data. Existing Budget/Policy names are aliases; Temporal
attribution remains a provider-defined value with its original diagnostic/error
semantics. Native options, private limit derivation and lifecycle stay here.

## Prepare, own and retain

1. Call `Prepare(Settings, NativeOptions)`. It does not dial or invoke extension
   callbacks. `Settings()` returns a detached, deliberately sensitive effective
   configuration. `Validate`/`Recommend` select the data-only native profile.
2. Read `Prepared.Policy()`; construct an `adapters.Runtime` and separate
   `Inbox[Result]`, `Inbox[WorkerResult]`, `Inbox[TaskResult]` using its limits.
   These are independently owned by composition, never by a handler.
3. Call `Prepared.Open(lifetime, Dependencies)`. Native settings/runtime objects
   come from the frozen preparation, not a second interpretation of Dependencies.
   A non-nil Owner requires cleanup even when Open also returns an error.
4. Use `Owner.Client()`, or `Client.Borrow(lifetime)` for an independently
   revocable use. `WithID` creates only a correlation view of that same identity.
5. Drain required evidence independently of method return errors. `Retry` of a
   delivery retries reception only; it never resends an SDK operation.
6. Close retained Clients and stop Workers, then close the Owner. Closing a Client
   also requests and joins its Worker aliases. Ordinary borrowed peer Clients
   remain independent. Owner.Close seals every alias and Worker.

`Using(lifetime, resource.Ref[Handle], Budget, Dependencies)` returns a Binding.
`Binding.Retain(ctx)` explicitly selects and retains one generation. A later
Follow replacement affects only a later Retain. Handles, decoders, ordinary
Borrow peers and Workers retain the exact original generation. Each retained
Client needs Close or cancellation of its Binding lifetime.

Independent records include a source-construction SourceID and retained-alias
UseID, alongside selected name/namespace and binding generation. Same-name direct
sources are not conflated. WithID copies keep the same use identity. These are
technical correlation data, not metric labels or durable Workflow IDs.

`PrepareFromExisting` preserves native shared-connection namespace derivation.
Endpoint/security are inherited when omitted; transport/authentication cannot be
replaced. Construction is still admitted by Open. A derived Owner retains its
parent transport independently, so closing the parent can remain incomplete.
No implicit namespace discovery, namespace creation or remote cancellation occurs.

Cancellation limits waiting/cooperative work; it does not forcibly end arbitrary
callbacks. A canceled Close waiter leaves an owned cleanup attempt running.
`ShutdownComplete` means observed physical source release. Client.Closed is
completion of that use and its Workers, not physical release of a shared source.
Repeated Close waits for the same running attempt or explicitly retries a completed
incomplete attempt; it never acquires new work/evidence capacity.

## Native capability families

Workflow starts, signal/query/update, combined starts, run/update handles, history,
visibility, cancel/terminate/reset and execution options have direct routes.
QueryWorkflow/QueryWorkflowWithOptions remain eager conveniences.
QueryWorkflowValue/QueryWorkflowValueWithOptions return a retained QueryValue
preserving native presence and repeated decoding of the same response without
re-query. HasValue is local; Get and RawPayloads are separately admitted and
serialized under the original use/converter. RawPayloads distinguishes optional
native accessor support from nil data and returns a bounded detached copy. Native
Query external-storage retrieval still happens during initial response processing;
this API does not defer or repeat it.

An interceptor's custom `converter.EncodedValue` wrapper that retains a borrowed
`Next` result must implement the cooperative method
`FathomryMapEncodedValueV1(func(converter.EncodedValue) converter.EncodedValue)
converter.EncodedValue`. Return an isolated copy of the same concrete type, map
each borrowed encoded child synchronously, and do not retain the mapper. The
Adapter gives those copied children only the current admitted consumption scope;
it seals them and joins entered decodes before returning. Original aliases stay
expired. Preserve optional `converter.ValuesPayloads` behavior in the copied
wrapper. Opaque custom graphs are not inspected or silently unwrapped. Mapping is
bounded to depth 64 and 4,096 visited nodes, independently of FamilyLimit; it is
not a bound on arbitrary callback work or heap. Custom callbacks must still return
for cleanup to finish.

Reset returns its effective RequestID even after an unknown response without
mutating the request. A WithStart intention can be consumed only by its original
retained-use identity; common physical source or namespace is insufficient.

Standalone Activities retain native handle identity, descriptions, completion,
heartbeats and gated operational controls. Nexus retains native services, helpers,
callback clients, links and asynchronous return semantics. Current Worker Deployment
controls and the deprecated Deployment/build-ID/reachability/versioning-rule
conveniences retain their native option validation/conversion and exact grants.
Deprecated/experimental labels do not imply success on a Server that removed or
disabled the API.

Schedules preserve native calendar/timezone/overlap/catchup and update-callback
semantics. The selected SDK has no Schedule compare-and-swap guarantee. Walk
methods own synchronous visitors and pagination, including empty continuation
pages. History/visibility/archive absence and expiration remain native results.

Descriptions expose field-only `Metadata()` snapshots and separately admitted
lazy getters. Metadata containers are copied at publication and on each read;
they do not expose a native converter/retriever or source owner. Native protobuf
payloads and metadata are deliberately sensitive caller-owned data.
Query, description and error-details reads retain the originating target evidence. If
initial capture omitted an oversized target, later successful decoding preserves
`IdentityOmitted`; it does not turn the original intention into a confirmed target.

Native deterministic Workflow APIs, replay/tests/mocks and optional contrib remain
native packages, not another Fathomry Workflow DSL or runtime.
See [selected coverage](capabilities.md).

## Options, mutation and transport

Settings use explicit JSON/mapstructure keys; durations are integer nanoseconds.
Version zero selects format 1. Endpoint and namespace are required; no environment
proxy, logger, credentials or other ambient discovery is installed. Native
selected zero/default rules are resolved by Internal preparation, not copied by Adapter
policy. Settings contain no live authority.

NativeOptions preserves logger, metrics, interceptors/tracing, propagation, data/
failure converters, codecs through the converter, external storage, plugins,
credentials/headers, structured connection options and bounded transport hooks.
Typed nil is rejected. Container copying does not clone arbitrary callback objects,
private keys, custom pools, storage drivers or user goroutines. Such dependencies
are borrowed, must be concurrency-safe and must outlive actual source/Worker use.
Do not mutate input during Prepare or a borrowed argument during a call.

TLS snapshots copy the Config value, protocol/cipher/curve lists, certificate
structs and chain/SCT slice containers, supported-signature lists,
NameToCertificate map and certificate structs, and ECH-key structs. Encoded
certificate/OCSP/SCT/ECH/private-key bytes, key objects, parsed certificates and
trust pools remain borrowed and must not be mutated until actual shutdown.
Callbacks, session caches and I/O objects remain borrowed under their native
concurrent-use contract. Copying containers does not freeze arbitrary runtime
objects; declared source costs account for copied slots, not hidden native heap.

- Host:port, DNS (including explicit resolver authority), and passthrough retain
  their meanings. Explicit source-local ResolverBindings may add named schemes.
- ContextDialer can explicitly select a proxy route. Entered dial/resolver/I/O
  callbacks and transferred connections remain owned until actual completion.
  Each transport allows at most 32 active dial/returned-connection slots.
- Structured TLS/mTLS, authentication, Authority, keepalive, compression, wire
  bounds and capability timeout remain subject to native semantics. In particular,
  native ConnectionOptions.Authority applies only without TLS.
- UserAgent is explicitly bounded. Opaque DialOptions, global resolvers,
  ambient proxy discovery and unreviewed service-config/retry/ownership overrides
  are refused.
- Generated direct/callback services accept StaticMethod, non-nil Header/Trailer/
  Peer destinations, FailFast, and bounded send/receive sizes. Tighter size limits
  are retained. At most 32 options including generated StaticMethod are allowed.
  Other options refuse before transmission. Output containers are borrowed until
  call return and must not be shared concurrently.

`KnownRPCs` is an inventory, not authorization. Raw WorkflowService and
OperatorService require exact configured method grants and enforce source
namespace, transport and bounds. Raw administrative or worker-polling grants are
never implied by ordinary execution capabilities. No owning native Client or
transport escapes.

## Admission, evidence and costs

One source record precedes native dial/plugin construction. A Worker root precedes
registration/start/polling and retains preprocessing, conversion/finalization and
cache/callback tails through actual SDK join. Explicit operations and delayed
decodes reserve before native serialization/interception/retrieval/RPC work.

Activity/Local Activity/Nexus task records reserve before handler and inner
execution-interceptor entry, **not** before native input decoding, header
propagation or factory/Init. Native preprocessing belongs to Worker lifetime.
TaskResult is a user-callback record, not a full task-pipeline or remote-completion
certificate.

Callback and live same-use visitor/converter calls use bounded children of the
existing family, not another root allowance. Expired/cross-use contexts are refused.
Cleanup/heartbeat flush/finalization use the existing Worker reservation.
Native replay-aware observer callbacks retain their native process contract; they
are not Workflow command-generating calls or mandatory per-event receipts.

Public and Internal evidence capacities are both enforced. The exact claimed
Internal record transfers synchronously into an already-reserved public record;
there is no forwarding pump or unnamed staging allowance. Separate Worker/task/
operation receivers prevent one long-lived Worker record from blocking finite
operation evidence. Ignoring a result never acknowledges its record.

FamilyLimit (default 64, including the root) bounds retained native/public nodes.
The native process root covers FamilyLimit times Internal's per-call wire
envelope; public policy adds per-node metadata/evidence costs. WorkerWorkBytes
defaults to this minimum and can explicitly reserve a larger native pipeline
envelope. WorkerSpec.Bytes must fit the selected range. MaxUses bounds retained
aliases; InnerEvidenceCapacity bounds each native evidence receiver.

FamilyLimit does not bound native preprocessing concurrency: input decoding and
retrieval can precede user-handler admission under the native slot/poller profile.
WorkerWorkBytes remains a caller-declared envelope for that entire profile, not
a computed hard memory bound from the number of callback records. Native tuner
and poller options are not silently clamped to manufacture such a guarantee.

These are declared working, copying and custody budgets, **not** hard RSS, native
heap, remote rate or arbitrary-user-code guarantees. Composing overlapping sources
requires summing source/resident/root capacities, not reusing a one-source policy.
No runtime is silently resized. Incompatible combinations with public runtime
ceilings are refused.

## Errors, privacy and native semantics

Public codes and English/Chinese resources are offline under facility `0x1C0`.
Ordinary formatting/localization reports safe technical context and retains
native causes for deliberate inspection. `NativeError(err)` accepts only a
direct provider-owned occurrence; it never searches arbitrary joins/wrappers.
`Result.NativeError()` preserves the independent captured return.

Passing the public error itself to a native FailureConverter is not transparent.
Use the explicit captured native error when that is the intended semantic return.
Native SDK handler callbacks retain their original errors/sentinels; the Adapter
does not guess a primary cause or rewrite arbitrary user errors. Custom lazy
errors must cooperate with the selected SDK's decoder-scope contract. The legacy
`FathomryScopeDecodersV1` guard-only hook remains supported for ordinary use;
`FathomryMapDecoderScopeV1` explicitly copies an opaque decoder scope and also
supports native finalization. Unknown user wrappers/joins are not rewritten.

Task-returned known callback errors are copied for native FailureConverter use
only. Their marked scopes work during the actual synchronous conversion window,
then expire; active decodes keep Worker cleanup pending until they finish. The
original aliases and callback Client never regain permission. Finalization uses
the existing Worker reservation and preserves custom serialization contexts.

Activity ErrResultPending and Nexus asynchronous returns end callback-client
authority. Later completion requires independently owned process-side authority.
Native observers may block, leak their own output or start application-owned work;
they are neither forcibly cancellable nor automatically sanitized. Adapter
diagnostics remain safe; no implicit concrete logging/OTel bridge is installed.

## Independent consumers and verification

The selected SDK is v1.49.0 plus the maintained
[compatibility replacement](../../../../../../third_party/temporal-sdk/FATHOMRY.md).
A consuming module must explicitly select that replacement; dependency-level
replace directives are ignored by Go. Independent fixtures verify their own
module graph and build information. Local development and published nested-module
distribution are different acceptance gates.

Core capability files have matching local public-boundary tests:
`workflow_test.go`, `activity_test.go`, `schedule_test.go`, `history_test.go`,
`nexus_test.go`, `deployment_test.go` and `worker_test.go`. They call the public
facade through the real SDK and controlled gRPC peers, asserting conversions,
permissions, failure semantics and retained-use boundaries rather than merely
method presence. Client, preparation, policy, diagnostics, description, query,
source and lifetime tests cover the shared contracts. Policy refuses both active
and queued envelopes beyond the Runtime ceiling. A prepared shared source accepts
same-use correlation facades, but not a different Borrow merely sharing transport.

Independent-module composition is in `integration_test.go`; actual opted-in
Temporal service scenarios remain in `service_integration_test.go`.
Loopback/testsuites and absent/skipped service fixtures are never real-service
qualification. Live tests require `FATHOMRY_TEMPORAL_TEST_CONFIG` and separately
authorized isolated operations/cleanup; no test auto-deploys or upgrades a Server.
