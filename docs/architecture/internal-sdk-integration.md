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

# Internal Architecture and SDK Integration Standard

**Status: architecture and integration standard; runtime not implemented.**
Scope: [Fathomry Issue #3](https://github.com/frost-leo/fathomry/issues/3).

Internal must be a reusable, constrained, and verifiable technical foundation,
not a collection of unrelated SDK wrappers and not a universal client that hides
incompatible SDK behavior. This standard establishes the responsibilities,
invariants, and evidence obligations for that foundation. It is self-contained;
private requirements discussions and local SDK snapshots are supporting material,
not dependencies of the product or prerequisites for reading this document.

“Must” defines an obligation for subsequent implementations, not a claim that a
runtime guarantee already exists. The architectural direction is settled here;
the specific mechanisms still requiring implementation decisions are listed in
S12. Earlier exploratory package names, APIs, and algorithms are not adopted.
Later integrations identify this document by its repository revision and applicable
section IDs, separately from any framework, configuration, or wire-format version.

Issue #3 does not deliver a runtime, public API implementation, SDK integration,
dependency selection, or tested service compatibility. Function signatures,
algorithms, data structures, and a fixed package/file tree are not prescribed.

## S01 — Outer roles, public paths, and dependencies

The role names below describe responsibilities, not mandatory Go packages.

| Role | Owns | Does not own |
| --- | --- | --- |
| Framework | Public capability contracts; execution identity and attribution rules; orchestration and control; interpretation of business results; reliable evidence recording and Item/Run disposition | Invented service guarantees or the mechanics of every SDK |
| Adapter | Implements a public capability using internal technical capabilities; enforces its public-operation constraints; preserves source and execution attribution; translates results and hands required evidence to the framework | An independent terminal-state policy, resource ownership obtained merely by borrowing, or a second error system |
| Internal | Process-local technical capabilities and reusable mechanisms for configuration preparation, ownership, controlled operations, technical evidence, safe diagnostics, and version accountability | Workflow-specific business meaning, Item/Run terminal decisions, or application-wide orchestration |
| Outer composition boundary | Resolves authorized configuration and dependencies; assembles selected implementations and named instances; grants bounded capabilities; owns application startup and shutdown coordination | Per-Item mutable state inside shared clients or an unrestricted runtime service locator |

Independent business projects use two public paths:

- **Framework orchestration/control:** the framework defines the operation and
  execution rules, delegates technical work through the appropriate Adapter,
  and interprets the returned business and technical evidence.
- **Business I/O:** business code chooses permitted queries, requests, transfers,
  or writes through public capabilities. Its Adapter still applies the relevant
  constraints, attribution, error contract, and evidence handoff. Flexibility
  does not mean taking an internal client or bypassing these obligations.

In both paths, technical results travel from Provider through Adapter to the
public caller and, where required, the framework's evidence boundary. This is
a collaboration path, not a requirement for a wrapper at every diagram arrow.

Public error, correlation, and required evidence contracts are usable by external
workflow authors and all three roles. Their dependency direction must not pull
in concrete Providers, SDK clients, framework orchestration, or business workflow
implementations. Adapters depend on the contracts they implement and the technical
capabilities they consume; Providers depend on those technical contracts and the
shared foundations, not concrete Adapters or business implementations. Composition
may know selected concrete implementations. All dependencies must remain acyclic.
Public construction surfaces must let an independent Go project assemble its
permitted capabilities without importing private implementation packages.

Go's `internal` boundary protects implementation visibility; it is not a security
sandbox or a reason to hide contracts external consumers require.
[Go module organization](https://go.dev/doc/modules/layout)

The I/O path operates in Activities or other appropriate process-local callers,
not directly in Temporal Workflow logic. A public wrapper does not make network
I/O, ordinary goroutines, or wall-clock control deterministic. Durable execution
receives serializable results or references, not live clients, streams, or native
handles. Workflow command and serialization changes require replay evidence.
[Temporal deterministic constraints](https://docs.temporal.io/workflow-definition#deterministic-constraints)

Two end-to-end examples make the division concrete:

- **Framework input preparation:** the framework requires the complete frozen
  input before starting business execution. Adapters translate authorized source
  reads and object writes into technical operations and retain their attribution.
  Internal bounds the resource lifetimes and returns read, transfer, and cleanup
  evidence. If only some objects were written, the framework must not treat that
  partial preparation as complete. Internal does not decide when a business Run
  may start or invent a cross-service transaction; it executes the authorized
  technical operations. Snapshot and publication mechanisms remain separate
  implementation decisions.
- **Flexible business output:** business code chooses a permitted operation and
  declares identity, conflicts, and success conditions. The Adapter preserves
  Item ownership through physical batching; the Provider reports delivery or
  write evidence at the granularity the SDK supports. Shared mechanisms protect
  the applicable bounds and evidence handoff. Handling a public error does not
  erase required facts, while a partial technical failure does not itself decide
  whether the Item or Run must fail. The framework makes that disposition under
  its public contract.

## S02 — Shared internal mechanisms, independent Providers

Internal has its own shared mechanisms; it is not just an assortment of SDK
wrappers. Three responsibilities cooperate without requiring three new packages
for every capability:

| Responsibility | Common obligation | Capability- or Provider-specific work |
| --- | --- | --- |
| Configuration and resources | Consistent preparation, source identity, ownership accounting, initialization failure reporting, and lifecycle evidence | Typed settings, defaults, SDK construction, probes, pooling, native resources, and actual dependency behavior |
| Controlled operations | Reuse applicable admission, budget, correlation, and result-handoff mechanisms; prevent silent bypass | Define operation phases, completion events, SDK attempts, batching, sessions, and cancellation limits |
| Results and diagnostics | One shared error contract, explicit effect/uncertainty reporting, safe observation, and version provenance | Interpret native codes and acknowledgements; expose the granularity actually supported by the service and SDK mode |

Capability contracts define what operations mean. Providers implement those
contracts using particular SDKs. A single Provider can support several execution
shapes. A stream, asynchronous delivery, subscription, remote task, and exporter
must not implement identical completion or transaction behavior merely to fit a
universal client. Shared mechanisms should become reusable implementation and
tests in separately authorized work, not repeated policy prose or independently
reinvented lifecycle/error systems in each integration.

Integrations must use applicable shared mechanisms rather than silently replace
their validation, ownership, constraints, or evidence rules. Where a required
mechanism is missing, record that prerequisite instead of claiming an ad hoc
wrapper provides the same guarantee. Conversely, sharing a mechanism does not
authorize it to redefine a capability's native completion or cancellation semantics.

Concrete Provider implementations must be independently selectable import units,
including capabilities with only one Provider today. Using one implementation
must not require an umbrella import of unrelated SDK implementations. This does
not promise zero unrelated module downloads or select a module-splitting strategy.
Do not expose whole SDK interfaces, add forwarding-only layers, or create empty
packages to make a diagram look complete.

Dependencies between capabilities are legitimate when necessary and explicit.
For example, a domain integration can use a public HTTP capability and credential
refresh capability. It must declare their ownership and limits, including hidden
dependencies captured in callbacks. Request and authentication-refresh traffic
must not secretly create a bypass transport. Provider selection does not by itself
migrate account-bearing sessions, cookies, or credentials.

The useful comparison with Go CDK is the separation of shared capability behavior
from drivers and explicit composition. Fathomry does **not** adopt its entire API,
dependency set, or graceful-fallback advice when a required guarantee would be
weakened. This is a project design inference, not a claim that Go CDK implements
Fathomry's execution model. [Go CDK structure](https://gocloud.dev/concepts/structure/)

## S03 — Named sources and outer selection

One Provider implementation must support multiple independently configured named
instances. A connection source is the identity of an assembled resource instance
within a documented composition scope, not a global singleton, a credential, or
the identity of a business Item. Equivalent endpoints do not authorize automatic
pool, session, account, or lifecycle sharing. Validate duplicate identities and
invalid selected configurations before constructing resources. Reuse an instance
for its declared lifetime rather than implicitly rebuilding it for each operation;
that lifetime is not necessarily the entire process.

| Identity | Responsible boundary and distinction |
| --- | --- |
| Provider identity | Identifies the selected technical implementation; neither a source instance nor its SDK version |
| Source identity | Assigned and bound by composition; identifies the actual resource used, with unambiguous scope |
| Effective source-configuration revision | Identifies the settings applied to that instance; distinct from configuration format and Run input |
| External account / credential scope | Domain and credential owners define authorization and account-bearing state; refreshed tokens do not change the account identity |
| Quota scope | Describes the actual shared allowance, potentially spanning sources, accounts, applications, and workers; not necessarily the ownership scope |
| Execution attribution | Supplied and validated per operation: logical operation, relevant Run/Item/attempt, and effect ownership; never a mutable “current Run” field on a shared client |

Composition determines the allowed source set. A developer can receive a bound
public capability or select from permitted, already-assembled sources through
the outer capability boundary, without acquiring client ownership. Unknown,
unavailable, or disallowed selection must be explicit; it must not silently choose
another source. Validate required capability, mode, and account/session scope and
retain the actual selected identity. User metadata cannot overwrite these facts.
Background tasks without a business Run must not manufacture one for correlation.

Including a Provider in a build, constructing instances at startup, and choosing
an available source for a call are different decisions. Whether configuration can
choose among compiled construction implementations remains open; permitted runtime
source selection is not thereby prohibited. No dynamic plugin loader is implied.

Framework control persistence and business database access are separate uses of
a technical database capability. Control Adapters own control-schema operations;
business I/O capabilities permit the business operations granted to their sources.
Reusing PostgreSQL support does not expose arbitrary control-database access or
imply a shared pool. Explicit sharing must document permissions, contention,
transaction boundaries, and shutdown ownership. Neither different source names
nor Go interfaces prove service/process isolation. A request spanning sources
must not acquire an implied cross-source transaction guarantee.

## S04 — Configuration responsibilities and effective revisions

Composition owns configuration input authorization, precedence, recursive overlays,
secret resolution, and provenance. Base configuration, environment-specific YAML,
local overrides, and environment variables must have a documented precedence;
nested overrides retain unrelated sibling fields. Providers receive explicit,
resolved typed settings. They must not
read process environment or global application settings during construction or calls.

The relevant capability/Provider owns its field meanings, units, bounds, default
values, option interactions, and supported modes. Shared configuration mechanisms
own consistent preparation and reporting, not one schema that erases SDK differences.
Composition validates the requested combination and required dependencies before
exposing a usable instance. Parsing settings, constructing a client, checking
readiness, and creating remote resources are separate operations and permissions.

SDK parsers may themselves consult environment, files, defaults, or callbacks.
The Provider must account for these inputs at the authorized preparation boundary
or reject that mode; passing a typed struct alone does not prove deterministic
configuration. Do not temporarily rewrite process-wide environment to simulate
per-instance configuration. Unused optional capabilities must not require services
or credentials solely because another Provider exists in the repository.

The instance must retain an accurate effective configuration and safe provenance,
not an aliased caller-owned map or an unchanged label over mutable settings.
Specify copying, borrowing, mutation, and concurrent-use rules for mutable values
and callbacks. Do not promise generic deep copying of arbitrary SDK objects.

Configuration schema evolution is owned by the format maintainer. A particular
source's revision is owned by composition and changes according to its declared
effective-setting semantics. Defaults and behavior-affecting options participate
in compatibility decisions. An unsupported configuration format must be rejected
before resource construction, not decoded using another version's defaults.
Dynamic credentials have a separately owned refresh
lifecycle: preserve account identity and secret references, not tokens in frozen
Run input, public diagnostics, or configuration-revision material.

List merging, null/deletion, empty values, duplicate/unknown keys, exact input and
environment-key naming, revision generation, and reload/rotation handoff remain
explicit implementation decisions. Their owners must define and test them before
claiming support. A replacement or reload must not retroactively relabel settings
already used by in-flight work or weaken its execution constraints.

## S05 — Ownership, operation lifetime, and shutdown

Every resource must have an accountable owner: clients, pools, borrowed connections,
transactions, streams, buffers, subscriptions, timers, goroutines, exporters, and
native resources. Ownership includes dependencies and background work created by
the SDK, not only handles created directly by Fathomry. Shared mechanisms maintain
one authoritative account of a managed resource's ownership and lifecycle; borrowed
views do not become separate owners of the same client. The Provider supplies the
actual SDK completion/release evidence for that account.

| Situation | Required responsibility |
| --- | --- |
| Creation | Identify the owner before the resource can escape; expose only the usable capability after required validation |
| Partial initialization | Keep ownership of every created resource, report primary and cleanup failures, and retain unresolved cleanup responsibility; do not advertise a complete instance |
| Borrowing | Define scope, allowed operations, concurrency, and return obligations; a borrower cannot close or reconfigure the shared owner |
| Ownership transfer | Make acceptance and failure behavior explicit, including SDKs that take responsibility for closing injected components; avoid double ownership or continued unauthorized sharing |
| Operation lifetime | Pass caller-owned contexts explicitly; define the owner of streams, callbacks, deliveries, and remote tasks that outlive the initiating call |
| Shutdown | Stop new work in the intended scope, retain paths needed to finish or clean up existing work, and release dependencies only when their consumers no longer need them |

Normal completion, cancellation request, stopping admission, resource release, and
reversal of an external effect are distinct facts. An API name such as `Close`
does not select among them: closing may flush writes, cancel a remote query, or
only signal a background task. Record primary completion, cleanup errors, and
remaining/unknown work separately; a later no-op success cannot erase an earlier
uncertain shutdown outcome.

Define budgets per execution shape: finite request and result consumption;
asynchronous enqueue, delivery and waiting; subscription establishment, session
and individual message handling; remote submission, polling, transfer and cancel;
background export and cleanup. A short initiating-call deadline is not automatically
the lifetime of all of these resources. Detached cleanup is not permission for
unowned or unbounded goroutines. Shared resource accounting must not count each
statement in a held transaction as another independent owned connection.

Specify whether a deadline bounds waiting, actual work, or both. Do not promise
hard cancellation of arbitrary Go callbacks, readers, native code, or remote
effects. A shutdown timeout does not prove that dependencies are no longer used.
Whether shutdown waits synchronously or returns while an owned cleanup operation
continues remains open. Unsupported termination or physical-isolation guarantees
must be disclosed or the corresponding mode rejected, never inferred from an
internal counter reaching zero.

## S06 — Shared errors, technical results, and reliable evidence

There is one public shared error system, not one per layer. Native SDK errors
retain their native behavior inside the integration. The Provider interprets native
codes, responses, and completion signals into the shared contract without losing
intentionally supported cause inspection. An Adapter adds missing source/execution
attribution or public-operation meaning; an already suitable error need not be
wrapped merely to identify a layer.

Keep these responsibilities separate:

| Concern | Owner and contract |
| --- | --- |
| Stable error identity and correlation | Public foundation; usable without importing a concrete SDK or workflow implementation |
| Native evidence and technical effect | Provider, interpreted for the exact operation/mode; preserve confirmed, partial, and unknown scope |
| Public-operation attribution and handoff | Adapter; preserve mapping between execution ownership and the technical evidence actually available |
| Retry, toleration, reconciliation, or termination | Framework/public capability policy with declared business input; not inferred solely from a native error category |
| Human presentation and localization | Presentation resources/boundary; language must not change machine identity or effect semantics |

Data and errors can coexist. Accepted, committed, visible, failed, partial, and
unknown effects are useful distinctions, not a mandated universal enum. A request
timeout or missing acknowledgement does not establish no effect; an affected-row
count or batch acknowledgement does not establish every Item's outcome. Preserve
main-operation and cleanup failures without letting one overwrite the other.
Capability results should expose relevant facts, not a giant cross-SDK result DTO.

The required evidence path is explicit: **Provider → Adapter → framework evidence
boundary**, alongside the operation's public result. It must work for asynchronous
and partial completion, not only a final return value. The framework owns which
facts require reliable recording, acknowledgement before advancement, and the
response to recording failure, process loss, or late/duplicate evidence. A local
callback is not proof of durable recording. Internal does not independently turn
each SDK operation into a control-database write.

A business handler may tolerate a failed operation without failing its Item. It
must not erase uncertain effects or other evidence required for correctness merely
by catching the error and returning success. Conversely, reporting an error must
not independently fail an Item or Run. Missing required evidence must remain
missing/unknown, not be manufactured from logs. The precise evidence reception,
persistence, and disposition protocol is separately scoped and still open.

Preserve intentional in-process `errors.Is`/`errors.As` behavior. At durable or
Temporal boundaries, preserve the supported semantic information through explicit
versioned contracts rather than serializing a native Go error graph. Temporal
cancellation, timeout, retry, and application-failure meanings must not be flattened
into a generic error; the detailed mapping and replay tests remain future work.

Diagnostics are bounded projections, never the only result path or terminal
ledger. Safe presentation must not emit credentials, raw configuration, private
payloads, arbitrary SQL/URLs, or unrestricted native error text. Native cause
inspection is not a promise that printing an entire error chain is safe. Audit
SDK log hooks, callbacks, returned dynamic types, and option escape hatches as well
as wrapper methods. Ordinary public capabilities must not leak uncontrolled client
ownership through these paths. Advanced native borrowing requires a separately
approved contract; this standard neither exposes all SDKs nor bans all native use.

Diagnostic failure must not recursively depend on the failing exporter or silently
block required cleanup. Run/Item identifiers, source names, and custom metadata
require explicit privacy and cardinality rules; being useful in a trace does not
automatically make a value safe as a metric label.

## S07 — Data-intensive guarantees and resource limits

An **Item** is the framework's business execution/accounting unit. A row or message
is a data unit. A logical operation can contain multiple SDK attempts and physical
batches; those batches may combine data from several Items. None of these mappings
is inherently one-to-one. Adapters preserve ownership across the mapping; Providers
return the technical granularity the SDK can actually establish. Missing per-Item
evidence remains unknown instead of being expanded from an aggregate success.

Business developers declare identity, ordering/conflict rules, required transaction
scope, and success conditions. The framework validates that its selected capability
and mode support the requested guarantee and provides the corresponding technical
protection. It must not silently downgrade the guarantee or shift undocumented
reliability obligations onto business code. Neither a database method nor a common
interface establishes cross-source, cross-service, or whole-Run atomicity.

Batching, streaming, retry, partial completion, and recovery must retain attribution
and distinguish complete empty output from missing output. Retry must not mix
unconfirmed old output with the current attempt or count attempts as new terminal
Items. Data/reference protocols must state completeness, ordering, expiry, and
recovery obligations; their formats and reconciliation algorithms are not chosen
here. Persisted references are not automatically still readable after retention.

Every supported mode must declare relevant bounds, units, enforcement scope, and
overload behavior. Review at least concurrent work, queued work and unresolved
receipts, records and bytes, per-record/per-batch size, connection and session
counts, SDK prefetch/decompression, background tasks, native memory, temporary disk,
and the evidence path itself. Bounds can use several owners, but must not have a
gap where admitted work or required evidence can grow without a declared limit.

Document whether overload waits, rejects, or uses an explicitly permitted lossy
path. Lossy telemetry cannot substitute for required effect evidence. A producer
buffer setting does not necessarily bound consumers; a count limit does not bound
bytes; Go heap metrics do not bound native memory. Guarantees unsupported by an
SDK mode need a restriction, an additional proven mechanism, or an explicit
unsupported status, not a claimed universal hard cap.

Local concurrency, rate, mutual exclusion, and shared quotas are different controls.
Framework constraints must remain effective across the workers and layers consuming
the actual resource scope. A per-instance limit does not prove an account-wide
limit. Provider retries, framework retries, and caller retries must have explicit
ownership and budgets; hidden multiplication of attempts is not acceptable. If
attempt-level evidence is unavailable, report that limitation instead of inventing
an exact count. No distributed limiter or feedback algorithm is selected here.

Millions of targets and long-running workflows motivate bounded processing, not
one client or goroutine per target. Workload bytes, simultaneous Runs, hardware,
latency objectives, and native-resource budgets remain unmeasured. Performance
claims require controlled comparisons at fixed correctness guarantees, including
overload and failure paths, not only favorable throughput.

## S08 — Version owners and compatibility

| Version or identity axis | Accountable owner and required distinction |
| --- | --- |
| Framework module and internal contracts | Framework maintainers; document behavior changes and test integrations without assuming independent releases for every private package |
| Public capability/error/correlation contracts | Public contract maintainers; preserve or explicitly change identity, cause inspection, defaults, and result/guarantee semantics |
| Configuration schema | Format and capability maintainers; define supported formats, field meanings, defaults, and handling of unknown or incompatible input |
| Effective source-configuration revision | Composition; identify the settings used by that source without confusing them with the schema version or exposing secrets |
| Actual SDK and Go build | Build evidence plus Provider requirements; distinguish dependency declarations and reference/test versions from what the consuming binary actually contains |
| Service, protocol, and SDK mode | Provider evidence and deployment selection; record observed values, enabled features, critical options, and what was or was not verified |
| Public/persisted results, messages, objects, and error formats | Respective contract/format owner; specify historical interpretation, missing/zero/unknown values, compatibility, and migration or rejection |
| Workflow and mode versions | Framework and workflow owners; compatibility and frozen execution selection are not inferred from an SDK upgrade |
| Deployment/build correspondence | Deployment and framework execution owners; runtime provenance informs, but does not implement, code-consistency validation or Temporal routing |
| Business schema | Business contract owner declares fields and write rules; technical capability validates support; automatic creation/migration is not implied |

A framework dependency declaration does not lock every consuming project's SDK
selection. Main-module or workspace requirements and replacements affect the Go
build list. Record this distinction rather than treating `go.sum` as the consuming
binary's exact dependency selection. [Go module version selection](https://go.dev/ref/mod#minimal-version-selection)

Actual build evidence must distinguish the main application from the framework
and SDK dependencies. Missing metadata, local replacements, and native-library
provenance remain explicit, with sensitive local paths redacted. Build metadata
describes provenance; it is not an integrity attestation or a compatibility test.
[Go build information](https://pkg.go.dev/runtime/debug#BuildInfo)

Record **tested support**, **observed build/runtime facts**, and **unknown or
unsupported combinations** separately. State the complete tested combination,
including behavior-affecting modes and options. A version string or successful
connection alone is not acceptance evidence. Changing defaults, error identity,
result meaning, or execution guarantees requires an explicit compatibility decision
even when function signatures are unchanged.

Known unsupported required guarantees must be rejected, not silently weakened.
The deployment response to untested or unknown combinations remains open and must
be explicit before use; neither blanket acceptance nor blanket rejection is selected
here. Durable readers must not reinterpret unknown versions/values as known success.
Define historical-reader and migration/refusal policy before publishing such a
format; no universal version registry or migration engine is required.

## S09 — Professional naming and cohesive organization

Package names must be concise, idiomatic English and describe a clear capability
or bounded responsibility. A package's documentation must explain its purpose,
ownership, consumers, and dependency direction. Names such as `common`, `utils`,
or `helpers` do not justify collecting unrelated functionality; nor does a package
called `types` justify moving every contract away from its owner. Naming is a
design review, not merely a prohibited-word scan.
[Go package naming guidance](https://go.dev/blog/package-names)

Create a package for a real boundary such as independent Provider selection,
dependency direction, a reusable capability, or access/ownership restrictions.
Do not create a package solely for a diagram box, a few moved types, or a speculative
future implementation. Prefer concrete types and native facilities; justify small
consumer-owned interfaces at actual seams rather than mirroring entire SDKs.

Files should contain meaningful functional units that can be understood together.
Do not default to a file per type, method, interface, or lifecycle phase. Split
when responsibilities or platform/build constraints genuinely differ; combine
when fragments jointly describe one straightforward behavior and obscure its
invariants. Adjacent tests should follow the same functional organization.
Compactness does not justify giant mixed-purpose files, duplicated mechanisms,
or merging unrelated Providers. No fixed tree or numerical file/line limit is set.

A package/file review must answer:

- Can a reader infer its actual purpose and find one behavior without hopping
  through many tiny files?
- Do its contents change for the same reason and share invariants and ownership?
- Does extracting a package remove a real dependency or enforce a real boundary,
  rather than add forwarding and navigation cost?
- Does combining files retain Provider separation, testability, and a clear owner,
  rather than hide several unrelated responsibilities?
- Does selecting one Provider avoid importing other concrete SDK implementations?

These questions make both fragmentation and overloaded packages reviewable.
Earlier candidate names and layouts are not approved by satisfying this section.

## S10 — Representative execution shapes and counterexamples

The following are bounded source observations from specific upstream snapshots,
not executed tests or selected runtime dependencies. The implications evaluate
this standard; they do not claim that a new Fathomry mechanism already works.

| Shape and inspected evidence | Assumption disproved; implication for acceptance |
| --- | --- |
| Database: pgx v5.10.0 documents that transaction-begin context cancellation does not automatically roll back. [Source](https://github.com/jackc/pgx/blob/v5.10.0/tx.go#L92-L110) | A returned transaction or cancelled begin context does not settle the transaction. Demonstrate transaction ownership, finalization, and uncertain commit/cleanup handling. Do not extrapolate PostgreSQL behavior to MySQL. |
| Asynchronous messaging: franz-go v1.21.5 reports delivery through promises; blocking a promise on client progress can deadlock. [Source](https://github.com/twmb/franz-go/blob/v1.21.5/pkg/kgo/producer.go#L510-L550) | Returning from an enqueue call is not delivery completion; generic blocking evidence collection inside a callback is unsafe. Demonstrate bounded receipt handoff, attribution, and callback/shutdown progress. |
| Object transfer: MinIO v7.2.1 ordinary object reads use request-driven background work; object close signals cancellation. A multipart error path invokes abort without propagating its error. [Read/close](https://github.com/minio/minio-go/blob/v7.2.1/api-get-object.go#L65-L88), [close body](https://github.com/minio/minio-go/blob/v7.2.1/api-get-object.go#L631-L658), [abort path](https://github.com/minio/minio-go/blob/v7.2.1/api-put-object-streaming.go#L112-L128) | Creating a reader does not prove data availability, and an upload error does not prove remote cleanup. Demonstrate partial reads, completion evidence, residual multipart state, and the limits of background-task termination evidence; other transfer modes need their own checks. |
| Persistent subscription: go-redis v9.21.0's message-channel loop receives in a background context and can drop a message after its channel-send timeout. [Source](https://github.com/redis/go-redis/blob/v9.21.0/pubsub.go#L749-L801) | A subscription is not a finite result stream with the initiating call's deadline or an implied lossless queue. Demonstrate session ownership, overflow visibility, cancellation, and the actual delivery guarantee. |
| Remote computation: Trino Go client v0.333.0 result close may request remote cancellation; spooling creates download/decode workers with separate contexts. [Close](https://github.com/trinodb/trino-go-client/blob/v0.333.0/trino/trino.go#L1586-L1630), [workers](https://github.com/trinodb/trino-go-client/blob/v0.333.0/trino/trino.go#L2112-L2139) | Closing local rows is not merely releasing a buffer or proof that a remote query stopped. Demonstrate submission/result/cancel evidence and distinguish direct from spooling mode, including worker cleanup. |
| Local/native computation: DuckDB Go v2.10505.0 appender close flushes buffered data; file-backed connectors use an instance cache. [Appender](https://github.com/duckdb/duckdb-go/blob/v2.10505.0/appender.go#L335-L373), [connector](https://github.com/duckdb/duckdb-go/blob/v2.10505.0/duckdb.go#L57-L103) | Cleanup is not necessarily effect-free; named instances are not proof of native isolation. Demonstrate flush/failure effects, native-resource ownership, and actual sharing and memory boundaries. |
| Background diagnostics: OTel trace SDK v1.43.0 queueing may drop spans; shutdown runs once and can return on context cancellation. [Queue](https://github.com/open-telemetry/opentelemetry-go/blob/sdk/v1.43.0/sdk/trace/batch_span_processor.go#L390-L429), [shutdown](https://github.com/open-telemetry/opentelemetry-go/blob/sdk/v1.43.0/sdk/trace/batch_span_processor.go#L159-L186) | Local acceptance, export, and durable business evidence are different; repeated shutdown success does not prove earlier cleanup completed. Demonstrate diagnostic-loss visibility and an independent required-evidence path. This observation is not a review of all metrics/logs exporters. |

The count/byte distinction is also concrete: franz-go's producer byte limit is
separate from its record limit and does not apply to consumer decompression.
This disproves a universal “one buffer setting bounds memory” assumption.
[Pinned configuration source](https://github.com/twmb/franz-go/blob/v1.21.5/pkg/kgo/config.go#L1292-L1313)

Across these shapes, review the same composition scenarios: two sources using one
Provider with different settings; explicitly shared dependencies; failed second
initialization; cancellation while results remain outstanding; mixed-Item partial
effects; a handled public error with required evidence still pending; unavailable
telemetry; and a consuming build with changed dependencies. None has been executed
as a Fathomry runtime test in this issue.

## S11 — Integration acceptance and traceability

Each later integration must link the accepted standard revision and applicable
section IDs, its actual implementation issue/PR/commit, and the shared mechanisms
it reuses. “Issue closed,” “SDK source read,” or “test double passed” is not enough
to claim a supported service combination. Record deviations for explicit review
rather than quietly interpreting the standard differently in each Provider.

The integration's cohesive documentation and tests must provide:

1. Its capability and execution shapes, public consumers, independent Provider
   selection, dependencies, and support/non-goals.
2. Named-source selection and control/business separation; typed configuration,
   defaults and provenance; schema/source revisions and redaction behavior.
3. Creation, borrowing/transfer, aliasing/concurrent-use rules, partial construction,
   operation completion and cleanup ownership, including background/native work.
4. Mode-specific constraints and units; count/byte/queue and shared-quota scopes;
   retry ownership, overload behavior, cancellation and shutdown limits.
5. Public results and shared errors; intended cause inspection; partial/unknown
   effects; Item/data/batch/attempt attribution; required evidence handoff and safe
   diagnostics, including when the public caller handles an error.
6. Actual Go/framework/SDK/native/service/protocol/mode and critical options, pinned
   upstream sources, tested compatibility, and unsupported or unknown combinations.
7. Reproducible commands and outcomes tied to a commit/worktree: focused contract
   tests; malformed input and fault cases; race/aliasing/cleanup checks as relevant;
   real service tests for guarantees that doubles cannot establish; replay tests
   for Temporal command/serialization changes; scale/resource evidence when claimed.

The following failure checks make those obligations observable. They are required
of the relevant future implementation, not tests already executed by this
architecture issue. Shared-mechanism and Provider tests do not replace tests of
the framework's durable evidence and terminal-state rules.

| Controlled case | Required evidence and failure condition |
| --- | --- |
| Invalid selected configuration or duplicate source identity | No resource construction begins; report the rejected input safely, without exposing values or credentials. |
| Two sources use one Provider; prepared inputs or original settings are later mutated | Each binding retains the intended source and effective settings; no mutable configuration or per-call attribution leaks between sources or assemblies. |
| Only one of several available Providers is selected | Unselected constructors do not run, and selecting that integration does not import an all-SDK implementation aggregator. |
| A consumer selects a missing/disallowed source or closes a borrowing scope | No silent fallback, ownership escalation, or shutdown of another scope's resource occurs. |
| Construction acquires a resource and then fails, or a later readiness check fails | Preserve the original failure, cleanup failure, and unresolved ownership; do not expose a partially usable assembly or claim that earlier effects were reversed. |
| Cancellation occurs after remote submission while receipts, rows, or background work remain | Distinguish caller waiting, operation completion, remote effect, and local cleanup; missing confirmation remains unknown. |
| The required-evidence path is saturated or diagnostics are unavailable | Required facts are not silently dropped, memory/queued work stays within the declared contract, and callback/shutdown progress does not depend on the failing exporter. |
| A mixed-Item batch partially completes and business code handles the error | Preserve known effects and unknown scope independently of the handled error; do not promote every Item from a batch acknowledgement or count late/retried results twice. |
| SDK close flushes, requests remote cancellation, times out, or later returns a no-op success | Preserve the actual effect and cleanup evidence; the method name or later return must not erase an earlier incomplete outcome. |
| A dependency, critical option, default, source revision, or persisted format changes | Distinguish observed configuration/build information from tested compatibility; apply the declared compatibility policy without relabeling history or silently weakening guarantees. |

Tests belong with each implementation, not postponed until a final conformance
issue. Reuse same-capability contract tests and exercise common mechanisms against
materially different shapes before claiming those mechanisms are general. No
universal SDK simulator, new benchmark platform, or specific test API is mandated.
External tests require separately authorized isolated resources and cleanup; no
production access or service mutation is authorized by this standard.

## S12 — Architectural decisions and remaining implementation choices

The established architecture consists of the three roles and two controlled
public paths; shared internal mechanisms with independent Providers; named
multi-source use; public shared errors and truthful evidence; version
accountability; and cohesive professional organization.

The alternatives below explain why these boundaries are necessary. Source
observations disprove unsafe assumptions; they do not prove an unimplemented
mechanism. Earlier exploratory package names, APIs, lifecycle algorithms, and
directory sketches remain unselected. A working mechanism must satisfy this
standard with evidence, not just resemble the previous sketches.

| Alternative | Assessment against the approved outcome |
| --- | --- |
| Independent ad hoc wrapper per SDK | Retains SDK detail, but repeats configuration, ownership, error, and attribution policy; no accountable shared mechanism |
| Universal client/lifecycle/result model | Simplifies a diagram while hiding incompatible completion, transaction, cancellation, and resource semantics exposed in S10 |
| Shared mechanisms with explicit capability semantics and independent Providers | Fits the approved direction, at the cost of maintaining capability-specific contracts and evidence rather than claiming all Providers are interchangeable |

The following remain open, with a responsible boundary and the point at which a
decision becomes necessary. They are not permission to drop the approved guarantees.

| Open decision | Responsible boundary and required checkpoint |
| --- | --- |
| Exact public assembly/source-selection APIs and whether configuration selects compiled constructors | Composition/capability maintainers, before configuration/resource implementation is accepted |
| Configuration precedence details, list/null/deletion/unknown-key behavior, source-revision identity, reload and credential rotation handoff | Configuration and credential owners, before those behaviors are exposed or claimed supported |
| Synchronous shutdown versus bounded waiting with continued owned cleanup | Composition and lifecycle owners, before a shutdown guarantee is published; keep actual completion and unknown cleanup distinct |
| Required evidence reception/acknowledgement, reliable storage, crash/duplicate/late-result handling, and handled-error disposition | Framework/public contract owners with Adapter evidence, before controlled-call correctness is claimed; no persistence protocol or terminal algorithm is selected |
| Native advanced access, borrowing and zero-copy lifetimes | Relevant public capability and Provider owners, before such an escape hatch is exposed |
| Policy for untested/unknown build/service combinations and historical formats | Compatibility/deployment and format owners, before startup or historical-read policy is relied on |
| Initial supported modes, real service combinations, workload/SLOs, and demonstrable resource limits | Explicit product constraints and each implementation issue, before support/performance claims; this source comparison selects no integration order |

These open choices limit future implementation claims; they do not turn Issue #3
into an umbrella that must wait for all runtime work. The current scope establishes
this standard and its evidence requirements. It does not implement the mechanisms
in Issues #4–#6 or any concrete SDK integration. Repository integration and Issue
closure are tracked separately; the presence of this document is not evidence
that runtime acceptance or a documentation merge has occurred.
