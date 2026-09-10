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

# SDK integration architecture

[Documentation](../README.md) / Architecture

**Audience:** framework and integration maintainers.
**Status:** accepted architecture; implementation coverage is limited.

This topic states accepted cross-package obligations, not a claim that every
SDK or framework feature is implemented. See the [documentation map](../README.md)
for current availability and package contracts.

**Standard sections:** S02, S10, S11.

## Contents

- [Shared mechanisms and independent Providers](#shared-mechanisms-and-independent-providers)
- [Execution shapes and counterexamples](#execution-shapes-and-counterexamples)
- [Acceptance and traceability](#acceptance-and-traceability)

## Shared mechanisms and independent Providers

Internal has its own shared mechanisms; it is not just an assortment of SDK
wrappers. Three responsibilities cooperate without requiring three new packages
for every capability:

| Responsibility | Common obligation | Capability- or Provider-specific work |
| --- | --- | --- |
| Configuration and resources | Consistent preparation, source identity, ownership accounting, initialization failure reporting, and lifecycle evidence | Typed settings, defaults, SDK construction, probes, pooling, native resources, and actual dependency behavior |
| Controlled operations | Reuse applicable admission, budget, correlation, and result-handoff mechanisms; prevent silent bypass | Define operation phases, completion events, SDK attempts, batching, sessions, and cancellation limits |
| Results and diagnostics | Shared private technical error/context handling, explicit effect/uncertainty reporting, safe observation and version provenance; public error meaning stays at the boundary | Interpret native codes and acknowledgements; expose the granularity actually supported by the service and SDK mode |

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

## Execution shapes and counterexamples

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
telemetry; and a consuming build with changed dependencies. These source observations are not Fathomry SDK/service execution evidence.

## Acceptance and traceability

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

## Implementation references

[SDK development workflow](../development/sdk-integration.md) and [conformance fixtures](../reference/internal/conformance/fixtures.md).

This topic carries forward the [accepted integration standard](internal-sdk-integration.md)
from [Issue #3](https://github.com/frost-leo/fathomry/issues/3), including the
[Issue #15](https://github.com/frost-leo/fathomry/issues/15) boundary refinements.
