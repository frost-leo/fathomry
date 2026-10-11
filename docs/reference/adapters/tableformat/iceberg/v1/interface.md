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

# Iceberg REST/S3 Adapter interface

[Documentation](../../../../../README.md) / Public Adapter reference

**Audience:** applications using the public Iceberg Adapter.
**Status:** implemented callback-scoped REST/S3 API with a published maintained
SDK and qualified native, isolated-service and versioned-consumer profiles.
**Package:** `github.com/frost-leo/fathomry/adapters/tableformat/iceberg/v1`.

## Maintained SDK dependency

The original dependency remains Iceberg Go v0.7.0. Controlled FileIO, admission,
retry and cleanup use the maintained source at
[`4b4b6612c1f2`](https://github.com/frost-leo/fathomry/commit/4b4b6612c1f217854e2f247aef6d5478d3e6a984),
published as `v0.0.0-20261011114113-4b4b6612c1f2`.
Existing generated-project SDK policy distributes this immutable replacement.
Consumers managing their own go.mod need the same replacement:

```go
replace github.com/apache/iceberg-go => github.com/frost-leo/fathomry/third_party/iceberg-go v0.0.0-20261011114113-4b4b6612c1f2
```

The module checksum is
`h1:2Zo37iCWYvqPouYNkcc4aBU8R+k1R2uCt2UMxuwm9fE=`.
A root checkout's local replacement does not propagate to another module.
This publication does not change the selected SDK version or automatically
upgrade existing Iceberg table formats.

## Call sequence

Use `Prepare(Settings, NativeOptions)` offline, then its `Policy` to construct
caller-owned `adapters.Runtime` and `adapters.Inbox[Result]`. `Open` owns one
REST Catalog, S3 client and source-local transport. Every non-nil Owner, including
one returned with an error, requires repeated `Close` until `ShutdownComplete`.

`Owner.Client().Use(ctx, func(*Session) error)` admits one synchronous family.
Session methods create controlled Table, Transaction, Scan and OrphanPlan handles.
All remain attached to that family and expire before Use returns. Concurrent or
recursive entry is rejected, not queued against the same already-held root.
`Using` lends a Fixed/Follow resource: only a new Use selects a new generation.
It does not grant owner/runtime cleanup authority.

The owned family contains abnormal exit on its callback stack: a panic becomes
a safe failed outcome (an error-valued cause remains deliberately inspectable),
and Goexit still performs family cleanup and evidence completion. Arrow visitors
release their borrowed batch and cancel before native producer join. This is not
a sandbox or forced termination guarantee for arbitrary callback-owned goroutines.

A public receipt and independent inbox delivery refer to the same facts.
Ignoring or inspecting a receipt does not acknowledge its inbox record. Requeue
redelivers evidence only. It does not repeat a Catalog submission or file write.

## Configuration and authority

REST Catalog plus S3-compatible storage is the selected backend. Namespace
components retain native semantics; authority confines them to the configured
namespace subtree. Object paths remain within an explicit canonical S3 prefix.
Endpoints, credentials, TLS, headers and OAuth issuer/audience are explicit.
Catalog FileIO endpoint/region hints do not replace selected source authority.
CreateTable/CreateView without a per-call location allocate a fresh directory
under the explicit source root, rather than delegating unchecked placement to
the Catalog. Explicit locations are checked before either staged-create phase.
Namespace location updates cannot introduce an undeclared storage root.
REST creation may assign field/spec/sort IDs. The provider compares normalized
native bindings at creation only, then retains the exact Catalog-assigned IDs.
It rejects changed field types, partition/sort sources, requested properties or
locations. Later commits must preserve their actual base identities.

Zero numerical limits choose defaults during Prepare. Configuration resolves
defaults once and rejects explicit zero limits. Permissions stay false unless
selected; metadata updates, writes, import, namespace management, multi-table,
rewrite, expiry, orphan cleanup, purge and file deletion are distinct grants.
Durations are nanoseconds. Settings JSON deliberately contains credentials;
ordinary formatting and logging are redacted.
MaxListEntries also bounds each metadata JSON collection; the managed profile
does not inherit the legacy facade's 2,048-entry decoder ceiling. JSON nesting
is limited to 64 levels, independently of the metadata byte envelope. Structured
default values retain case-sensitive data keys, not protocol-field casing rules.

NativeOptions are borrowed cooperative dependencies. TLS containers are copied;
credential, authentication, transport, reporter and encryption callbacks remain
caller-owned until actual source shutdown. Offline preparation calls none.
OAuthTLS independently selects token-endpoint trust/client TLS; like the native
option, this uses a separate owned transport rather than the Catalog transport
hook. TLS containers, including CA pools and ECH configuration bytes, are copied.
Catalog SigV4 and storage credentials may be supplied independently. Vended-only
table I/O needs no static S3 secret; direct file operations without table-vended
credentials are refused rather than falling back to ambient discovery.
Global reporter-registry selection is not source authority: inject the native
reporter explicitly through NativeOptions. The separate native REST metrics
reporting option remains opt-in and its bounded dispatcher belongs to the source.
NativeOptions.Logger selects a borrowed source-local native diagnostic logger;
nil discards native diagnostics rather than selecting the process-global sink.
Explicit reporters/loggers can receive identifiers, errors and callback payloads.
Their privacy, cardinality, buffering, export and flush policy belong to the caller;
the provider does not close a borrowed reporter, logger or OTel MeterProvider.
Optional reporting failure or loss never replaces the required evidence channel.
Close can return incomplete when its caller's wait ends; source custody remains
until native report workers, final logging and transport callbacks actually join.
Native import-time EnvConfig reading still occurs; per-operation worker/retry
caps do not mutate globals or use ambient source discovery. Worker limits bound
individual native pools, not all goroutines or hard RSS.

## Data and owned consumption

Native schema/type/expression/metadata constructors remain SDK APIs. Table and
transaction metadata inspections return detached containers. Scans support
snapshot/ref/time selection, native predicates and projection, incremental tasks,
local/remote planning, Arrow visitors and whole-table conveniences. Metadata
inspection selects the native inspection table by name.
MetadataLocation identifies the loaded metadata object without a head lookup.
Native DataFile implementations, expressions and option callbacks are borrowed
cooperative values: keep them stable through the family, including any native
conflict replay. A copied interface/slice is not a clone of a custom implementation.

Incremental ranges use zero From for all retained ancestors and zero To for
the ScanOptions snapshot/ref/time selection (current by default). A nonzero To
selects an explicit historical end. From is exclusive unless Inclusive is true.
Append planning skips overwrite/delete/replace snapshots; changelog planning
preserves native change ordinals, delete-before-insert order and snapshot IDs.
The native incremental planners are local; explicit remote mode is refused.
IncrementalAppendRecords and IncrementalChangelogRecords consume their own
origin-bound tasks with read permission, without requiring raw-task import
authority. Changelog visitors receive the native operation, ordinal and commit
snapshot; early break stops the whole sequence and Limit applies across tasks.

`Plan` owns a separate native scan and its exact plan-scoped credentials. Closing
or replanning the parent Scan cannot rebind an existing Plan. Plan/Scan cleanup
does not acquire another operation slot. Detached `PlanFiles` tasks are data;
adopting them through `ReadTasks` requires import permission. Ordinary plan
consumption uses `Plan.Records` instead.
Compaction groups are likewise detached native task DTOs. Executing or rewriting
caller-supplied groups requires import permission as well as rewrite/write grants;
the native compaction worker uses the Table's FileIO, not a remote Scan's plan
credentials. Group planning closes its temporary scan instead of retaining an
unreachable plan owner. These DTOs are not cross-source runtime capabilities.

Low-level remote planning retains native request/response DTOs. Plan IDs and task
handles may only be used within the family and table that acquired them. Every
issued plan reserves one independent cancellation request before setup; ignored
results are canceled during family cleanup, even after normal operation/request
capacity is exhausted. Cancellation failures remain in independent evidence.
The acknowledgement is a server protocol fact, not proof remote workers joined.
Requests that require snapshot identity must select it explicitly; high-level
Scan planning derives it from the originating loaded metadata.

A Records visitor borrows each Arrow batch until it returns. Retain creates a
caller-owned reference that must be released. ToArrowTable returns a scoped
ArrowTable owner, including valid prefixes accompanying errors. Its Visit lends
native table/column data only during the callback; Close releases the shared
output-byte reservation, and family termination closes ignored owners.
Input RecordReaders are
borrowed for the entire write call. WriteRecords transfers each yielded input
reference to the native writer. Early stop cancels before native producers join.
In contrast, `WriteEqualityDeletes` and `CommitEqualityDeletes` borrow batches
until yield returns; the native writer retains queued batches and the caller
still releases its own references. `WriteEqualityDeletes` returns staged owned
delete files for RowDelta. `CommitEqualityDeletes` is the native delete-only
staging shortcut, not the final Catalog commit. Equality field IDs, NULL matching,
partition routing, sequence numbers and format restrictions remain native.

ReadFile/CreateFile callbacks receive only owned file handles, not a native FS
owner. EncryptionManager interfaces preserve their actual native contract:
their presence does not implement generic transparent table envelope encryption.

## Effects and destructive boundaries

Append, overwrite, delete, RowDelta, imports, schema/spec/sort/property updates,
references and format upgrades stage native intent. Commit consumes its
transaction even if the reply is lost. Default automatic conflict retries are
not enabled. Each effect's Submissions retains every actual single-table Catalog
attempt, including native retries and durable partial-progress rewrite batches.
These records preserve their own base, intent, validated acknowledgement and
metadata location; a later failed batch cannot erase an earlier committed one.
The enclosing Effect is the latest request outcome, not an atomicity guarantee
for all batches. HTTP status alone does not establish a matching acknowledgement.
An explicit source ceiling caps native known-conflict replay.
Staged creation accepts the native first-phase metadata without a persisted
metadata URI. It validates the complete prospective update before I/O and binds
the final commit to the returned staging UUID. A staging acknowledgement alone
does not mark the final Catalog commit submitted.

Do not infer committed identity from a later reload. Catalog replies are checked
against the submitted metadata change and original UUID, and metadata objects
are independently read through managed S3. Drop is not physical purge. Purge
acknowledgement is not proof every object version disappeared.

View updates validate their native requirements, prospective location and commit
UUID/version intent. Returned Views and UDFs are detached pure metadata, not
runtime handles. UDF metadata locations are optional under the native protocol;
present locations still obey source authority. The SDK supplies read-only UDF
metadata, not execution or creation. Native unsupported function listing is an
empty sequence; unsupported loading/exists preserves ErrEndpointNotSupported.
Multi-table receipts retain each table's UUID and all changed reference intents,
including absent/removed references and native retention fields. Low-level DTO
summary snapshot IDs refer to main; `References` records other changed refs.
Post-commit reload has its own effect and refuses a replacement table UUID.

Orphan planning and execution are separate. Execution requires the same scope,
table identity and unchanged base plus explicit caller-declared external-writer
quiescence. That declaration is not a distributed lock. No background cleanup or
automatic deletion of unknown-commit objects is performed.
Server purge also requires an explicit quiescence declaration and bounded
inspection of metadata/manifests/file targets before submission. The declaration
covers all writers; it is not verified by, or a substitute for, external coordination.
`Table.PurgeFiles` is the distinct client-side native file operation: keep the
Table in its family, drop its Catalog entry first, and explicitly declare
quiescence. It also requires file-delete permission, preserves native `gc.enabled`
behavior and never certifies versioned-object absence. An attempted physical purge
or orphan execution cannot be replayed through the same handle after partial or
unknown deletion. Inspect independent per-file evidence before any new action.
`ExpireSnapshotsWithOptions` preserves native table/ref retention defaults when
fields are omitted and accepts explicit zero age. Physical post-commit deletion
is opt-in; callers must coordinate readers that still use expired snapshots.

OrphanExecutionOptions also retains native dry-run and custom deletion behavior.
DryRun neither calls Delete nor consumes the plan. A custom Delete callback sees
only inspected targets after source capacity checks and must finish its work
before returning; its acknowledgement is recorded separately from S3 deletion.
Errors/panic/Goexit retain negative evidence, and the native worker group is
joined. Arbitrary callback-owned goroutines and actual remote object absence are
not inferred from a nil callback error.

## Verification and distribution

Focused Internal/public race tests and the opt-in public REST/S3 fixture are
executable in this repository. Source, simulated peers, native fixtures and live
tests are distinct evidence levels. The issue's G01–G16 / S01–S06 ledger remains
the qualification authority; untested combinations remain unqualified.
The independent consumer embeds immutable old format-2 data, checks exact rows
and NULLs, exercises retained plans and incremental streams, and checks
direct/Fixed/Follow ownership. The existing versioned-project test also executes
it from an isolated module cache using the published SDK checksum, without a
checkout replacement. Service-specific unavailable endpoints remain explicit
refusals rather than simulated live successes.

The maintained v0.7.0 source and narrow patches are described in
[SDK maintenance](../../../../../../third_party/iceberg-go/FATHOMRY.md).
Independent consumers must select that maintained SDK explicitly. A root-only
replace is not a versioned distribution; use the immutable replacement above.
