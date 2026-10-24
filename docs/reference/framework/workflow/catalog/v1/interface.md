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

# Workflow catalog registration and reads

[Documentation](../../../../../README.md) / Framework reference

**Audience:** project authors and catalog implementers.
**Status:** preparation, atomic registration, bounded localized reads, selected
comparison and project register/list/show/compare commands are implemented.
**Package:** `github.com/frost-leo/fathomry/framework/workflow/catalog/v1`.

## Purpose and ownership

The catalog owns workflow publication semantics, not database creation, migration,
connection pools, resource scopes or Worker execution. Prepared input and persisted
publication are separate capabilities and observations.

Compile or parse declarations through the existing
[declaration authority](../../declaration/v1/interface.md), author a Registration,
then call Prepare with a caller-owned context. The Bundle must contain exactly one
Namespace/code and all explicitly selected version/mode revisions. No graph grammar,
normalization or fingerprint logic is reimplemented here.

Prepare returns an immutable PreparedRegistration. Inspect returns detached input
counts/identity only. The snapshot is not a durable protocol, DB ID allocation,
schema-readiness token or commit receipt. It has no Close or resource-owning method.
DocumentBytes and MetadataBytes are logical retained-input charges, not RSS.

Authoring input is borrowed during the call and must not be concurrently mutated.
Accepted text/tags are copied; callers cannot mutate prepared content through later
edits. Ordinary formatting and logging redact authoring/prepared data. Prepared
snapshots refuse JSON serialization; explicit authoring data may be sensitive.

## Register through a borrowed Resource client

Construct Catalog with PostgreSQL or MySQL, an explicit target and Options.
Construction validates local inputs/embedded assets only. Supply an Adapter Using
client whose source belongs to Resource, not an Owner.Client shortcut. Register
checks the captured Resource generation before business writes. The caller owns
the client lifetime, source/runtime shutdown and incremental evidence reception.

Call Register with one PreparedRegistration and caller-owned context. Each call
uses one read-committed transaction and checks schema on that exact connection.
Options defaults to 45 seconds of work and 5 seconds of cleanup waiting; each
nonzero bound is 1 ms through 1 minute. These are wait/admission bounds, not proof
that all native cleanup finishes when a deadline expires.

Copies of Catalog share authority. Calls refuse overlap; unfinished cleanup blocks
new registration. Close seals future work and joins the retained transaction, never
the borrowed client. An overlapping Close does not silently cancel an active call.
When waiting expires, keep this Catalog, retry Close with a fresh context, then
read LastPublication. Returned Publication values are detached historical observations;
they do not change when later cleanup learns more. LastPublication performs no DB query.

Publication separates:

| Fact | Meaning |
| --- | --- |
| not_started | Business registration was not entered |
| applied / unchanged | Successful create/compare with native COMMIT acknowledgement |
| rolled_back | Native rollback/commit-rolled-back acknowledgement |
| unknown | No sufficient finalization proof; never an automatic retry instruction |
| Acknowledgement | Native transaction evidence, separately recorded |
| CleanupComplete | No catalog-owned retained transaction remains; not source shutdown |
| CleanupFailed | A terminal native cleanup error was observed |

Only applied/unchanged expose DefinitionID (a decimal string) and committed Changes.
Unknown/rolled-back results contain no tentative IDs/counts. Known commit facts stay
available alongside cleanup errors. Formatting/logging redact handles and publication
identities; explicit Publication JSON is supported, while Catalog/PreparedRegistration
serialization is rejected. Registered does not mean enabled, defaulted or deployed.

Reserve WorkBytes (128 MiB) for a non-overlapping operation, plus retained prepared
input and provider evidence/copy envelopes. This is a declared charge, not measured
RSS. The tested full-document client profile uses PostgreSQL 16 MiB argument/message/
result limits, or MySQL 8 MiB argument/packet, 16 MiB result and 32 MiB response limits.
The shared CLI explicitly opts into these defaults for catalog operations and charges
all selected prepared documents/metadata; explicit smaller byte limits fail before
source acquisition. Existing schema/creation consumers retain their prior defaults.

After an unknown outcome, first join cleanup, then inspect stored state or explicitly
retry the same immutable registration. No automatic reconciliation, retry loop,
connection replacement policy or request ledger is introduced by Catalog.

## Read metadata and explicitly selected documents

ListRequest.Parent requires Namespace. Namespace alone lists definitions; adding
Code lists versions, adding Version lists modes, and adding Mode lists revisions.
Parent.Revision is invalid. Code filters the listed child identity exactly;
Category filters definitions only; Enabled distinguishes absent from false.
Limit defaults to 20 and accepts 1..64.

Ordering is byte-exact machine-code order, not display-name or SemVer order.
Next is a canonical versioned cursor bound to target/dialect, parent, exact filters
and normalized locale. It is not authenticated authority, a source-generation token
or a permanent snapshot. A changed page size may reuse a cursor. Missing parents
return ErrMissing; an existing parent with no selected children returns an empty page.
Missing/unproven schema is ErrSchema, never an empty catalog.

ReadRequest.Identity names one exact definition/version/mode/revision. No default
pointer or latest label is followed. Metadata includes IDs/revisions as decimal
strings, UTC-microsecond timestamps, admission/default intent and localized names/
descriptions. Definition reads include category metadata and bounded raw tags.
Lists do not fetch tag collections or document bodies.

Document opt-in requires a version or complete mode-revision identity. Read returns
that specification, or the revision plus its exact parent specification, verified
by declaration.ParseContents. StoredDocument.Canonical is a detached byte slice;
JSON represents it as base64 to preserve exact canonical bytes without HTML-escape
expansion. Metadata-only results explicitly keep DocumentVerified=false. Unknown
formats/profiles, corrupt/noncanonical bytes and missing required English are refused
without repair. Failed reads return no partial View/Page.

All mutable entity/category/tag/text/locale facts come from one bounded nonlocking
anchor SELECT on the retained read-only transaction. Requested document bytes are
then fetched only by the anchor's immutable ID/owner/label/format/profile/digest and
verified through the declaration authority. Disappearance or mismatch refuses the
whole result; later management changes cannot be spliced into the anchored metadata.
This relies on the checked immutable guards and supported coordinated-DDL protocol,
not protection against a privileged administrator bypassing that protocol.

Each request has its own view; separate pages/calls may observe intervening changes.
Reads require no registration/UPDATE privileges: PostgreSQL schema USAGE and table
SELECT, or MySQL SELECT plus the existing TRIGGER metadata visibility requirement,
are qualified. MySQL TRIGGER is not a read-only metadata privilege. The service
profile still requires the selected writable target; replicas are not qualified.
Read cleanup uses the same Catalog owner and never overwrites LastPublication.

Empty display locale means English. The existing i18n.NormalizeLocale request
policy validates/canonicalizes it; Unicode extensions remain part of the exact key.
Only an exact enabled locale is selected. Localized records Present, Value, actual
Locale and one of none/resource_absent/unknown_locale/disabled_locale/
missing_translation. A present empty description does not fall back. A selected
resource without English fails even if its requested translation exists. Optional
absent names remain absent; callers may display the machine identity separately.

Returned view encoding is bounded to 8 MiB; cursors to 512 bytes. An over-budget
metadata page is refused, not silently truncated; request a smaller page. The
4 MiB specification / 1 MiB mode document profile is retained. ValidateList and
ValidateRead perform offline shape/locale checks; List additionally validates
cursor binding against the actual target. Neither helper grants schema readiness.

## Compare an explicit prepared selection

Compare borrows the same immutable PreparedRegistration used for Register and owns
one read-only operation. It does not register, enable, repair or retry. Missing
definitions/documents/resources are completed non-matches. Existing stored bodies
are fetched by the single anchor's immutable identities/profile/digest and validated
together through declaration.ParseContents before exact byte/profile/hash comparison.
Recorded fingerprint equality alone is not acceptance.

One bound JSON argument carries only selected document identities and generated
resource addresses; it is a private SQL parameter, not another portable publication
format. Both dialects expand it with their native JSON table facilities and produce
one anchor result. This avoids independent mutable reads, thousands of parameters
or a SQL statement that grows per resource.

Comparison includes selected executable/control content and publisher-owned English/
translator context. It ignores initialization-only category/tags, admission/defaults,
translations and omitted versions/resources. Equal content under another label
does not satisfy a missing requested identity.

Comparison.Complete/Equal describe observed current state, never native mutation
acknowledgement or Worker readiness. Matched/Missing/Different count selected document
and resource units. At most 128 Problems retain fixed kinds and numeric prepared-input
paths, with document identity where useful; counts remain complete and Truncated
indicates omitted details. No payload differences, English values or resource keys
are printed. Missing English in an existing resource and corrupt/unsupported stored
content are errors, not ordinary differences; failed comparison returns no complete
report. Schema absence/unavailability also remains an error.

The full anchor ceiling is 4417 rows. The caller's source must admit the selected
shape; the CLI raises only an unspecified MaxRows for compare (at least the normal
1024 default) and refuses explicit smaller limits before source acquisition.
Registration/list/show source defaults do not change. The private JSON request is
bounded to 6 MiB, selected stored document bytes to 16 MiB, and selected stored
English/context bytes to 4 MiB, in addition to existing native/cumulative read bounds.

After a lost COMMIT reply, an explicitly requested successful comparison can establish
that the selected state currently matches. It does not identify who published it or
rewrite LastPublication's unknown original acknowledgement. There is no automatic
reconciliation or mutation replay.

## What is admitted

Registration contains a verified Bundle, display Resources, optional Category and
initial raw Tags. There are no credentials, connection settings, enable/default
fields or operator revision values. Display targets use a closed union:

Metadata groups the Resources/Category/Tags authoring values without a Bundle.
Project assembly can keep it beside the declaration in one workflow registration;
the shared CLI compiles the declaration before supplying these same values to
Prepare. It adds no second workflow list, validator or runtime.

| Kind | Required selectors |
| --- | --- |
| definition | None; Namespace/code come from the Bundle |
| version | Version |
| mode | Version, Mode |
| mode_revision | Version, Mode, Revision |
| node | Version, Node |
| activity | Version, Node, Activity |
| control | Version, Control |

Every unused selector must be empty. Every referenced owner must exist in the
selected bundle. Activity identifies a call within its node, not a binding type.
Owner projection uses already verified canonical documents; it is not a second
executable validator. Each referenced graph is projected at most once per version.

Only name and description fields are supported. The definition and each selected
version-owned mode require one nonblank English name. Multiple revisions of the
same mode share that mode name. Other metadata is optional. An explicitly empty
description is present; omitting an optional resource does not request deletion.
Duplicate owner/field declarations reject, even if their text is equal.

Category Code is exact nonempty UTF-8, at most 128 bytes, without control characters.
Name nil means an existing-category reference. A supplied Name declares a category;
Description requires Name. This data and its resources remain separate from
always-compared workflow metadata. Persistence consults them only on
first definition creation, so old initialization cannot recreate a category after
an operator changes the association.

Tags are first-creation data, not updates. At most 64 entries are admitted, with
1..128-byte keys and 0..256-byte values. Keys/values retain their original bytes.
Reserved names are id, created_at, updated_at, row_revision, namespace, code,
category_id, enabled, version, mode, revision, default_version_id, default_mode_id
and default_mode_version_id. Reserved-name checks and fathomry./temporal. prefix
checks ignore case; ordinary accepted keys are not normalized. Do not supply secrets;
automatic secret detection is not promised.

## Resource keys and bounds

The catalog derives addresses, never accepts an opaque resource key. Scope is
workflow.definition, workflow.version, workflow.mode, workflow.mode_revision,
workflow.node, workflow.activity or workflow.control. Category initialization uses
workflow.category. The key is `v1;` followed by each tuple segment's canonical
decimal UTF-8 byte length, a colon, and the exact segment bytes.

Workflow tuples start with Namespace/code, then the selectors listed above in
order, then the field. Category tuples are Namespace/category-code/field. This
preserves case, Unicode distinctions and trailing bytes without delimiter ambiguity.
No hash-only owner mapping is used. The generated key is at most 1024 bytes.

| Boundary | Maximum |
| --- | --- |
| Selected display resources, including category initialization | 4096 |
| Metadata bytes, including selectors, generated addresses, text/context and tags | 4 MiB |
| English text | 16 KiB UTF-8 |
| Name | 256 Unicode characters, nonblank |
| Translator context | 1024 UTF-8 bytes |
| Tags | 64 entries; 128-byte keys and 256-byte values |

Empty Context means no translator note. The declaration's existing 4 MiB spec,
1 MiB mode and 16 MiB aggregate limits are unchanged; metadata limits do not shrink
document admission. Bounds are logical retained-input budgets, not measured RSS,
SQL operation budgets or database quota qualification.

## Failures and current evidence

Failures use the workflow_catalog component and facility 0x1C2 with English/zh-CN
resources. Diagnose returns a fixed reason and bounded numeric path, never owner
names, resource keys, tag values or payload differences. Original declaration and
cancellation causes remain inspectable. Failed preparation returns no partial result.

[Preparation tests](../../../../../../framework/workflow/catalog/v1/registration_test.go)
cover required/optional names, ownership, mixed aggregates, category separation,
tags, metadata bounds, copying, concurrent inspection, safe diagnostics and key
injectivity. The [existing exact-cap fixture](../../../../../../schema/fathomry/document_test.go)
also passes an actual valid 4 MiB specification through Prepare. The independent
[consumer gate](../../../../../../internal/conformance/imports_test.go) uses public
declaration/catalog APIs without a database or private import.

## Private persistence boundary

The private [transaction kernel](../../../../../../framework/workflow/catalog/v1/persistence.go)
borrows an explicit writable read-committed public Adapter transaction. It does not
open a source, create a Resource scope, begin a transaction or install a schema.
It is single-use per aggregate. The caller joins its native finalization after
success or failure; incomplete cleanup remains owned by the same retained handle.
Counts describe tentative changes, never a commit receipt or retry decision.

New definitions, versions, modes and revisions start disabled with no default
pointers. Identity row locks serialize competing publications. Existing canonical
content is verified through the declaration authority before exact comparison;
equal content performs no update. Different content, translator context or selected
English text rejects the whole transaction. Missing English for an existing resource
is corruption, not a repair request. Existing operator state, omitted metadata,
translations and initial category/tags are not reset by repeated publication.

First creation resolves the category in the same Namespace under a row lock;
disabled categories reject new associations. Concurrent shared-category publication
also compares the declared English/context. Category changes after creation do not
cause a later registration to recreate the original initialization category.

The kernel bounds cumulative reads to 64 MiB and SQL calls to 32768, with a
conservative call-budget check before mutation. These bounds are not service
latency, memory or deployment-capacity guarantees.

Before business writes, the private [admission check](../../../../../../framework/workflow/catalog/v1/admission.go)
requires evidence of an active Resource generation and checks the service/session
profile, current owned structure, exact terminal migration history and English seed.
It uses the same transaction as publication, not a prior status report, token or
connection acquired from the client again. Immutable inspection SQL/normalization
is shared with the schema command through the existing dialect asset packages;
neither those helpers nor their query results independently grant readiness.

PostgreSQL shares the installer's advisory-lock identity using a transaction-scoped
shared lock and pins owned relations. MySQL uses transaction-owned metadata locks
and shared history-row locks, checks installer contention, and never acquires a
session named lock. It validates rather than changes reusable MySQL session settings.
The supported installation protocol must coordinate DDL through its existing lock;
uncoordinated administrative mutation is not a supported concurrent migration path.

The tested writer grants SELECT on owned tables and INSERT/UPDATE on the eight
publication tables, not on history or the locale directory. PostgreSQL additionally
needs schema USAGE and identity-sequence USAGE. Its protected history/English reads
do not demand UPDATE merely to lock a read-only prerequisite. MySQL also requires
TRIGGER metadata visibility; that privilege is not a read-only metadata privilege.
Missing visibility is an unproven structure, not proof of physical drift. These
grants do not constrain a privileged administrator or replace management-write rules.

Finalization distinguishes native acknowledgement, native primary/cleanup errors
and local release. A timed-out wait is not rollback. COMMIT is never sent twice on
the same transaction. Subsequent finalization calls can only join the original
receipt and request rollback of still-unresolved native work. Lost acknowledgement
remains unknown even when a separate test witness sees the committed rows; that
witness is not substituted for the original attempt's acknowledgement.

Opt-in [real-service tests](../../../../../../framework/workflow/catalog/v1/integration_test.go)
use Resource-backed PostgreSQL/MySQL Adapter clients and build a separate public-CLI
host that actually runs schema apply for each isolated target. They check native
transaction acknowledgements and joined resource cleanup, and
cover initial/equal/conflicting publication, operator-state preservation, concurrent
creators, category-disable ordering, stored-content refusal, cancellation, exact
4 MiB documents and the admitted resource/entity counts. Negative controls cover
missing/incomplete/drifted history, changed columns/guards, a missing English seed,
installer contention, metadata-lock blocking and lost commit replies. They do not
fabricate a successful migration journal or use raw drivers to qualify the path.

The same suite exercises public Catalog methods and an independently compiled
application's real workflow register commands, including full/selected preflight,
restart/no-op, partial batches, output failure and lost-commit batch termination.
The read controls also exercise a separate restarted application with no registered
local workflows, exact locale/empty-text behavior, metadata snapshot coherence,
anchored-document disappearance, restricted readers and exact 4 MiB document reads.
Comparison controls additionally verify the near-16 MiB selected document aggregate,
exact 4 MiB metadata, bounded/truncated differences, later integrity failures, and
explicit coherent read-back after a lost COMMIT reply. Automatic reconciliation and
multi-page snapshots remain outside this contract.
