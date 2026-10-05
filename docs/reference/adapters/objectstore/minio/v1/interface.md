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

# MinIO object-storage Adapter

[Documentation](../../../../../README.md) / Public package reference

**Audience:** independent Go applications and explicit Framework composition.
**Status:** implemented and locally qualified, including an isolated HTTP,
version-enabled MinIO service fixture. Publication/merge under
[#104](https://github.com/frost-leo/fathomry/issues/104) remains separate.
**Package:** `github.com/frost-leo/fathomry/adapters/objectstore/minio/v1`.

## Responsibility and construction

The Adapter exposes the bounded [Internal MinIO profile](../../../../internal/objectstore/minio/v7/interface.md)
without public Internal types, a native client or source-shutdown authority.
Public v1 versions this API; it is not MinIO SDK major 7. The selected dependency
remains minio-go/v7 v7.3.0.

1. Prepare `Settings` with pure `Validate` and `Recommend`. Settings is ordinary
   strict-loadable data with exact JSON field names and integer nanosecond durations.
   Use public configsource strict preparation to reject unknown/duplicate fields;
   plain `encoding/json.Unmarshal` alone is not strict preparation.
2. Construct caller-owned `adapters.Runtime` and `adapters.Inbox[Result]` using
   the recommendation. Both must outlive source/operation cleanup and required
   evidence reception. No global defaults or receiver are installed.
3. `Open(ctx, settings, dependencies)` freezes one source and owns its native
   transport. **Open performs bucket HEAD readiness I/O.** This does not establish
   write, version, tag or delegation permission.
4. Retain and close every non-nil Owner, including partial construction failures.
   A failed Open does not return a usable Client or Handle.
5. `Owner.Client()` is concurrent-safe and non-owning. `WithID` returns an
   immutable facade copy with bounded opaque public correlation.

The endpoint is one literal-IP HTTP(S) URL with explicit port and no path,
query or userinfo. Region, bucket, literal key prefix and static V4 credentials
are explicit. HTTP requires Plaintext; HTTPS requires the complete explicit CA
bundle. There is no ambient credential/CA/proxy discovery, STS refresh, arbitrary
native access or administration API.

Settings strings are frozen by value. Maps/slices in operation inputs are
borrowed for validation/copying until the method returns, not safe for concurrent
mutation. Readers and Writers have the longer lifetime described below.

## Actual capability coverage

| SDK / Internal capability | Public route and important limit |
| --- | --- |
| Source preparation/readiness | Validate, Recommend, Open; pure validation is distinct from bucket HEAD |
| Stat / Core GET | Stat, Read, Download; current/exact granted versions, exact ranges, optional ETag/SHA256 |
| Single / serial Core upload | Put; Size=-1 means unknown, zero means empty; exact-size checks and original final conditions |
| Owned Core multipart | BeginMultipart, Part, Complete, Abort, Close; serial parts, retained generation, separate budgets |
| CopyObject | Copy; one full same-source object, source version/ETag; destination conditions explicitly reject |
| Contextual ListObjectsIter | List retains its finite legacy limit; Enumerate/Next owns incremental object/version continuation |
| Multipart inspection | ListUploads, ListParts, explicit Abort; opaque markers and per-page continuations |
| RemoveObject | Remove; one result per input, including unsubmitted trailing targets and observed delete markers |
| Tagging | GetTags/SetTags with explicit Tags grant; empty SetTags removes the tag set |
| PresignHeader | Presign with explicit GET/HEAD/PUT grants, typed signed conditions and bounded expiry |

Metadata, checksum and condition observations remain distinct. ETag is not a
universal digest. Missing/empty/native-zero fields do not acquire invented
presence semantics. Read returns a non-nil empty byte slice for a consumed empty
body, versus nil for absent data. Partial payload and an error can coexist.

There is no bucket/policy/versioning administration in the product API, presigned
POST, KMS/STS discovery, notification subsystem, file/seek helper, automatic
parallel multipart, cross-bucket compose, durable upload/cursor format or
Run/Item/business publication protocol. Process-local I/O is not Temporal
Workflow code.

## Receipts, independent evidence and source ownership

Every operation returns a read-only `*adapters.Receipt[Result]` once publicly
admitted. A nil immediate error means acceptance, not successful native work.
Native validation/admission failures can therefore appear in the accepted public
receipt without any native submission. Check waiting errors separately from
the final snapshot's Primary, Cleanup and Err.

Use `WaitReleased` before reusing caller-owned transfer objects. Put and Part
borrow Readers; Download borrows Writers. They never close those objects.
Waiting timeout neither cancels work nor proves an external sink was untouched.
An uncooperative Reader/Writer can delay actual release and shutdown indefinitely.

Native evidence is claimed into a bounded per-root private inbox and projected
into immutable public facts **before** native custody ends. A live session's root
is claimed immediately, so it cannot block incremental child records. Public
`Inbox.NextReleased` can receive released children while the root remains live.
Ack only after necessary evidence handling; Retry requeues the same facts and
does not resend an object mutation. Ignoring a direct result does not erase the
independent record. Saturation rejects admission rather than dropping evidence.

`Result.Source()`, Attribution and Attempts use
[shared object-storage contracts](../../v1/interface.md). Public ID and borrowed
generation are frozen for the admitted root and its children. Native technical
IDs are separately generated and are not a durable Run/Item protocol.
Data/map/header/list accessors copy mutable storage; explicit errors are borrowed
immutable graphs and can reveal sensitive data.

Owner.Close seals new work, cancels retained work, joins actual transfers and
sessions, then joins transport cleanup. A timeout leaves the same Owner reachable.
`ShutdownComplete` and `Release` report local release independently of errors.
Remote orphan reconciliation is not retained local socket ownership.

## Independently driven multipart

`BeginMultipart(setup, lifetime, cleanup, request)` returns a session and its
unresolved root receipt. An accepted setup failure returns nil session with a
completed receipt; partial matching allocation identity and cleanup remain facts.

- Setup cancellation after successful acquisition does not cancel the retained
  session. SessionTimeout bounds lifetime; Timeout bounds each Part/Complete.
- Parts must be consecutive from 1, have an exact positive size at most PartBytes,
  and fit MaxParts/MaxTransferBytes and any declared total Size. A part below
  5 MiB must be final. Replacements, concurrent native use and automatic retries reject.
  BeginMultipart requires Size=-1 or a positive total; empty objects use Put.
- Each accepted Part borrows its Reader until child release and emits independent
  evidence. Hashes are private to the session/request.
- A failed accepted Part poisons the session and attempts one authorized abort.
  Duplicate/invalid preflight input does not silently replay or finalize it.
- Complete and Abort use the root's existing work/evidence responsibility, even
  at saturation. Complete validates exact total size and at least one part.
  Once attempted, completion is never retried automatically.
- A lost completion reply can coexist with a stored object and an acknowledged
  abort. Root Primary/Cleanup, CompletionAttempted and AbortAcknowledged stay separate.
- Close or lifetime cancellation stops the session, waits for local users and
  attempts one abort using the original separately supplied cleanup context.
  Failed/expired cleanup retains uncertainty but does not hold an idle source forever.

Known allocation IDs are adopted only after matching bounded XML identity checks.
HTTP 200 alone is insufficient for multipart/copy success; wrong roots, embedded
errors, malformed trailing documents and completion identity failures reject.

## Incremental enumeration and opaque markers

`Enumerate(setup, lifetime, request)` retains the contextual SDK iterator.
Next returns at most MaxEntries observations per child result. It may require an
additional empty Next to discover EOF after an exactly full output. Inspect
Complete, not merely an empty slice or nil error.

Version enumeration retains both native markers and does not restart after the
last lexical key. Repeated marker pairs, malformed terminal flags/identity and
out-of-scope objects reject. **Marker ordering is not assumed:** fresh service
testing disproved a lexical-monotonicity check. The integration forwards the SDK's
continuation rather than constructing a restart key. Enumeration is not a snapshot.

Before native decoding, all listing routes bound pages to 1000 entries and enforce
bounded XML structure. This is separate from MaxEntries, which requests/returns
smaller chunks. Recursive CommonPrefixes and unrequested Grant/UserMetadata/UserTags
extensions reject instead of creating objects or unbounded native collections.
Bucket identity, multipart inspection identity and scalar fields must be unambiguous.
Malformed data is not successful empty output; the public receipt and independent
evidence retain the protocol failure without redispatching requests.

MaxRequests and MaxResponseBytes are cumulative for the entire cursor; MaxEntries
bounds each returned result. Exhaustion reports incomplete/limit evidence, not
EOF or a guessed durable continuation. A canceled admitted fetch terminates the
cursor because the SDK owns one retained context. Close joins iterator, dials and
response bodies before releasing the root.

ListUploads uses the original bounded, validated response to preserve opaque
upload IDs; it never reverse-decodes an already changed ID. If the SDK itself
raises an escape error on an opaque marker, the original cause remains alongside
validated recovered entries and continuation. That is inspectable partial/error
evidence, not an excuse to drop the native error or guess an ID.

## Restricted delegation

Settings grants `PresignGET`, `PresignHEAD` and `PresignPUT` independently;
PUT also requires Writes. Requests permit only GET/HEAD/PUT, authorized address,
whole-second Expiry and MatchETag or PUT-only IfAbsent. PUT cannot select an
existing version. No raw method/header/query/host escape is exposed.

Issuance performs no network I/O after source construction. A Delegation explicitly
extracts the sensitive URL and copied required headers. It is not an object
read/write ACK. Requested expiry is only an upper limit: static token validity,
credential revocation and service authorization can shorten it. Closing or
replacing the issuing client does not revoke an issued capability.

Native transfer/admission limits govern issuance and Adapter operations, not bytes
later sent by an external bearer-capability user. Default formatting/logging hides
keys, payloads, credentials, native error text and URLs. Runtime JSON persistence
or reconstruction rejects; explicit extraction remains caller responsibility.

## Defaults, reservations and Framework composition

Zero defaults match the native profile: four active calls, no queued calls,
64 MiB transfer, 8 MiB retained read, 5 MiB parts, 64 parts, 128 entries,
1 MiB control responses, 128 data requests, 30s operation/admission and 5s cleanup.
New defaults are 5m session lifetime and 15m maximum presign expiry.
SessionTimeout accepts 1ms..1h; MaxPresignExpiry accepts whole seconds from 1s..7d.
Grants default false. The native root permits one active nested child.

For multipart, MaxRequests is cumulative across setup/parts/Complete; one additional
abort request is reserved independently. MaxResponseBytes is per multipart phase.
For ordinary operations it remains the aggregate per-call response bound; for a
cursor it is cumulative. Request-body closure precedes buffer reuse.

Recommend derives native envelopes from the same normalized settings and includes
a source owner, retained roots, private evidence, public copies, one incremental
child and queued roots. These are declared accounting envelopes, **not measured
RSS or production capacity recommendations**. Caller-retained copies and arbitrary
borrowed error graphs are not a global memory ceiling.

For public resource composition, Build calls Open and returns an
`Instance[Handle]{Value: owner.Handle(), Release: owner.Release}`, including a
non-nil instance on partial acquisition errors. Using borrows one Fixed/Follow
generation per root and retains it through descendants and cleanup. A source whose
effective budget exceeds Using's frozen envelope rejects before native dispatch.
Use `Policy.ForGenerations` to account for allowed overlap; failed replacement
does not replace the last-good generation. No project-generator profile or
Framework core provider registry is added.

See the executable
[independent consumer](../../../../../../adapters/objectstore/minio/v1/testdata/consumer/main.go),
[Fixed/Follow tests](../../../../../../adapters/objectstore/minio/v1/integration_test.go),
and [verification](verification.md). Facility 0x140 belongs to
`fathomry/objectstore_minio`; English/zh-CN resources are included in the existing
offline CLI catalog, without constructing a service client.
