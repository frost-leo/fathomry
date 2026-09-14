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

# MinIO object-storage interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** framework composition and capability maintainers.
**Status:** implemented private integration with local protocol qualification and
an owner-authorized HTTP/unversioned service fixture; not unrestricted SDK support.
**Package:** `github.com/frost-leo/fathomry/internal/objectstore/minio/v7`.

## Responsibility and call sequence

This is the MinIO Go SDK boundary, not a universal object-store interface or a
one-for-one SDK facade. Use it from process-local/Activity code, never Temporal
Workflow logic. Run input freezing, unsuccessful-target archiving, business
publication, schemas/manifests and durable evidence persistence are not supplied.

1. `Select(OptionsV1, layers...)` validates and freezes configuration without I/O.
   Attach `resource.WithLimits` before assembly; `LimitsV1` describes **defaulted
   input**, not an independently changed overlay.
2. `resource.Assemble` owns one static-credential native client and HTTP transport.
   Readiness performs bucket HEAD only; it neither lists objects nor creates a
   bucket. Readiness is not proof of write/optional-feature permission.
3. `Bind` requires the exact selection, compatible shared limits and a separately
   owned `invocation.Inbox[Result]`. A borrowing alias shares the original native
   client, identity, configuration revision, admission and lifecycle.
4. Operations validate inputs and reserve evidence/admission before native work.
   They return an asynchronous receipt. A refusal returns no receipt and does no
   native operation. After admission, inspect both the receipt waiting error and
   the final `invocation.Result.Err()`.
5. Consume independent evidence using `Inbox.Next`; releasing a delivery is the
   receiver's responsibility. Handled direct errors or failed diagnostics do not
   erase required results. Unreleased inbox entries intentionally prevent new work
   when capacity is exhausted.
6. Wait for actual operation release before relinquishing input/output ownership.
   Close the assembly after its users finish; retain unresolved shutdown reports
   and use resource's continuation rules. No consumer-facing native handle or
   source shutdown method escapes the facade.

## Supported and unsupported modes

| Family | Supported profile | Not exposed / not guaranteed |
| --- | --- | --- |
| Connection/authentication | One literal-IP URL with explicit port; fixed region; one bucket and literal key prefix; path addressing; static V4 access/secret/optional session token | DNS/automatic addressing, region discovery, ambient environment/files, IAM/STS/SSO refresh, custom credential providers, anonymous/V2 credentials, acceleration/dualstack, arbitrary transports/callbacks |
| TLS | Explicit complete PEM trust set and hostname/IP verification, TLS 1.2 minimum; HTTP only with explicit `Plaintext` | System/ambient CA discovery, insecure certificate bypass, mTLS, proxy discovery; local TLS tests are not deployment qualification |
| Read/stat | Full bounded `Read`, streaming `Download`, HEAD metadata, current or explicitly granted exact versions; exact offset/length ranges; optional ETag and expected SHA256 | Lazy native `Object`, Seek/ReadAt sessions, suffix/open-ended ranges, unbounded read-all, local file download/resume/rename |
| Upload | Known length or `Size=-1`; successful empty; small single PUT; bounded serial Core multipart with one part buffer; final If-None-Match/If-Match | Native automatic/concurrent multipart, streamed V4 signing/trailers, arbitrary part workers/hashers/progress callbacks, force/fan-out/append, automatic replay |
| Integrity | Precomputed per-request MD5 and SHA256; native full-object checksum checking when metadata supports it; separately computed digest and caller-supplied SHA256 comparison | ETag as universal MD5, early/partial/ranged/composite/absent-checksum reads as full-object verification, server ACK as persistence or independent read-back |
| Copy/compose | One whole source within the same source scope, HEAD size check, source ETag condition and optional exact version, native metadata/tag COPY semantics | Destination-conditional copy, cross-bucket copy, multipart/range copy, ComposeObject, annotations policy, snapshot guarantees for mutable metadata with the same ETag |
| Enumeration | Synchronous native recursive V2 or version iterator with bounded output, response bytes, requests and duration; lexical StartAfter | Unbounded channel producers, reverse-version buffering, V1/delimiter/owner/metadata extension selection, atomic snapshots, stable cross-call version cursors |
| Incomplete multipart | One bounded page of uploads or parts with explicit safe next markers; one explicitly requested exact-ID abort | Ambiguous URL-encoded upload continuations; enumeration as ownership proof; automatic adoption/garbage collection; independently controlled multipart construction sessions |
| Removal | Bounded serial individual DELETEs, one outcome per input, optional exact version and observed delete-marker headers | Failure-only bulk-delete inference, recursive deletion, governance bypass, force delete, automatic historical-version removal |
| Metadata/tags | Bounded copied user metadata on PUT; copied stat headers; optional bounded get/replace/remove tag sets | Arbitrary reserved headers, metadata-only self-copy/update protocol, tag-derived business identity |
| Encryption/versioning/retention | Explicit SSE-S3 PUT over HTTPS; passive header/checksum/version/delete-marker observations; server-default encryption remains server-owned | SSE-C/KMS credentials or key management, bucket encryption/versioning changes, lock/retention/legal-hold mutation or governance bypass; dedicated GetObjectAttributes/ACL/retention APIs |
| Delegation/files/notifications | None | Presigned URLs/POST forms, file helpers, background notification subscriptions or bucket notification configuration |
| Administration/specialized servers | None | Bucket create/remove/policy/CORS/lifecycle/replication/inventory/QoS controls, madmin-go, Select/Restore/Prompt/Snowball, S3 Express/directory buckets, RDMA/native buffers |

`Writes`, `Versions` and `Tags` are explicit source capability grants, not IAM
verification. Bucket names ending in `--x-s3` are rejected. Unsupported mode
fields cannot enter through an unrecognized configuration layer. Copy explicitly
rejects requested destination conditions before its HEAD.

The selected SDK is **v7.3.0**, not a fork or unreleased comparison.
Its automatic multipart options reduction drops final conditions and its parallel
hasher path has reproduced races. The integration uses the released Core
initiation/part/completion primitives with independent hashes and the original
final options. Disabling *streaming* SHA256 does not disable payload hashing:
the explicit Core hash argument signs the already buffered part. Copy-source
versions are query-escaped and ETags HTTP-quoted without changing their bytes
before the native header marshaler. Copy and list operations inspect bounded
control-response captures for their expected XML root, complete document and
embedded HTTP-200 native errors; observed HTTP status is never rewritten.
These choices have rejecting wire controls; an unsupported native path is not
certified as working.
[Pinned multipart source](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/api-put-object-streaming.go),
[Core primitives](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/core.go),
[copy marshaling](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/api-compose-object.go).

## Bounds, copying and configuration

All durations in layers are nanoseconds. Options format zero means version 1;
unknown versions reject. Source revision and SDK/module/service versions remain
different axes. Inputs are borrowed during the method call for validation/copying;
do not concurrently mutate them. After return, options/maps/slices are frozen.
Reader/writer lifetimes are the exception described below.

Defaults and ranges are defined in
[`options.go`](../../../../../../internal/objectstore/minio/v7/options.go):

- Four active calls (1–16), no queued calls (0–64).
- 64 MiB transfer limit, 8 MiB retained-read limit (at most 16 MiB).
- 5 MiB parts (5–16 MiB), at most 64 parts (1–1000), exactly one uploader per call.
  Transfer limit cannot exceed part-size times part-count. The default request
  budget can stop a differently configured larger transfer before this bound.
- 128 output entries (1–1000), independently of 1 MiB aggregate control-response
  bytes (1 KiB–8 MiB), 32 KiB HTTP response headers per exchange and 128 SDK
  exchanges (1–2048), including cleanup.
- Thirty seconds separately for admission and work (1 ms–5 min); five seconds
  for cleanup (1 ms–30 s), additionally constrained by its caller context.
- Keys/prefixes: at most 1024 UTF-8 bytes, without controls, backslashes or dot/
  dot-dot segments. Prefix matching is literal, not a filesystem hierarchy.
  Use a trailing slash when a directory-like namespace is intended.
- Up to 32 lowercase ASCII metadata names, 8 KiB combined key/value bytes;
  up to ten tags with bounded keys/values and native tag validation. Metadata
  receives an explicit `x-amz-meta-` prefix, so `content-type` cannot replace the
  actual header. TLS bundles contain only valid certificate PEM blocks and
  whitespace; malformed or unknown blocks do not silently reduce the trust set.

`MaxReadBytes` bounds retained payload; `MaxTransferBytes` bounds streamed
payload or consumed input, with at most one extra input byte to detect overflow.
Exact declared length is checked before multipart completion. A large invalid
input can already have staged parts; that is not an effect-free validation error.
Server-side copy's bound relies on the source HEAD and ETag condition, not a
measurement of server work.

Control byte limits are enforced while reading HTTP bodies **before native XML
decoding**. A one-byte overflow probe detects an over-limit response and the
remaining budget cannot underflow or be reset by another response. Native page
decoding precedes output-entry projection; entry limits do not claim to count
every native allocation. XML/object overhead, HTTP/TLS/socket buffers and caller
retention are not a process RSS limit. Admission/evidence byte reservations are
declared working/retention envelopes, not allocator telemetry or a global quota.

List responses require one explicit valid `IsTruncated` value; a missing flag
cannot manufacture complete-empty output. Version listings require actual version
IDs, and part continuations must match the last observed part. Truncated upload
pages using `EncodingType=url` with an upload marker return `ErrUnsupported`:
v7.3.0 decodes a field that S3 treats as opaque. Valid entries remain available
when native decoding succeeded; a native decoding failure may leave mixed
encoded/decoded fields, so those entries are deliberately not exposed. No
corrupted or inferred next marker escapes. Narrow the inspected prefix or use
an explicitly known upload ID rather than treating partial inspection as complete.

`Result` is immutable shared in-process evidence. Returned payload/maps/headers/
lists copy mutable storage. Returned `Object` values contain private immutable
maps; changing a returned scalar field cannot mutate the shared result.
`PartsCopy` deliberately exposes copied, handle-free native part DTOs.
Native metadata retains its native absent/zero semantics: a zero size, false
latest/delete-marker flag or empty checksum field alone is not proof of an empty
body, historical status or lack of a server checksum. `Read` retains nil payload
when none was observed and a non-nil empty slice for a consumed empty body.
Upload checksums are retained when present in the native DTO; v7.3.0's single-copy
DTO omits some parsed checksum fields, which are not invented by this integration.
Arbitrary native/caller error graphs are borrowed for deliberate inspection and
are not deep-copied or bounded by an encoded-result byte reservation.

## Cancellation, cleanup and truthful effects

The assembly owns its transport, without SDK health-check workers, credential
refresh, environment proxy/CA discovery or cookie retention. Caller context
values cannot activate hidden SDK modes; deadlines and cancellation still
propagate. Redirects do not expand endpoint authority.

`Put` borrows a reader and `Download` borrows a writer until **receipt release**,
not just until the method or a waiting context returns. They never close caller
objects. An uncooperative reader/writer can delay cancellation and shutdown
indefinitely; its active lease remains held. Do not concurrently access or close
these values unless their own contract explicitly permits cancellation that way.

Native HTTP can detach dial cancellation and close request bodies after
RoundTrip returns. The owned transport restores cancellation from the original
call, joins actual plain/TLS dials and fences late dial callbacks. It owns the
raw sockets until actual close and retains close errors at the source boundary.
Request-body reads and closure finish before a part buffer is reused. For early
replies to body-bearing requests, the bounded reply is drained/replayed before
joining the writer, avoiding a read/write deadlock without unbounded buffering.
Resource close is cooperative: its waiting context cannot force an arbitrary
native Close implementation to return.

Core GET returns the actual body synchronously. The integration consumes it,
closes it, and accounts for every captured response body, including SDK error
paths that discard close errors. A gated body close retains the call/lease and
its eventual error. There is no inference from native `Object.Close`:
that lazy native object is not used.
[Pinned GET lifecycle](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/api-get-object.go).

`Put` requires a separate caller-owned cleanup context. After a failure with a
known upload ID, including a valid ID accompanying a native decoding error,
it attempts **one** bounded abort under that context. Initiation capture must
identify the expected allocation root and exactly one matching bucket/key/ID;
wrong or ambiguous identities never authorize part writes or automatic abort.
A matched identity remains useful when a later XML syntax error occurs. Expired
cleanup context, request-budget exhaustion, failed abort and late body-close
failures remain inspectable. No context is detached and no new cleanup loop is
started. A lost initiation response can leave an unknown upload ID that cannot
be safely auto-aborted; a lost completion reply can leave a stored final object
even after an acknowledged abort. Retain the evidence for explicit reconciliation.

`NotSubmitted`, `Unknown` and `Acknowledged` describe technical observations:
- An ACK does not prove business success, crash durability or prior existence.
- A DELETE ACK for a missing key is still an ACK, not evidence it existed.
- `Transfer.Bytes` records consumed input or accepted sink bytes; the computed
  digest covers those bytes. `Verified` is true only after a supplied expected
  SHA256 matches at full consumption. Partial data remains available with errors.
- `Transfer.Complete` describes consumption/final transfer completion, not
  independent persistence or all cleanup. Check primary and cleanup errors.
- `CompletionAttempted` and `AbortAttempted` identify native method invocation;
  the request budget can still prevent their HTTP handoff. A failing sink call
  remains `Unknown`, even if it reports an invalid byte count. Simultaneous
  read/checksum and sink failures are retained rather than overwriting one another.
- `Result.Complete` on a listing means this bounded enumeration reached its
  native end; it does not freeze a changing bucket.
- Stat and deletion retain available version/delete-marker data alongside errors.
  Denial, expired credentials, failed conditions and missing output differ.
- `Attempts.Exact` counts intercepted SDK HTTP RoundTrip handoffs, including
  auxiliary pages and cleanup, **not TCP retries or accepted remote effects**.
  Native `MaxRetries=1` disables its retry loop; no outer retry is added.
  Replaying a receipt's operation can still repeat external effects.

Technical error identities live in `errors.go`. Original native
`ErrorResponse` values remain reachable with `errors.As`, including wrapped
responses; native `ToErrorResponse` does not traverse wrappers. Captured transport
causes also survive the SDK's URL-bearing EOF rewrite. An error that merely
*contains* EOF is not a successful terminal EOF. Default formatting/JSON/logging
of runtime values is restricted. Explicit metadata/error inspection can reveal
object data and must not be used as unrestricted diagnostics.

## Verification and provenance

[Verification procedure and evidence boundaries](verification.md) distinguish
protocol peers, native observations, consuming builds and service acceptance.
The actual assembly/invocation/evidence/lifecycle cooperation is exercised in
[`integration_test.go`](../../../../../../internal/objectstore/minio/v7/integration_test.go);
fault and lifecycle regressions remain beside their implementation.

Baseline: [Issue #38](https://github.com/frost-leo/fathomry/issues/38),
`develop@1937fe2bfeda05b6b9ddf5e3fc0d4879c9e199fa`, accepted
[SDK integration standard](../../../../../architecture/internal-sdk-integration.md),
S01–S11. Resource, invocation, fault, compatibility and conformance are reused;
no shared mechanism or other SDK implementation is replaced.
