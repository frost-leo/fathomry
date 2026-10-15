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

# Managed Mail notifications

[Documentation](../../../../../README.md) / Public package reference

**Audience:** independent Go consumers and Framework composition authors.

**Status:** implemented local preparation, isolated SMTP/TLS and local-process
routes; real delivery and recipient rendering require separate authorization.

**Package:** `github.com/frost-leo/fathomry/adapters/notification/mail/v1`.

## Start with explicit owners

1. Declare loadable `Settings`: source name, endpoint, HELO, TLS and authentication.
2. Use `Recommend` for data-only budgets, or `Prepare(settings, NativeOptions)`
   and `Prepared.Policy` for the exact frozen native-aware selection.
3. Construct the existing public Runtime and `Inbox[Result]` from that policy.
   Supply them as `Dependencies`; the adapter never owns or closes them.
4. `Prepared.Open` creates local source ownership without dialing. Its context
   owns the full source lifetime. Keep any non-nil Owner, including on error.
5. Use the non-owning Client. Read returned Result/error **and** drain independent
   evidence with `NextReleased` (or Framework's Receiver), then Ack it.
6. Close the Owner before joining the shared Runtime. A timed-out Close retains
   the same owner; observe `ShutdownComplete` or continue Close/Release.

`Prepare`, `Validate` and `Recommend` never dial, execute delivery processes
or invoke borrowed callbacks. Validate checks data; Prepare also checks runtime
declarations such as a custom authentication factory. A data-only recommendation
does not include a later arbitrary TLS configuration: prepare that selection first.
Preparation copies mutable containers and fixes Internal defaults once. Open
uses that same selection. Passing a second native declaration to Prepared.Open
is rejected.

An Open canceled after public admission can return no Owner while retaining a
released failed-open record in the caller's Inbox. A nil Owner is not evidence
that no call was admitted. Observe that record after Runtime completion; do not
wait for a record when admission itself was rejected and no reservation exists.

Budget.AdmissionTimeout bounds the outer public admission queue, including Open.
That timer never replaces the accepted source's lifetime context. Compose selects
the shortest prepared source timeout, and Using refuses a generation with a
shorter source deadline than its frozen declaration. Public admission, Internal
admission and execution are separately bounded phases; a caller deadline can cap
their aggregate. Close waiting is independent of the single owned cleanup stack,
including a cooperative QUIT response hook that outlives a wait deadline.

The [public-only consumer](../../../../../../adapters/notification/mail/v1/testdata/consumer/direct_test.go)
is compiled and executed in an independent module. It constructs real loopback TLS
peers and parses their MIME independently; there is no real mail service involved.

The [welcome application](../../../../../../adapters/notification/mail/v1/testdata/welcome/README.md)
is a second independently compiled public consumer. It preserves the welcome
layout and optional four-CID repository report, adds an admitted welcome pack,
and uses the same preparation/export path for offline previews and explicitly
guarded sends. Its isolated tests exercise welcome payloads through MIME, TLS,
partial effects, DSN/MDN, batches, signatures, imports and fake sendmail. It owns
presentation and optional renderer tools; neither public nor Internal libraries
gain a report engine. The fixture's checklist distinguishes these cases from
authentication-family and retained-generation controls elsewhere.

## Composition and evidence

Owner.Client and Owner.Handle cannot close or reconfigure the source. WithID
returns an immutable correlation view, not new capacity. Using accepts an existing
typed `resource.Ref[Handle]` and a frozen Budget. Fixed/Follow roots borrow one
generation through actual work and evidence transfer. Larger adopted profiles
are refused before dispatch rather than undercharged.

Use Compose with **all concurrently owned preparations**, repeating equal
preparations for equal physical owners. Source records and idle sessions remain
charged through actual release, including retired generations. The
[Framework fixture](../../../../../../adapters/notification/mail/v1/testdata/consumer/framework_test.go)
demonstrates Fixed, Follow, replacement during an in-flight DATA exchange, partial
construction and independent sink redelivery.

Finite public methods are synchronous. Their producer stack retains actual work;
they do not pretend an arbitrary Reader, template function, transform, TLS hook
or custom Auth implementation can be forcibly stopped by a deadline. A separate
source Close wait may expire while such work remains owned. The
[blocked-reader control](../../../../../../adapters/notification/mail/v1/testdata/consumer/lifetime_test.go)
exercises this distinction. Borrowed callbacks must be bounded, cooperative,
concurrency-safe, non-panicking and must not mutate or retain scoped authority.

Evidence is reserved before side effects or runtime callbacks. Each public root
binds a bounded private Internal Inbox, transfers its exact released record, then
releases Internal custody. Public Retry requeues the same facts; it never
re-prepares or resends mail. Ack is only local custody release, not a transport,
recipient, durable-store or business acknowledgement.

The live source's own record can be queued but not released. NextReleased skips
it while finite records are ready, then waits if only that source remains. Do not
use the total queued count to drain every record before closing the source:
acknowledge the expected finite records, close/join the Owner, then acknowledge
its source record. Inbox.Inspect is custody accounting, not a readiness predicate.

## Messages and technical preparation

NewMessage copies bounded Content. Omitted ID/date are generated once and remain
stable in that Message; they are not deduplication keys. Empty subjects/bodies,
valid quoted/apostrophe/international mailboxes and repeated recipients are
preserved. From/recipients are required for sending, not for constructing MIME.
EnvelopeFrom empty derives from From; `<>` selects a null reverse path.
ReplyToList, MDN, To/Cc/Bcc and the SMTP envelope have separate meanings.

Headers and Fields support non-structural standard/custom headers, ordered values
and explicit preformatted values. Use them for organization, priority, threading,
bulk and user-agent fields. Unsubscribe constructs the native list/one-click
headers. Structural/address/MIME headers have dedicated fields; Bcc/Resent-Bcc,
control-character injection and conflicting case-insensitive field declarations
are refused. BCC stays in the envelope, never a MIME header.
Native general-header values are comma-space joined, not emitted as repeated
field lines. File Fields preserve their separate MIME-header values.
Subject is literal text unless SubjectPreformatted explicitly selects encoded
RFC 2047 words. Import uses that explicit representation to preserve charset
labels, including labels unknown to this process, rather than reinterpret bytes.

Parts provide arbitrary leaf content types, descriptions, charset labels and
quoted-printable/base64/7bit/8bit encodings. Data is already in the declared
charset; no transcoding is inferred. Text/HTML are UTF-8 convenience alternatives.
Attachments and Inline preserve opaque bytes and RFC2231 filenames. Files do not
execute or validate document contents. Explicit PGP modes build two-part
encrypted/signed MIME envelopes; signed mode requires PGPHash. Caller-supplied
PGP payloads are not encrypted, signed or verified by this integration.
Content-IDs are bounded opaque labels, including native filename-based IDs without
an @; angle brackets, path separators and controls are refused. Callers own valid
CID URI references and their escaping, not the adapter's filesystem.
An explicit Boundary names the unsigned outer multipart only; nested boundaries
are independently generated. Parameters are quoted and collisions reject. 7bit
is identity encoding, not the SDK's mislabeled quoted-printable fallback. Raw
7bit/8bit output must contain no NUL or bare CR/LF; SMTP cannot silently change
bytes after signing. Quoted-printable remains the default text encoding.

PrepareMessage is an **admitted operation**, not source preparation. It can:
- Acquire bounded Input bytes, a borrowed Reader, an explicit absolute Path,
  FS/Name, or a scoped Writer callback. Acquired files are closed on every path;
  borrowed readers remain caller-owned. A scoped Writer is revoked after return
  and retains an overflow error even if the callback ignores it.
- Append body PartInput, attachments/CID resources and supplied text/HTML named
  templates. Templates use bounded output writers before native materialization.
- Apply uniquely named content transforms in FIFO order, with an explicit skip.
  Each transform sees detached content and its output is revalidated/frozen.
- Sign with an immutable DKIM or S/MIME key owner. Key PEM is bounded and parsed
  outside Settings; each operation creates fresh native signing state.

NewDKIM supports the SDK's RSA/Ed25519 keys, canonicalization, AUID, signing/
oversigning headers, timestamp, expiry and optional body-length limit. A positive
body limit intentionally leaves an unsigned suffix and is discouraged. Native
v0.8.1 rejects arbitrary crypto.Signer implementations; no production verifier is
invented. NewSMIME supports RSA/ECDSA certificate/key pairs and the first
intermediate, matching the SDK. Signing does not establish certificate trust.
Unsigned MIME is bounded before signing; all output is bounded afterward.
The selected SDK's unchanged, private
[CMS signing primitive](../../../../internal/notification/mail/v0/internal/pkcs7/interface.md)
signs the exact corrected MIME entity. OpenSSL controls verify that emitted and
signed bytes match, including 7bit, attachments, PGP and RSA/ECDSA.

NativeOptions.AlwaysDKIM applies to SMTP submission and overrides managed
per-message DKIM, matching the native client option. Prepared messages retain
their unsigned bytes for that substitution; their observable EML never mutates.
Explicit caller-supplied DKIM header fields are not deleted. Direct/Fixed/Follow
calls use the signer from their original source generation. AUID is validated
against the signing domain and safely encoded as a tag value.

PreparedMessage and EMLCopy expose immutable preparation plus detached sensitive
wire bytes. Export borrows a Writer through actual return, never closes it and
reports partial counts. For file export, the caller explicitly owns the file and
its final Close; bytes.NewReader over EMLCopy is an independent seekable reader.

Import bounds bytes and MIME structure with standard-library parsing. It exposes
HeaderChanges for top-level fields added, dropped or changed. Native import
drops Reply-To and unknown headers, normalizes MIME, and can invalidate existing
signatures. It is **not arbitrary lossless EML or signed-message round-tripping**.
The managed parser retains the selected native header profile while avoiding its
unquoted-filename panic, semicolon/RFC2231 filename damage and stale nested
boundary state. ImportOptions independently controls the envelope and signing.
Absent attachment/inline filenames use the native generic.attachment default.
Prepared opaque EML cannot be mixed with content transforms; reimport/rebuild
explicitly rather than silently changing signed bytes.

## SMTP, authentication and local delivery

TCP and explicit Unix sockets share verified implicit TLS or mandatory STARTTLS.
An explicit AllowPlaintext plus TLSMode=none permits unauthenticated plaintext;
credentials are never sent over that mode. No port or TLS downgrade is implicit.
An explicit AllowPlaintext plus TLSMode=opportunistic upgrades when STARTTLS is
advertised and otherwise permits unauthenticated plaintext. Authentication still
requires verified TLS. FallbackPort permits one additional TCP/implicit-TLS dial
after an initial transport failure, using the same TLS mode, host verification,
context and incoming-byte envelope. It never retries SMTP or a message. SessionInfo
reports the actual port/connection-attempt count, and ConnectionErrors retains
the initial failure even when fallback succeeds.
NativeOptions supplies explicit dial/TLS dependencies under the original call's
lifetime. TLS containers, including optional OCSP/SCT/algorithm/CA indices, are
bounded before cloning and counted in source residency. Immutable certificate
payloads shared by CertPool and external keys/caches/callbacks remain borrowed.

Authentication supports none, PLAIN (including authorization identity), LOGIN,
XOAUTH2, CRAM-MD5, NTLM/domain, SCRAM SHA-1/SHA-256 and PLUS, custom exchange
factories, auto selection and ordered preferred selection. Preferred fallback
requires AuthFallback. The selected mechanism must actually be advertised.
Challenge/response size, exchange count and SCRAM iteration checks precede native
expensive work. PLUS uses the negotiated TLS channel binding. Credentials do not
refresh through hidden HTTP traffic.

Session performs bounded connection/authentication, extension inspection,
NOOP/RSET/VRFY and optional session closure without exposing owning SMTP handles.
SkipNoop, SkipReset and DisableSMTPUTF8 are explicit. SMTPUTF8/8BITMIME requirements
and requested DSN support are checked before MAIL. DSNReturn/DSNNotify set native
RET/NOTIFY requests; MDN requests never imply reading.

ResponseHandlers are copied per-command source declarations, with an optional
default. Their scoped reader may consume bounded recovery lines but cannot write,
close or retain a live connection. Nil asks the SDK to read an actual replacement
response. ResponseErrors preserves original errors even after successful recovery.
Raw credential/body debug logging is not installed; use the existing payload-free
Observer and deliberate native-error/session inspection.

SendBatch defaults to fail-fast. ContinueOnError advances to later positions,
closing failed sessions and reconnecting for later messages when needed, without
resending a previous position. A connect/budget/cancellation failure stops the
remaining suffix. The whole batch shares one execution/incoming-byte envelope.

Sendmail requires an explicit absolute executable and bounded arguments. Known
envelope/mode override flags, including -r and combined -t forms, refuse. It adds
`-oi -f sender -- recipients`, including BCC, without a leaking Bcc header or
header-driven `-t`. It invokes no shell or PATH discovery. Captured stderr is
bounded and drained concurrently; all post-Start paths join Wait and stdin work.
ProcessInfo separates Start, observed exit, input bytes and truncated stderr.
A zero exit is local process success, **not SMTP 250 or per-recipient acceptance**.
Only fake capture processes are used by tests.
Executable-specific arguments are explicit caller authority; argv preservation
does not certify that an arbitrary program honors sendmail semantics.

## Results, limits and errors

Each Delivery retains input position, sender, recipient RCPT facts, last stage,
code/enhanced code, original error and effect:
NotAttempted, NotAccepted, Unknown or Accepted. Accepted means relay 250 after a
successfully flushed DATA terminator. It survives RSET/QUIT/close failures.
ServerResponse deliberately exposes the bounded successful DATA reply text,
including potentially private queue identifiers; it is empty without that reply
and survives later cleanup failure. It is not emitted by normal fmt/slog/JSON.
A successful response may itself have empty text; Effect/Code determine acceptance.
Lost acknowledgements remain Unknown. Recipient.Accepted is only RCPT acceptance.
Result.Attempts counts observed SMTP message-submission entries, not process
starts, authentication commands, readers, business attempts or delivered recipients.

The [Internal contract](../../../../internal/notification/mail/v0/interface.md)
owns effective defaults and bounds. Typical defaults are two active calls,
no queue, 16 messages/batch, 100 recipients/message, 4 MiB input, 8 MiB MIME,
256 KiB incoming wire, 30-second admission/execution phases and three-second
source cleanup. Source/evidence/work budgets are declared envelopes, not measured
RSS or cross-worker quotas. Caller-retained immutable messages/keys and arbitrary
callback storage are outside those private reservations.

Public errors use notification facility 0x2C0 and existing bounded error bridging.
Results, primary and cleanup errors remain distinct; no code prescribes resend,
reconciliation, Run/Item disposition or success. Normal fmt/slog is redacted and
runtime JSON refuses; Settings JSON is deliberately sensitive. Intentional
errors.Is/As or explicit result inspection may expose private native text.

[Source](../../../../../../adapters/notification/mail/v1/doc.go),
[independent consumer harness](../../../../../../adapters/notification/mail/v1/integration_test.go),
[Internal controls](../../../../../../internal/notification/mail/v0/capabilities_test.go)
and [authentication peers](../../../../../../internal/notification/mail/v0/authentication_test.go)
provide executable evidence. Linux process/Unix and TLS 1.2/1.3 controls do not
certify all operating systems, relay deployments, recipient rendering or live
delivery. This issue adds no report engine, outbox, automatic resend or scheduler.
