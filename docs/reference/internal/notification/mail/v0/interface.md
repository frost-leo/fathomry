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

# Mail interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** in-module composition and notification adapters.
**Status:** implemented technical preparation, SMTP and explicit local delivery;
not a mailbox client, chart renderer or production delivery guarantee. Public
consumers use the [Mail Adapter](../../../../adapters/notification/mail/v1/interface.md).
**Package:** `github.com/frost-leo/fathomry/internal/notification/mail/v0`.

## Responsibilities and call sequence

1. `PrepareV1(OptionsV1, layers...)` freezes exact data/native selections without
   I/O or borrowed callbacks. Version zero selects configuration format 1.
2. Attach `resource.WithLimits(prepared.Select(), prepared.Metadata().Limits)`
   and assemble it. Metadata owns the actual resident/work/evidence arithmetic.
   `Select`/`LimitsV1` remain convenience routes without later overlays;
   `DataBudgetV1` validates data-only settings, not runtime declarations.
3. Composition creates and owns an `invocation.Inbox[Result]`, then calls `Bind`.
   The optional observer receives payload-free diagnostics, not required evidence.
4. `NewMessage(Content)` copies bounded caller input. `Send` and `SendBatch`
   reserve evidence and shared admission before MIME construction or dialing.
5. Inspect the receipt and independently receive/release the inbox record.
   Handling an error does not release evidence capacity.
6. Close the owning assembly with a caller-owned cleanup context. A borrowed
   Client cannot close the source or create another connection/admission quota.

The facade owns no background sender, retry queue, browser or authentication HTTP
client. Connections are opened lazily and reused only after successful submission
and RSET unless SkipReset explicitly omits it. SendBatch stops on the first failure.
SendBatchWithOptions can advance after composition/transaction errors, reconnecting
for later positions without resending earlier ones. At most MaxActive connections are retained,
including idle sockets; they live until use fails or the assembly closes.

## Configuration and limits

Host, TCP Port, Hello, TLSMode and Auth are explicit. Network can select an explicit
Unix SocketPath. TLSMode is `implicit` or mandatory `starttls`; unauthenticated
`none` requires AllowPlaintext. There is no implicit TLS/port/credential fallback.
Explicit `opportunistic` plus AllowPlaintext upgrades when advertised, without
permitting unverified authentication. FallbackPort allows one same-mode transport
fallback before SMTP starts; initial errors and actual connection attempts remain
observable. It is not a message retry or a mode downgrade.
TLS verifies the configured Host with TLS 1.2 or newer. Empty RootCAPEM uses
system roots; a supplied PEM replaces them. Auth is `none`, `plain`, `login`
or `xoauth2`, CRAM-MD5, NTLM/domain, SCRAM SHA-1/SHA-256 and PLUS, custom,
auto or ordered preferred selection. Preferred fallback requires AuthFallback.
A selected authentication mechanism must be advertised.
Username/password or externally obtained OAuth token are frozen. Replace the
source for credential rotation; no unaccounted refresh callback is installed.

| Limit | Default | Supported range |
| --- | --- | --- |
| Active calls / sockets | 2 | 1–16 |
| Waiting calls | 0 | 0–32 |
| Messages per batch | 16 | 1–64 |
| Recipients per message | 100 | 1–100 |
| Total unencoded input per message | 4 MiB | 1 KiB–16 MiB |
| Encoded MIME per message | 8 MiB | 1 KiB–32 MiB |
| Incoming wire per batch | 256 KiB | 4 KiB–2 MiB |
| Admission / execution timeout (each) | 30 s | 1 ms–5 min |
| Source-close timeout | 3 s | 1 ms–1 min |

Durations in settings/overlays are nanoseconds. Admission and execution have
separate bounded phases; supply a caller deadline to cap their aggregate time.
Incoming wire accounting includes
TLS handshake/record overhead and is reset for each call. Working/evidence
reservations are conservative process-local declarations, not heap/RSS or
distributed account quotas. Frozen caller-owned Message storage is outside the
source quota. Native MIME construction may allocate within the input reservation
before the encoded-output writer refuses an oversized result; no oversized MIME
is sent. Explicit admitted preparation may borrow readers, writers, templates and
callbacks. They must cooperate; cancellation does not forcibly terminate Go code.
Handles, permits and evidence remain owned through actual return.

MaxAuthBytes defaults to 16 KiB (1–64 KiB); MaxSCRAMIterations defaults to 100000
(1–1000000). Authentication is limited to 16 exchanges. Challenge structure,
nonce/salt and iteration limits are checked before native context-free PBKDF2.
Input/template acquisition is bounded before eager native materialization. Owned
FS/files are opened once and always closed; borrowed readers remain caller-owned.
MIME input/output and frozen results participate in the declared work/evidence
envelopes. Key/callback storage owned by callers is not a private RSS reservation.

## Message contracts

Content supports explicit or generated Message-ID/date, From, optional
EnvelopeFrom/ReplyTo/ReplyToList, ordered To/Cc/Bcc, UTF-8 subject and plain/HTML.
Empty subjects/bodies are valid. Empty recipient sets are valid for preparation,
but Send rejects them. Repeated recipients and Message-IDs are not deduplicated.
Both bodies produce multipart/alternative; a single body keeps its native type.
Native text-body CRLF normalization applies; attachment bytes are preserved exactly.
The default envelope sender is From's mailbox; `<>` selects a null reverse path.
Addresses use standard mailbox parsing with quoted/apostrophe/international
addresses. International envelopes require negotiated SMTPUTF8.

Headers/Fields share 32 case-insensitively unique non-structural names. Fields
preserves native ordered values (comma-space joined) or one preformatted value.
Dedicated address/MIME/envelope controls cannot be overwritten. Values reject control
characters. Subject encoding preserves literal encoded-words instead of allowing
recipients to decode them as a different subject, unless SubjectPreformatted
explicitly selects an already encoded field. Import preserves those encoded words
and their charset labels rather than decoding and rejecting unknown labels.
BCC is present in RCPT commands,
not the generated top-level headers. Deliberately placing private addresses in a
body/attachment is the caller's responsibility.

Encoded MIME is checked against the RFC 5322 hard limit of 998 bytes per line
before dialing. A header within the input length limit can still fail this
encoded-output limit. File metadata uses RFC 2231 parameter encoding rather than
native filename rewriting or encoded-words in MIME parameters. The `name`,
`filename` and `boundary` parameters cannot be supplied through ContentType;
filenames and multipart boundaries have one explicit owner.

The selected SDK mutates general-header input slices, misencodes 7bit as
quoted-printable, ignores its QP file option, and emits/reuses explicit boundaries
incorrectly. The managed route clones native inputs, selects File.Enc directly,
normalizes bounded MIME before signing, and keeps nested boundaries distinct.
Its 7bit correction applies only to known native output, never to genuine EML
input or already-frozen bytes. The scoped standard EML parser avoids native
filename slicing and reconstructs the declared retained-header profile.
Missing attachment/inline filenames use native's generic.attachment default.
The unchanged private CMS helper signs those exact corrected entity bytes;
its source/license record is [here](internal/pkcs7/interface.md).
AlwaysDKIM is frozen source authority that overrides managed message signing
only on SMTP submission, without mutating caller messages or replacing explicit
caller-supplied header fields.

Inline resources have explicit leaf filenames, MIME media types and unique bare
Content-IDs, including native filename-based labels without @. Brackets, controls
and path separators are refused. The Provider supplies the angle brackets; the caller owns
matching `cid:` references in HTML. Attachments use explicit names/types and
opaque bytes; empty/nil bytes mean an empty file. Both collections are limited to
16 elements. Multipart media types and malformed metadata are rejected, but the
Provider does not sniff, validate, sanitize or execute file contents.

HTML is caller-authored, not sanitized by the SMTP integration. The upper
presentation boundary owns accessibility, safe content, remote images/tracking,
chart computation, JavaScript compatibility and client rendering. No ECharts,
AMP, signing keys, public image host or renderer is provisioned. The
[public welcome consumer](../../../../../../adapters/notification/mail/v1/testdata/welcome/README.md)
owns its authored presentation; it is not a business-facing template API.

NewMessage copies slices, maps and bytes. Callers must not mutate input during
construction, or the batch slice during SendBatch. Messages and completed results
can then be read concurrently. Result accessors return independent slice storage;
native errors remain read-only, deliberately inspectable causes. Runtime values
reject JSON serialization/reconstruction and redact normal fmt/slog projection.

## Outcomes and failures

Setup/admission failures return an error without a receipt or native send.
Admitted failures live in the receipt and independent inbox: inspect
`Outcome.Primary`, `Outcome.Cleanup`, and every `Result.Messages()` entry.

| Effect | Evidence |
| --- | --- |
| NotAttempted | MAIL was not entered for this message |
| NotAccepted | Submission did not reach DATA completion, or a final 4xx/5xx explicitly rejected it |
| Unknown | Body/completion may have reached the relay without a trustworthy final acknowledgement |
| Accepted | The relay returned 250 after the DATA terminator |

Recipient snapshots retain address/order, attempted/accepted RCPT state and
captured rejection codes. RCPT acceptance is not DATA acceptance. Code zero means
no exact code captured; successful RCPT reply codes are not exposed by the selected
native method. Delivery includes message identity, envelope sender, last submission
stage, exact DATA/error code and a syntactically valid leading enhanced status
when present. ServerResponse deliberately exposes the successful DATA reply text,
bounded by the incoming-wire budget, including potentially private relay queue IDs.
It is empty without successful DATA acknowledgement and survives later cleanup.
DSNReturn/DSNNotify and MDN explicitly request notifications; they
do not establish recipient delivery or reading. Session exposes actual extension,
TLS/authentication, NOOP/RSET/VRFY and session-close facts without native ownership.

Accepted remains accepted if RSET/QUIT/close later fails. Unsent suffixes remain
NotAttempted after a partial batch. A cancellation callback closes its socket and
is joined before reuse or admission release. Stopping caller waiting is not a
remote rollback. Exact attempt counts concern entries into message submission,
not DNS/TCP/TLS commands or higher-level business attempts.

The implementation uses go-mail MIME and SMTP components, not the mutable
high-level Client.Send status. In v0.8.1, DataCloser.Close discards terminator-flush
errors, so the Provider explicitly checks that flush before reading the final
reply. It always retains raw-close authority after native QUIT failures.
Execution and source-close failures retain cancellation causes and
`context.DeadlineExceeded`, including socket deadlines that fire before the
corresponding context timer is scheduled.

Native text/cause inspection may reveal private data; ordinary fault diagnostics
do not. No error or Message-ID implies retry safety, inbox placement, reading,
durability, idempotency, or a Run/Item terminal decision.

## Native capability classification

| Family | Classification |
| --- | --- |
| Text/HTML/generic parts, charset/encoding, standard headers, envelopes and CC/BCC | Controlled immutable data |
| Generic MIME attachments and CID-related assets | Controlled, opaque caller bytes |
| Implicit TLS, mandatory STARTTLS, PLAIN/LOGIN/XOAUTH2 | Controlled; no fallback |
| Single/batch sending, connection reuse, RSET/QUIT, native errors | Controlled with independent effects/cleanup |
| DSN/MDN, DKIM, S/MIME | Explicit requests and immutable-key signing; independent verification is a test, not a new native verifier API |
| PGP MIME | Corrected two-part structure with caller-owned opaque cryptographic payloads; no PGP cryptography |
| CRAM-MD5, NTLM, SCRAM/PLUS, custom/auto/preferred authentication | Bounded advertised selection and actual TLS channel binding |
| Files/FS/readers/writer callbacks, templates and transformations | Admitted bounded acquisition; no native Msg/File/SMTP owner escapes |
| EML import/export | Bounded native normalization with HeaderChanges; Reply-To/unknown headers may be lost, not arbitrary lossless round trips |
| Sendmail | Explicit executable/envelope, bounded drained pipes and joined process; process exit is not SMTP acceptance |
| Raw debug callbacks, mailbox access, campaigns and automatic retry | No owning/debug escape or business engine |
| Chart rendering, AMP and arbitrary recipient-side JavaScript | Upper-layer/client responsibility, not SMTP guarantees |

## Evidence and upgrade gates

Loopback TLS/SMTP tests independently capture envelopes and decoded MIME, exercise
partial RCPT rejection, final rejection/lost acknowledgement, reset/quit failure,
reply bounds, cancellation, borrowing, saturation, concurrency and privacy.
The consuming-executable test verifies the actual unmodified go-mail v0.8.1
module/checksum. Missing service evidence cannot be certified by Profile alone.

The historical #63 owner-authorized live test observed verified TLS/LOGIN, one SMTP 250/2.0.0
acceptance and successful cleanup using synthetic MIME content. This does not
certify recipient rendering or every relay configuration. The welcome template
also has an authorized SMTP acceptance run and separate Chrome desktop/mobile
preview evidence, not an email-client rendering certificate.

The technical regression suite uses isolated peers and fake local processes. Its
capability, authentication and completion regressions independently verify signing,
FS cleanup, malformed/excessive SCRAM challenges, PGP structure and process join.
That is not new live-service or cross-platform acceptance.

Tests have explicit ownership: in-memory unit tests cover configuration/errors/
MIME, `*_integration_test.go` covers native protocol/lifetime composition, and
`example_test.go` uses `package mail_test` for exported calling examples.
The minimal Internal consumer and fuzz corpus remain in
[technical testdata](../../../../../../internal/notification/mail/v0/testdata/README.md).
Welcome/report behavior and optional renderers belong to the public-only caller,
with its own independent-module tests; no browser or report engine enters Internal.

Run from the repository root:

```sh
go test -race -count=1 ./internal/notification/mail/v0
go test -run '^Example' ./internal/notification/mail/v0
go test -run '^$' -fuzz '^FuzzMessageHeaders$' -fuzztime=15s -parallel=2 ./internal/notification/mail/v0
go test -run '^$' -bench '^BenchmarkComposeMIME$' -benchmem ./internal/notification/mail/v0
```

Core tests and documentation examples send no external mail. Explicit preview/send
commands and their authorization/attempt-guard requirements are documented with
the public welcome consumer, not duplicated in the Internal calling contract.

See [source](../../../../../../internal/notification/mail/v0/doc.go),
[composition tests](../../../../../../internal/notification/mail/v0/integration_test.go),
[public caller](../../../../../../adapters/notification/mail/v1/testdata/welcome/main.go),
[resource ownership](../../../resource/interface.md),
[invocation evidence](../../../invocation/interface.md), and
[issue #63](https://github.com/frost-leo/fathomry/issues/63).
The owner's implementation-session scope refinement leaves chart presentation
above the SDK. Requalify native flush, TLS/auth, privacy, error and lifetime
counterexamples before dependency upgrades.
