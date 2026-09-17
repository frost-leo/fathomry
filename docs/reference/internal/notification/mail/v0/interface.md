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
**Status:** implemented bounded SMTP/MIME profile; not a public mail API, mailbox
client, chart renderer or production delivery guarantee.
**Package:** `github.com/frost-leo/fathomry/internal/notification/mail/v0`.

## Responsibilities and call sequence

1. `Select(OptionsV1, layers...)` validates and freezes named, versioned settings
   without network I/O. Version zero selects configuration format 1.
2. Attach `resource.WithLimits(selection, LimitsV1(options))` and assemble it.
   Configuration overlays require corresponding resolved limits; Bind checks
   that the policy covers the Provider's working envelope.
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
and RSET. A batch uses one connection and stops on the first failure. It never
reconnects/resends within that call. At most MaxActive connections are retained,
including idle sockets; they live until use fails or the assembly closes.

## Configuration and limits

Host, numeric Port, Hello, TLSMode and Auth are explicit. TLSMode is `implicit`
or mandatory `starttls`; there is no plaintext/opportunistic/port fallback.
TLS verifies the configured Host with TLS 1.2 or newer. Empty RootCAPEM uses
system roots; a supplied PEM replaces them. Auth is `none`, `plain`, `login`
or `xoauth2`. A selected authentication mechanism must be advertised.
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
is sent. No caller-supplied reader or callback can block construction.

## Message contracts

Content supports explicit Message-ID, From, optional EnvelopeFrom/ReplyTo,
ordered To/Cc/Bcc recipients, UTF-8 subject and plain/HTML alternatives. Subject
must be nonblank without surrounding whitespace. At least one body is required.
Both bodies produce multipart/alternative; a single body keeps its native type.
Native text-body CRLF normalization applies; attachment bytes are preserved exactly.
The default envelope sender is From's mailbox. Addresses use a bounded ASCII
dot-atom subset with optional UTF-8 display names; internationalized envelope
addresses, quoted local parts and null reverse paths are not in this profile.

Custom Headers are limited to 32 unique case-insensitive `X-` names; standard
headers and envelope controls cannot be overwritten. Header values reject control
characters. Subject encoding preserves literal encoded-words instead of allowing
recipients to decode them as a different subject. BCC is present in RCPT commands,
not the generated top-level headers. Deliberately placing private addresses in a
body/attachment is the caller's responsibility.

Encoded MIME is checked against the RFC 5322 hard limit of 998 bytes per line
before dialing. A header within the input length limit can still fail this
encoded-output limit. File metadata uses RFC 2231 parameter encoding rather than
native filename rewriting or encoded-words in MIME parameters. The `name`,
`filename` and `boundary` parameters cannot be supplied through ContentType;
filenames and multipart boundaries have one explicit owner.

Inline resources have explicit leaf filenames, MIME media types and unique bare
Content-IDs. The Provider supplies the required angle brackets; the caller owns
matching `cid:` references in HTML. Attachments use explicit names/types and
opaque bytes; empty/nil bytes mean an empty file. Both collections are limited to
16 elements. Multipart media types and malformed metadata are rejected, but the
Provider does not sniff, validate, sanitize or execute file contents.

HTML is caller-authored, not sanitized by the SMTP integration. The upper
presentation boundary owns accessibility, safe content, remote images/tracking,
chart computation, JavaScript compatibility and client rendering. No ECharts,
AMP, signing keys, public image host or renderer is provisioned. The welcome
HTML in testdata is an example, not a business-facing template API.

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
when present. This does not certify DSN/MDN support.

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
| Text/HTML alternatives, UTF-8 headers, envelope, CC/BCC, custom X- headers | Controlled |
| Generic MIME attachments and CID-related assets | Controlled, opaque caller bytes |
| Implicit TLS, mandatory STARTTLS, PLAIN/LOGIN/XOAUTH2 | Controlled; no fallback |
| Single/batch sending, connection reuse, RSET/QUIT, native errors | Controlled with independent effects/cleanup |
| DSN/MDN, DKIM, S/MIME, PGP | Not exposed; require separately qualified semantics/key ownership |
| CRAM-MD5, NTLM, SCRAM, custom authentication | Not exposed in this profile |
| Templates, middleware, file readers, raw Msg/File/SMTP handles | Not exposed; prepare bounded content upstream |
| Sendmail commands, mailbox access, campaign/retry workers | Outside this Provider |
| Chart rendering, AMP and arbitrary recipient-side JavaScript | Upper-layer/client responsibility, not SMTP guarantees |

## Evidence and upgrade gates

Loopback TLS/SMTP tests independently capture envelopes and decoded MIME, exercise
partial RCPT rejection, final rejection/lost acknowledgement, reset/quit failure,
reply bounds, cancellation, borrowing, saturation, concurrency and privacy.
The consuming-executable test verifies the actual unmodified go-mail v0.8.1
module/checksum. Missing service evidence cannot be certified by Profile alone.

An owner-authorized live test observed verified TLS/LOGIN, one SMTP 250/2.0.0
acceptance and successful cleanup using synthetic MIME content. This does not
certify recipient rendering or every relay configuration. The welcome template
also has an authorized SMTP acceptance run and separate Chrome desktop/mobile
preview evidence, not an email-client rendering certificate.

Optional repository-report fixtures capture bounded public GitHub data before
sending and render four CID charts outside the Provider: a yearly commit calendar,
weekly new stars, language-byte shares, and separate monthly Issue/PR creation
counts. The calendar uses react-activity-calendar; the other charts use ECharts.
No browser, React, chart engine or GitHub access is added to the Go runtime.
See the [fixture workflow](../../../../../../internal/notification/mail/v0/testdata/README.md).

Tests have explicit ownership: in-memory unit tests cover configuration/errors/
MIME, `*_integration_test.go` covers native protocol/lifetime composition, and
`example_test.go` uses `package mail_test` for exported calling examples.
Welcome/report behavior belongs to the standalone
[`testdata/welcome` caller](../../../../../../internal/notification/mail/v0/testdata/welcome/README.md),
with its own tests. It imports the Provider rather than sharing its private helpers.

Run from the repository root:

```sh
go test -race -count=1 ./internal/notification/mail/v0
go test -run '^Example' ./internal/notification/mail/v0
go test -race -count=1 ./internal/notification/mail/v0/testdata/welcome
go test -run '^$' -fuzz '^FuzzMessageHeaders$' -fuzztime=15s -parallel=2 ./internal/notification/mail/v0
go test -run '^$' -bench '^BenchmarkComposeMIME$' -benchmem ./internal/notification/mail/v0
```

Live sending is an explicit `testdata/welcome -send -config FILE` command, not a
test side effect. Only the private file's `fathomry_mail_gh63` section is consumed;
the file must be owner-only, bounded and explicitly enabled. The optional `-report`
directory supplies already-rendered fresh data. Never use production recipients
without authorization or repeat an ambiguous send blindly. The command does not
read/delete mailboxes or automatically disable external configuration. Core tests,
documentation examples and ordinary example-application tests send no external mail.

See [source](../../../../../../internal/notification/mail/v0/doc.go),
[composition tests](../../../../../../internal/notification/mail/v0/integration_test.go),
[standalone caller](../../../../../../internal/notification/mail/v0/testdata/welcome/main.go),
[resource ownership](../../../resource/interface.md),
[invocation evidence](../../../invocation/interface.md), and
[issue #63](https://github.com/frost-leo/fathomry/issues/63).
The owner's implementation-session scope refinement leaves chart presentation
above the SDK. Requalify native flush, TLS/auth, privacy, error and lifetime
counterexamples before dependency upgrades.
