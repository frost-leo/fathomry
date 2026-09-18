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

# Lark / Feishu interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** in-module composition, notification and callback adapters.
**Status:** implemented bounded notification profiles; application report delivery
and WebSocket reception were observed. CardKit write operations passed service tests.
The recipient confirmed the rendered Welcome/report. Cross-client/version rendering
and the exact-text persisted positive-ACK service probe remain unqualified. This is
not the entire SDK.
**Package:** `github.com/frost-leo/fathomry/internal/notification/lark/v3`.

The package name follows the official `larksuite` SDK. It does not select a service
region: the default remains Feishu at `https://open.feishu.cn`. Lark's endpoint
requires an explicit BaseURL and credentials for that authority. The example's
existing Feishu YAML keys and opt-in environment variables remain unchanged.

## Responsibilities and call sequence

1. `Select(OptionsV1, layers...)` freezes explicitly supplied named configuration;
   version zero selects format 1. Construction performs no network requests.
2. Attach `resource.WithLimits(selection, LimitsV1(options))` and assemble.
   Strict overlays need matching resolved limits, not the original defaults.
3. Create an independently owned `invocation.Inbox[Result]` with
   `capacity * EvidenceBytesV1(options)` bytes, then `Bind`.
4. Construct immutable `Content` / `JSON` values and call a specific operation
   with a caller-owned context and `fault.Correlation.Call`.
5. Inspect the receipt **and** receive/release the independent inbox record.
   Handling the direct error does not release required evidence capacity.
6. Close the assembly with an independently bounded cleanup context. A copied or
   borrowed Client cannot close the source, replace credentials or add quota.

These synchronous operations are Activity/ordinary Go code, not Temporal Workflow
logic. The HTTP facade owns no worker, poller, retry queue, callback server or business
report engine. The opt-in [WebSocket receiver](websocket.md) has an explicitly owned
connection lifetime. Source-owned token acquisition is lazy and shares the operation's
admission, execution deadline, HTTP-attempt accounting and byte reservation.

## Capabilities

| Family | Implemented boundary |
| --- | --- |
| Application messages | Explicit recipient kind; text, localized rich post, image, file, audio, media, sticker, shared chat/user, native interactive card/template/entity content |
| Message lifecycle | Send, reply/thread reply, text/post edit, card patch, recall, forward, merged forward with invalid-message evidence |
| Inspection | Get original card JSON and native message flags, including merged-message parents and related descendants; one explicit chat/thread history, reader or reaction page; no auto-pagination |
| Identity | Explicit single-email lookup to a same-email app-scoped open_id; no directory enumeration or silent account substitution |
| CardKit | JSON 2.0 / template entity creation, complete card update, settings, native batch actions; element insert, replace, patch, delete and finite full-text streaming update |
| Components | Native VChart/chart_spec and table constructors; complete JSON preserves native layouts, Markdown, images, buttons, forms, localization and supported future fields |
| Assets | Bounded image/file upload, same-app download and received-message resource download; CSV/PNG report example |
| Reactions | Add, list and delete application emoji reactions |
| Signed custom bot | Fixed HTTPS webhook; text/post/image/share-chat/JSON 2.0 cards; signature and distinct 20 KiB envelope |
| HTTP reception | Signed schema 2.0 IM message events and card.action.trigger, AES-CBC/PKCS#7 decryption, identity/token checks, freshness, URL verification challenge |
| WebSocket reception | [Bounded connection, heartbeat/reconnect, fragment reassembly and explicit ACK capabilities](websocket.md), with independent event and acknowledgement inboxes |

The complete native JSON representation is intentionally not a partial Go mirror
of every VChart/component property. Syntax, ownership and size are checked locally;
the platform still validates component-specific schemas, permissions, account
availability, templates, resource ownership and client capabilities. Unknown
fields are not dropped or silently turned into images.

`Chart` preserves native declarative interaction. `ElementContent` is one finite
CardKit request, not a WebSocket stream. Components that submit actions require
an HTTP callback adapter or the opt-in WebSocket receiver; accepting JSON does not
deploy an adapter or change event subscriptions.
The example includes an unsent form/select/input template for that integration.

## Configuration, ownership and limits

Profiles are explicit: `application` acquires a self-built tenant token;
`tenant-token` uses an externally supplied app/tenant-scoped token and expiry;
`webhook` uses only a signed custom-bot URL/secret. Marketplace acquisition,
user OAuth refresh and client-assertion providers are not implemented.
Rotation replaces a source, never a mutable global Client.

The API base defaults to `https://open.feishu.cn`. Endpoint overrides require
verified HTTPS and explicit trust roots when needed. There is no ambient proxy,
redirect, TLS verification bypass, compression negotiation or plaintext fallback.
Fresh HTTP/1 connections deliberately avoid native/transport replay. This trades
connection reuse and throughput for a small, auditable no-retry profile.
Per-request TLS dialing restores the original deadline that net/http detaches,
seals late callback entry, joins the callback and closes its owned socket before
the invocation relinquishes resource use.

| Limit | Default | Supported range |
| --- | --- | --- |
| Active calls / waiting calls | 2 / 0 | 1–16 / 0–32 |
| Encoded JSON request / callback body | 256 KiB | 1 KiB–1 MiB |
| Asset input | 10 MiB | 1 KiB–30 MiB; images additionally capped at 10 MiB |
| Response body | 12 MiB | 1 KiB–32 MiB |
| Admission / execution timeout, each | 30 s | 1 ms–5 min |
| Callback timestamp tolerance | 5 min | 1 s–10 min |
| Explicit page size | 20 | 1–50 |
| Frozen JSON | 1 MiB | 32 nesting levels, 32768 value tokens |

Message envelopes also enforce 150 KiB for text and 30 KiB for other message
content; custom-bot envelopes enforce 20 KiB including the generated signature.
Native template expansion may exceed platform limits even when input is small.
HTTP response headers are bounded separately at 32 KiB. Body limits count received
body bytes, not TCP/TLS overhead. Frozen caller inputs, getter copies and application
presentation storage are caller-owned; working/evidence budgets are conservative
local reservations, not measured heap/RSS guarantees or distributed account quotas.
Server `Retry-After` remains advice, not an automatic retry action.

Durations/overlays use nanoseconds. `TokenExpiresAt` is normalized to
`token_expires_unix_seconds`; zero is absent and is invalid for `tenant-token`.
A self-built source caches at most one token, serializes refresh with cancellable
admission and refreshes before expiry. A rejected invalid token is invalidated for
the **next explicit call**, never followed by an inline resend.

No native HTTP/cache/logger/serializer/assertion callback or raw SDK handle is
exposed. Native global token managers are bypassed rather than configured through
`NewClient`/`NewCache`. A one-shot exact-method/URL permit blocks indirect native
authentication and dial retries. Native diagnostic hooks are discarded; the
optional invocation observer is separate from required result evidence.

## Input, result and failure meanings

`Content` and `JSON` own immutable strings; their byte getters return copies.
JSON preserves exact numbers and rejects duplicate keys, malformed UTF-8,
trailing values and excessive complexity. Interpreted protocol fields require
their exact native casing; aliases cannot override authentication, ACKs, event
identity, card schema or chart type. Uninterpreted native fields remain preserved.
`Upload.Data`, merged message IDs and
callback bytes are borrowed only for the synchronous method; do not mutate them
until it returns. No readers or mutable native result pointers escape.

`Result` freezes target/UUID, message/card/asset identity, explicit related and
invalid IDs, private response data, optional callback data and independent auth/
operation exchanges. APICode's presence flag distinguishes explicit zero from an
absent code. Message deletion/edit getters similarly distinguish absence.
`Messages`, `JSONData`, `Bytes` and ID-slice getters return independent storage.
Message entries and pagination are validated before their typed snapshots become
available; getters do not re-decode an unchecked response. Merged GetMessage results
must contain the requested parent and distinct related descendants, not unrelated
messages or cyclic parent links. Sparse recalled messages retain absence semantics.

- `NotAttempted`: the requested operation did not enter transport. An auxiliary
  authentication request may nevertheless have occurred.
- `Unknown`: transport was entered without a trustworthy operation ACK. Dial
  failures are conservatively included; timeout/cancellation is not rollback.
- `Rejected`: an explicit non-success platform result, excluding server-error
  HTTP status where the effect remains unknown; for Receive, a callback not
  accepted locally. It does not decide the HTTP adapter's response.
- `Accepted`: the operation's required ACK is valid. Reads and authenticated
  callback receipt use the same technical term; neither implies business effects.
- `Partial`: a merged message was acknowledged with explicit invalid input IDs.

Setup/admission failures return directly. Admitted outcomes are retained in both
receipt and inbox, with primary and cleanup failures separate. `ErrHTTP`,
`ErrAPI`, `ErrAuth`, `ErrResponse` and other fault kinds remain inspectable
under `ErrCall`. Intentional `errors.As` can inspect native `CodeError`; its
message/body is private, not a safe log value. Ordinary formatting is restricted.
Native error causes are inspection-only borrowed objects; do not mutate their
graphs. The immutable typed result does not promise arbitrary native-error copying.

UUID deduplication is bounded by the platform contract, not exactly-once delivery.
CardKit revision sequence is caller-owned and strictly increasing across every
operation on the same card. Upload, send, edit and recall are separate effects.
Recalling a message cannot erase prior visibility or delete an uploaded asset.
IM upload and CardKit expose no deletion endpoint in this profile; no physical
asset/entity purge or retention duration is asserted.

## HTTP receiving adapter obligations

Configure AppID, TenantKey, VerificationToken and EncryptKey together. The HTTP
adapter owns server shutdown, body-read limits/timeouts and duplicate-header
rejection; pass the **exact** body plus X-Lark-Request-Timestamp/Nonce/Signature.
`Receive` authenticates signed bodies before decryption, checking signatures in
constant time, fresh timestamps and app/tenant/
verification-token association. Encrypted bodies require complete JSON and strict
PKCS#7 padding, not the native decryptor's permissive brace cropping.

URL verification uses the separate token-authenticated challenge contract.
Unsigned challenge failures expose one authentication classification rather than
cryptographic/JSON validation details.
Serialize the returned Challenge with `encoding/json`; do not interpolate it.
Normal event data excludes authentication secrets. Unsupported event families and
schema 1.0 are rejected. Duplicate IDs can be delivered again: freshness is not
replay prevention. Application-owned durable deduplication and processing must
precede whatever acknowledgement policy that application requires. An inbox is
process-local evidence, not durable callback acceptance.

## Executable evidence and compatibility

- [Unit/native TLS integration tests](../../../../../../internal/notification/lark/v3)
  cover independent evidence, source borrowing, saturation, malformed ACKs,
  hidden-auth refusal, cancellation, cache isolation, native JSON preservation,
  assets, card revisions, signed webhooks and authenticated callback failures.
- [Standalone Welcome and lifecycle example](../../../../../../internal/notification/lark/v3/testdata/showcase/README.md)
  sends email-inspired welcome copy with native typography, layouts and navigation. An optional
  explicitly captured repository snapshot supplies native charts and a complete CSV.
  Fixed chart/PNG/rich-post fixtures remain separate lifecycle/test inputs.
- [Issue #64](https://github.com/frost-leo/fathomry/issues/64) tracks remaining
  qualification. Local service evidence is not published implicitly.

The consuming executable test verifies official SDK **v3.12.0** and its checksum,
not only go.mod. SDK major, configuration format, IM/CardKit API v1, card schema
2.0 and Feishu/VChart client versions are different axes. API ACKs and local
previews do not establish desktop/mobile rendering or reader comprehension.
After the owner corrected the recipient, the authorized live run delivered a native
chart/table report and CSV attachment. WSS bootstrap, handshake and ping/pong also
passed. After the owner enabled missing permissions, authorized inbound messages
were observed. After write permission was enabled, CardKit create, element changes,
batch updates, settings, finite content updates and whole-card updates passed;
a stale sequence was explicitly rejected. The styled Welcome with a freshly captured
public-repository report, native charts/table and CSV also received valid message
ACKs. These are API observations, not recipient rendering evidence. Native tables
must be placed at the body root, not nested in a collapsible container.
The recipient separately confirmed the rendered card, without specifying a client
platform/version; that observation does not qualify every desktop/mobile client.

Primary contracts: [message API](https://open.feishu.cn/document/server-docs/im-v1/message/create),
[native charts](https://open.feishu.cn/document/feishu-cards/card-json-v2-components/content-components/chart),
[native tables](https://open.feishu.cn/document/feishu-cards/card-json-v2-components/content-components/table),
[custom bots](https://open.feishu.cn/document/client-docs/bot-v3/add-custom-bot),
[pinned SDK](https://github.com/larksuite/oapi-sdk-go/tree/v3.12.0).
