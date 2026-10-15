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

# Lark / Feishu technical integration

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** in-module provider maintainers and public Adapter integration.
**Status:** implemented selected notification domain, with local qualification;
historical #64 service evidence does not certify every #142 route.
**Package:** `github.com/frost-leo/fathomry/internal/notification/lark/v3`.

## Responsibility and construction

Internal owns protocol/encoding/authentication, source identity, byte/admission
bounds, cancellation/join, native errors and technical effects. It owns no report
calculation, business recipient selection, automatic reply/retry policy or Run/Item
terminal state. Welcome/report content lives in the
[public caller showcase](../../../../../../adapters/notification/lark/v1/testdata/showcase/README.md).

PrepareV1 freezes strict versioned settings and separately borrowed NativeOptions,
without network/callback invocation. Prepared.Metadata is the authoritative effective
work/source/evidence reservation; Select constructs precisely that configuration.
The legacy Select/ LimitsV1 / EvidenceBytesV1 helpers remain, but callers using
overlays should use the resolved Prepared metadata rather than original defaults.

Attach Metadata.Limits through internal/resource, assemble, then bind a Client with
a separately owned invocation.Inbox[Result]. Finite calls return a read-only receipt;
consume and release the independent inbox record after actual work ends. Close the
assembly with independent cleanup waiting. Copies/borrowers grant no close authority.
[WSS composition](websocket.md) uses the same source and separate event/ACK inboxes.

## Native surface and fields

The pinned closed catalog contains every generated finite IM v1/v2 and CardKit v1
operation, selected Contact batch_get_id, SDK bot/v3/info and documented v4 batch
submission. Iterators are explicit repeated page calls, not hidden pagination.
NewRequest/Do preserve native path/query/body options without arbitrary endpoint
or SDK Client/Transport/Cache/Dispatcher access. Native pure builders can produce
caller-owned JSON; nested models are not duplicated.

The catalog covers message create/reply/update/patch/recall/get/list/forward/merge,
search/follow-up/all urgency; batch delete/progress/readers; reactions/BatchQuery,
pins/thread forward; chat/member/manager/announcement/menu/moderation/tab/top-notice;
all card/element operations including IdConvert; feed/app-feed/buttons/tags/relations/
URL preview; selected email/mobile/identity/resigned contact lookup and bot metadata.
Conveniences preserve their established defaults; Do exposes full native options.

JSON/Content own bounded immutable values. Exact numbers, unknown native fields,
null, empty, false and zero survive. Limits are 1 MiB, 32 nesting levels and 32768
value tokens. Duplicate keys, malformed UTF-8 and trailing JSON are rejected.
Interpreted authentication/identity/envelope keys reject case-folded aliases.
Generic fields remain platform-validated; a code-zero envelope does not prove all
optional response fields exist. ResponseJSON retains the full sensitive envelope.

Chart/Table construct native representations, not calculated reports. ComposeCard
no longer chooses blue/update_multi; ComposeCardWithOptions makes those choices
explicit. Legacy cards, JSON 2.0, templates/entities and system message content
remain distinct. UUID omission is valid. CardKit sequencing remains caller-owned.

UploadAsset bounds Bytes or Reader acquisition before native multipart encoding,
supports message/avatar purpose and optional duration including zero. Borrowed
readers remain owned until actual return and are not closed. Downloads return copied
bounded bytes. No source file/path, stream, callback or owning SDK handle escapes.

## Credentials, transport and limits

Profiles are application, marketplace, tenant-token, user-token, client-assertion
and signed webhook. User tokens are explicitly external, not an OAuth refresh
implementation. Marketplace cold acquisition performs app-token, tenant-token and
operation requests under one three-attempt bound. AuthenticationStages preserves
each auxiliary attempted/exchange/error/cleanup fact separately. Authentication()
remains a compatibility view of the last exchange, not the complete ledger.

Dynamic providers return one complete source-bound credential tuple per request.
Errors, panic, cancellation, empty/expired or wrong-source results fail closed.
Encrypted callback sources cannot become token-only through rotation. Native global
token/app-ticket managers are not used. Invalid-token results invalidate only the
next explicit operation's cache; no mutation is retried. Attempts count intercepted
managed HTTP writes, not arbitrary work inside a borrowed callback.

The default API base is Feishu; Lark requires an explicit region. Client-assertion
OAuth defaults come from the SDK's region mapping; custom API bases require explicit
OAuthBaseURL. Every endpoint requires verified HTTPS. No ambient proxy, redirect,
or unscoped callback-selected destination is used. Explicit AssertionProxyURL/Prefix
preserve native assertion delegation with separate OAuth/WSS audiences. No
compression negotiation, insecure TLS or replay is enabled. Source-owned fresh
HTTP/1 connections join dialing/reads/close before relinquishing use.

Default active/queued calls: 2/0 (ranges 1–16 / 0–32). Request/callback JSON:
256 KiB (1 KiB–1 MiB); asset: 10 MiB (1 KiB–30 MiB, images additionally 10 MiB);
response: 12 MiB (1 KiB–32 MiB). Admission/execution timeout: 30 s
(1 ms–5 min); callback age: 5 min (1 s–10 min). Encoded message/webhook limits remain
150 KiB text, 30 KiB other messages and 20 KiB signed webhook envelopes.
Durations are nanoseconds; explicit token expiry is Unix seconds.

Reservations are declared local envelopes, not physical RSS or account-wide quotas.
Source budgets include frozen configuration and managed transport residence.
Arbitrary providers/readers must cooperate; timeout cannot prove they stopped.
Dynamic credential providers and borrowed asset readers run in one synchronously
joined callback worker per admitted operation. Panic/Goexit become failures while
normal completion retains evidence and source ownership. The native work
declaration includes a 64 KiB worker allowance, not a hard Go-stack/RSS limit.
Callback goroutine affinity is not guaranteed.

## Reception and effects

Receive verifies modern HTTP callbacks; ReceiveLegacy handles the distinct legacy
SHA-1/token CardAction protocol. Token-only sources require a valid verification
token. Encrypted profiles retain SHA-256/EncryptKey signatures, freshness, strict
AES-CBC/PKCS#7, app/tenant binding and complete JSON. URL verification has a separate
token-authenticated challenge. The HTTP caller owns duplicate-header rejection,
body-read bounds, handler policy, response writing and server shutdown; the public
Adapter supplies bounded HandleHTTP composition without hosting a server.

Selected schema-2 events include IM messages/chats/members, card.action.trigger,
url.preview.get, application.bot.menu_v6 and auth.user_access_token.revoked_v4.
Schema-1 app_ticket is delivered to caller custody, never the native global manager.
Legacy CardAction contains tenant/user/message identity but no app_id to invent.
Exact payload inspection may reveal action tokens or app tickets. Unknown event
schemas/types are rejected. Freshness is not durable replay prevention.

Effects describe the exact operation: NotAttempted, Unknown, Rejected, Accepted,
Partial. HTTP 2xx alone is insufficient. Code presence is explicit; missing required
identity/data is not acceptance. Partial urgency users, merged message IDs and legacy
batch target namespaces retain different meanings. PartialFields also retains
chat membership, reaction, feed/button and tag scopes; pending approval is not
rejection. Partial does not prove that other targets succeeded. Cancellation is
not rollback. An API ACK, callback authentication,
local WSS write, evidence release and human reading are different facts.

Native errors remain deliberately inspectable through errors.Is/As; ordinary fault
formatting redacts raw text. Primary and cleanup failures remain separate. Technical
receipts and JSON values are process-local and refuse runtime serialization.

## Qualification and sources

The actual consuming build verifies SDK v3.12.1, commit
c11ccc66d80b5ab438115233158555daa6bea339, checksum
h1:5Zu2z+meHR/ETyTSWOB6gyRN0FIiImXdq/ReQGlGYH8=, and unchanged qualified Gorilla
v1.5.4-0.20240701034025-d67f41855da4. The dispatcher patch does not demonstrate
a vulnerability in the previous custom Receive and does not replace its checks.

[Package tests](../../../../../../internal/notification/lark/v3) compare the closed
catalog to the selected SDK source and independently capture TLS requests, malformed
ACKs, auxiliary authentication, callback protocols and WSS lifecycle failures.
The [public contract](../../../../adapters/notification/lark/v1/interface.md) describes
Direct/Fixed/Follow/Framework and independent consumers. Native-only diagnostic
comparisons stay in [nativeprobe](../../../../../../internal/notification/lark/v3/testdata/nativeprobe).

Earlier #64 real message/CardKit/WSS and recipient observations remain historical.
This issue's local controls are not real recipient, rendering, production callback,
platform permission or all-client certification. Live tests require separate
synthetic-target authorization; they never change subscriptions or permissions.

Primary sources: [pinned SDK](https://github.com/larksuite/oapi-sdk-go/tree/c11ccc66d80b5ab438115233158555daa6bea339),
[message API](https://open.feishu.cn/document/server-docs/im-v1/message/create),
[legacy batch submission](https://open.feishu.cn/document/ukTMukTMukTM/ucDO1EjL3gTNx4yN4UTM).
