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

# Lark notification adapter

[Documentation](../../../../../README.md) / Public package reference

**Audience:** notification callers and application composition owners.
**Status:** implemented local technical integration; live service/permission and
recipient rendering qualification of this revision is not claimed.
**Package:** `github.com/frost-leo/fathomry/adapters/notification/lark/v1`.

## Responsibilities and call sequence

This provider implements the selected notification domain, not the whole Feishu
enterprise platform. It reuses [notification data](../../v1/interface.md),
[public operations](../../../v1/interface.md) and
[resource generations](../../../../resource/v1/interface.md). It introduces no
Notifier, scheduler, outbox, business report model or automatic notification retry.

1. Prepare loadable `Settings` and separately borrowed `NativeOptions`.
   `Validate`, `Recommend` and `Prepare` are offline and invoke no callbacks.
2. Use `Prepared.Policy` for one source, or `Compose` for every overlapping source,
   including retired generations. Create the caller-owned Runtime and Result Inbox.
3. `Prepared.Open` constructs that exact source; its context owns the full lifetime.
   A non-nil Owner returned with an error still needs cleanup.
4. Use `Owner.Client` directly. For generation adoption, bind `Owner.Handle` and
   `Owner.Release` through resource/v1, then construct `Using(lifetime, ref, policy,
   dependencies)`. Settings publication is not instance adoption. WithID changes
   correlation only, not the source or quota.
5. Invoke operations and consume independent evidence with `NextReleased`.
   Inspect the Snapshot, then `Ack` or `Retry`. Retry requeues facts only.
6. Cancel listeners, close/join source owners and Runtime, and drain remaining
   released source evidence. Repeat Close after an incomplete wait; timeout is not
   proof that arbitrary borrowed code stopped.

These are process-local Activity/ordinary Go APIs, not deterministic Temporal
Workflow operations. Runtime handles refuse JSON serialization. Settings permit
explicit sensitive JSON but redact normal fmt/slog output.

## Native operations and values

`NewRequest` freezes a closed `Operation`, path values in placeholder order,
native query values and an immutable JSON body. `Client.Do` routes all 85 generated
finite IM v1/v2, CardKit v1 and selected Contact batch lookup operations.
`BotInfo` adds the SDK-used bot metadata endpoint. `LegacyBatchSend` is the
separately documented HTTP v4 batch submission, not an invented generated Create.

The catalog includes message lifecycle/search/follow-up/urgency; batch deletion,
progress/readers; reactions/pins/thread forward; chats/members/managers/menus/
announcements/moderation/tabs/top notices; every CardKit card/element operation,
including IdConvert; IM v2 feed/app-feed/buttons/native tags and relations/URL
preview; and email/mobile batch_get_id with identity and IncludeResigned.

Native request/response fields remain exact JSON instead of a second SDK model.
Native pure-value builders may be encoded by the caller. No Client, Transport,
Cache or Dispatcher authority escapes. Missing data, null, empty collections,
false, zero and exact JSON numbers are not normalized. Platform validation and
permissions remain external. A generic code-zero ACK does not assert presence of
every optional native field; inspect retained data. Required operation identity
and response-envelope checks remain enforced.

Convenience methods retain established defaults. Use Do for complete native
identity/query/field choices. UUID is optional native deduplication, not a Framework
business intent or an exactly-once guarantee. CardKit sequence remains caller-owned.
`JSON`, `Content`, native Chart/Table and `ComposeCard` are technical constructors;
header style and update_multi are explicit. They do not calculate business metrics.

`UploadAsset` supports image purpose, native file type and optional duration,
including explicit zero. Exactly one Bytes or Reader input is borrowed through
actual return; no Reader is closed or forcibly interrupted. Downloads retain
bounded copied bytes. All getters return detached containers or immutable views.

Legacy batch submission preserves invalid department/open/user/union ID lists
separately. Urgency invalid users are not merged-forward invalid message IDs.
Invalid-target strings are preserved even when they are not valid identity syntax;
they are failure evidence, not authority to address another operation.
PartialFields preserves chat membership, reaction, feed/button and tag failure or
pending scopes. Partial reports explicit non-complete scope, not proof that other
targets succeeded; pending approval is not rejection.
Malformed known failure members return a response error and Unknown effect;
the original response remains available for deliberate inspection.
The [official batch contract](https://open.feishu.cn/document/ukTMukTMukTM/ucDO1EjL3gTNx4yN4UTM)
defines asynchronous batch IDs and independent permissions.

## Authentication and budgets

Profiles: self-built `application`, `marketplace`, explicit `tenant-token` /
`user-token`, `client-assertion`, or signed fixed `webhook`.
Marketplace uses the supplied app ticket, then app token, tenant token and the
requested operation. Each auxiliary stage retains attempted/exchange/error/
cleanup facts. User tokens are explicitly external, not an implemented OAuth
refresh service. App tickets are caller-owned; no global SDK manager is mutated.

Dynamic providers return one complete app/tenant-bound tuple per request. Empty,
wrong-source, expired, canceled, error and panic results fail closed. Callback
credential rotation cannot downgrade an encrypted source to token-only.
Client assertions use the SDK's region-aware OAuth base defaults; a custom base
requires an explicit OAuth endpoint. CredentialRequest supplies the distinct API
or WebSocket purpose and assertion audience. AssertionProxyURL/Prefix are explicit
frozen HTTPS delegation routes; dynamic providers cannot redirect credentials.
A token rejection only invalidates the local
cache for the next explicit operation; it never resends a notification.

HTTP uses verified HTTPS, bounded bodies/headers, fresh HTTP/1 connections, no
ambient proxy, redirect, compression negotiation or transport replay. RequestOptions
allows bounded request IDs/non-authority headers, not Authorization, Host, framing
or routing overrides. Attempts count managed native dispatches, not arbitrary
activity inside user callbacks.

Provider Policy embeds unchanged notification.Policy and adds ReceiverLifetime
from the same frozen Internal preparation. Compose takes the strictest positive
receiver lifetime, intentionally shortening longer/unbounded sources in that
explicit composition. Using takes the complete provider policy; adoption rejects
a missing or too-large declared receiver lifetime.
AdmissionTimeout bounds outer queue waiting. Source, finite work, WSS buffers,
events, pending ACKs and fallback evidence are declared envelopes, not measured
RSS, kernel bounds or cross-worker quotas. Callbacks/readers remain cooperative.
Borrowed credential/asset and HTTP body/handler/writer callbacks execute in
synchronously joined workers, not detached timeout tasks. Panic/Goexit become
errors while observed authentication and HTTP-write facts remain inspectable.
There is no goroutine-affinity guarantee; cancellation still waits for actual
callback termination. The native declaration includes a 64 KiB worker allowance.

## HTTP callbacks

Receive and ReceiveLegacy verify an already-bounded exact body. HandleHTTP also
rejects duplicate authentication headers, caps reading, runs the explicit handler
and writes its bounded response. It starts no server and changes no subscription.

Modern schema-2 IM chat/member/message events, card.action.trigger, url.preview.get,
bot-menu and token-revocation events retain their own payloads. Schema-1 app_ticket
is delivered without updating global caches. Token-only HTTP uses a mandatory
verification token; encrypted profiles use the separate SHA-256/EncryptKey signature
and strict AES-CBC/PKCS#7 decryption. Legacy CardAction uses SHA-1 with the verification
token, not the modern signature. Legacy identity has no app_id field: configured
app credentials authenticate it, and the supplied tenant must match.

Challenges are token-authenticated and JSON-escaped. Legacy raw bytes/card/custom-status
and modern toast/card/inline JSON responses are explicitly supplied and bounded.
Raw legacy bytes are not forced through JSON.
Authentication, handler completion, local response writing and platform receipt
are separate. HTTPWrite.Written only means the local writer returned all bytes.
Event IDs can be redelivered; no durable deduplication is claimed. Event payloads
may contain action tokens or app tickets and are sensitive deliberate inspection.

## WebSocket reception and acknowledgements

Listen supports self-built app-secret or client-assertion authentication, blocks
through actual receiver and bridge join and retains one original
source generation. A positive ReceiverLifetime starts before public admission and
permits context.Background; otherwise Listen requires a cancellable context. The
same deadline reaches native setup/listening. It needs a separate public
`Inbox[Incoming]`; the prepared Evidence declaration can size both inboxes.
Its active slot is distinct from concurrent finite calls. Native ACK descendants
and public ACK children reuse the listener root, never a new root permit.

Both public event and ACK evidence are reserved before an Incoming capability
escapes. Inbox.Ack is local evidence custody; Incoming.Acknowledge/Reject makes the
one protocol decision shared by copied handles. Incoming.Receipt observes the
reserved ACK even if nobody claims the event. Its source attribution and
ConnectionGeneration are different axes. Follow never retargets an old ACK.

Unclaimed/expired events get a negative ACK, not success. Disconnect abandons
original-connection work; ACK writes never retry on a new connection. Written is
local transmission, not platform confirmation. Saturation stops reception; facts
that could not receive a separate public record remain on the admitted listener
as UnforwardedEvents/UnforwardedAcknowledgements, without an ACK capability.
UnforwardedEvent retains FrameID, ConnectionGeneration and AcknowledgementID;
that ID matches the ACK Result.TechnicalID. Fallback facts share the admitted
listener's public attribution rather than inventing a separate public sequence.

The preserved Gorilla transport handles bounded reassembly, heartbeat fairness,
reconnect and cleanup. Selected native WSS dispatch supports event frames, not
legacy card frames. This is protocol support, not proof that every platform app
can subscribe to every event/transport combination.

## Errors, examples and qualification

Notification facility `0x2C1` owns safe public errors and offline EN/zh-CN resources.
Native causes remain deliberately available through errors.Is/As; printing arbitrary
unwrapped causes is not safe. Public/shared errors keep their existing ownership.

- [Independent public consumer](../../../../../../adapters/notification/lark/v1/testdata/consumer)
  verifies Direct, Framework Fixed/Follow, dual generations, media, cards,
  conversation/feed routes, HTTP and independent WSS wire capture.
- [Public showcase](../../../../../../adapters/notification/lark/v1/testdata/showcase/README.md)
  preserves the Welcome/report, lifecycle exercise and file-before-ACK probe.
  Preview is offline; live modes require explicit config, targets and authorization.
- [Internal contract](../../../../internal/notification/lark/v3/interface.md) and
  [WSS mechanics](../../../../internal/notification/lark/v3/websocket.md).

The actual consumer pins SDK v3.12.1 / c11ccc66d80b5ab438115233158555daa6bea339
and checksum h1:5Zu2z+meHR/ETyTSWOB6gyRN0FIiImXdq/ReQGlGYH8=.
Gorilla remains v1.5.4-0.20240701034025-d67f41855da4. Native dispatcher tests do not
certify ingress by themselves; this provider retains its stricter verification.
Local peers and API ACKs do not establish live receipt, rendering or permissions.
