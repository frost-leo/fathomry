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

# Lark / Feishu WebSocket reception

[Lark / Feishu interface](interface.md) / Application reception

**Audience:** in-module adapters owning a long-connection receiver.
**Status:** implemented self-built application profile. Real bootstrap, WSS
handshake and ping/pong exchange were observed. After the owner enabled missing
permissions, real messages from the authorized account were received. Earlier
native-probe zero counters had a counting defect and do not establish absent receipt.
The exact-text
file-before-code-200 probe remains distinct from successful reception.

## Composition and ownership

Set `OptionsV1.WebSocket = &WebSocketOptions{}` before Select. The same named
source owns HTTP transport, credentials and receiver admission. Configuration v1,
SDK v3 and Feishu's protobuf WebSocket protocol are separate versions.

1. Select, attach `LimitsV1`, then assemble normally.
2. Create `Inbox[WebSocketResult]` using `WebSocketEvidenceBytes()` per slot.
   Bind it with `BindReceiver`. It retains the long-lived session result and
   independently reserved acknowledgement results.
3. Create `Inbox[WebSocketEvent]` using `WebSocketEventBytesV1(options)` per slot.
4. Call `Receiver.Listen(ctx, correlation, events)`. It blocks until the runtime
   stops. A caller goroutine may run it, but the caller must own cancellation/join.
5. Drain both inboxes concurrently. Do not block all result handling on the first,
   still-unresolved session record. Retain unresolved records separately.
6. Cancel Listen, wait for its return/receipt release, then close the assembly.

Listen holds one active source slot. ACKs and event evidence are bounded descendants
of that same lease, not a second allowance. `LimitsV1` reserves MaxPending + 2
borrowing nodes. Sending while listening needs another configured active slot or
an independently selected source. Borrowed/copied Receivers cannot create another
receiver on the same source or close its owner.

There is one read goroutine per physical connection; the Listen goroutine owns
heartbeat, reassembly, ACK writes and reconnect. No application callbacks run
inside the Provider. Stop closes the socket, joins the reader, abandons unresolved
ACKs and releases fragments/timers before relinquishing invocation use.
`Status` returns a copied numeric snapshot shared by aliases of the source.

## Explicit acknowledgements

A completed event handoff contains immutable `Event` data and a one-shot
`WebSocketEvent` capability. Copies share its decision, not another ACK.
The ACK's independent result slot and borrowing obligation are reserved **before**
the event is published.

- `Acknowledge(ctx, JSON{})` requests protocol code 200 after caller-owned
  processing or durable handoff. A bounded JSON object can carry a native
  card-action response.
- `Reject(ctx)` requests protocol code 500 without transmitting application errors.
- `Receipt()` observes the independently retained result even if nobody claims
  the event or the direct caller stops waiting.
- Unclaimed/expired events receive code 500, never implicit success.
- A disconnect abandons pending ACKs on that generation. An old event cannot
  write to a new connection, even if the event ID is later redelivered.
- Saturated evidence/pending capacity stops reception without a success ACK.
  No arbitrary drop-and-ack policy is hidden in the runtime.

`WebSocketNotWritten`, `WebSocketWriteUnknown` and `WebSocketWritten` describe
local transmission. Written is **not** a platform confirmation, human reading,
business completion or durable commit. A partial write can leave acceptance
unknown. No ACK write is retried on another connection.

Feishu can redeliver events. Callers must deduplicate using the immutable event
identity before performing business effects. Releasing an inbox record does not
acknowledge the Feishu event. Reconnection does not provide a durable replay log.

## Authentication, identity and bounds

Only the `application` profile is supported. Bootstrap uses the explicitly frozen
AppID/AppSecret through the existing bounded HTTP mechanism; no implicit token
cache or client-assertion callback is created. Gateway URLs are private credentials.
Only verified `wss` URLs on an exact configured host[:port] allowlist are accepted.
Default Feishu/Lark bases select their public frontier host; custom bases need an
explicit allowlist. No redirects, ambient proxy, cookie jar or compression extension
is accepted. The HTTP Upgrade header read is bounded before native parsing.

WebSocket transport authentication replaces HTTP callback signature/decryption:
payloads must be schema 2.0, match AppID and carry a valid tenant identity.
A configured TenantKey must match; absent TenantKey trusts the authenticated
self-built application's tenant scope and retains the observed key in the event.
VerificationToken and EncryptKey are not required for WSS. Supported events remain
IM message lifecycle events and `card.action.trigger`, not unrelated enterprise APIs.

| Dimension | Default |
| --- | --- |
| WebSocket message/frame bytes | 256 KiB (maximum 1 MiB) |
| Aggregate encoded routing metadata | 8 KiB, including LogIDNew and protobuf overhead |
| Reassembled event bytes | 1 MiB (maximum 4 MiB) |
| Pending acknowledgements | 16 (maximum 64) |
| Partial messages / fragments per message | 8 / 64 |
| Total retained fragment bytes | 2 MiB (maximum 32 MiB) |
| Fragment TTL | 5 s from first fragment; duplicates do not extend it |
| ACK deadline / write timeout | 2 s / 1 s |
| Fallback ping interval / pong timeout | 120 s / 15 s |
| Connection attempts per Listen | 8, including initial |
| Reconnect delay | 1 s–2 min, bounded jitter and clamped server floor |
| Callback response JSON | 30 KiB |
| Lifetime | Zero requires a cancellable caller context or deadline |

Explicit options can tighten or expand limits within documented Go validation
bounds. Durations are nanoseconds. Duplicate metadata, invalid sequence indices,
conflicting fragments, excessive reassembly and unsupported payload encodings
are rejected before dispatch. Completed/disconnected fragments are discarded.
Server timing/count hints cannot remove local bounds or create a hot retry loop.
A configured lifetime includes waiting for source admission. Expired heartbeat,
fragment and ACK deadlines are checked before processing more queued frames;
continuous input cannot postpone them by repeatedly rearming a timer. Bootstrap,
event and timing fields reject case-folded aliases of their native JSON names.
A zero server reconnect count prevents reconnection. There is no background work
after Listen returns.

Session attempts count intercepted bootstrap/Upgrade and application-protocol
heartbeat writes; ACK attempts belong to their own nested results. These are not
TCP packets, TLS records or RFC 6455 control-frame counts. `LastFailure` retains
the last recoverable connection failure even if a later reconnect succeeds.

## Why not expose ws.Client

The pinned official SDK supplies the protobuf Frame, heartbeat/response models,
bootstrap request and configuration shapes. The transport is Gorilla at the exact
upstream crypto/rand mask-key fix, `v1.5.4-0.20240701034025-d67f41855da4`, overriding
the SDK's v1.5.0 minimum without modifying upstream source. This is a qualified
pseudo-version, not a tagged v1.5.4 release. Despite the advisory's fixed-version
label, inspected v1.5.3 source still uses math/rand; its tag predates the actual fix.
The consuming executable test verifies both SDK and transport versions/checksums.
The Provider deliberately does not construct the native owning ws.Client:

- native receive starts a goroutine for each message, without admission bounds;
- fragment count/index/allocation and concurrent cache mutation are uncontrolled;
- the cache creates a cron goroutine without a close path;
- asynchronous handlers can outlive a connection and send through the current,
  different connection;
- native transport/configuration hooks do not impose these resource boundaries.

A subprocess rejecting control reproduces the invalid-fragment panic and retained
cache worker without leaking a worker in the main test process. The Provider uses
the official wire models with bounded ownership instead of vendoring the whole
platform or silently weakening the resource contract.

## Example and service gates

The [showcase](../../../../../../internal/notification/lark/v3/testdata/showcase/README.md)
has a bounded `-listen` mode. Its optional exact-message probe validates the
authorized sender/text, writes an owner-only event file, syncs it and its parent
directory, then requests an ACK. This is an example-owned file format, not a
Fathomry durable history contract. Up to eight different-text messages from the
same authorized sender receive code 500 while the probe continues; other mismatches
stop it without a success ACK.
The final numeric summary is emitted after receiver and assembly cleanup, including
terminal/cleanup failures rather than only the last healthy heartbeat.

Real reception additionally requires the application owner to choose long
connection subscription and enable the desired event/permission (for example
`im.message.receive_v1`). This code does not change or publish application
configuration. A successful WSS handshake or heartbeat alone does not establish
that subscription is configured, nor that a user's message reached this listener.

Primary references:
[Feishu subscription configuration](https://open.feishu.cn/document/server-docs/event-subscription-guide/event-subscription-configure-/request-url-configuration-case),
[pinned SDK WebSocket source](https://github.com/larksuite/oapi-sdk-go/tree/v3.12.0/ws),
[pinned Gorilla transport](https://github.com/gorilla/websocket/tree/d67f41855da42d7bccd9ef050c49f7e54e783b95),
[mask-key advisory](https://github.com/advisories/GHSA-w67g-5rqw-f597).
