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

# Standard HTTP Adapter

[Documentation](../../../../../README.md) / Public package reference

**Audience:** independent Go applications and Framework composition authors.
**Status:** implemented bounded standard HTTP provider; local protocol qualification
does not certify production sites, arbitrary callbacks or a supported release.
**Package:** `github.com/frost-leo/fathomry/adapters/httpclient/nethttp/v1`.
**Selected authority:** Go 1.27.0 standard library and its selected
[Internal boundary](../../../../internal/httpclient/nethttp/v1/interface.md).

## Prepare and compose

`Prepare(Settings, NativeOptions)` performs no network/native callback work.
It freezes data and copied native containers once. `Prepared.Policy` consumes
authoritative Internal root/evidence/source metadata; `Prepared.Open` constructs
exactly that selection. `Validate` and `Recommend` are data-only conveniences.
`Open` additionally consumes `Dependencies.Native`; `Prepared.Open` rejects
a second native selection instead of silently applying or ignoring it.

`Compose` lists exactly the simultaneous source instances, including retiring
generations. It sums source/root/queue/evidence recommendations and uses the
largest root envelope. It rejects public-runtime ceilings rather than reducing
native concurrency; native MaxActive=1024 is unrepresentable together with the
source-lifetime root. No caller-owned Runtime is resized.

Open checks normalized configured Runtime and Inbox ceilings before admission.
These are configuration checks, not reservations of all future free capacity.
Ordinary admission still arbitrates shared capacity, and callers must provision
the aggregate policy for overlapping owners. Insufficient or saturated capacity
does not authorize constructing another source or replaying a request.

The public bridge adds two native result slots and 64 KiB metadata per root, and
64 KiB per independent public result/source owner. Internal separately accounts
effective H2 receive windows, transport/header-table buffers, copied containers,
routes and sockets. Borrowed callback/key/cache/jar memory, Go/kernel overhead
and arbitrary foreign allocations are not hard RSS guarantees.

## Settings and native dependencies

Settings are strict-loadable version-1 plain data. No custom JSON/Text codec,
context or callback is present. Pointer omission selects native defaults;
explicit false and zero are preserved without a second default pass.
All duration JSON fields use integer nanoseconds.

| Fields | Defaults / valid meaning |
| --- | --- |
| Name, Version | Required source name; Version 0 selects 1 |
| HTTP1, HTTP2, UnencryptedHTTP2 | H1 and TLS H2 enabled by default; h2c disabled; explicit h2c requires H1=false |
| ProxyURL, RoutingLocked | Empty is direct unless a native Proxy hook is supplied; locked forbids per-call overrides |
| ProxyConnectHeader | Copied proxy-only metadata; empty by default; bounded by MaxHeaderBytes |
| RootCAPEM, ServerName, ClientCertPEM, ClientKeyPEM | Explicit TLS data; cannot conflict with native TLS |
| MaxActive, QueuedCalls, MaxConnections | 8, 0, 32; ranges 1..1024, 0..4096, 1..4096, with public aggregate checks |
| MaxRequestBytes, MaxResponseBytes | 8 MiB each, 1..1 GiB; request reads are cumulative across replays |
| MaxHeaderBytes, MaxExchanges | 64 KiB and 10; ranges 1024..1 MiB and 1..128 |
| AdmissionTimeout, Timeout | 30 s each; positive, at most 24 h |
| DialTimeout, TLSHandshakeTimeout | 10 s each; positive, at most 24 h |
| ResponseHeaderTimeout, IdleConnTimeout | 30 s / 90 s; positive, at most 24 h |
| ExpectContinueTimeout | 1 s; zero means immediate body send; independent of response-header waiting |
| DisableCompression, DisableKeepAlives | False; retain native meanings |

TLS/HTTP2 containers are copied. Private keys, randomness, session caches, Jar
and callbacks are explicitly borrowed until source shutdown and must be safe for
concurrent cooperative use. Prompt time/cache/diagnostic hooks must not block.
No native owning client, transport, raw ClientConn, RoundTripper/httptrace or
101 upgrade authority escapes.

Dynamic CONNECT headers replace static headers. Selection runs **before every
applicable exchange's pool lookup**, not only before new tunnels. The result is
bounded and copied; native proxy-URL Basic authentication takes precedence and
the effective headers participate in pool isolation. Only HTTPS targets through
HTTP(S) proxies use this path: direct requests, ordinary HTTP forwarding and
SOCKS do not. Framing/authority/upgrade headers are rejected. Response observation
receives bounded immutable status/protocol/proxy/target/header metadata, including
407 before rejection. It has no Body/Request/connection. Callback failure never
falls back to direct routing.

## Calls, contexts and ownership

1. Construct public Runtime/Inbox from the policy and open a source Owner.
2. Use `Owner.Client` directly, or bind `Owner.Handle` and `Owner.Release`
   through public resource instances. `Using` supports Fixed/Follow references.
3. `Do` retains a bounded response; `Open` retains a response stream;
   `Connect` creates a controlled direct connection with one outstanding child.
4. Close streams and connections, even after EOF. Receive and acknowledge every
   independent public result. Close source owners after actual work ends.

Client and Handle cannot close their source, Runtime or Inbox. `WithID` creates
an immutable correlation view. Each root pins its actual generation; connection
children never reacquire a newer one. An adopted source exceeding the Using
budget is rejected before native dispatch.

The explicit method context replaces Request.Context, including values; it is
not a setup-only context. Connection children also inherit parent cancellation.
AdmissionTimeout bounds Internal admission after public dispatch; waiting in the
preceding shared Runtime queue is bounded by the caller's method context, not by
a duplicated provider timer. Supply a method deadline when queueing is enabled.
Bodies/GetBody are borrowed through released receipts and closed by native work;
unadmitted requests neither read nor close them. Fixed outbound trailers are
snapshots, not body-mutated producers. Inbound trailers are available after
complete EOF. Redirect and jar semantics remain those of the selected Internal
boundary; per-call routes remain frozen through redirects/retries.

Cancellation of a waiter does not prove native termination. The public bridge
holds work/source/generation ownership until native release and evidence transfer.
Owner/method cancellation initiates retained-handle cleanup even if abandoned.
Cleanup uses existing reservations under admission/evidence saturation, and a
timed-out Close may be repeated. A non-nil Owner returned with an error remains
responsible for cleanup. Native callbacks/readers are cooperative, not a sandbox.

## Results and failures

Direct method errors, final primary errors, cleanup errors and required evidence
are separate. A cleanup-wait error is not promoted into final primary evidence.
A non-nil receipt denotes accepted work even alongside an error. Receipt waiting
does not acknowledge Inbox custody; retrying evidence delivery never replays HTTP.

Metadata is immutable; header/trailer/body accessors return copies.
Do's successfully retained empty body is non-nil; streams/connections and absent
responses return nil data. Partial bytes remain available on truncation/limit
failure. RequestBytesRead is input consumption, not wire receipt. Exchanges and
Attempts do not count every native retry. HTTP status is data.

For responses, Complete means EOF without primary failure; early Close can
succeed without Complete. For Connect, Complete means its established lifetime
ended; it is not response EOF or business success. Cleanup is independent.

Public errors preserve bounded errorbridge classification and deliberate native
Is/As. Network facility `0x200`, component `http_nethttp`, owns this provider's
offline English/zh-CN definitions. Runtime values redact fmt/slog and refuse JSON;
explicit Settings JSON and data/native-cause inspection may expose secrets.

Build inspects the running executable: net/http is identified by Go, not a
separate SDK module. SDKs also records the already-selected `golang.org/x/net
v0.58.0` SOCKS/IDNA helper. SOCKS establishment completes inside the owned dial
callback before the standard Transport performs TLS; its native pool remains
in use. This preserves remote DNS/authentication and avoids releasing a root
before a detached post-SOCKS TLS callback has ended. Planned TLS references are
reserved before native handshake scheduling, including cancellation before
TLSHandshakeStart and HTTPS-proxy nested TLS. Profile describes effective local choices, not a tested
service/protocol certificate. Normal generated projects consume the versioned
root module and the existing published replacement policy; checkout replacements
are explicit development mode only. No SDK correction/pin upgrade is introduced.

## Coverage and limits

See the [capability matrix](capabilities.md), maintained protocol/error/preparation
tests and separate [direct](../../../../../../adapters/httpclient/nethttp/v1/testdata/direct/main.go) /
[Framework](../../../../../../adapters/httpclient/nethttp/v1/testdata/framework/main.go)
consumers. The generator's versioned-project test executes both from a root module
ZIP without a developer-checkout replacement.

Excluded: HTTP/3, WebSocket/101 upgrades, arbitrary RoundTripper/RegisterProtocol/
TLSNextProto, server APIs, dynamically produced outbound trailers, business retry,
credential refresh, automatic HTTPS rewriting and other HTTP providers.
