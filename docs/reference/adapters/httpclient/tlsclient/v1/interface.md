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



# tls-client HTTP Adapter v1

**Audience:** independent Go consumers and Framework composers.
**Status:** implemented public API; local protocol qualification is separate from
deployment, SDK licensing and release support. This provider depends on the shared
[HTTP vocabulary](../../v1/interface.md), not another concrete provider.
[Issue #118](https://github.com/frost-leo/fathomry/issues/118) owns this work.

## Selected authority and delivery

Import `github.com/frost-leo/fathomry/adapters/httpclient/tlsclient/v1`.
Requests/headers are **github.com/bogdanfinn/fhttp**, not standard net/http.
The selected tls-client SDK is v1.16.0 at acquisition origin
`291b8f9e1b86cc35f210bdb6bf44770bf2660ab5`, with local compatibility revision v3.
The moving upstream tag is not an equivalent provenance assertion. See
[Internal coverage](../../../../internal/httpclient/tlsclient/v1/interface.md)
and [correction provenance](../../../../../../third_party/tls-client/FATHOMRY.md).

Independent modules must select the generator's published replacement graph;
root-module replace directives are **not inherited**, and nested SDK modules are
absent from the root ZIP. `fathomry new` normal mode uses immutable versioned
replacements; development mode explicitly uses a checkout. Do not remove the
correction because an upstream module happens to compile. `Build()` reports the
executing binary's selected and replacement module identities; unknown stays
unknown, and local paths are redacted.

The recorded BSD-4-Clause/GPL redistribution question remains unresolved.
Implementation and tests do not grant new permissions or certify a release.

## Preparation and source ownership

1. Supply strict-loadable `Settings` and explicitly borrowed `NativeOptions`.
2. `Prepare(settings, native)` freezes the final selection offline. It creates no
   SDK client, socket or profile-factory call.
3. `Prepared.Policy()`, native-aware `Recommend(settings, native)`, or
   `Compose(preparations...)` obtain authoritative declarations. Compose covers
   exactly the listed simultaneous owners, including retired generations.
4. Construct caller-owned `adapters.Runtime` and `adapters.Inbox[Result]` from the
   policy. `Prepared.Open(ctx, dependencies)` rejects a second Native selection
   and insufficient capacity **before** constructing a source.
5. Retain the returned `Owner`, including a non-nil owner alongside error.
   `Owner.Client()` and `Owner.Handle()` are non-owning; only Owner closes sources.
   `Owner.Release` adapts repeatable cleanup to `resource.Instance`.

`Validate(Settings)` checks data only; it never chooses a browser profile or
certifies compatibility with runtime dependencies. An explicit profile is required
for actual preparation. Defaults and budgets come only from Internal's final
selection. Prepared copies share frozen containers and the declared opaque borrows.

Open's ctx owns the entire source lifetime. Construction may be local-only without
proving endpoint readiness. Internally this lifetime is distinct from construction
and cleanup waiting contexts. Close cancels admitted work and starts native shutdown
**before** waiting for roots, including contextless SDK HTTP/2 reconnects. Cancellation
does not release a callback, stream or physical connection before actual cleanup.
Timed-out Close remains repeatable; an uncooperative borrowed object can retain
ownership indefinitely. Never invoke source Close reentrantly from a hook.

## Settings and native selection

All durations are integer nanoseconds. Pointer nil is unset, explicit false/zero
is retained, and explicit empty Mode is rejected. Version zero selects format1.
Settings JSON may contain proxy credentials; ordinary fmt/slog is redacted.

| Settings | Default / rule |
| --- | --- |
| Name, Version | Named instance; version0 or1 |
| Mode | Unset → negotiated; http1; race-http3 |
| ProxyURL, RoutingLocked | No configured proxy, unlocked; no failed-route direct fallback |
| ServerName, InsecureSkipVerify | No overwrite, verification enabled; insecure+pins is a conflict |
| RandomTLSExtensionOrder, DisableSessionTickets | false; native TCP behavior, not fingerprint certification |
| DisableIPV4, DisableIPV6 | false; both true invalid; refused in H3 racing |
| Bandwidth | false; enables only the native measured TLS/TCP lane |
| MaxActive, QueuedCalls | 8,0; active1..1024, queued0..4096 |
| MaxBindings, MaxTCPConnections, MaxHTTP3Transports | 16,32,16; shared source physical/route ceilings |
| MaxRequestBytes, MaxResponseBytes | 8MiB each; positive up to1GiB; aggregate input includes replay |
| MaxHeaderBytes | 64KiB; 1KiB..1MiB, bounded metadata |
| MaxNativeHeaderBytes | 10MiB; at least metadata limit, at most64MiB |
| MaxProfileBytes | 1MiB; 1KiB..1MiB declared lazy TLS-profile envelope |
| MaxExchanges, MaxReplays | 10,32; maxima128,1024 |
| AdmissionTimeout, Timeout, IdleConnTimeout | 30s,30s,90s; 1ms..24h |

Public aggregate ceilings may reject individually valid large configurations rather
than silently reducing concurrency. Policy is declared work/source/evidence
accounting, not hard heap/RSS. It includes final native buffer choices, origin and
HTTPS-proxy H2 buffers, QUIC receive limits, lazy profile residence, native 32-entry
session caches and selected uTLS compressed-certificate decoded envelopes.

MaxProfileBytes is a declaration for **cooperative** fresh factories and dynamic
extension callbacks, not a memory sandbox or universal ClientHello wire cap.
Initial factory container counts and extension Len values are checked before SDK
use. Native SNI/session/padding mutation happens later; callbacks must include their
dynamic output in the declaration. Preparation never invokes them to guess output.
Opaque borrowed memory remains caller-owned and outside exact accounting.
Native Go/randomized/preset placeholder modes do not invoke a supplied factory;
their authoritative profile allowance instead uses the native 24-bit ClientHello
envelope. MaxProfileBytes does not impose a custom-factory cap on those modes.

NativeOptions exposes the selected profile; TransportOptions (roots, client
certificates, key-log writer, compression, keepalive, buffers and header/idle limits);
DialContext; copied net.Dialer and LocalAddr; proxy factory; Jar; default and CONNECT
headers; certificate pins/handler; redirect, pre and post hooks. Containers, profile
maps/order/slices, certificate byte containers and addresses are copied. Keys,
writers, Jar, factories, resolver/control/dial callbacks and opaque TLS-pool contents
remain immutable/cooperative/concurrent borrows through source shutdown.

The built-in TCP proxy uses the supplied native Dialer/resolver/local bind; callers
need not implement CONNECT. Effective Timeout overrides Dialer.Timeout as in the
SDK. ControlContext takes precedence over Control. Conflicting custom DialContext,
Dialer and proxy-factory selections are rejected, not silently ignored. H3 racing
rejects TCP-only dial/local/pin/IP-family/key-log/keepalive options. Legal native
special and custom profiles remain usable; factory failures preserve identity.
No owning native client, tracker, transport, raw connection or WebSocket escapes.

## Requests, credentials and results

`Client.Do(ctx, cleanupCtx, *fhttp.Request, ...RequestOptions)` retains finite
bounded data. `Client.Open(ctx, request, ...)` returns a retained Stream and receipt.
The **method context replaces Request.Context**; its values propagate where the
native API accepts a context. SDK HTTP/2 reconnects have no per-request dial context;
source lifetime still cancels them. The source, method context and prepared Timeout
bound live work. CleanupCtx only bounds waiting, not continued cleanup.

Request metadata is copied; Body/GetBody are borrowed only after admission and
closed natively. Provide fresh independent replay readers, not aliases. H3 racing
with a nonempty HTTPS body requires GetBody before sending. Keep fhttp native
Header-Order:/PHeader-Order: inputs; no lossy net/http conversion is performed.
Native retries/races/redirects can produce more than one external attempt.

Hooks receive bounded metadata-only views. Legitimate target/header mutation,
redirect hooks, jar and post notices remain supported. Effective initial target is
prepared before jar selection and automatic URL-userinfo Authorization.
Redirect-inherited Cookie/Authorization that remain unchanged are removed when the
final hook-rewritten origin differs (scheme/host/effective port). Explicitly changed
values are preserved. **Same-value reassertion is indistinguishable from no change
and is removed**. Initial explicitly supplied application headers retain native
semantics. Response cookies belong to the actual effective response request.
Request.Host alone is not a URL-origin change.

RequestOptions selects provider/direct/explicit proxy and optional CONNECT headers.
Configured defaults plus effective request headers partition route pools, including
authentication. Contextual SDK CONNECT headers remain supported but conflict with an
explicit map. RoutingLocked rejects route/header overrides. SOCKS4/4a/5/5h and native
proxy-mode limitations remain as described by Internal; H3 requires UDP-capable
SOCKS5 and does not invent remote DNS support for a hostname.

A non-nil receipt identifies accepted work even alongside an error. Result exposes
detached source identity, actual generation/correlation, metadata, finite bytes,
trailers, body/input byte counts, exchanges, nonexact attempts, hook notices and
input-reader errors. RequestBytesRead is not proof of transmission. HookErrorsCopy
and InputErrorsCopy preserve explicitly inspectable causes; they do not become
Primary merely because a losing leg or advisory post hook failed.

Stream permits one Read plus concurrent repeatable Close. Close is required after
EOF; native tls-client Close returns primary and cleanup failures together. Public
final receipt Primary/Cleanup remain separate. A cleanup-wait timeout is not final
Primary. Complete means final body EOF without primary failure, not HTTP/business
success, absence of effects or durable receipt. Headers/EOF/Close/ack are distinct.

## Bandwidth

`Client.Bandwidth(ctx)` borrows the current Fixed/Follow generation and returns
its generation number and preparation identity; direct generation is0.
`Handle.Bandwidth()` observes that exact source, including after confirmed shutdown.
Neither path dispatches a request or exposes Reset/TrackConnection.

Scope is **origin-tls-over-tcp**: native read/write counters around origin TLS on a
TCP or tunnel stream. It excludes cleartext HTTP, HTTP/3, proxy negotiation, outer
proxy TLS/framing and arbitrary traffic inside a borrowed dialer. It is not the
complete wire total and not per-request attribution. Counts survive binding
retirement. Separate atomic counters are not a jointly atomic snapshot. Disabled
or overflowed observations return availability=false; known zero has meaning only
within the declared measured lane. No H3 or cleartext total is synthesized from
application reads.

## Framework and evidence

Use `resource.Binding[Settings, Handle]` with Fixed or Follow. Build an Owner using
the generation lifetime, return Handle and Owner.Release, and compose with
`Using(lifetime, ref, policy.Budget, dependencies)`. Do not pass Native to Using.
An operation borrows the actual generation through native completion and bridge
transfer. Old retained streams continue on their original source after replacement.
Failed/obsolete candidates retain cleanup ownership; an oversized generation is
refused before dispatch. WithID changes facade correlation only.

One root has one public record and one private native record; there is no artificial
Connection/child API. Losing native racing work stays in its root. Inbox admission
is independent from operation admission. A released result can still occupy
evidence capacity until Ack. Receiver Retry redelivers evidence, never sends HTTP
again. Stream/Owner cleanup bypasses admission and record saturation.

Errors use Network facility0x201/component http_tlsclient; existing owners are
unchanged. Definitions/Resources and CLI catalogs work offline in en/zh-CN.
Errorbridge preserves errors.Is/As while redacting ordinary diagnostics. Runtime
values refuse JSON serialization; Settings remains loadable. See the
[capability matrix](capabilities.md) and executable public-only direct/Framework
fixtures for positive/refusing behavior and actual delivery qualification.
