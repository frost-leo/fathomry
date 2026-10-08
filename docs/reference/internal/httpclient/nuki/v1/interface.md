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

# Nuki Provider interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** internal HTTP composition and integration maintainers.
**Status:** implemented internal contract with bounded native regression coverage.
Production qualification and the HTTP/SOCKS dependencies' publication permissions
are not established.
**Package:** `github.com/frost-leo/fathomry/internal/httpclient/nuki/v1`.

## Responsibilities and call sequence

This independently selectable Provider uses `github.com/nukilabs/tlsclient`,
not the bogdanfinn SDK or a smart-lock client. It has no common HTTP facade,
business credentials, Cookie refresh, business retry, Provider rotation/eviction,
Redis policy or Workflow runtime.

1. Supply one named `OptionsV1` and an explicit `NativeOptionsV1.Profile`.
   Any valid native/custom profile may be supplied; there is no profile default
   or allowlist. Optional `resource.Layer` settings are plain configuration,
   never runtime functions/handles.
2. `PrepareV1` freezes exact data/native configuration and supplies authoritative
   root, evidence and source-resident `Metadata`. Use its `Select` and `Metadata().Limits`;
   `Select` and unlayered `LimitsV1` remain conveniences. `ValidateDataV1` checks
   data without a native profile. Apply `resource.WithLimits`, then `resource.Assemble`. Native route
   clients are constructed lazily under an admitted call, not during preparation.
3. Create a separately bounded `invocation.Inbox[Result]`, then `Bind`.
   All borrowed aliases retain the original source and allowance.
4. `Do` admits a finite call and returns its receipt before native execution.
   `Consume` instead supplies a callback-scoped `Response` reader. Use receipt
   waiting contexts separately from the work context.
5. Inspect and release every independent Inbox delivery even when the direct
   error was handled. Close borrowing assemblies, then the owning assembly.
   Incomplete cleanup provides a continuation; it is not permission to discard
   the assembly or retry cleanup forever without an aggregate caller budget.

## Native configuration and per-call inputs

`Native` mode retains native H1/H2 and profile-dependent Alt-Svc H3 behavior.
`HTTP1Only` narrows ordinary ALPN; `HTTP2Negotiated` disables H3 while retaining
H1/H2 negotiation; `HTTP3Only` refuses routes without H3. Native forced-H3 context
selection and `RequestOptionsV1.ForceHTTP3` cannot override disabled protocols.
The actual negotiated protocol is response evidence, not an inference from mode.

Native profile factories transfer fresh independent ClientHello specs. H2 settings,
priority callbacks and H3 settings are preserved. TCP ClientHello factories do not
define the H3 TLS fingerprint; native H3 uses the supplied QUIC/TLS configuration.
H2 `ConnectionFlow` is the initial connection `WINDOW_UPDATE` increment, not the
total receive window. Values below 65535 retain the native 1 GiB increment;
values above 2147418112 are refused before construction because the protocol's
initial 65535 bytes must also fit the signed 31-bit flow-control window.
Nil/panicking factories are errors, never implicit profile fallback.
`MaxProfileBytes` bounds initial encoded lazy ClientHello installation; callback
allocations before return and subsequent native expansion remain cooperative
foreign work, not a hard allocation sandbox.
Lazy output is bounded before the controlled H1 ALPN transformation and again
afterward. H3 setting IDs/values must fit QUIC's 62-bit wire integer; reserved H2
IDs, invalid Boolean settings and duplicate non-GREASE IDs are refused offline.

Accepted native configuration is not a promise of byte-for-byte wire settings.
The SDK imposes `TLS.OmitEmptyPsk=true`, enables H3 datagrams and disables QUIC
path-MTU discovery. H3 selects its protocol ALPN; TCP ALPN comes from the native
profile, narrowed by `HTTP1Only`. Explicit destination SNI and session caches
are honored. QUIC idle timeout is capped by the configured source idle ceiling;
a smaller explicit timeout remains effective. Receive-header ceilings override
larger native profile values. These are transport mechanisms, not business defaults.

TLS/profile/QUIC/transport data containers are copied. Functions, private keys,
certificate leaf objects, RootCAs/ClientCAs pools, randomness, session caches, Jar
and Tracker retain their documented borrowing through exact source release.
Trust-pool pointer selection is frozen independently for origin and proxy; pools
are neither copied nor enumerated and their foreign memory is not source bytes.
This replaces the older Internal trust-pool container clone with native
Config.Clone borrowing semantics: valid immutable pools preserve handshake
behavior, but callers must keep the pools immutable through source release.
To change trust, clone/populate a separate pool and prepare a replacement source.
Certificate leaf objects must also remain immutable. Borrowed functions/objects must be
concurrent-safe and cooperative; they cannot retain callback arguments or start
hidden work. Pure/prompt native callbacks must not block or panic. This is a Go
ownership contract, not an untrusted-code sandbox.

Native pre-hooks receive copied, body-free request metadata per exchange; they can
change method, URL, Host, headers/trailers, Priority and native cookie controls,
not install owning bodies, callbacks
or transports. Post-hooks receive copied metadata and their failures remain causal.
Redirect callbacks receive body-free views. No native client, connection, response
body, shared mutable Setter or owning HTTP trace escapes through the facade.
Server-only TLS callbacks and QUIC's owning-connection callback are refused;
`AllowConnectionWindowIncrease` supplies the bounded non-owning alternative.
Preparation precedes native jar/userinfo credential selection on initial and
redirected requests. Authority-changing hooks drop unchanged inherited Cookie
and Authorization values and unchanged URL userinfo; explicitly replaced values
retain the hook's authority. Equivalent default-port authorities remain equivalent.
Response-cookie association and relative redirects use the effective URL. A
rejected redirect retains available response metadata without claiming complete
consumption of its already-closed body.

Manual certificate pins are snapshotted. Automatic pin probing is refused because
upstream opens an independent direct connection outside caller routing/cancellation.
Pins and destinations share IDNA, trailing-dot, IP and numeric-port normalization;
conflicting aliases and empty canonical hosts are refused.
Cookie jars are explicit borrowed native state, disabled when absent. Deliberately
passing the same jar/cache to two configurations authorizes that sharing; names
alone do not isolate explicitly shared mutable dependencies.

The explicit call context replaces `Request.Context`. Request URL/header/trailer
containers are copied before admission. Body and GetBody are acquired only after
admission and borrowed through receipt release. Close must interrupt a blocked
Read; replay factories must return independent readers and return promptly.
Server-side request state, upgrades and owning httptrace callbacks are not a
client request contract. Outbound trailers are frozen, not modified at body EOF.
`ValidateRequestV1` performs source-independent hard-bound preflight without copy
or input acquisition. `SnapshotRequestV1` applies the adopted source's exact
`RequestPolicyV1`; native operations use this before Internal admission. Snapshots
preserve Priority, ExcludedCookies and DiscardResponseCookies but omit hidden
server path-value containers. Exclusions have bounded copied count/bytes and exact
native-name semantics. Native H1 excludes the canonical Priority header created
by SetPriority; H2 profile callbacks can translate Priority to wire weights.
Field retention does not promise universal scheduling behavior.

`ProxyFromProvider`, `ProxyDirect` and `ProxyAddress` are distinct runtime
choices. They partition native route clients without changing Provider identity.
`ConnectHeaders` is copied and applies only to CONNECT; URL Basic credentials
are decoded before encoding that header. `RoutingLocked` refuses overrides.
Native HTTP/HTTPS CONNECT and SOCKS5 TCP/UDP paths use the original shared physical
socket ceiling, including SOCKS control connections. No failed proxy silently
falls back to direct. H3 via socks5h requires a literal destination, since this
native QUIC path resolves hostnames locally. Fixed-authority HTTPS templates with
target_host and target_port select controlled native H3 CONNECT-UDP/MASQUE.
Templates require an ASCII URI with a nonempty path and the admitted RFC 6570
level-3 operators; reserved/label/path-segment/path-parameter expansion and
level-4 prefix/explode modifiers are refused. Percent-encoded literals and
properly escaped IPv6 target expansion remain supported.
ProxyTLS is independent of origin TLS; nil selects ordinary proxy verification,
not inherited origin trust or credentials. Resolver configuration/callback
selection is frozen; custom DNS callback connections are source-owned and retain
UDP framing. Without a custom Dial, the platform resolver remains a borrowed OS
facility; its internal file descriptors are not represented as controlled callback
sockets. Origin/proxy DNS is local, not an inferred remote-DNS capability.
MaxProxyTunnels bounds retained HTTP CONNECT and CONNECT-UDP tunnels separately
from physical proxy sockets, including cached or still-handshaking H2 tunnels.
Deadline changes interrupt real datagram I/O; Close joins entered operations and
capsule draining. Canceling local enqueue does not close healthy sibling tunnels.
Capsule negotiation/authentication failures never authorize direct fallback.
Capsule-Protocol is a structured Boolean Item: true with unknown parameters is
valid; false, malformed or duplicated fields are not. Responses with
Content-Length, Content-Type or Transfer-Encoding, or status 204/205/206, cannot
establish a capsule tunnel. The native synthesized ContentLength zero is not an
observed Content-Length field.

QUIC datagrams and type-0 DATAGRAM capsules share a source-owned 32-packet ingress
queue. Context zero, including empty UDP payloads and non-minimal valid varints,
is supported; unknown capsule types/context IDs are streamed past. Known payloads
are bounded to 65507 bytes before allocation. Saturation or terminal tunnel
closure may drop packets; this is not reliable delivery. Complete control EOF,
truncated capsule framing and peer reset retain distinct causes. Capsule parsing
owns terminal classification, and both receiver workers and entered I/O are
joined before tunnel admission is released. A failed tunnel does not close its
healthy shared outer QUIC connection.
The route must support encapsulated datagram sizes: local qualification uses a
proxy packet size of 1400 for 1280-byte inner QUIC packets.

## Bounds, lifetimes and separate evidence

Zero Go options choose these technical defaults; explicit layered zero values
are validated rather than silently defaulted again.

| Dimension | Default / scope |
| --- | --- |
| Active / queued calls | 8 / 0, one authoritative source across aliases |
| Route clients | 16, including all runtime routes |
| Physical sockets and pending acquisitions | 32, shared across routes and proxy layers |
| CONNECT / CONNECT-UDP tunnels | 64 per source, separate from shared proxy sockets |
| Lazy ClientHello bytes | 1 MiB initial encoded installation ceiling |
| Native routing tasks | 32, using the same source's MaxConnections ceiling before proxy serialization |
| Route-origin pairs | 64 total per source, not 64 per internal client |
| Request / decoded / encoded body bytes | 8 MiB each; separate cumulative counters |
| Retained / native header ceilings | 64 KiB / 1 MiB |
| Client exchanges / replay readers | 10 / 16; not exact physical attempt counts |
| Admission / work lifetime | 30 seconds each |
| Native idle timeout ceiling | 90 seconds |

Durations and layered `*_ns` values are nanoseconds. Exact ranges are defined by
`OptionsV1` validation. Route/origin exhaustion is explicit; this version does
not evict cached identities to conceal capacity. Physical release failures keep
their original charge until positive closure is observed.
Dead H3 origin entries retain their slot until the owning HTTP/3 control work is
joined, not merely until the raw QUIC context ends. Cached MASQUE outer clients
are likewise joined before replacement. An isolated stream failure on a live
multiplexed connection does not evict that connection.
Explicit terminal QPACK Close releases its owned dynamic-table entries and wakes
waiters while preserving immutable limits, counters and the earlier error cause.
Finite encoder-stream EOF is different: native helper callers may still decode
already received entries until they explicitly close the decoder.

Worker joining and socket release do not prove immediate heap reclamation.
An entered callback or a closed redirect response can retain its old connection
graph while another connection serves later work. `Budget.TCPGraphBytes`,
`ProxyTCPGraphBytes`, `H3GraphBytes` and `ProxyH3GraphBytes` declare the relevant native graphs;
`RetainedNativeBytes` reserves the admitted protocol envelope for every possible
exchange under `MaxExchanges`. This belongs to each operation's `WorkBytes`,
independently of cached/source-resident capacity. Increasing concurrent roots
therefore increases aggregate work capacity, not a fictitious number of free
cache slots. `H2FrameBytes` comes from the same selected native configuration
resolver used by HTTP2 construction; dormant H2 settings do not add H2 state to
an H1-only graph.
The outer CONNECT TCP/H2 graph uses the proxy's native default frame/table
selection, independently of the origin's custom profile. Its TLS.Client path
is the selected nukilabs uTLS stack, not the origin's fingerprinted UClient;
the outer certificate-message limit is 256 KiB. The outer graph is reserved both
for cached proxy routes and for old response scopes that still retain it.

A one-byte witness distinguishes exact EOF from excess body data. Retained output
never exceeds its limit. Encoded Content-Length is validated below transparent
decoding; decoded limits are independent. HEAD/no-content responses and absent
length (-1) retain native meanings. gzip, deflate, brotli and zstd are supported;
unknown encodings stay encoded instead of falsely claiming successful decoding.
The native zstd window ceiling is 64 MiB. Reservations are declared envelopes,
not measurements or universal bounds on Go heap, kernel memory, custom callbacks
or arbitrary borrowed native error graphs.
The HTTP entity decoder's 64 MiB window is not the TLS certificate decoder's
envelope. Selected uTLS permits uint24 expanded certificates and a zstd streaming
window of 512 MiB plus bounded block scratch. These belong to source-owned TCP
and multiplexed proxy-tunnel reservations. Automatic 32-entry TLS session caches
are separately source-resident per route and cleared on terminal Close; explicit
borrowed caches are never cleared. Bounded H3 control workers are counted for both
origin clients and independent proxy H3 clients.
The QUIC receive envelope uses the larger of each resolved initial and maximum
window, not just the maximum-field spelling. `Budget.ProxyTunnelBytes` includes
the additional native ingress queue/workers and structured-field parser containers;
`SourceBytes` reserves this for every configured proxy tunnel. None of these
declarations silently resizes an external Runtime or releases a retired generation.
Native HPACK and QPACK table capacity is limited to 64 MiB per connection.
The [selected HTTP correction](../../../../../../third_party/nuki-http/FATHOMRY.md)
derives H2 receive credit from emitted settings: a custom omission means 65535,
explicit zero remains zero, and nonzero advertised windows are not replaced by
an unrelated 4 MiB default. HPACK decoder capacity likewise follows the advertised
setting, including zero; a dynamic reference cannot rely on a silently recreated
default table. Invalid settings close the transferred connection.
CONNECT header limits apply during H1/H2 decoding, before retention; H1 tunnel
bytes prefetched with the header remain readable and are not charged as headers.

A consumer returning before EOF yields `Complete == false`, not an invented
read error. The response view is revoked; already-entered reads and replay
callbacks remain owned until they end. A noncooperative callback/reader can keep
cleanup pending after the caller stops waiting. Body Close errors and input
reader errors are retained independently of a successful response.
Callback return interrupts an entered response read before inspecting its byte
budget or EOF state; any actual late read error remains observable.
Observed response-read failures remain required errors even if a consumer handles
them. Bodyless HEAD/204/304 representation metadata is not decompressed; an empty
encoded entity without its required compression frame is not a valid empty body.

H3 warmups and native pools remain source-owned after individual calls finish.
An early H3 response cancels and joins the affected upload, including trailers;
input failures from intermediate redirects remain in `InputErrorsCopy`. It does
not cancel unrelated streams. Native HTTP pooling may detach a dial from the
request context, but the entire routing task retains a source-wide charge until
it ends. CONNECT setup cancellation and timeout callbacks are stopped and joined
before transferring tunnel ownership.
Blocked QPACK header/trailer decoding is stream-context-aware; ordinary
cancellation does not retire the shared connection. Decoded field accumulation
and encoder literals are bounded. QPACK control instructions use atomic,
context-aware writes; its bounded cancellation queue may retire an overloaded
connection rather than silently lose required control messages.
Every dynamic reference must lie below its field section's Required Insert Count;
table membership alone cannot authorize an undeclared reference.
SOCKS and native qlog output Close failures retain ownership for retry; historical
errors are preserved after positive physical release. Qlog encoding failures
remain cleanup evidence, not a reason to claim an already-closed output is live.

## Results, errors and compatibility

`Metadata` and `Result` expose deliberate copies of bytes/headers/trailers.
An empty retained Do result is a non-nil empty slice; streaming results do not
retain the body. Status codes are data, not business success/failure classification.

Shared `fault.Kind` occurrences preserve native errors.Is/As causes without
formatting private payloads, endpoints or credentials. Primary and cleanup errors
remain separate. Complete means response framing/decode completion, not business
success, mutation durability or proof that cancellation prevented an effect.
Input byte counts measure reads, not acknowledged wire delivery. Invocation
attempts and Exchanges are inexact observations; native retries/warmups are not
invented exact counts. Handling a direct error never consumes required evidence.

Import major 1, OptionsV1 format 1, native/request runtime contracts 1, SDK v1.8.8,
Go 1.27.0 and local compatibility revisions are distinct axes. `Build` inspects
the actual consuming executable and replacements. `Profile` records non-secret
effective bounds while leaving opaque native profile/factory facts unknown.
Neither method authenticates deployment or automatically certifies a service.

## Details and executable evidence

The accepted [SDK architecture](../../../../../architecture/sdk-integration.md)
and [internal standard](../../../../../architecture/internal-sdk-integration.md)
S01–S12 govern applicable obligations. See [#53](https://github.com/frost-leo/fathomry/issues/53),
[package documentation](../../../../../../internal/httpclient/nuki/v1/doc.go),
[tests](../../../../../../internal/httpclient/nuki/v1/) and
[local SDK provenance](../../../../../../third_party/nuki/FATHOMRY.md).

From the repository root with dependencies available and Go 1.27.0 or later:

```sh
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOWORK=off go test -race -count=1 ./internal/httpclient/nuki/v1
GOTOOLCHAIN=local go test -run '^$' -fuzz FuzzProviderRequestMetadata -fuzztime=5s ./internal/httpclient/nuki/v1
```

The suite uses loopback peers, original-source regressions, shared-mechanism
assertions, actual same-process existing Providers, and offline native subtests
using consumer-selected versions. Its restricted-cache check retains only
consumer test dependency source trees and graph metadata with an empty build
cache; it does not permit network fallback. No public-site pressure run, private
HTML fixture, business credential workflow or production proxy service is claimed.

The ordinary coexistence test executes the five HTTP Providers and verifies
chromedp module linkage. Actual browser execution additionally requires an
explicit installed executable:

```sh
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOWORK=off go run -race ./internal/httpclient/nuki/v1/testdata/coexist -chrome /absolute/path/to/chrome
```

This runs all six Providers against the same loopback peer and checks browser
context/profile disposal. Use a short caller-owned temporary directory: Chrome's
Unix socket paths can exceed the platform limit under a deeply nested `TMPDIR`.
No sandbox-disabling flag or external service is needed.
