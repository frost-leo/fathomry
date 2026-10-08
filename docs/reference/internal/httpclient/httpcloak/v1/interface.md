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

# HTTPcloak v1 interface

[Documentation](../../../../../README.md) / Internal HTTP capability

**Audience:** framework composition and HTTP capability maintainers.
**Status:** implemented internal boundary; no production-site or universal
fingerprint/stability qualification.
**Package:** `github.com/frost-leo/fathomry/internal/httpclient/httpcloak/v1`.

## Responsibilities and call sequence

An explicit configuration supplies one named Provider and exactly one
`PresetName`, strict `PresetJSON`, or native `fingerprint.Preset`.
There is no default browser profile or catalog of permitted experimental recipes.
Native profile names are resolved and copied during selection, not per route.
The integration never registers an instance in HTTPcloak's global preset registry.
Managed ECH failure hints are also instance-local; unrelated global hints cannot
silently disable an instance's ECH input.

Call `PrepareV1(options, layers...)` once, inspect its `Metadata`, and construct
from that same object's `Select`. Attach the metadata's exact `Limits` with
`resource.WithLimits`, assemble, then `Bind` with a required evidence Inbox.
`Description` and `RequestPolicy` describe that resolved selection, not a second
registry read. `Select` and `LimitsV1` remain convenience routes
for unoverridden Go options; separate calls do not provide a shared frozen preset.
Layered settings must have matching composition limits; Bind refuses amplification.
Borrowed aliases retain the authoritative source identity, limits and ownership.

`Do` retains bounded decoded bytes. `Open` returns a controlled stream, which must
be closed even after EOF. Both return a receipt once accepted; an error does not
discard the receipt. Drain the independent Inbox and release each delivery after
technical and local-use completion have been observed.

This is not an external public package, complete Workflow runtime, account policy,
Cookie/token acquisition or refresh, business retry, rotation/eviction service,
Redis coordinator, or a general wrapper around every HTTPcloak API.

## Configuration and native capabilities

Version 1 supports explicitly selected HTTP/1.1, HTTP/2 and HTTP/3. Go option
zero defaults select technical bounds and HTTP/2, not a browser preset. Layered
values are validated without silently reapplying those defaults.

Native options retain custom fingerprint data, JA3 extras, H2 and TCP settings,
header/pseudo-header order, ConnectTo, ECH inputs, local address binding, TLS
verification, supplied cookie jars and explicit key-log writers. Bounded native
containers and verification wrappers are copied. `RootCAs` pools are borrowed
immutable authority through actual source release, not enumerated or cloned;
callers needing a separate mutable root set must supply their own clone. Closures,
jars and writers are borrowed through source release and must support concurrent
invocation. Preparation does not invoke them.

The managed path returns wire bytes before decoding. H2/H3 header limits are
effective native receive settings and may alter the advertised receive profile;
this is not a claim of byte-identical browser fingerprint equivalence.
An absent key-log writer does not enable the native SSLKEYLOGFILE fallback.

Important supported-boundary limits:

- There is no implicit protocol downgrade or H3/H2 race.
- HTTP/HTTPS CONNECT and SOCKS5/SOCKS5h TCP routes are supported for H1/H2.
  H3 supports explicit direct, SOCKS5/SOCKS5h UDP and `masque://` routes. HTTP(S)
  proxy URLs do not auto-detect MASQUE, and failed proxy setup never selects direct.
- Nonzero `QuicIdleTimeout` is H3-only, `CustomH2Settings` is H2-only, and custom
  pseudo-header order applies to H2/H3. Explicit H1 ECH, H3 custom JA3/extras,
  orphan JA3 extras and `EnableSpeculativeTLS=true` are refused, not accepted as
  inert support. Normal CONNECT is retained without speculative TLS.
- TCP fingerprints affect TCP legs, including SOCKS UDP's TCP control connection,
  not direct or MASQUE QUIC packets. Nonzero native overrides retain their native
  merge semantics; zero keeps the preset. Kernel/platform limits still apply:
  Linux `WindowScale` is advisory, not a controllable wire value, and DF is
  IPv4-only best effort. No kernel/network reconfiguration is performed.
- Native distributed session-cache backends and mutable client/Session setters
  are not exposed. Original unconsumed SDK paths remain unqualified.
- Bindings use exclusive checkouts through input/body/callback completion. Idle
  connections can be reused, but separate operations are not multiplexed on the
  same H2/H3 binding. Socket exhaustion rejects rather than adding a hidden queue.
- Admission covers actual managed TCP/UDP sockets, concurrent DNS exchanges,
  QUIC connections and owning CONNECT-UDP requests. These are source-wide
  allowances, not one nominal socket per binding. They are not a process RSS,
  kernel socket-buffer, remote capacity or distributed quota guarantee.

## Exact preparation and resource meaning

`Metadata` separates `WorkBytes`, independent `EvidenceBytes`, `SourceBytes` and
queue limits. The selected native builders determine actual H2 decoder/encoder
tables and stream credit, H3 QPACK/receive/stream limits and TLS state/scratch.
Explicit H2 zero receive window and zero decoder table mean zero on the wire
**and** in the local receiver. TLS compressed-certificate expansion and streaming
decoder scratch are separate from the small TLS message-size limit. DNS parsing
similarly reserves expanded records, not only its 65535-byte wire message.
H2 connection credit is distinct from stream credit: its initial WINDOW_UPDATE
increment governs actual native inflow and preparation residence. Zero selects
the native default increment; a total above the signed-31-bit protocol bound is
refused before construction.

`RetainedBindingBytes` is the selected native binding envelope plus its bounded
preset/ECH containers. Each root additionally reserves `MaxExchanges` copies of
that unit: closed redirect bodies retain their binding/native response graph
until the whole operation releases, even when that binding was evicted from the
source cache. This applies to H1/H2/H3 using their different native envelopes.
Active and queued limits scale this root cost; independent final-result evidence
does not inherit it. Source-cache and overlapping-generation charges remain
separate, deliberately conservative rather than relying on immediate GC.

Source residence includes frozen configuration/presets, all possible bindings,
managed physical sockets, active DNS parse storage, ECH cache capacity and H3
QUIC/tunnel queues. MASQUE additionally reserves 64 times the resolved header
limit for structured-field parser containers per allowed owning CONNECT request;
this is separate from raw response-header bytes and fixed queue/worker storage.
`H3RetainedBytes` also charges each binding for the worst-case inner and outer
HTTP/3 graphs after active QUIC slots are released: a closed connection context
does not prove decoder/control/cache residence was reclaimed. Retry replacement
joins the old client before constructing another, and retiring bindings still
count against `MaxBindings`. The declared bound conservatively overlaps active
QUIC-core and retained-binding reservations rather than relying on a prompt GC
or allowing `MaxQUICConnections < MaxBindings` to undercount retired state.
Borrowed callback/jar/root-pool memory is not misrepresented
as owned bytes. These are declared envelopes; public composition must add its
bridge/evidence costs and account overlapping generations without resizing the
caller's Runtime.

The Go option zero defaults and explicit layers are intentionally different:

| Fields | Go default | Valid resolved bound |
| --- | --- | --- |
| `MaxActive`, `QueuedCalls` | 8, 0 | 1–1024, 0–4096 |
| `MaxConnections`, `MaxBindings` | 16, 16 | 1–4096, 1–1024 |
| Request / decoded / encoded byte limits | 8 MiB each | 1 byte–1 GiB each |
| `MaxHeaderBytes` | 64 KiB | 1 KiB–1 MiB |
| `MaxExchanges`, `MaxReplays` | 10, 16 | 1–128, 1–1024 |
| Admission / operation timeout | 30 s each | 1 ms–24 h each |
| `ResolverNetwork`, `ResolverTimeout` | UDP, 5 s | UDP/TCP, 1 ms–1 min |
| `MaxDNSActive`, `MaxResolvedAddresses` | 4, 16 | 1–1024, 1–256 |
| `MaxAddressRaces`, `AddressRaceDelay` | 2, 250 ms | 1–address ceiling, 1 ms–1 s |
| `MaxECHEntries`, `MaxECHConfigBytes` | 64, 64 KiB | 1–4096, 1 byte–64 KiB |
| `MaxQUICConnections`, `MaxControlStreams` | 32, 16 | 1–4096, 1–1024 |

Durations are nanoseconds in layers. Name/preset/proxy/native containers have
separate pre-copy shape bounds; `ValidateDataInputV1` and `ValidateRequestV1`
check those before serialization or input acquisition. Invalid explicit zero is
rejected rather than defaulted again. A source declaration above 1 TiB is refused.
Protocol-inapplicable technical allowances do not enable an additional protocol.

## Resolver, ECH and route authority

`ResolverAddress` authorizes a literal IP:port resolver, with UDP or TCP chosen
explicitly. Empty authority permits literal targets and literal `ConnectTo`
mapping; a direct hostname that requires DNS is refused before native effects.
There is no process-global resolver fallback. DNS slots own their real socket,
deadline and cancellation work until it is closed and joined. Address answers are
bounded by `MaxResolvedAddresses`; native staggered TCP and direct QUIC races
honor `MaxAddressRaces` and join successful late losers before returning.

ECH semantics remain protocol-specific:

| Path | Selection and observation |
| --- | --- |
| H1 | No ECH. Explicit bytes/domain are refused as ineffective. |
| H2 | Explicit `ECHConfig`, otherwise configured `ECHConfigDomain` discovery. No automatic target discovery. |
| Direct H3 | Explicit bytes, configured domain, otherwise target-name discovery through declared resolver authority. Literal targets need no discovery. |
| Proxied H3 | Explicit bytes or configured-domain discovery only; no implicit target HTTPS query. Proxy route is preserved if ECH fails. |
| `DisableECH` | Suppresses applied bytes and HTTPS discovery on admitted H2/H3 paths. |

Every source owns its bounded entry cache; no global cache is consulted or
invalidated. Input/output ECH bytes are copied. Positive responses use their DNS
TTL; empty responses are cached for five minutes. At capacity the oldest stored
entry is evicted. An expired entry may serve for up to five additional minutes
when refresh fails, but caller cancellation/source stopping is not successful
stale discovery. Failed handshakes can invalidate this source's entry only.
Native best-effort behavior, including the direct-H3 ECH retry without ECH, is
not a fail-closed concealment guarantee. `VerifyConnection` observes actual
`ECHAccepted`; configured/nonempty bytes alone prove no handshake outcome.

TCP proxy names resolve locally through the source; CONNECT and SOCKS TCP carry
the target name to the proxy. SOCKS UDP preserves that name on its datagrams but
may also resolve it locally for native response-dispatch keys. A failed optional
dispatch lookup does not authorize ambient DNS; target-confined relay dispatch
remains native behavior. A managed SOCKS tunnel owns one control TCP socket, one
relay-facing UDP socket and two loopback UDP sockets, plus QUIC and relay workers.
Automatic reconnect is disabled. Native socket-buffer requests are kernel hints.

MASQUE resolves both the proxy and target locally, then owns one outer UDP/QUIC
connection, an HTTP/3 CONNECT-UDP request and an inner QUIC connection. Proxy and
origin TLS names, roots and verifier callbacks are independent; origin
`InsecureSkipVerify` never weakens proxy authentication. Basic proxy credentials
come from its URL and never become origin headers. The default well-known
CONNECT-UDP path, capsule negotiation, context-zero datagrams and bounded queue
remain native; malformed settings/status/capsule responses fail setup. Setup
cancellation, changing read/write deadlines, failed inner acquisition and source
shutdown terminate actual work and retain cleanup errors. Tunnel flow-control
and packet-size restrictions do not promise an identical direct-H3 fingerprint.
No packet connection or owning tunnel escapes this boundary.
A joined control-body reader receives DATAGRAM capsules and discards unknown
capsules with fixed scratch. Truncated capsule fields, FIN and RESET terminate
tunnel reads/writes even when the outer QUIC connection remains alive.
Negotiation requires a parsed true Structured Field Boolean Item; legal unknown
parameters are ignored, while malformed/list/non-Boolean/false values fail setup.
Capsule responses with Content-Length, Content-Type, Transfer-Encoding or status
204/205/206 are malformed rather than empty successful tunnels.

## Runtime input and copying

Use native `github.com/sardanioss/http.Request` values. Bounds are checked before
headers, URL, trailers and runtime route containers are copied before queueing.
Server-only form/TLS/remote-address/request-URI/response state and legacy Cancel
authority are refused, not silently discarded by conversion. Caller-provided headers,
Cookie values and replay factories stay available; no business defaults overwrite
them. Request readers are taken only after admission and remain borrowed until
the receipt confirms release. GetBody must return independent readers. Close must
unblock Read, including a canceled or abandoned transfer.

`RequestOptionsV1` chooses configured routing, explicit direct access or a
per-call proxy. A routing-locked source refuses overrides. CONNECT headers are
proxy-only and cannot leak into the origin request. Route changes create/reuse
bounded immutable bindings; they never call a shared mutable routing Setter.

Header order, exact header pairs, TLS-only mode and client-hint suppression retain
native request expressiveness. Exact headers bypass the normal header/jar pipeline;
protocol framing still applies. Optional redirects remain bounded, preserve
replay obligations and apply native redirect header filtering. The redirect
callback receives metadata-only copies and can veto; modifying those copies does
not retarget the operation. Cookie jar URL and cookie containers are likewise
isolated. Jar output is bounded before copying or entering a network exchange,
including empty native extension containers; response cookies retain the response
URL and native fields rather than being reassigned to a future redirect target.
Profile headers obey the same framing, value and size boundaries as runtime input.
Preset authorization/Cookie defaults are applied only when TLS-only mode is off,
then participate in redirect credential filtering rather than being reapplied
at each destination. H1 informational responses are consumed within one aggregate
header budget; a 1xx block is not a final response.
Exact H2 Cookie pairs are checked at the HPACK field boundary, not by a server
header accessor that may join separately encoded fields.

The explicit call context supplies context values and bounds the native lifetime.
The native request's context also cancels admission/work. A separate cleanup
context controls waiting only. Callbacks must not reenter their own operation or
start unaccounted background work; cancellation cannot forcibly terminate them.
Native `httptrace.GotConn` is refused on both contexts before admission because
its argument exposes an owning socket. Other legal trace callbacks, including
`WroteRequest` and `Got1xxResponse`, retain their native timing and completion.

## Completion, errors and resource release

These are separate facts:

| Event | Meaning |
| --- | --- |
| Open/Do returns | The caller received a stream, result or waiting error. |
| Caller stops waiting | Its wait ended; native work may still exist. |
| Receipt Final | Required technical and cleanup evidence is finalized. |
| Receipt Released | This call and its borrowed work no longer use the source. |
| Source Quiescent/Released | Source users/background dependencies and owned resources have been reconciled. |

Response integrity checks wire length before decoding and decoded limits after
decoding. HEAD/204/304 representation lengths are not mistaken for payload lengths.
Partial streams, malformed compressed input, short framing and failed input replay
cannot become complete merely because a peer returned 200 or Close returned nil.
Decoder-buffered trailing input is checked as well as the underlying wire stream.
Known-length H1 uploads cannot transmit surplus input beyond their declared
framing; the bounded excess-byte witness is still counted as input consumption.

`Result.Complete` requires completed framing/decoding and input consumption,
without a primary error. `InputComplete` and `RequestBytesRead` concern readers,
not peer receipt. `WritesObserved` records native write notifications; observed
attempts remain inexact because native connection/request retries can occur.
None of these facts establishes business success, no external effect or rollback.

Primary, cleanup and native callback failures retain intentional `errors.Is/As`
inspection. Native errors can carry sensitive details; default formatting is
restricted. Metadata/body/cause access is deliberate sensitive inspection.
Results own their immutable containers; accessors return copies. Runtime values
refuse JSON persistence and are not durable Workflow/history DTOs.
Response header ordering/casing is optional native evidence: nil means unavailable.
Bounded managed H1 parsing does not collect original response spelling; this does
not limit the separately supported exact request-header casing.

Source cleanup retains unresolved responsibilities and explicit continuations.
Earlier cleanup causes remain inspectable after eventual release. Composition
must bound aggregate cleanup attempts/history, not merely each individual wait.

## Version axes and evidence

- Package/API: import major v1.
- Configuration: OptionsV1 / resource format 1; Options.Version zero aliases format 1.
- Runtime input: RequestOptionsV1, not a serialized configuration/history format.
- SDK: HTTPcloak v1.7.2 plus exact independently versioned dependencies.
- Local compatibility: the explicit revision markers and original-source manifests.
- Go/toolchain, framework module, deployment and Workflow/mode versions are separate.

`Build` reports the consuming binary and replacements. `Profile` exposes only
non-secret effective choices. Missing service/build facts stay unknown; no default
support catalog or production qualification is invented.

See [native corrections](../../../../../../third_party/httpcloak/FATHOMRY.md),
[package source](../../../../../../internal/httpclient/httpcloak/v1/doc.go),
[request/stream tests](../../../../../../internal/httpclient/httpcloak/v1/client_test.go),
[isolated routing tests](../../../../../../internal/httpclient/httpcloak/v1/proxy_test.go),
[decoding tests](../../../../../../internal/httpclient/httpcloak/v1/encoding_test.go),
[raw framing/HPACK tests](../../../../../../internal/httpclient/httpcloak/v1/framing_test.go)
the [ECH peer controls](../../../../../../internal/httpclient/httpcloak/v1/ech_protocol_test.go),
[source resolver controls](../../../../../../internal/httpclient/httpcloak/v1/resolver_test.go),
[SOCKS UDP controls](../../../../../../internal/httpclient/httpcloak/v1/socks_udp_test.go),
[MASQUE controls](../../../../../../internal/httpclient/httpcloak/v1/masque_test.go),
[issue #51](https://github.com/frost-leo/fathomry/issues/51) and
[issue #120](https://github.com/frost-leo/fathomry/issues/120).
Private failed controls and corrective-review logs remain owner-local.
