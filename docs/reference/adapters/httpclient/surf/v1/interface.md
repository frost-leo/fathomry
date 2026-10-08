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



# Surf HTTP Adapter v1

**Audience:** independent Go consumers and Framework composers.
**Status:** implemented API with local protocol/ownership tests; release, licensing
and deployment qualification remain separate. [Issue119](https://github.com/frost-leo/fathomry/issues/119)
owns this provider and its necessary native corrections.

Import `github.com/frost-leo/fathomry/adapters/httpclient/surf/v1`. Requests and
headers are **github.com/enetx/http**, not a converted net/http DTO. No owning SDK
client, Builder, transport, Multipart, raw connection or filesystem path escapes.

## Selected source and delivery

Selected SDK: Surf v1.0.206, origin
`7da0502899af06f8318f95e632797cb2ac0c6c20`; HTTP2 v1.0.26 and HTTP3 v1.0.9.
All three local corrections have revision v2, separate from upstream versions.
See [Surf provenance](../../../../../../third_party/surf/FATHOMRY.md),
[H2](../../../../../../third_party/surf-http2/FATHOMRY.md) and
[H3](../../../../../../third_party/surf-http3/FATHOMRY.md). Original source notices
and transport-fork redistribution uncertainties are retained; tests do not grant
source permissions or release clearance.

Root replace directives are not inherited and nested modules are absent from root
ZIPs. Independent applications use the generator's actual immutable versioned
replacement policy; development mode explicitly selects a checkout. Build reports
the executing module/replacement graph, not source attestation. Do not remove the
selected corrections merely because another graph compiles.

## Prepare and own

`Prepare(Settings, NativeOptions)` freezes the final selection offline, without
sockets, SDK client construction or arbitrary profile/factory calls.
`Validate(Settings)` checks strict data only. `Recommend(Settings)` covers the
data-only native-standard-TLS selection; native-aware callers use Prepared.Policy.

Prepared.Policy and Compose come from Internal's authoritative metadata plus
public bridge/source-record costs. Compose covers exactly the listed overlapping
owners, including retired generations. Policies reject unrepresentable totals,
insufficient Runtime/Inbox capacity and oversized replacement roots before native
dispatch; they never silently enlarge caller mechanisms.

Create `adapters.Runtime` and `adapters.Inbox[Result]` from the policy, then call
Prepared.Open. Passing a second Native selection is rejected. Open returns an
Owner; retain and close any non-nil owner even alongside an error. Owner alone has
shutdown authority. Client/Handle are non-owning. Open's ctx owns the entire source
lifetime, not just construction waiting. Source construction is local-only and is
not endpoint readiness evidence.

Close seals work, cancels retained operations, joins native writers/producers and
closes actual resources. Close waiting can time out while cleanup continues;
ShutdownComplete and repeatable Release/Close report actual local completion.
Borrowed code/readers must cooperate; an uncooperative callback can retain ownership.

## Settings and native borrowing

Version zero selects format1. Pointer nil means unset; explicit zero/false/empty
mode is preserved. Durations are integer nanoseconds. Settings serialization may
contain proxy credentials; normal fmt/slog is redacted.

| Setting | Default / applicability |
| --- | --- |
| Name, Version | Named source; version0/1 |
| Mode | negotiated; http1, http2, prefer-http3, h2c |
| ProxyURL, RoutingLocked | Native configured route; unlocked by default |
| DisableCompression | false; retains native framing/decoding distinctions |
| MaxActive, QueuedCalls | 8,0; zero queue disables queueing |
| MaxRoutes, MaxTCPConnections, MaxUDPSockets | 16,32,16; source-wide ownership |
| MaxHTTP3Clients | 32; cached, pending and retiring client entries, not UDP sockets |
| MaxProfileBytes | 1MiB; 1KiB..1MiB declared lazy/profile envelope |
| MaxHTTP2StreamBytes | 8MiB; 4MiB..1GiB lazy receive-stream ceiling |
| MaxRequestBytes, MaxResponseBytes | 8MiB each; positive up to1GiB |
| MaxHeaderBytes, MaxNativeHeaderBytes | 64KiB,10MiB; distinct retained/native parser limits |
| MaxRoundTrips, MaxReplays | 10,32; maxima128,1024 |
| NativeRetries, RetryCodes, RetryDelay | 0, native default codes when enabled,0; no added business retry |
| AdmissionTimeout, Timeout, IdleConnTimeout | 30s,30s,90s; positive1ms..24h |

AdmissionTimeout is Internal admission; public queue waiting is controlled by the
method context. Timeout bounds admitted root lifetime, including automatic retained
stream cleanup. Public policy includes source slots as well as operation slots.
IdleConnTimeout governs idle TCP H1/H2/JA/h2c connections, not active streams or
H3's separate native QUIC idle/keepalive policy. DisableCompression controls Surf's
automatic response decoding: true preserves encoded bytes, while default/false
decodes and validates them. It does not rewrite a caller's Accept-Encoding choice.
MaxNativeHeaderBytes uses each selected parser's accounting, not a universal wire
byte unit: H1 counts its received header block; H2/H3 include per-field accounting,
and ordinary enetx/http H2 adds its native 320-byte allowance. JA-H2/h2c and H3 use
the supplied field-list limit directly. MaxHeaderBytes separately bounds retained
metadata; duplicate wire fields can exceed a native limit while retained data fits.

NativeOptions retains Profile/OS, fresh context-bearing HelloSpecFactory,
standard TLSConfig, separate JAConfig and ProxyTLSConfig, native Headers/Jar,
DialContext/ListenPacket/Resolver, header-only RequestMiddleware, metadata-only
ResponseMiddleware and CheckRedirect. Profile is optional; omission means standard
TLS, never automatic browser selection. H3 uses standard TLS, not JA/uTLS.

Client-used data containers are copied: profile/spec data, headers, TLS/JA vectors,
certificate byte containers and named certificates, ECH client data and JA ALPS
ApplicationSettings. The checked aggregate copied-container declaration is at most
1GiB before cloning. Certificate-pool indexes are cloned; their opaque lazy backing
remains immutable borrowed data. Keys, caches, writers and functions are concurrent,
cooperative borrows through source release. Server-only TLS fields (including
server ECH keys) are not client behavior; their opaque data is not promised as a
new client capability or deeply frozen resource.

Lazy ConfigureH2/H3, BuildHeaders, boundary and Hello callbacks do not run during
preparation. Outputs are checked where owned. Dynamic native SNI/session/padding
and foreign callback memory remain cooperative declarations, not a hard allocation
sandbox. Budgets cover selected copied containers, native frame/codec buffers,
stream receive ceilings, peer TLS state and actual H3 client cardinality. UDP
socket count alone cannot bound QUIC connections. Declared bytes are not heap/RSS.
Custom Resolver.Dial callbacks and their returned DNS connections remain owned
until actual Close. DNS network sockets use the same TCP/UDP source ceilings;
PacketConn framing is preserved. Native lookup-group cancellation initiates Close
without canceling a healthy shared lookup waiter. A resolver panic disables its
route and retains the original cause through source Close, rather than losing it
to Go's DNSError stringification. Ordinary DNS errors retain native retry semantics.

## h2c and native profiles

H2C is explicit **prior-knowledge HTTP/2**, not HTTP/1 Upgrade or HTTPS rewriting.
It uses the already managed effective dial path, including supported proxies,
socket admission, cancellation and writer ownership. HTTPS origins (including
redirects) and actual origin TLS/JA/Hello/shuffle inputs reject before dispatch.
ProxyTLSConfig remains meaningful for a TLS proxy and is not confused with origin
TLS. To use only HTTP facets of a Variant, explicitly clear HelloSpec, HelloID and
ShuffleExtensions while retaining its headers, H2 settings and boundary functions.

Emitted H2 SETTINGS govern receive credit: omitted custom INITIAL_WINDOW_SIZE
means65535, explicit zero remains zero, and an empty list keeps native generated
defaults. MAX_FRAME_SIZE governs incoming frames, not peer-advertised outgoing
limits. Normal larger-window/frame controls and rejecting overflow peers are
maintained alongside original-source ablations.
Native fluent profile setters keep their own omission rules: a setter that omits
zero does not become an explicit zero wire setting. The receive-credit guarantees
above concern the SETTINGS actually emitted, not a universal options DTO.

H3 cached/retiring clients are bounded separately. A failed stream may retire its
client without killing a retained sibling response. Bodies and real request writers
retain their client. Failed/canceled pending dials release reservations once finished.
A safe rejected-request replay joins only its previous writer before acquiring the
replacement; a genuinely retained sibling may still cause capacity refusal.
An explicit remote H3_VERSION_FALLBACK signal uses a separate owned HTTP/1.1-only
fallback transport with independent ALPN configuration and the same TCP quota.
It does not mutate the shared H2-capable fallback. One-shot and other non-fallback
errors still stop before resending; replayable multipart acquires fresh bodies.

### Selected native QPACK limitation

The selected `github.com/quic-go/qpack v0.6.0` decoder is static-table-only, and
the selected H3 transport does not consume dynamic encoder-stream instructions.
Native profile SETTINGS are preserved, including profiles advertising nonzero
QPACK capacity; that advertisement does not add dynamic decoding. A peer using
the advertised dynamic table can fail. Static/literal H3 response controls pass;
this is an inherited native limitation, not H3/ConfigureH3 API removal or a claim
of arbitrary browser-profile fidelity. Adding dynamic QPACK is distinct from the
implemented h2c, multipart and owned replay paths.

## Requests and incremental multipart

Do(ctx, cleanupCtx, *enetx/http.Request, ...RequestOptions) returns finite bounded
response data; Open(ctx, request, ...) returns a response Stream and receipt.
The **method context replaces Request.Context**, rather than merging them.
Native header order, method/Host fields, fixed trailers, native retries and response
protocol behavior remain provider-specific. Middleware cannot replace contexts,
owning bodies or transports.

RequestOptions version0/1 supports nil ProxyURL inheritance, explicit empty direct
routing, per-call proxy addresses, bounded CONNECT headers and Multipart. At most
one options value is admitted. Routing is copied; invalid/refused routes never
silently become direct. Origin and proxy trust are separate.
Request metadata, ProxyURL values and request/options containers must remain
immutable until Do/Open returns. Their native snapshot is taken after public
admission, not before queued waiting. Admitted readers and factory closures stay
borrowed through actual cleanup; caller return is not permission to reuse them.

Multipart contains up to128 combined Fields and Parts. Native ordered unique fields
come first, then files in supplied order. Duplicate field names replace their value
at the original position; this is not arbitrary field/file interleaving. Each Part
has Name/FileName/ContentType and exactly one Input or Open. FileName is only MIME
metadata, not a filesystem-access request. Request Body/GetBody conflicts reject.

Input is one-shot. Open(ctx) must return a fresh independent closable reader yielding
the same logical bytes on each replay; reader-plus-error still transfers cleanup
responsibility. Initial factories are lazy; replay preflight acquires fresh readers
before any repeated dispatch, without reading/materializing the whole upload.
Comparable aliases are refused; opaque independence and byte equivalence remain
caller obligations. Static fields alone are replayable; mixed inputs are one-shot.

The native encoder writes incrementally through an owned pipe. One native/profile
boundary is chosen per logical upload and reused for replay. Multipart's generated
Content-Type overrides initial caller defaults as in the native API; middleware
cannot corrupt the selected boundary. Producer, pipe, already acquired and unused
one-shot inputs remain owned until cleanup ends. Cancellation independently closes
pipe/inputs before waiting for production; no transport-writer/producer join cycle
is used. Every generated body is tracked, including GetBody results that redirect
policy does not send.

One-shot input plus NativeRetries>0 rejects before borrowing/sending. A one-shot
307/308 keeps the native redirect response instead of manufacturing replayability.
Replayable status retries, redirects and supported protocol fallbacks obtain fresh
bodies with the same boundary. A failed/aliased replay stops before another send;
native behavior may close the old response body, but observed metadata/causes remain.
Nothing adds business idempotence, retries, credential refresh or buffering to fake
streaming. Incrementality tests require peer receipt before the tail is generated.

## Results, cleanup and evidence

Result/Metadata expose immutable copies of response headers, finite bytes, trailers,
protocol/status/URL, source identity, actual generation/correlation, body/input-read
counts, RoundTrips and nonexact Attempts. Counts are not exact physical attempts or
wire traffic. HTTP status remains data. Complete means final response EOF/integrity
without primary failure, not business success or proof of full upload/remote effects.

InputErrorsCopy records input/producer/replay notices separately from an early
complete response. Primary, Cleanup, waiter timeout, EOF, Close and Ack remain
different facts. Stream permits one reader and concurrent repeatable Close;
Close is required after EOF and retains native Surf primary+cleanup semantics.
Cleanup does not require a new admission/evidence slot.

For Fixed/Follow, bind Settings to Handle and return Owner.Release in the resource
Instance. Using borrows caller mechanisms and an exact generation; Native is not
accepted there. Old streams/producers retain their actual source through replacement.
Failed/obsolete candidates retain cleanup authority. Receiver Retry redelivers
evidence, never reissues HTTP. Direct handling does not acknowledge the Inbox.

Network facility0x202/http_surf is additive. Definitions/Resources and en/zh-CN CLI
catalogs work offline; errorbridge preserves deliberate errors.Is/As and redacts
diagnostics. Runtime values refuse JSON serialization. See [capabilities](capabilities.md)
and the executable public-only direct/Framework fixtures.
