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

# Surf v1 interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** framework composition and HTTP integration maintainers.
**Status:** implemented internal Provider with maintained local native corrections;
bounded loopback verification, not production-site or arbitrary-profile qualification.
**Package:** `github.com/frost-leo/fathomry/internal/httpclient/surf/v1`.

## Responsibilities

One external configuration selects one named Surf Provider. Multiple instances,
borrowed aliases and per-call routes do not introduce default browser profiles,
business credential acquisition/refresh, retry policy, rotation, eviction, Redis
or a shared public HTTP abstraction. Selecting this package does not import other
HTTP Provider implementations.

The package reuses `resource.Prepare`, assembly ownership and authoritative
`resource.Access`; `invocation.Begin`, budgets and required independent receipts;
private `fault` causes; and `compatibility` facts. Native transports are not handed
to callers.

## Capabilities and call sequence

1. Supply `OptionsV1` with a name and external `NativeOptionsV1`.
   Native profiles may be Surf variants or custom configurations; absent Profile
   means standard TLS, not an implicit browser identity.
2. Call `PrepareV1` to freeze final data/native choices without network effects or
   lazy callbacks. Its `Metadata` supplies authoritative work, evidence and source
   envelopes; `Description` records the strict effective configuration. Call the
   prepared `Select` (or use `Select` as a preparation shorthand), attach matching
   `resource.WithLimits`, then
   `resource.Assemble` with caller-owned initialization and cleanup contexts.
   Assembly owns the Provider record; native route clients are created lazily
   under an admitted operation. Assembly readiness is not a network probe.
3. Bind an `invocation.Inbox[Result]` using `Bind`.
   Size required evidence using `EvidenceBytes`; telemetry is optional.
4. Use a native `github.com/enetx/http.Request` with `Do` or `Open`.
   Runtime headers, Cookie and body are caller inputs, not business preparation
   implemented by this Provider. `Do` returns a receipt and retains a bounded body.
   `Open` returns a controlled `Stream`; close it with an independent context.
   Server-only Form/PostForm/MultipartForm, TLS, RemoteAddr and Pattern are refused
   before input acquisition. Outbound snapshots copy client metadata but omit
   hidden server routing/path-value state. Cookie bounds include copied Unparsed
   slice descriptors, including empty strings.
5. Drain and release inbox deliveries independently of direct error handling.
   Close assemblies and retain any explicit incomplete-cleanup continuation.

`Negotiated`, `HTTP1Only`, `HTTP2Only`, `H2C` and `PreferHTTP3` identify native transport
behavior. H2-only refuses H1 fallback. H3 prefers native QUIC but can use the SDK's
fallback; inspect `Metadata.Protocol`, not the configured name. H3 uses
crypto/tls and does **not** implement JA fingerprinting. HTTP(S) proxies use
CONNECT fallback rather than pretending to carry H3; SOCKS5 can relay UDP.
`H2C` is cleartext prior-knowledge HTTP/2 through the managed dialer, not an
Upgrade request. Initial and redirect HTTPS targets, origin TLS/JA configuration
and TLS-bearing profiles reject; an HTTP-only Variant may still supply native H2
settings and header behavior. Proxy TLS remains independent. Omitted and zero
H2 initial stream windows retain their actual wire meaning, and advertised frame
size controls receive framing rather than the peer's send allowance.

Native status retries are opt-in through `NativeRetries`, codes and delay;
Surf's Retry-After handling remains bounded by caller lifetime. Redirects, jar
operations and native retries remain controlled. `RoundTrips` and invocation
attempts count observed transport entries, not exact physical attempts. Internal
fallback or native transport retries can add work; they share original admission
and physical connection limits. No distributed rate limit or exactly-once effect
is supplied.

## Configuration, runtime routes and bounds

Package major `v1`, configuration format `1`, runtime input format `1`,
Surf SDK `v1.0.206` and native compatibility revisions are separate axes.
Version zero selects format 1; unknown nonzero versions are refused.
The project minimum is Go 1.27.0. `Build` inspects the actual executable's
selected versions and replacements; local revision labels are not attestation.

Layered settings are plain, explicitly tagged values validated through resource
preparation. Durations and `*_ns` fields are nanoseconds. Unknown or malformed
fields fail; layered zero values are not silently replaced with Go defaults.
Native handles/functions are separate runtime objects, never layered DTOs.

`RequestOptionsV1.ProxyURL == nil` inherits the configured route. An explicit
empty string selects direct routing. Each route/CONNECT-header combination has
an immutable native client; no shared Setter changes concurrent request routes.
`RoutingLocked` refuses proxy changes. `MaxRoutes` bounds retained route clients;
capacity exhaustion is explicit, without business eviction. Aliases use the same
original resource admission and global TCP/UDP ceilings.
Failed construction releases its slot only after native cleanup is confirmed;
healthy concurrent waiters and later calls do not inherit the constructor's
cancellation. Static construction errors are not retried in a loop.
CONNECT field names are canonicalized, and ambiguous duplicate spellings fail.
SOCKS4/4a uses the route's USERID, including explicit empty USERID; a nonempty
password is refused because that protocol cannot carry it.
A configured resolver's error is retained before proxy acquisition; it does not
silently fall back to system or remote proxy DNS.
Controlled Resolver.Dial callbacks and transferred DNS connections retain work
through actual Close, share the existing TCP/UDP quota and preserve PacketConn
framing. A native lookup group's cancellation initiates closure; shared lookups
remain live while another waiter needs them. Panic disables the route and retains
the original error-valued cause through request/source failure; ordinary DNS errors
keep native retry behavior.

Request/response bytes, retained headers, native receive headers, queued/active
calls, routes, TCP connections, UDP sockets, native H3 clients, transport entries and replay-factory
calls have distinct bounds. Byte reservations describe bounded data handoff, not
exact native heap/RSS or kernel buffer usage. Streaming reads are serialized.
Data at a read limit is not certified as complete.

`MaxHTTP3Clients` (default 32) separately counts cached, pending and retiring QUIC
clients, including retained response bodies and actual request writers. A failed
stream cannot detach an unaccounted connection or kill a retained sibling.
`MaxProfileBytes` (default 1 MiB) declares a cooperative lazy-profile envelope;
`MaxHTTP2StreamBytes` (default 8 MiB) bounds emitted native receive credit. Lazy
callbacks run only at owned construction/handshake boundaries, never during
preparation. Encoded initial Hello and H2/H3/header outputs are checked there;
this is not a preallocation sandbox or a universal final-wire/RSS bound. The
copied native aggregate is checked against1GiB before cloning. `Metadata`
accounts for final native TLS/profile/protocol choices, sockets, clients and
configuration separately from per-call work and independently retained evidence.
PreferHTTP3 also declares a separate fixed H1-only fallback transport. Explicit
remote H3_VERSION_FALLBACK uses independent H1 ALPN without mutating concurrent
H2-capable fallback state; both share the same TCP ceiling and replay obligations.
IdleConnTimeout governs TCP idle pools, not H3's separate native QUIC policy.
DisableCompression disables Surf decoding rather than inventing Accept-Encoding
policy; default/false decoded integrity and true raw-byte completion remain distinct.

## Incremental multipart and replay

`RequestOptionsV1.Multipart` supplies ordered `Fields` and `Parts`, with at most
`MaxMultipartParts` (128) combined entries. Each part supplies exactly one
transferred `Input` or fresh-reader `Open` factory. No raw mutable SDK Multipart,
filesystem path or Builder escapes. Request Body/GetBody conflicts are refused.
The public [multipart contract](../../../../adapters/httpclient/surf/v1/interface.md#requests-and-incremental-multipart)
states transfer and replay duties in detail; both layers use this same owned path.

The native encoder writes incrementally to an owned pipe: it does not use
DrainBody or the SDK's buffering Multipart.Retry helper. Native duplicate-field
replacement, field/part order and boundary generation remain intact; the chosen
boundary is stable across safe replays. Producer work is retained before launch.
Cancellation closes the pipe and all inputs and then joins the producer; partial
factory results and unused replay bodies are also owned until release.

A one-shot part with configured status retries rejects before ownership/dispatch.
Response-triggered unsafe replay stops before a second send. Factories must return
fresh independent readers; no content is read or buffered merely to make a replay
possible. All part metadata and encoded bytes remain under existing input/header
ceilings. `InputErrorsCopy` retains producer/reader notices independently from a
possibly complete early response and from cleanup errors.

## Native copying and extension obligations

Headers, ordinary native profile containers, TLS containers, certificate bytes
and root pools are copied. A pool's existing certificates and opaque lazy state
must remain immutable. Native functions, private keys, session caches, resolvers,
jars and writers remain borrowed until assembly release: they must support the
documented concurrency, obey contexts where available, and not retain callback
arguments or start unowned work. Closure-owned construction configuration must
remain stable; per-connection factories explicitly own their variability.

`HelloSpecFactory` transfers a fresh mutable spec for each handshake and retains
opaque/custom extensions, including native state that cannot safely be cloned.
Such state must not be passed through the ordinary profile-container copier.
The native per-connection shuffle still applies when enabled by the selected
variant. No Fathomry browser/profile allowlist is installed.

`JAConfig` is a full uTLS configuration; it is not a crypto/tls configuration.
Its native flags must match the supplied spec (for example, PSK omission).
Go-TLS-specific callbacks or client certificates for a JA path require the
matching explicit uTLS configuration instead of being silently ignored.
H3 refuses JA-only configuration/factories because that path cannot use them.
TLS and proxy trust are separate. An explicit JA configuration takes precedence,
including its cache and session-ticket flags; the Go-TLS clock and random source
are retained on the implicit JA bridge.

Request middleware receives a native metadata view and can use native header
operations; body ownership and execution are unavailable. Response middleware
receives detached native metadata, without live TLS state, bodies or the original
client. Redirect callbacks receive metadata-only views and may modify headers or
return their native decision. Native trace callbacks embedded directly in a
caller context are refused, not silently run outside ownership.

Managed middleware, profile, factory, dial/resolver, CookieJar, TLS callback and
body callback panics are contained without formatting arbitrary panic payloads;
error-valued causes remain inspectable. Cache/clock callbacks cannot return native
errors: a panic disables their native route and is retained through assembly
cleanup, which can occur after the operation's result. This is not a sandbox for
arbitrary Go code. Callbacks must not call `Goexit`, terminate the process or start
unowned work. Custom connections and private keys must honor their native
interfaces, including non-panicking methods. A transferred closer must terminate
its I/O even when it reports an error; the Provider cannot enforce that property
inside an arbitrary caller implementation.
An observed native connection cleanup failure cancels that route and refuses new
native work, retaining the failed route until assembly release. This bounds error
history without dropping causes, automatic rotation or business eviction.

## Results, cancellation and failures

A non-nil receipt establishes admission, including when direct waiting ends.
Cancellation can stop the caller's wait while an input reader, closer, dial or
callback remains active. Its original lease and evidence reservation remain held
until actual work/cleanup ends; uncooperative dependencies cannot be force-stopped.

`Metadata` is a header observation. `DataCopy` may contain a bounded prefix.
`Complete` requires a clean final native EOF, framing/decoding checks and no primary
failure. HTTP status, body completeness, final reporting, local release and remote
effects are separate facts. A 200 or a nil cleanup error does not certify business
success; cancellation or closure does not prove a mutation never happened.

The method context is authoritative: it replaces the request's context, including
an already-canceled request context. A canceled method context still refuses or
stops work. There is no new dual-context merge; configured timeout further bounds
the method lifetime, including retained responses.

Primary, input/producer notices and cleanup causes remain inspectable through `errors.Is/As`.
Ordinary formatting/logging omits native error text and secrets. Explicit
URL/header/body/cause access is sensitive. Runtime values refuse JSON persistence
and reconstruction; these are not Temporal history payloads or public DTOs.

## Verification and maintenance

Maintained tests exercise H1/H2/H3, JA trust and actual custom Hello extensions,
native callbacks, native retries/redirect replay, input isolation, CONNECT/SOCKS,
failure cleanup, body/encoding completeness, admission and independent evidence.
Native SDK subtests preserve consuming selections and offline restrictions.
A constrained-cache run is required in addition to ordinary development caches.
Compatibility profiles include the normalized effective native retry-code set
in token-safe chunks of at most 32 codes (`none` when disabled), not just retry
count and delay. Profiles are checked with the actual `compatibility.Assess`
contract. Native callback completion, per-call cleanup and
whole-instance release have separate tests and evidence boundaries.
External-service and public-site qualification are not implied.
Since H3 revision v3, the upstream static-only QPACK decoder is supplemented with bounded
connection-local dynamic response headers/trailers. Prepared metadata separately
charges declared table/blocked/feedback state per H3 client and eager section/scratch
work per root. MaxHTTP3QPACKTableBytes defaults64KiB and MaxHTTP3QPACKBlockedStreams
defaults128; strict layers preserve explicit zero. Native SETTINGS are never clamped
or replaced by these ceilings. See the public [dynamic QPACK contract](../../../../adapters/httpclient/surf/v1/interface.md#dynamic-qpack-ownership).

Surf's original MIT license is retained. The enetx/http2 and enetx/http3 archives
lack standalone LICENSE files; their fork-contribution/distribution permissions
remain an owner-review prerequisite, not a resolved claim.
See the [Surf compatibility record](../../../../../../third_party/surf/FATHOMRY.md)
and separately recorded transport corrections. Owner-authorized agent review is
not independent human approval. Upgrade or integrate only after rechecking exact
sources, dependency selections, native controls and the full consuming build.
