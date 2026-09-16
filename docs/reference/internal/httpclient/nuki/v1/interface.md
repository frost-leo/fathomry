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
Production qualification and the SOCKS dependency's publication permissions are
not established.
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
2. `Select` freezes configuration; `LimitsV1` supplies limits for unlayered Go
   options. Apply `resource.WithLimits`, then `resource.Assemble`. Native route
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
Nil/panicking factories are errors, never implicit profile fallback.

Accepted native configuration is not a promise of byte-for-byte wire settings.
The SDK imposes `TLS.OmitEmptyPsk=true`, enables H3 datagrams and disables QUIC
path-MTU discovery. H3 selects its protocol ALPN; TCP ALPN comes from the native
profile, narrowed by `HTTP1Only`. Explicit destination SNI and session caches
are honored. QUIC idle timeout is capped by the configured source idle ceiling;
a smaller explicit timeout remains effective. Receive-header ceilings override
larger native profile values. These are transport mechanisms, not business defaults.

TLS/profile/QUIC/transport containers are copied. Functions, private keys,
certificate leaf objects, randomness, session caches, Jar and Tracker retain their
documented borrowing through source release. Certificate leaf objects and objects
behind root-pool clones must remain immutable. Borrowed functions/objects must be
concurrent-safe and cooperative; they cannot retain callback arguments or start
hidden work. Pure/prompt native callbacks must not block or panic. This is a Go
ownership contract, not an untrusted-code sandbox.

Native pre-hooks receive copied, body-free request metadata per exchange; they can
change method, URL, Host and headers/trailers, not install owning bodies, callbacks
or transports. Post-hooks receive copied metadata and their failures remain causal.
Redirect callbacks receive body-free views. No native client, connection, response
body, shared mutable Setter or owning HTTP trace escapes through the facade.
Server-only TLS callbacks and QUIC's owning-connection callback are refused;
`AllowConnectionWindowIncrease` supplies the bounded non-owning alternative.

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

`ProxyFromProvider`, `ProxyDirect` and `ProxyAddress` are distinct runtime
choices. They partition native route clients without changing Provider identity.
`ConnectHeaders` is copied and applies only to CONNECT; URL Basic credentials
are decoded before encoding that header. `RoutingLocked` refuses overrides.
Native HTTP/HTTPS CONNECT and SOCKS5 TCP/UDP paths use the original shared physical
socket ceiling, including SOCKS control connections. No failed proxy silently
falls back to direct. H3 via socks5h requires a literal destination, since this
native QUIC path resolves hostnames locally. MASQUE URL templates are explicitly
unqualified/refused: the current native adapter does not honor write deadlines.
Those refusals are mechanism boundaries, not a business profile whitelist.

## Bounds, lifetimes and separate evidence

Zero Go options choose these technical defaults; explicit layered zero values
are validated rather than silently defaulted again.

| Dimension | Default / scope |
| --- | --- |
| Active / queued calls | 8 / 0, one authoritative source across aliases |
| Route clients | 16, including all runtime routes |
| Physical sockets and pending acquisitions | 32, shared across routes and proxy layers |
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

A one-byte witness distinguishes exact EOF from excess body data. Retained output
never exceeds its limit. Encoded Content-Length is validated below transparent
decoding; decoded limits are independent. HEAD/no-content responses and absent
length (-1) retain native meanings. gzip, deflate, brotli and zstd are supported;
unknown encodings stay encoded instead of falsely claiming successful decoding.
The native zstd window ceiling is 64 MiB. Reservations are declared envelopes,
not measurements or universal bounds on Go heap, kernel memory, custom callbacks
or arbitrary borrowed native error graphs.
Native HPACK and QPACK table capacity is limited to 64 MiB per connection.
CONNECT header limits apply during H1/H2 decoding, before retention; H1 tunnel
bytes prefetched with the header remain readable and are not charged as headers.

A consumer returning before EOF yields `Complete == false`, not an invented
read error. The response view is revoked; already-entered reads and replay
callbacks remain owned until they end. A noncooperative callback/reader can keep
cleanup pending after the caller stops waiting. Body Close errors and input
reader errors are retained independently of a successful response.
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
