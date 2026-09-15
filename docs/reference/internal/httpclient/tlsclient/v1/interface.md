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

# tls-client v1 interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** internal composition and HTTP capability maintainers.
**Status:** implemented internal Provider. Upstream license compatibility remains
unresolved; delivery does not establish distribution permission or production-site
qualification.
**Package:** `github.com/frost-leo/fathomry/internal/httpclient/tlsclient/v1`.

## Responsibilities and call sequence

One externally configured entry selects one named Provider. The profile is required:
there is no built-in business default or profile allowlist. Different entries own
different clients, certificate pin stores and resource limits. An explicitly borrowed
alias shares the original identity and allowance rather than acquiring another client.

1. Supply `OptionsV1` and `NativeOptionsV1.Profile`; optional ordered
   `resource.Layer` values contain plain YAML/JSON settings, never runtime handles.
2. `Select` validates and snapshots configuration without network I/O or calling
   native factories. Use `LimitsV1` for unoverridden Go options, apply
   `resource.WithLimits`, then `resource.Assemble`.
3. Create a separately bounded `invocation.Inbox[Result]`, then `Bind`.
   `Client.EvidenceBytes` reports the per-call evidence reservation. An insufficient
   Inbox byte/count allowance rejects calls before native work.
4. Pass native fhttp requests to `Do` for retained bytes or `Open` for streaming.
   Close streams even after EOF, using an independent cleanup context.
5. Inspect and release every independent Inbox delivery, even when the direct
   caller has handled its error. Finally close borrowing scopes and the owning assembly.

Public framework/Workflow APIs and the later application anti-corruption layer are
not supplied. This package does not import other HTTP Providers.

## Native capabilities and runtime routing

| Capability | Contract |
| --- | --- |
| `HTTP1Only` | Native cleartext/TLS H1, including externally supplied TCP ClientHello profiles. |
| `Negotiated` | TLS ALPN H1/H2 and cleartext H1. No implicit h2c or H3. |
| `HTTP3Racing` | Native H3 versus delayed TCP racing; both legs can cause external effects. A nonempty HTTPS upload requires independent `GetBody` readers. |
| Native profile | Container copies retain custom factories, H2 settings/order/priorities and native H3 settings. Factories transfer fresh mutable specs. TCP ClientHello settings do not establish the H3 TLS fingerprint. |
| Native TLS/transport | Root pools and certificate containers are copied; keys, writers and functions are borrowed. Positive native header limits and shorter positive TCP idle timeouts are honored. |
| Native redirects/jars | A logical fhttp client handles redirects and an optional caller-supplied jar. There is no business Cookie policy or implicit jar. Redirect callbacks receive metadata-only copies. |
| Native hooks | Pre-hooks can modify method, URL, Host and headers/trailers per exchange. Owning body/response injection is refused. Post-hooks observe copies; their errors and native continue notices remain inspectable, not retry instructions. |

Explicit profile-factory errors retain their causes at construction and handshake;
they cannot silently select a built-in fallback. The SDK-designated placeholder
still permits native Go/randomized and other built-in modes. A custom factory
combined with a special native mode that ignores it is refused. Native randomized
ALPN modes are refused with `HTTP1Only` because uTLS skips that mode's ALPN filtering;
RandomizedNoALPN and ordinary explicit/native profiles remain available.
Pin names use canonical DNS/IP matching; conflicting canonical entries are rejected.
Pre/post hook panics retain native hook containment without formatting arbitrary
panic values. A post-hook admission refusal is retained as a notice, not dropped.

The call's explicit context replaces `Request.Context`. Request URL/header/trailer
containers are copied before queueing; Body and GetBody are acquired only after
admission and remain borrowed until receipt release. Readers must support Close
interrupting Read, obey io.Reader counts and supply independent replay readers.
Outbound trailers are frozen values, not Body-populated EOF mutation.

`RequestOptionsV1` has three distinct route choices:

- `ProxyFromProvider`: use the configured default URL/native proxy factory.
- `ProxyDirect`: explicitly bypass the default proxy, not a fallback on error.
- `ProxyAddress`: use this call's explicit address, without mutating the Provider.

`RoutingLocked` refuses nondefault choices and CONNECT-header changes.
`ConnectHeaders` overlays native CONNECT headers case-insensitively. Alternatively
a native `sdk.ContextKeyHeader{}` context value may supply that map; supplying both
is an error. HTTP proxy URL Basic authentication is applied before explicit header
overrides. Effective values are copied and partition cached clients by origin,
scheme, route, factory choice and CONNECT-header digest. A later call can change
the route without changing Provider identity or multiplying quotas.

HTTP/HTTPS CONNECT and SOCKS4/4a/5/5h are native proxy paths. SOCKS addresses require
an explicit port; SOCKS4 URL credentials are refused because the native SDK ignores
them in favor of its fixed native ident. SOCKS5/HTTP URL credentials remain supported.
H3 racing accepts direct or SOCKS5/5h routing only; it refuses
TCP-only dial/proxy factories, IP-family restrictions, certificate pins, key-log
writers and disabled keep-alives because those native options do not govern H3.
These are mechanism restrictions, not a profile whitelist. HTTPS CONNECT uses the
SDK's system-trusted proxy TLS path; origin RootCAs are not proxy trust roots.
Native SOCKS5 QUIC resolves destination names locally. Therefore H3 via
`socks5h` refuses nonliteral destination hosts, including after hooks/redirects,
rather than falsely promising remote DNS. TCP SOCKS5 uses native remote-name input.

Every managed H2 CONNECT tunnel owns a dedicated physical proxy connection.
The native tunnel's socket deadlines therefore cannot affect another tunnel.
This deliberately gives up physical proxy-session multiplexing, not H2 CONNECT
or ordinary origin HTTP keep-alive/multiplexing. Shared TCP ceilings still cover
all physical/virtual native handles. Unconfigured raw SDK sharing is not qualified.
Some H2 proxy rejection responses may wait for the configured CONNECT setup budget
before native body writing unblocks; both the status and deadline cause are retained.
The Provider does not promise immediate rejection reporting or implement another
dependency's per-stream deadline subsystem.

No raw client, SetProxy/SetJar, arbitrary transport, httptrace owning handle,
WebSocket, 101 upgrade or direct native connection is exposed. Native TCP-specific
buffer/pool settings and TCP idle timeout are not H3 transport controls. Native H3
uses its own QUIC settings; global Provider H3 transport admission remains effective.

## Bounds, ownership and evidence

Go zero values select format 1 and these technical defaults. Layer values are
validated as supplied; explicit layered zero does not reapply a default.

| Bound | Default | Valid range / meaning |
| --- | --- | --- |
| Active / queued calls | 8 / 0 | 1–1024 / 0–4096, shared by aliases |
| Cached + retiring bindings | 16 | 1–1024, includes unresolved retirement |
| Native TCP handles/pending dials | 32 | 1–4096, includes proxy layers; not an exact FD count |
| Native H3 transports | 16 | 1–1024, not an exact QUIC stream/attempt count |
| Request / response bytes | 8 MiB each | 1 byte–1 GiB, shared across the logical call's reads/replays/redirect bodies |
| Retained metadata bytes | 64 KiB | 1 KiB–1 MiB per bounded container |
| Native H2 header ceiling | 10 MiB | At least retained metadata bound, at most 64 MiB |
| Exchanges / replay readers | 10 / 32 | 1–128 / 1–1024 |
| Admission / lifetime timeout | 30 seconds each | 1 ms–24 hours |
| TCP idle timeout ceiling | 90 seconds | 1 ms–24 hours |

Durations and layered `*_ns` fields use nanoseconds. SDK client timeouts are rounded
down to whole milliseconds; the caller lifetime context still bounds the logical call.

H2's native parser limit comes from the selected profile's
SETTINGS_MAX_HEADER_LIST_SIZE (zero/absent means native 10 MiB).
It must fit `MaxNativeHeaderBytes`; an unbounded native value is refused.
`MaxHeaderBytes` separately bounds retained metadata including container overhead.
H1/H3 receive bounds use a positive native limit no larger than that metadata bound.
H3's negative limit is refused because it disables native receive checks as well
as omitting a wire setting. The effective positive limit may change H3 SETTINGS:
acceptance is not a claim of bit-identical browser fingerprints.

Body limits permit a one-byte witness to distinguish exact EOF from excess data.
Counters include that observed byte; retained bytes do not exceed the configured
limit. Declared Content-Length is checked independently, including native H3
short-body EOF. HEAD/no-content/uncompressed responses use their native semantics.
HTTP/3 encoded Content-Length is checked below transparent gzip before its native
length/header metadata is removed. Decoded byte limits remain independent. Native
H1/H2 and H3 differ in automatic/explicit compression behavior; this integration
does not silently standardize those SDK differences.

Finite calls and streams each hold one invocation reservation. Working bytes,
evidence bytes, source container bounds and native handle quotas are distinct;
none is exact Go/native RSS or kernel memory. Idle cached bindings may be retired
to admit a new route, but live bindings are never evicted to evade limits.
Unconfirmed native release retains capacity. This is technical cleanup, not
business Provider rotation/eviction.

A non-nil receipt means accepted work and survives early wait cancellation.
Headers are not final completion. A stream is complete only after final EOF and
successful integrity checks; an early Close is incomplete data, not a fabricated
read failure. Native operation and reader cleanup retain call ownership.
Pool workers and canceled SDK racing drains remain source-owned; late drain errors
are retained by native source cleanup. Assembly shutdown joins managed native work.
Noncooperative callbacks/readers can keep cleanup pending; later success cannot
erase prior cleanup errors. Do not invoke shutdown reentrantly from callbacks.

Native callback objects, private keys, writers and jars must be concurrently usable
and remain borrowed through assembly release. They must not retain arguments or
start unaccounted work. Returned errors must remain immutable and not carry owning
handles. These extension contracts are cooperative Go contracts, not a sandbox.
Root pools use native Clone; certificates already added to them must remain
immutable because their opaque lazy objects cannot be independently frozen.

## Results, errors and compatibility

`Result` and `Metadata` expose deliberate copies of headers, trailers and bytes.
Empty retained data differs from absent/unretained data. Missing Content-Length
is -1. HTTP status codes are data, not business errors.

Private `fault.Kind` errors classify input/state/unsupported/transport/read/
integrity/limit/cleanup boundaries and preserve intentional errors.Is/As inspection.
Primary and cleanup outcomes remain separate. Hook notices and observed input-read
errors are explicit copies: a canceled racing loser need not invalidate a complete
winning response. Ordinary formatting/logging/JSON cannot expose private runtime data.

`Exchanges` and invocation attempts count observed SDK submissions, not hidden
racing/reconnect attempts. Native retries can repeat remote effects. Cancellation
does not prove non-effect, Complete does not mean business success, and the Inbox
is process-local evidence rather than a durable ledger.

The integration import major, OptionsV1 format, native/request runtime contracts,
SDK v1.16.0 and local compatibility revision v2 are separate axes.
`Build` reads actual consuming-binary module/replacement facts; `Profile` reports
non-secret effective choices, never inferred deployment or service support.
See [local SDK provenance and license compatibility](../../../../../../third_party/tls-client/FATHOMRY.md).

## Details and executable evidence

The accepted [SDK architecture](../../../../../architecture/sdk-integration.md)
and [integration standards](../../../../../architecture/internal-sdk-integration.md)
S01–S12 govern applicable resource/call/evidence obligations.
[Issue #49](https://github.com/frost-leo/fathomry/issues/49) defines this SDK's scope.

Source [package documentation](../../../../../../internal/httpclient/tlsclient/v1/doc.go)
and [tests](../../../../../../internal/httpclient/tlsclient/v1/) cover actual loopback
H1/H2/H3, independent receipt/peer data, aliases/admission, proxy credentials,
cancellation, body integrity, native extensions, private errors and consuming builds.
The SDK-local tests exercise acquisition controls and racing/cleanup regressions.
No public stress request, VPN, business HTML fixture or production proxy service is used.

From the repository root, with service opt-ins and proxy variables unset:

```sh
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOWORK=off go test -race -count=1 ./internal/httpclient/tlsclient/v1
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOWORK=off go test -run '^$' -fuzz FuzzProviderRequestMetadata -fuzztime=5s ./internal/httpclient/tlsclient/v1
```

Offline commands require cached dependencies. Loopback TLS/H3, authenticated IPv4
SOCKS UDP relay and CONNECT/SOCKS cancellation are not production anti-bot, mTLS,
deployment or arbitrary custom-profile certification. No Temporal command or
durable serialization behavior changes; workflow replay is not an acceptance claim.
The relay may retire its association before a QUIC close frame reaches the peer.
Confirmed local socket/work cleanup is not acknowledgement of remote shutdown.
