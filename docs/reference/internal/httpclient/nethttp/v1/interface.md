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

# net/http v1 interface

[Documentation](../../../../../README.md) / Internal package reference

**Audience:** framework composition, Adapter and integration maintainers.
**Status:** implemented internal contract; not a public HTTP API,
release, complete workflow runtime or production-site qualification.
**Package:** `github.com/frost-leo/fathomry/internal/httpclient/nethttp/v1`.

## Responsibilities

This Provider supplies standard-library pooled HTTP calls, bounded response streams
and separately owned direct connections. It reuses
[resource ownership/admission](../../../resource/interface.md),
[controlled calls and independent evidence](../../../invocation/interface.md),
technical faults and [compatibility assessment](../../../compatibility/interface.md).
It does not import any other HTTP SDK.

The import-path `v1` is this integration's major contract. `OptionsV1` and
`NativeOptionsV1` independently version configuration and runtime dependencies.
`RequestOptionsV1` versions the supported per-call access input.
The native implementation is identified by the consuming **Go toolchain**, not a
fictional `net/http` module version. All these values are process-local; none
defines a durable DTO or a Workflow version.

Business request preparation, response interpretation, Cookie/token acquisition
or injection policy, refresh, retries, rotation, eviction and Redis coordination
remain outside this package. A supplied jar's native cookie behavior is not an
implemented business credential strategy.

## Capabilities and call sequence

Framework composition, not each business project, owns the private assembly path:

1. Supply one named `OptionsV1` and any explicitly authorized native dependencies.
2. Call `Select`, then `resource.WithLimits`, `resource.Assemble` and `Bind`.
   `LimitsV1` provides a validated policy for unoverridden Go options. If layers
   override settings, composition must supply matching limits; `Bind` checks them.
3. Supply a bounded `invocation.Inbox[Result]` independently of caller error
   handling. An optional Observer is lossy and never replaces this receiver.
4. Choose a controlled operation and retain its accepted receipt.
5. Inspect every independent delivery and release its inbox capacity only after
   the necessary evidence handling. Close streams/connections before closing
   their assembly; preserve incomplete cleanup and explicitly continue it.

Construction performs no network readiness probe. A configured name supplies one
instance; several names may select the same SDK. Borrowed aliases refer to the
same original resource and limits, not another pool or allowance.

The executable [Do example](../../../../../../internal/httpclient/nethttp/v1/example_test.go)
shows this maintainer-owned assembly/evidence path against a local peer.

| Operation | Native capability and caller duty |
| --- | --- |
| `Client.Do` | Retains a bounded response body and closes it. Supply separate request and cleanup contexts. Inspect the non-nil receipt even alongside an error. |
| `Client.Open` | Returns a controlled Stream after headers. Read incrementally and always call `Close(ctx)`, including after EOF. The terminal receipt remains unresolved while the stream is live. |
| `Client.Connect` | Creates a native direct connection outside the pool, but inside source-wide connection limits. Its receipt represents the connection lifetime and completes on Close. |
| `Connection.Open/Do` | One outstanding child response at a time, borrowing the parent reservation. Correlation.Parent must be the Connect call. Requires at least two borrowing nodes for the parent and child. |
| `Connection.Close` | Interrupts native work, closes an outstanding child stream and joins actual cleanup. An early wait return does not imply release. |
| `Profile` / `Build` | Safe effective settings and actual consumer build facts. Neither supplies default compatibility records or service attestation. |

Direct requests require the exact connection URL scheme/authority. An explicit
Request.Host remains available for native virtual-host/cookie semantics.
Cross-authority redirects are refused, not silently sent over the original
connection. Direct connections use separate native transport state without
`MaxConnsPerHost`; the Provider's shared socket limit remains effective.
No raw ClientConn, Reserve/Release operation or state-hook authority escapes.

## Configuration and runtime inputs

The [Go options contract](../../../../../../internal/httpclient/nethttp/v1/options.go)
defines units, defaults and ranges. Version zero selects format 1; unknown formats
are refused. Zero Go option fields receive technical defaults; explicit layer
values retain their own zero/null/override meaning and are validated, not silently
replaced again. There is no business profile, experimental recipe catalog or
closed list of allowed sites.

| Setting | Default and meaning |
| --- | --- |
| Protocols | Go options with no protocol flags select H1 and TLS H2. Cleartext prior-knowledge H2 is explicit and cannot be combined with H1 in this client. |
| Proxy | ProxyURL is a configured default; empty with no native hook means direct. There is no environment proxy discovery. Native routing hooks and configured HTTP/HTTPS/SOCKS proxy URLs retain their supported semantics. |
| RoutingLocked | False by default. True requires every call to use configured routing and refuses explicit proxy/direct selections before admission. This is a capability constraint, not a complete network sandbox or an application proxy allowlist. |
| TLS | System roots and native verification by default, or explicit PEM roots/client certificate/name. A native TLS configuration cannot silently override those textual fields. |
| Active / queued roots | 8 / 0. Aliases share these process-local limits; they are not a distributed account rate. |
| Owned connections | 32 across route transports, direct connections and in-progress dials. Full capacity retires idle pooled connections, then refuses if still full; active requests are not evicted to make room. |
| Route transports | At most MaxConnections cached or retiring bindings. Each has an independent native H2 pool keyed by the private effective proxy/authentication URI, with direct access separate. Only bindings with no live operations can be retired. Failed socket cleanup remains owned and charged separately. |
| Request / response bytes | 8 MiB each. Request accounting is cumulative across body readers/replays. Response accounting includes native-decoded bodies and redirect-body reads. |
| Header metadata | 64 KiB per checked map, including conservative name/value overhead; also configures the native response-header limit. URL metadata and the combined method/Host/transfer-encoding strings are separately bounded by this value. |
| Client exchanges | 10; replay-reader and dial-attempt storage have related finite ceilings. This is not exact counting of internal native wire retries. |
| Admission / operation timeout | 30 s each; an earlier caller deadline wins. A direct connection's operation timeout bounds its lifetime. |
| Dial / TLS handshake timeout | 10 s each. Native/custom callbacks remain cooperative. |
| Response-header / idle timeout | 30 s / 90 s. Idle cleanup is not active-request cancellation. |
| Native callbacks | Fallible extension entry is capped at four times connection capacity. Native time/cache/diagnostic hooks must be prompt and nonblocking and retain lifetime accounting; their signatures cannot report admission errors. |

PEM roots are parsed strictly; mixed garbage or unknown blocks do not silently
change the trust set. No weaker certificate verification is selected to make a
connection pass. Explicit native TLS options retain their native meaning and must
be reviewed as such.

### Fixed defaults and runtime route selection

`Client.Open`, `Client.Do` and `Client.Connect` accept at most one
`RequestOptionsV1`. They do not mutate the Provider, create another source identity
or grant another allowance. A zero/omitted options value preserves existing calls.

| Selection | Required input and meaning |
| --- | --- |
| `ProxyFromProvider` | Empty ProxyURL; use configured default/native routing. |
| `ProxyAddress` | Nonempty valid ProxyURL; bind that proxy before admission through the logical call's redirects and native retries. |
| `ProxyDirect` | Empty ProxyURL; explicitly request direct access. |

Unknown states, mismatched fields, an empty explicit proxy address and multiple
options values are rejected. Invalid or failed routes never become unrequested
direct/default access. `RoutingLocked` sources reject both non-default choices,
even if a supplied URI happens to equal the default. A configured native hook may
still choose per destination: the lock preserves the configured routing behavior,
not an assertion that a dynamic hook always returns the same address.

Native proxy routing is resolved before entering the selected native transport.
Different proxy addresses or authentication values use independent native H2
transport state; relying only on a shared Transport.Proxy callback is insufficient.
ALPN containers are also copied independently before native initialization.
The source-wide connection/callback/admission limits do not multiply with routes.

A Stream retains its transport binding until native work and cleanup finish.
Changing the route of a later call does not close or reroute that stream. When a
binding is unused, technical retirement closes its managed sockets as well as its
native idle pool; native H2 bookkeeping may lag the caller's completed read.
This is not business Provider rotation or eviction.

Connect binds its route for the connection lifetime. Child methods cannot replace
the route of an established socket/tunnel. `Result.ProxyMode` records `provider`,
`proxy` or `direct` without URI credentials; the caller's correlation associates
its requested route with the independent evidence. This is not exit-IP attestation.

Request Cookie/token values and proxy selection are independent inputs. No shared
jar is created automatically and no account/session migration is inferred when a
route changes. Business orchestration can express access choices, but actual I/O
belongs on the Activity/process-local side of the future public boundary; live
clients and credentials must not become implicit durable Workflow state.

The explicit method context replaces `Request.Context` for execution and values.
Request metadata is copied, while Body and GetBody data remain borrowed until
receipt release. They must not be mutated/reused while native work is outstanding;
each replay reader must have independent reader state and immutable backing data.
Outbound Trailer values must be fixed before submission. A Body that populates
the caller's Trailer map at EOF cannot update the copied request; dynamic outbound
trailers are unsupported. Native response trailers are collected after EOF.
Declared ContentLength must be consistent with the native reader. Native legacy
Cancel channels remain supported but do not create an effect-rollback guarantee.

Native TLS/HTTP2 containers, root-pool containers, certificate bytes and slices are
copied. Functions, opaque signing keys, native caches/jars and randomness remain
explicit borrowed dependencies. Native pools may retain immutable parsed
certificate objects; copying is not a sandbox for concurrently mutated native
objects. Supplying the same runtime object to two instances explicitly shares it.

Redirect hooks receive bounded metadata without owning Body/GetBody/Response
handles; supported metadata edits are copied back. They cannot introduce a new
owning body through that callback. Native `httptrace` contexts and arbitrary
RoundTrippers are refused because they can expose owning connections or replace
the tracked transport. Use the independent evidence and optional Observer instead
of an unqualified native-handle escape.

## Ownership, cancellation and cleanup

A nil receipt means rejection before admission. After admission, the Inbox owns
independent evidence even if the direct caller receives an error or stops waiting.

Native entry and cleanup use owned workers, bounded by retained root/session
responsibilities. Request cancellation can end the caller's header/connection
wait without pretending the native function has returned. A late response is
closed instead of being handed to an absent consumer. A blocking Body.Close,
dialer or other native extension retains the relevant ownership until it actually
returns. Repeated Close calls observe the same owned cleanup rather than launch
competing cleanup attempts.

Stream reads are single-reader; Close may run concurrently. EOF establishes a
body-read observation but does not replace mandatory Close. Close does not
silently drain unlimited data. An intentionally abandoned stream can have a
successful cleanup with `Complete()==false`.

Assembly release fences new native dependency callbacks, closes managed sockets,
joins protected callback/socket use and preserves failed or unconfirmed cleanup.
Socket ownership covers all native connection methods, including deadlines and
address inspection; returned addresses are frozen before leaving that boundary.
Earlier cleanup causes remain inspectable after eventual release. These are
managed-handle/dependency guarantees, not a promise that every private Go runtime
goroutine has been individually enumerated and joined. Arbitrary native callback
panics, noncooperative user code and caller-owned native memory are not sandboxed.

## Results and failure boundaries

`Result` and `Metadata` are immutable observations. Headers, trailers and retained
body accessors return copies. Their contents may be sensitive and are deliberately
not diagnostic labels.

- HTTP 200, 404 and 429 remain response data. The Provider does not decide whether
  the content is a challenge, a missing business entity or a retryable result.
- Present response data, retained empty data, missing response data and partial or
  abandoned output remain distinct. A retained empty Do body is non-nil; stream
  and connection results have no retained body.
- For request results, Complete means native EOF with no primary failure. For a
  Connect result it means the established session lifetime ended, with cleanup
  reported separately. Neither means remote commit, rollback, business success or
  success of another operation. Response presence is independent of numeric status.
- RequestBytesRead counts input bytes consumed, not transmitted or acknowledged
  bytes. BytesRead counts decoded response bytes, including redirects and at most
  one over-limit witness. The witness is not delivered as within-limit output.
- Exchanges and observed attempts are lower-level observations, not every physical
  native retry. Attempts.Exact is false; observed zero is not proof of no effect.
- Primary and cleanup errors stay separately inspectable through standard
  `errors.Is/As`. Native URL, TLS, read and cancellation causes remain available.
  A caller's cleanup-wait timeout is separate from the eventual native outcome.
- Native framing/decode errors and byte-limit failures cannot become successful
  complete data. Native close-delimited EOF is not a business-content integrity
  certificate.
- Ordinary fmt/slog/JSON paths protect runtime values. JSON reconstruction and
  persistence are refused. Deliberately extracted headers/data/native causes are
  not sanitized and must not be indiscriminately logged.

## Support and verification boundaries

The functional tests use test-owned local peers and actual standard H1, TLS H2,
explicit cleartext H2, CONNECT proxying, certificate rejection, mTLS, native
redirect/replay and direct-connection behavior. The mechanism integration tests
independently check peer bytes, attribution, limits, receipts and cleanup, with
rejecting controls. Consumer tests build the actual package and inspect the
consuming toolchain/import selection.

| Evidence | Source |
| --- | --- |
| Native calls and admission | [client tests](../../../../../../internal/httpclient/nethttp/v1/client_test.go), [request tests](../../../../../../internal/httpclient/nethttp/v1/request_test.go) |
| Streams, decode and native retries | [stream tests](../../../../../../internal/httpclient/nethttp/v1/stream_test.go), [native boundaries](../../../../../../internal/httpclient/nethttp/v1/native_boundaries_test.go) |
| TLS and runtime hooks | [TLS tests](../../../../../../internal/httpclient/nethttp/v1/tls_test.go), [native options](../../../../../../internal/httpclient/nethttp/v1/native_options_test.go) |
| Proxy, connections and shutdown | [transport tests](../../../../../../internal/httpclient/nethttp/v1/transport_test.go), [direct connections](../../../../../../internal/httpclient/nethttp/v1/connection_test.go), [lifecycle tests](../../../../../../internal/httpclient/nethttp/v1/lifecycle_test.go) |
| Runtime routes and independent proxy/accounting oracles | [proxy tests](../../../../../../internal/httpclient/nethttp/v1/proxy_test.go) |
| Independent composition and rejection | [integration tests](../../../../../../internal/httpclient/nethttp/v1/integration_test.go) |
| Options, copies, errors and consumer facts | [options](../../../../../../internal/httpclient/nethttp/v1/options_test.go), [results](../../../../../../internal/httpclient/nethttp/v1/result_test.go), [errors](../../../../../../internal/httpclient/nethttp/v1/errors_test.go), [compatibility](../../../../../../internal/httpclient/nethttp/v1/compatibility_test.go) |

Raw private experiments and their results remain outside this repository.
These tests do not establish production-site success, anti-bot capability,
distributed quotas, exact wire-attempt ceilings, hard RSS/native-memory limits,
arbitrary native-hook qualification or complete durable framework behavior.
SOCKS and HTTPS-proxy modes retain native selection but are not separately
service-qualified by the local CONNECT test.

H3, rendered browser execution, 101 upgrades/tunnels, raw native handles and
server-only TLS ECH keys are not supported by this contract. Profile reports
unknown multi-origin service facts and declared protocol choices separately from
per-response observations; it supplies no universal tested-combination catalog.
See the [SDK integration standard](../../../../../architecture/sdk-integration.md)
and [#48](https://github.com/frost-leo/fathomry/issues/48) for the governing scope.
