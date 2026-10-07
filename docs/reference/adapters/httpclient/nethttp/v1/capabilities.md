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

# nethttp capability coverage

[Documentation](../../../../../README.md) / [Standard HTTP Adapter](interface.md)

**Audience:** maintainers tracing selected native behavior to public acceptance.
**Status:** implementation/test matrix for #117; no production qualification.
Native authority is the selected Go 1.27.0 `net/http`, `crypto/tls` and
`net/http/internal/http2` sources, plus the baseline `golang.org/x/net v0.58.0`
SOCKS/IDNA helpers used inside owned dialing. No SDK version is upgraded.

Each public route retains the corresponding Internal behavior; absence and
inapplicability are not blanket unsupported substitutes.

| Native family / fields | Internal path | Public path | Positive / rejecting evidence |
| --- | --- | --- | --- |
| Client.Do: Method, URL, Host, Header, Body, GetBody, ContentLength, TransferEncoding, fixed Trailer | request.go validate/copy/nativeOpen/exchange | Client.Do/Open, Connection.Do/Open use native request without conversion | direct consumer; request/native-boundaries tests; canceled/oversized/malformed requests and rejected raw trace |
| Client redirects, CheckRedirect, CookieJar | redirect and operationJar | NativeOptions.CheckRedirect/Jar | native-options/request tests; cross-origin credentials, replay and bounded hooks; direct consumer context control |
| H1, TLS H2, prior-knowledge h2c | options/protocols/transport | Settings HTTP1/HTTP2/UnencryptedHTTP2 | public TestPublicProtocolsStreamsAndConnections; native transport/protocol refusals |
| Trust, server name, mTLS, session cache, TLS callbacks, HTTP2Config | copyNative/copyTLS/guardTLS/guardHTTP2 | NativeOptions TLS/HTTP2 and textual Settings | TLS/native-options tests; malformed PEM, conflict and typed-nil tests; public preparation/H2 tests |
| Dialing, proxy selection, routing lock | owned dial/proxy, request_options.go, bindTransport | NativeOptions.DialContext/Proxy, RequestOptions | proxy/transport tests; blocked callbacks, route saturation, locked-route and failed-route no-fallback controls |
| SOCKS5/SOCKS5h remote DNS/auth and pooled TLS lifetime | owned x/net ContextDialer, planned TLS references | Existing proxy choices; no second pool or new public routing mode | socks_lifetime_test.go: independent wire peers, late TLS start, idle-connection winner, 1 ns timeout, refused/stalled SOCKS and HTTP/HTTPS binding reuse |
| ExpectContinueTimeout independently of ResponseHeaderTimeout | resolved settings and Transport | pointer Settings.ExpectContinueTimeout | TestContinueNativeDifferential, TestContinueIndependentWaitAndExplicitZero, header deadline/cancel controls |
| ProxyConnectHeader/GetProxyConnectHeader | connectHeaders + effective-auth routeKey | Settings.ProxyConnectHeader; NativeOptions.GetProxyConnectHeader | static/dynamic H1/H2 tunnel isolation and mutation-after-snapshot; oversized/static/framing/auth and canceled selector controls |
| OnProxyConnectResponse | bounded connectResponse with source/operation fences | NativeOptions.OnProxyConnectResponse and immutable ConnectResponse | 407 observation, copies, wire/map limits, observer veto and late callback retention |
| Do retention, decoded response limits, trailers | consume/Stream/responseBody/result | Result/Metadata/DataCopy/TrailersCopy | public truncation test; stream/native boundaries; absent vs empty, early close and over-limit witness controls |
| Transport.NewClientConn + exact authority child calls | Connection, directExchange, own sockets | Client.Connect; Connection.Do/Open/Close | direct consumer and native direct metadata test; busy/cross-authority/refused callback controls |
| Final resolved options/defaults/native containers | PrepareV1/Prepared.Metadata/Select | Prepare/Prepared.Policy/Open; Validate/Recommend | native/public preparation tests; resolved zero/false, mutation, conflicts, invalid ceilings before construction |
| Root/evidence/source/queued capacity and native H2 buffers | preparation.go Budget and source owner | Compose; Runtime/Inbox normalized Options checks | prepared accounting tests; insufficient policy controls; consumer saturation and actual socket release |
| Idle peer-certificate/native TLS residence | Selected TLS certificate/message limits and transport-retained peer state | Authoritative source reservation, independent of root/evidence release | peer_certificate_test.go uses a normally verified 240 KiB leaf, one reused TLS socket, released roots/evidence and known DER payload lower bound |
| Source and descendant cleanup | resource assembly + invocation lifetime | guarded public source/operation and resource Fixed/Follow | Framework consumer holds old connection/stream across replacement, failed/obsolete candidates; native late-work/lifecycle tests |
| Primary/cleanup/attempt attribution | native Result + independent Inbox | Result, public receipts and Inbox | error contract/protocol tests, cleanup-wait projection control, delivery Retry/Ack with no additional peer request |
| Build/Profile and offline definitions | compatibility.Build/Profile | Build, Client.Profile, Definitions/Resources | native compatibility tests, public preparation, external binary graph and CLI TestNetHTTPOfflineAtlas |
| Unsafe owning/extension escape | explicit refusal | No raw transport/ClientConn API | external forbidden-compilation tests; 101/httptrace/native extension rejecting controls |

All Settings fields map into the final Internal layer without copied defaults:
name/version identify preparation; proxy/TLS strings, protocol/compression/
keep-alive flags, active/queued/connection/request/response/header/exchange bounds
and all timeout fields use the corresponding selected Internal setting. Nil
pointers are omitted, not encoded as native zero. Every non-nil pointer is encoded
as supplied and validated once. Native borrowed containers are separately
validated before construction. See [defaults and units](interface.md#settings-and-native-dependencies).

The retained test tree is the reproducible authority; private issue evidence
contains original controls and commands. Runtime/provider/SDK/configuration
versions remain separate. Technical tests are not licensing or release clearance.
