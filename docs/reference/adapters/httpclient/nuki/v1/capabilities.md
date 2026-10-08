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

# Nuki capability and verification matrix

[Documentation](../../../../../README.md) /
[Nuki interface](interface.md)

**Audience:** maintainers reviewing issue #121 completeness.
**Status:** implemented capability/test map with local protocol and independent
consumer qualification; not production deployment or upstream release certification.

| Selected native authority | Internal ownership | Public route | Positive and rejecting evidence |
| --- | --- | --- | --- |
| nukilabs profiles, uTLS/QUIC and TransportOptions | Frozen PrepareV1 selection and authoritative Budget; bounded lazy profile output | NativeOptions, Prepare, Prepared.Policy, Recommend, Compose | preparation_test.go; Internal native/profile bounds and protocol peers |
| Lazy profile ALPN and H3 wire settings | Bounds before/after native ALPN edits; 62-bit/settings-identity validation before effects | NativeOptions.Profile | Internal TestLazyProfileBoundsPrecedeALPNTransform, TestProfileCeilingIncludesControlledALPNRewrite and TestH3SettingsWireBoundsBeforeEffects |
| Effective QUIC receive and proxy residence | max(initial, maximum) windows; native ingress and structured-parser ProxyTunnelBytes, multiplied source reservation | Frozen Prepare/Policy/Compose, no Runtime expansion | Internal TestPreparedQUICBudgetFreezesActualInitialWindow and TestPreparedProxyIngressAndParserBudget |
| Retained native graphs and native H2 frame bound | Per-exchange TCP/H3/proxy graphs belong to each operation, independently of cached-source slots; H2 frame state uses the selected constructor's resolver | Internal metadata consumed by Policy/Compose/Using | TestPreparedRetainedNativeGraphCardinality separates MaxExchanges, MaxActive and MaxOrigins; TestPreparedH2FrameAndTableGraphUsesSelectedProtocol preserves H1 non-applicability |
| Outer CONNECT TCP/H2 graph | Default outer frame/table state and native TLS.Client certificate state are distinct from inner origin profile and remain charged through retained response scopes | Existing configured/per-call proxies and prepared policy | TestPreparedProxyTCPGraphUsesNativeDefaultOuterSelection; larger custom inner HPACK/frame controls leave ProxyTCPGraphBytes unchanged |
| Native profile-required configuration and explicit-zero data | ValidateDataV1 versus full PrepareV1 | Validate and pointer-valued Settings | TestPreparationUsesFrozenNativeBudget; shared strict-config conformance |
| Jar, Tracker, Pinner, Before/After and redirect callbacks | Borrowed guarded callbacks, copied containers; no owning native exposure | Complete NativeOptions mapping; public immutable After metadata | Native priority/cookie control test; Internal callback/privacy and identity peers |
| Origin/proxy TLS and DNS authority | TLS and ProxyTLS remain separate; resolver/socket ownership is source-scoped | NativeOptions.TLS, ProxyTLS, Resolver, DialContext, ListenPacket | second-native-selection rejection; Internal trust/proxy/DNS peers |
| TCP proxies, supported SOCKS UDP, controlled MASQUE/CONNECT-UDP | Actual socket and CONNECT/CONNECT-UDP tunnel/control quotas; multiplexing does not bypass ownership | Settings route/tunnel bounds, native dependencies, RequestOptions | Internal independent proxy/UDP/MASQUE positive and failure controls; public request options are unchanged native inputs |
| Native CONNECT-UDP URI templates | Fixed authority, ASCII level-3 admission, nonempty path and escaped IPv6 targets | Settings.ProxyURL and RequestOptions.ProxyURL | Native TestFathomryTemplateOperatorsAndIPv6, including valid path/query/escaped literals and invalid operators/modifiers/empty path |
| Capsule negotiation and mixed ingress | Structured true Item, prohibited-content refusal; bounded shared capsule/QUIC queue and joined receivers | Existing Do/Consume routes only | Internal TestMASQUECapsuleAndDatagramIngress, TestMASQUECapsuleNegotiationRefusesBeforeTunnelTransfer and TestMASQUECapsuleTerminationPreservesCauseAndSibling; native saturation, malformed context, deadline and repeated-stop controls |
| Native H3 cache replacement | Dead origin/proxy entries retain ownership until actual HTTP3 control workers join; live stream failure preserves shared connection | Existing native routing and source shutdown beneath Do/Consume | Native TestFathomryRacerRetirementJoinsActualH3ControlWork and TestFathomryMASQUEReplacementJoinsOldHTTP3Control hold real control-parser callbacks beyond raw QUIC termination |
| Terminal QPACK storage | Explicit Close clears owned table entries without mutating immutable limits/counters or conflating finite encoder EOF | Existing native H3 source/request cleanup, no raw decoder access | Native TestFathomryCloseDropsTableStorage pairs live and finite-input positive decoding with terminal clearing and no repopulation |
| Priority, ExcludedCookies, DiscardResponseCookies and exact headers | No-copy hard preflight; source-specific snapshot before native admission | Native nukilabs/http.Request directly | request_test.go preserves native H1 Priority non-applicability and actual cookie controls; priority_test.go observes H2 frames; oversized-extension and exact-source rejection controls |
| Asynchronous finite native Do | Original source root, bounded body and final native record | Do and adapters.Receipt[Result] | held-response asynchronous/wait-cancellation test; independent direct consumer |
| Callback-native Consume | Revocation, body Close and entered-read join precede native release | Consume and callback-only Response.Read/Metadata | EOF versus early-return tests; TestCallbackReturnJoinsAlreadyEnteredReadBeforeGenerationRelease |
| Partial/error/native observations | Immutable metadata/body/trailers/input notices and unknown/encoded/decoded lengths | Result, Metadata, Attempts, Attribution, Profile, Build | immutable result/metadata copies, error_contract_test.go; native compression/truncation tests |
| Shared source generations | Internal assembly remains owned through actual native cleanup | Owner/Handle/Using and resource Fixed/Follow | separate Framework consumer holds an old callback across replacement; failed and obsolete candidates release actual sources/sockets |
| Independent evidence and cleanup under saturation | One private record per native root; source/root guards until release | NextReleased, Receipt.WaitReleased, Delivery.Ack/Retry, Owner.Close | public saturation test and both independent consumers; receipt-wait cancellation does not release work |
| Consumer module graph | Selected corrected nuki, nuki-http, nuki-quic-go, nuki-qpack, nuki-socks | Existing project-generator pins, not an Adapter-owned loader | independent local replacement graph/import/refused-authority checks; root versioned-consumer and actual-published-module gates remain required |

The scope does not include bogdanfinn/tls-client, a Session platform, raw clients,
owning tunnels/sockets, business token refresh, retry/provider rotation, automatic
HTTPS rewriting or anti-inspection behavior. These exclusions do not remove
controlled UDP/MASQUE from the approved scope.

The public package follows the existing [Adapter maintenance contract](../../../../../development/public-adapters.md).
Exact private/native protocol proofs belong to their Internal tests and the
issue evidence; the public tests establish that corresponding bounded routes,
ownership, input fields, metadata and error identities survive the boundary.
