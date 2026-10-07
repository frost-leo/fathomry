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



# tls-client capability traceability

**Audience:** Adapter maintainers and reviewers.
**Status:** executable local protocol coverage; final repository/PR checks qualify
the exact delivered commit. This is not arbitrary deployment or license clearance.

Selected authorities: tls-client v1.16.0 acquired at
`291b8f9e1b86cc35f210bdb6bf44770bf2660ab5`, local correction v3; fhttp v0.6.9;
uTLS v1.7.8-barnius; quic-go-utls v1.0.10-utls. Internal paths below are relative to
`internal/httpclient/tlsclient/v1`; public paths to
`adapters/httpclient/tlsclient/v1`. Named tests remain executable source, not
claims about unrun real services.

| Selected SDK ability | Internal path | Public path | Positive and failure/refusal evidence |
| --- | --- | --- | --- |
| Explicit profiles, native special modes and fresh custom factories | native.go copyNative/validateNative/profile; owner.nativeClient | NativeOptions.Profile; Prepare | TestProviderNativeProfileFamiliesAndAnonymousFactory, TestProviderNativeSpecialProfilesRemainUsable, TestProviderExplicitFactoryErrorsPreserveIdentity, TestProviderSpecialModesRefuseIgnoredExplicitFactories |
| Offline final data/native choice | PrepareV1; Prepared.Metadata/Description/Select | Prepare, Prepared.Policy/Open, Recommend, Compose; Validate data only | TestFrozenPreparationMetadataAndLayerAuthority, TestPreparationBudgetCoversEffectiveNativeBuffers, TestPreparationOfflineExactNativeAndExplicitValues, TestPolicyRefusesInsufficientCapacityBeforeConstruction |
| Lazy custom profile declarations and native placeholder applicability | preparation.go budget; native.go profile | Settings.MaxProfileBytes, authoritative policy | TestLazyProfileDeclarationRefusesOversizedFactoryInput, TestNativePlaceholderProfileHasSeparateEnvelope; dynamic borrowed output remains cooperative |
| H1, negotiated H2, native H3 racing and native retry/replay | owner.go; request.go; SDK racer.go | Settings.Mode; native fhttp requests, Do/Open | TestProviderNativeProtocolsAndEvidence, TestNativeBothRacingLegsKeepReplayAndWinnerLifetime, TestProviderPreflightRacingInputDoesNotAcquireReader; independent direct consumer observes H1/H2/H3 and TCP racing winners |
| TLS trust, client authentication, keys, key logging and session tickets | copyCertificate, guardedSigner, keyWriter, transportOptions | NativeOptions.Transport; DisableSessionTickets | TestPublicMTLSActualPeerIdentityAndRefusal, TestPublicKeyLogActualTLSAndCancellationOwnership, TestPublicNativeSessionResumptionAndDisabledControl |
| Pin identity and source isolation | native.go; SDK pinner.go | CertificatePins, BadPinHandler | TestProviderCertificatePinsAreInstanceLocal, TestProviderCertificatePinsCoverEquivalentHosts; invalid/insecure conflicts in native validation tests |
| Native header order, pseudo-header order and profile SETTINGS order | copyRequest, copyNative; native fhttp transports | fhttp Header-Order:/PHeader-Order: keys and explicit Profile | TestPublicNativeHeaderAndSettingsWireOrder observes raw H1 and independent H2/HPACK peers, default-order ablations, malformed-header no-dispatch |
| Legitimate metadata hooks, effective URL, redirects, Cookie/Authorization, response cookies | hooks.go before/redirect; request.go nativeOpen; operationJar | NativeOptions.PreHooks/PostHooks/CheckRedirect/Jar/DefaultHeaders | TestRedirectCredentialScopeMatrix, TestURLUserinfoRewriteCredentialOrigins, TestProviderRedirectJarAndMetadataHooks; direct consumer observes actual307 replay/jar/notices |
| Native built-in TCP proxy resolver, control and local bind | dialer.go; owner.nativeClient; SDK connect.go | NativeOptions.Dialer/LocalAddr | TestBuiltinProxyDialerResolverLocalBindAndCopy, TestBuiltinDialerControlCancellationRetainsOwnership, TestBuiltinDialerH3InapplicabilityRejected |
| Direct/configured/explicit proxy, bounded CONNECT authentication and SOCKS modes | request_options.go; owner.bindingKey/getBinding | Settings.ProxyURL/RoutingLocked; RequestOptions.Proxy/ProxyURL/ConnectHeaders; SDK contextual headers | TestProviderDynamicProxyCredentialsDoNotSharePools, TestProviderProxyHandshakeCancellationAndParserBound, TestProviderH3RemoteDNSIsNotSilentlyBypassed; proxy/SOCKS peer suites |
| H1/H2/H3 framing, compression, trailers and bounded finite output | request.go, stream.go; SDK fathomry_http3.go | Do, Result/Metadata copies; Transport.DisableCompression | TestProviderEncodedFramingBeforeDecompression, TestProviderNativeCompressionSelection, TestProviderResponseIntegrityAndByteLimits, TestPublicTruncationAndCancellationEvidence |
| Streaming and independent cleanup continuation | Stream.Read/Close; operation.finish; receipt release | Client.Open, Stream.Metadata/Read/Close/Receipt | TestPublicProtocolsStreamsAndBandwidth, TestPreparedTimeoutClosesAbandonedRetainedStream, TestCleanupWaitDoesNotBecomeFinalPrimary; old-generation consumer retains stream across Follow replacement |
| Native TLS/TCP byte tracking, disabled/unknown and retired counters | bandwidth.go; SDK bandwidth tracker | Bandwidth setting; Client/Handle.Bandwidth, immutable availability/source/generation | TestNativeBandwidthScopeAndRetirement, TestNativeBandwidthDoesNotInventCleartextTraffic, TestPublicProtocolsStreamsAndBandwidth; direct racing consumer checks explicit scope, never invents H3 totals |
| Late TLS/profile reconnect work and H2 contextless dial | SDK beginCall/RetainWork; Prepared.SelectWithLifetime | Owner.Close/Release and root guard | TestOwnerLifetimeReconnectCancellation, TestProviderCanceledNativeDialRetainsOwnership, TestFathomryCallRetentionScopeAndRefusal; uncooperative callbacks retain ownership after waiter cancellation |
| Shared physical source limits and failed release retention | owner.acquire/getBinding/retire/release | Native bounds; Prepared.Policy; Owner.ShutdownComplete | TestProviderBindingCeilingAndConfirmedRetirement, TestProviderFailedNativeCloseRetainsQuotaAndHistory; SDK control/racer cleanup tests |
| Public queue, evidence and failed/obsolete generations | Internal invocation custody plus public single-root bridge | Using/WithID; public Runtime/Inbox; Fixed/Follow | TestPublicQueuedCancellationAndSaturationStayBeforeNativeWork; direct consumer fills evidence; Framework consumer verifies actual old/new sockets, failed/obsolete candidates and pre-dispatch oversized refusal |
| Safe errors, notices, independent evidence and offline registration | private fault causes; public errorbridge | Result.HookErrorsCopy/InputErrorsCopy; Definitions/Resources; CLI Network0x201 | TestErrorIdentityPrivacyAndNativeCause, TestRuntimeBoundariesRefuseSerialization, TestTLSClientOfflineAtlas, bounded error/preparation fuzz; compile-negative Client/Handle/tracker authority fixtures |
| Actual public dependency delivery | selected nested SDK correction; generator sdkPins | public-only direct and Framework modules; normal generator replacement graph | TestIndependentConsumers, TestSDKReplacementPolicy, TestVersionedProjects; local replacement alone is not normal-mode acceptance |

## Field coverage and meanings

The [interface](interface.md) lists every Settings and NativeOptions field, defaults,
units, omission/zero/false and applicability. The following groups share authority,
not a uniform SDK semantics assumption:

- Data bounds: MaxActive/QueuedCalls; MaxBindings/MaxTCPConnections/MaxHTTP3Transports;
  MaxRequestBytes/MaxResponseBytes/MaxHeaderBytes/MaxNativeHeaderBytes;
  MaxProfileBytes/MaxExchanges/MaxReplays; AdmissionTimeout/Timeout/IdleConnTimeout.
  Metadata comes from Internal's final selection; public code adds only its own
  source and evidence-transfer envelopes.
- Native profile containers: ID/seed/weights, H2 settings/order/priorities/header
  priority/flow/stream ID/allowHTTP, H3 settings/order/priority/pseudo-order/GREASE and
  factory. Containers copy; opaque dynamic factories remain fresh/cooperative.
- Transport: root pools, certificates, borrowed signer/key-log writer, compression,
  keepalive/pool counts, read/write buffers, native header ceiling and shorter idle
  timeout. Protocol applicability remains explicit rather than silently dropping
  TCP-only inputs in racing mode.
- Native requests: method/URL/Host/header/order fields, Body/GetBody,
  ContentLength/TransferEncoding/Close/Cancel and fixed Trailer. Body reads close
  through admitted work; no source owner or arbitrary transport can be injected.

Ordinary source construction is local-only. Readiness is established only by the
actual protocol operations being tested. No forced H3/h2c, raw dial/WebSocket,
mutable owning client or business credential refresh is introduced. Early-close
incomplete data, HTTP status, native notices, cancellation, physical attempts,
cleanup completion and evidence acknowledgement remain different facts.
