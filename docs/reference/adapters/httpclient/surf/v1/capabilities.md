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

# Surf capability traceability

[Documentation](../../../../../README.md) / Public Adapter reference

**Audience:** maintainers and reviewers. **Status:** implementation tests, not
production or license qualification. Selected native authority is recorded in the
[interface](interface.md) and each corrected module's FATHOMRY.md.

Internal paths below refer to internal/httpclient/surf/v1, public paths to
adapters/httpclient/surf/v1. Final command results belong to the tested commit.

| Native capability | Internal path | Public path | Positive / failing controls |
| --- | --- | --- | --- |
| Final configuration and lazy native profile selection | PrepareV1, Prepared.Metadata/Select, native.go, preparation.go | Settings, NativeOptions, Prepare/Policy/Compose | preparation_test, H2C lazy-output guards, public offline/explicit-value and insufficient-policy tests |
| H1/H2/H3 preference/fallback | owner.nativeClient, request.go; Surf/H2/H3 transports | Mode, native enetx/http request, Do/Open | existing native protocol/fallback/trust suites; public direct protocol consumer |
| Owned h2c and HTTP-only Variant | managed h2cMW; origin/profile preflight | H2C mode, explicit cleared Variant | h2_native_test, h2c_test; authority refusal/normal socket controls, HTTPS initial/redirect refusal |
| Native H2 window/frame/settings behavior | corrected surf-http2 construction | Profile.ConfigureH2 with lazy ceiling | h2_settings_test: omitted/zero/default windows, valid5MiB under6MiB before app reads,32KiB incoming frame with16KiB outgoing limit, invalid-setting socket close; original-source ablation |
| TLS/JA/proxy trust, certificates, caches and profile containers | copyTLS/copyJA, guarded callbacks and owned installation | NativeOptions and Build/Profile | existing native TLS/JA/proxy tests; preparation copied-container/ALPS mutation contrast and aggregate guard |
| Lazy H2/H3/header/Hello output | SDK FathomryApplyVariant and Hello checks | Native profile/factory plus declared MaxProfileBytes/MaxHTTP2StreamBytes | no offline factory, oversized output before native dispatch, existing custom-profile positives |
| H3 source residence and partial retirement | native cached/pending/retired client quota, body/writer references | MaxHTTP3Clients; Owner/Handle lifecycle | h3_pool_test: repeated same-origin errors, retained sibling, canceled failed dial, close continuation; original live-connection-growth peer |
| Dynamic QPACK responses and trailers | private receive codec, critical streams, joined per-client workers | native ConfigureH3 SETTINGS; declared table/blocked ceilings and exact policy | independent raw QUIC representations, blocked/canceled/sibling sections, wrap/eviction, error codes, bounded feedback and public direct/Fixed/Follow dynamic consumers |
| Header ordering, jar, redirect policy and restricted middleware | hooks.go, request_options.go, native profile functions | Native Headers/Jar/CheckRedirect/middleware | existing request/routing/recheck suites; multipart boundary corruption refusal |
| Native ordered incremental multipart | multipart.go, body.go; shared native encoder and managed producer | RequestOptions.Multipart, Field, Part.Input/Open | multipart_test: peer prefix before tail generation, native order/duplicate replacement, stable boundary, exact replay bytes and cleanup |
| Explicit native H1 fallback after H3 error | dedicated managed H1 transport and independent ALPN; shared TCP quota | PreferHTTP3 and replayable request input | routing_limits_test: H1-only and h2-first peers both receive H1, one-shot and H3 cancellation refuse replay |
| Managed DNS callback and connection | FathomrySetResolver, bounded route panic channel, existing TCP/UDP/work ownership | NativeOptions.Resolver | public resolver tests: live DNS success/refusal, PacketConn framing, partial/nil inputs, late callback/Close, shared healthy waiter and panic cause custody |
| Native timing and decoding selections | native retry/client/idle options and decode middleware | RetryDelay, AdmissionTimeout, Timeout, IdleConnTimeout, DisableCompression | settings_behavior_test, options_behavior_test: observed delay and cancellation, admission rollback, active-vs-idle TCP, distinct QUIC policy, raw-vs-decoded gzip on six paths |
| Native parser versus retained header bounds | protocol-native header accounting and metadata validation | MaxNativeHeaderBytes, MaxHeaderBytes | native_header_limits_test: exact native boundary versus one less with identical retained data across H1/H2/JA/h2c/H3 |
| One-shot/replayable upload distinction | preflight, eager replay reader acquisition, GetBody registry | Part union and native retry settings | one-shot retry pre-admission refusal,307 retention, redirect decline with unused replay, alias/factory-error no-second-send |
| H3 rejected-request replay | prior body close and per-request native writer fence | same Multipart/Open factory contract | h3_multipart_test: quota1/2 complete replay, deterministic old-writer gate, one-shot reset refusal; old-source race failure retained |
| Producer/input/callback failure and cancellation | independent pipe/input closure, activity join, notice custody | InputErrorsCopy, receipts, Stream.Close | multipart_lifecycle_test: encoded limit, reader+error custody, late factory, blocked Close, ordinary body cannot inject producer inspection |
| Finite/incremental response, encoded/decoded integrity | request.go, stream.go, native framing | Do/Open, immutable Result/Metadata | existing integrity/compression suites; public truncation, timeout and stream EOF/Close tests |
| Controlled proxy/SOCKS/dial/resolver/packet paths | owner, route identity, native control hooks | explicit runtime routes and Native callbacks | existing proxy/SOCKS/routing tests, no-direct-fallback/credentials/cancellation controls |
| Shared quotas, evidence and generation overlap | source/private invocation plus public single-root bridge | Runtime/Inbox, Using, Fixed/Follow | public queue/evidence saturation, direct consumer; Framework old streams/sockets and failed/obsolete/oversized candidates |
| Failure identities, privacy and distribution | errorbridge, immutable SDK replacements | Network0x202, Definitions/Resources, Build | public errors/serialization/fuzz, actual CLI catalogs, independent public modules, nested consuming-graph tests, versioned root ZIP/generated consumers |

The interface specifies every Settings and NativeOptions field and its applicability.
There is no raw Builder/client/transport, arbitrary client middleware, WebSocket/101,
automatic profile/provider rotation, anti-inspection, HTTPS rewrite or new business
retry/credential policy. Native parsers need no owning SDK escape and are not
duplicated. Technical evidence does not resolve preserved source-license questions.
