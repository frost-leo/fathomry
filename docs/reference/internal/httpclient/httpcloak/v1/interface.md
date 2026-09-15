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

Call `Select`, obtain `LimitsV1` for unoverridden Go options, attach them with
`resource.WithLimits`, assemble, then `Bind` with a required evidence Inbox.
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

Version 1 supports explicitly selected HTTP/1.1, HTTP/2 and direct HTTP/3. Go option
zero defaults select technical bounds and HTTP/2, not a browser preset. Layered
values are validated without silently reapplying those defaults.

Native options retain custom fingerprint data, JA3 extras, H2 and TCP settings,
header/pseudo-header order, ConnectTo, ECH inputs, local address binding, TLS
verification, supplied cookie jars and explicit key-log writers. Configuration
containers and root pools are copied; certificate objects already placed in a
pool remain immutable. Closures, jars and writers are borrowed through source
release and must support concurrent invocation.

The managed path returns wire bytes before decoding. H2/H3 header limits are
effective native receive settings and may alter the advertised receive profile;
this is not a claim of byte-identical browser fingerprint equivalence.
An absent key-log writer does not enable the native SSLKEYLOGFILE fallback.

Important supported-boundary limits:

- There is no implicit protocol downgrade or H3/H2 race.
- HTTP/HTTPS/SOCKS5 TCP proxy routing is supported. H3 UDP/MASQUE proxy routing is
  explicitly refused before native entry; the UDP dependency repair alone does
  not qualify a Provider proxy path.
- Native distributed session-cache backends and mutable client/Session setters
  are not exposed. Original unconsumed SDK paths remain unqualified.
- Bindings use exclusive checkouts through input/body/callback completion. Idle
  connections can be reused, but separate operations are not multiplexed on the
  same H2/H3 binding. Socket exhaustion rejects rather than adding a hidden queue.
- Counters cover admitted calls, retained bytes, managed TCP sockets and direct H3
  UDP sockets. They are not a process RSS/native allocator or DNS-socket bound.

## Runtime input and copying

Use native `github.com/sardanioss/http.Request` values. Headers, URL, trailers and
runtime route containers are copied before queueing. Caller-provided headers,
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
not retarget the operation.
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
and [issue #51](https://github.com/frost-leo/fathomry/issues/51).
Private failed controls and corrective-review logs remain owner-local.
