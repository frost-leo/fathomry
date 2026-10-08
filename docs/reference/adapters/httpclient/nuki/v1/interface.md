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

# Nuki HTTP Adapter interface

[Documentation](../../../../../README.md) / Public Adapter reference

**Audience:** independent Go applications and Framework composition owners.
**Status:** implemented native-preserving boundary with local protocol and
public-consumer tests; not production deployment or upstream release certification.
**Package:** `github.com/frost-leo/fathomry/adapters/httpclient/nuki/v1`.

## Responsibilities

Expose the selected **nukilabs/tlsclient** integration, not bogdanfinn/tls-client.
The Adapter preserves native request extensions and callback lifetime while
using the existing [operation runtime](../../../v1/interface.md) and
[source generations](../../../../resource/v1/interface.md). There is no second
admission engine, concrete-provider dependency, SDK client escape or Session API.

The [capability matrix](capabilities.md) connects supported public routes to
Internal/native authority and executable checks. Native protocol applicability,
TLS/QUIC/profile choices and controlled proxy limits remain provider-specific.

## Prepare and compose explicitly

1. Select loadable `Settings` and explicit `NativeOptions`; a nukilabs profile is
   required. `Validate` checks data only. `Prepare` also freezes the native
   selection without dialing or invoking lazy profile factories.
2. Obtain `Prepared.Policy()`, or `Compose` all simultaneously owned
   preparations, including retired generations and pending candidates.
   `Recommend(settings, native)` is a convenience for that exact one-source
   selection; it never chooses a browser/profile automatically.
3. Construct the caller-owned `adapters.Runtime` and `adapters.Inbox[Result]`
   from the declared policy. `Prepared.Open` requires empty
   `Dependencies.Native`; a second native selection is rejected.
4. Use `Owner.Client()` directly, or publish `Owner.Handle()` in a typed
   `resource.Instance` with `Release: owner.Release`. `Using` borrows Fixed or
   Follow generations with an explicitly frozen root/evidence budget.
5. Close every non-nil Owner, including one returned with an error. Drain required
   evidence independently; do not close shared mechanisms from a Client.

Internal owns the effective limits and source/root/evidence formulas. The public
policy adds its own bridge and attribution custody; it does not duplicate native
defaults or resize a Runtime. `Compose` accepts 1–16 preparations and refuses
unrepresentable aggregates. A newly adopted source larger than `Using`'s budget
is refused before native dispatch. Configured ceilings are not free capacity,
measured heap/RSS, server quotas or automatic generation reservations.

`Settings` is strict JSON/configsource data with matching explicit mapstructure
keys. Duration fields use integer nanoseconds. Nil pointers select Internal
defaults; explicit zero/false are validated as supplied. Configuration, API, SDK,
replacement-module and deployment versions are separate axes.

## Requests and callback lifetime

`Do` accepts the native `github.com/nukilabs/http.Request` and returns a public
receipt before response completion. It retains bounded finite response data.
`Consume` returns a receipt and runs a callback with a read-only `Response`.
It is **not** a retained `Open` stream: there is no Response.Close, underlying
Body, PacketConn, owning tunnel, direct connection or raw QUIC API.

Source-independent request/options bounds are checked before public admission,
container copying or input acquisition. After adopting a generation, Internal
validates and snapshots against that exact source before its own admission.
Keep request/options containers immutable until Do/Consume returns, including
queue waiting. Body/GetBody transfer the documented native borrowing and Close
obligations only after native admission, through actual release.

The explicit method context replaces Request.Context. A canceled receipt waiter
does not cancel the operation. Native work follows method, source and Runtime
lifetimes and the selected operation timeout. `WithID` creates an immutable
correlation view, not another pool or admission allowance.

Native `Priority`, `ExcludedCookies` and `DiscardResponseCookies` remain
native fields, not a lossy net/http conversion. The selected H1 writer deliberately
omits the canonical Priority header; preserving the field does not invent H1 wire
priority. The independent H2 frame test verifies the selected profile's actual
priority mapping. Cookie exclusion and response-cookie discard retain their native
effects. Explicit provider/direct/per-call proxy choices and CONNECT headers do
not mutate a shared native client.
ForceHTTP3 cannot override a source-disabled protocol or authorize direct
fallback after proxy failure.

The callback must obey its context and cannot retain the reader or start
unaccounted work. New reads are revoked when the callback returns. Already-entered
reads and their errors remain owned until actual termination; callback return
does not release the generation early. Metadata may be retained as detached
immutable observations. Reads are serialized by Internal.

## Source and evidence ownership

Native TLS/QUIC/profile containers freeze at Prepare. Jar, tracker, keys, session
caches, randomness and admitted callbacks remain cooperative concurrent borrows
until source release. RootCAs and ClientCAs are immutable borrowed trust pools:
their selected pointers freeze, not the pool contents. Do not mutate those pools
while a preparation or source uses them. Clone a pool before caller mutation and
prepare a new generation to adopt changed trust. This explicitly replaces the
older Internal pool-copy behavior; foreign trust storage is not source-accounted.
Dial/Listen callbacks transfer ownership of physical
sockets to the source. Proxy TLS identity is separate from origin TLS; the Nuki
resolver dependency retains its own documented DNS/routing authority. A custom
Resolver.Dial transfers sockets into source accounting and entered-I/O joining.
Without that callback, platform resolver machinery remains borrowed; its internal
descriptors are not claimed as controlled source sockets.
`MaxProxyTunnels` bounds both controlled CONNECT and CONNECT-UDP tunnels, including
multiplexed tunnels sharing one physical socket. Owned session caches are bounded,
charged and cleared at source shutdown; explicitly borrowed caches remain
caller-owned.
Controlled MASQUE admits bounded fixed-authority, ASCII level-3 target templates
and structured true Capsule-Protocol values with ignored unknown parameters.
Malformed/duplicate negotiation and prohibited response content semantics are
refused. Type-0/context-zero capsules and QUIC datagrams use one bounded owned
ingress queue; unknown capsule types/context IDs are streamed past. Complete EOF,
truncation and reset remain distinct, with joined native receivers and entered
I/O. Queue saturation or terminal closure may drop packets, and local send
acceptance never proves delivery. This routing capability still exposes no raw
PacketConn or owning tunnel.

Prepared source accounting includes native proxy ingress/parser residence and
the larger effective initial/maximum QUIC receive windows. Lazy ClientHello
bounds precede and follow controlled ALPN transformation, and unrepresentable H3
settings are refused before native work. See the Internal contract for exact
protocol/profile applicability rather than inferring it from field retention.
The operation envelope separately covers native connection graphs retained by
entered callbacks and prior redirect responses, up to the selected exchange
ceiling. They do not disappear from accounting when a socket closes or a cache
slot is reused. An outer CONNECT TCP/H2 graph is separate from its inner
origin graph and follows its own selected native defaults, not a second copy
of the origin's custom profile. Terminal QPACK Close clears owned table storage; finite encoder
input EOF and independent application result retention remain distinct. Policy
composition consumes these Internal declarations rather than duplicating their
formulas or assuming worker joining forces Go heap reclamation.

A source-open operation reserves residence and evidence until source shutdown.
Each request retains its source, actual-work guard and private native record
through native release and public evidence transfer. Use `Inbox.NextReleased`
to receive finished operations without blocking behind a live source-open record.

`Owner.Close` seals new work and joins callbacks, entered reads and native
shutdown without new operation/evidence admission. A timeout stops only that
wait; cleanup remains reachable on the same Owner. `ShutdownComplete` means
confirmed local termination, not successful remote cleanup. Acknowledging evidence
is separate; `Delivery.Retry` requeues facts and never resends HTTP.

## Results, errors and diagnostics

`Result.HasData`, finite `DataCopy`, metadata, trailers, encoded/decoded byte
counts, input notices, native exchange/replay counts and attribution remain
separate. An absent body, finite empty body and Consume's unretained body differ.
Unknown ContentLength/EncodedContentLength are -1.

`Complete` means final native response EOF plus framing/decode checks.
Returning a callback early leaves Complete false without inventing a read error.
HTTP status, EOF, local release, evidence acknowledgement and business completion
are not interchangeable. Cancellation does not prove absent remote effects.
Observed exchanges/attempts are not necessarily exact wire totals.

Network facility `0x204` / `http_nuki` owns the provider's stable public
definitions. Shared public occurrences remain public; bounded errorbridge
translation retains intentional native errors.Is/As and independent primary and
cleanup causes. Error strings/formatting are redacted; deliberate native cause,
URL/header or Settings JSON inspection can reveal sensitive values.

`Info`, `Profile` and `Build` expose local preparation/effective selection and
actual binary module provenance, not source attestation or deployment readiness.
Runtime handles refuse JSON serialization; Settings permits explicit JSON and
redacts ordinary fmt/slog. Normalize optional typed-nil Settings pointers before slog.

## Executable boundaries

- [Public preparation, callback and ownership tests](../../../../../../adapters/httpclient/nuki/v1/).
- [Independent direct consumer](../../../../../../adapters/httpclient/nuki/v1/testdata/direct/main.go).
- [Framework Fixed/Follow consumer](../../../../../../adapters/httpclient/nuki/v1/testdata/framework/main.go).
- [Internal contract](../../../../internal/httpclient/nuki/v1/interface.md) and
  [shared HTTP vocabulary](../../v1/interface.md).

Development consumers explicitly replace the root and all five selected Nuki
nested modules. Published consumers must use the existing generator's immutable
replacement pins: root module replacements are not inherited and nested modules
are absent from the root ZIP. A root compilation or development replacement
consumer is not proof of module publication.
