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

# HTTPcloak Adapter interface

[Documentation](../../../../../README.md) / Public Adapter reference

**Audience:** applications using the selected HTTPcloak transport capabilities.
**Status:** implemented native-preserving boundary with local protocol and
public-consumer tests; not production deployment or upstream release certification.
**Package:** `github.com/frost-leo/fathomry/adapters/httpclient/httpcloak/v1`.

## Responsibilities and sequence

This Adapter exposes the selected HTTPcloak transport/fingerprint/DNS/proxy
boundary, not the upstream Client/Session platform. It uses
[public operations](../../../v1/interface.md) and
[resource generations](../../../../resource/v1/interface.md); another concrete
HTTP provider is not part of its runtime.

1. Supply strict-loadable `Settings` and separate runtime `NativeOptions`.
   Exactly one named preset, strict preset JSON or native preset is required.
2. Call `Prepare`, then `Prepared.Policy`. Preparation freezes the exact
   selection, including a named registry entry, before any source is constructed.
   Later registry/caller mutations cannot change that preparation's budget or
   constructed preset. `Recommend(settings, native)` uses this same preparation.
3. Create caller-owned Runtime and Inbox from the recommendation, or compose all
   simultaneous owners with `Compose`. The recommendation covers one source,
   not arbitrary concurrent or retired generations.
4. Use `Prepared.Open` with the prepared native dependencies already frozen;
   supplying another native selection is an error. `Open` is the convenience
   preparation/construction path. Construction is local ownership, not readiness.
5. Use `Owner.Client`, or bind its non-owning `Handle` into Framework
   Fixed/Follow resources and call `Using`. Streams and late callbacks retain
   the generation actually used. An adopted larger generation is refused before
   native dispatch instead of enlarging the caller's Runtime.
6. Receive required evidence independently. Close the owner and acknowledge
   completed evidence; neither operation substitutes for the other.

`Build` inspects actual executable/module metadata. `Profile` reports frozen
non-secret source choices, not browser fidelity or deployed-service certification.

## Settings, native authority and budgets

Settings have explicit JSON/mapstructure names and no runtime callbacks or custom
codecs. Durations are integer nanoseconds. Nil pointers select Internal defaults;
explicit zero/false/empty values retain their meaning and may be invalid.
Protocol omission selects H2; it does not choose a browser or race protocols.
`ResolverNetwork` is a pointer: omitted selects UDP, explicit empty is invalid.
An empty resolver address grants no ambient resolver authority.

Internal owns all effective defaults and native work/evidence/source envelopes.
Public policy adds bridge/attribution/private-record storage only. Source budgets
include actual preset/codec/receive state, ECH/DNS work and physical socket, QUIC
and proxy-control resources. Logical tunnels and QUIC connections are not equated
to one socket. TLS compressed-certificate expansion and codec scratch are separate
from compressed wire limits. These are conservative declarations, not hard RSS,
foreign callback memory or remote capacity.
Root work also covers one selected native binding graph per possible exchange:
closed redirect bodies can retain evicted H1/H2/H3 bindings until the operation
ends. This is separate from cached source residence and final evidence storage.
Larger exchange/concurrency limits therefore require a larger explicitly supplied
Runtime; an older smaller policy is refused, not expanded during construction.

Native preset/configuration containers and verification wrappers are copied.
Opaque RootCAs pools are caller-owned immutable borrows through actual source
release; their pointers are frozen, not their foreign storage. This deliberately
replaces the earlier Internal pool Clone behavior, which could allocate unbounded
containers before validation. Callers requiring independent trust-set mutation
must prepare their own clone/new pool and a replacement source generation.
Jar, verification/redirect callbacks and key-log writers remain cooperative
concurrent borrows. They must not retain owning native handles, synchronously
close their own operation/source, or start unaccounted work. Root/queue/evidence
policies are never silently expanded.

## Protocols, ECH and routes

H1/H2/H3 are explicit. Native TCP/QUIC hello and H2/pseudo-header settings retain
their real protocol applicability; incompatible or inert combinations are refused.
Speculative TLS optimization is refused rather than accepted as an ineffective
managed switch. H1 supports ordinary cleartext HTTP; there is no forced HTTPS
rewrite or automatic protocol downgrade.

H1 does not apply ECH and refuses explicit ECH inputs. H2 supports explicit bytes
or authorized configured-domain discovery; H3 distinguishes disabled, explicit
and discovery paths. Disable prevents discovery. ECH state is source-owned,
bounded and copied, with configured DNS authority and actual-work cancellation.
Configuration/discovery is not evidence that a handshake accepted ECH: native
best-effort/fallback semantics remain. Verification callbacks expose genuine TLS
observations; no universal name-concealment guarantee is made.

Configured/direct/per-request routes remain distinct. Ordinary CONNECT/SOCKS TCP
and controlled selected-native SOCKS UDP/MASQUE paths retain independent proxy
and origin trust, authentication and DNS placement. Proxy failures never fall
back to direct networking. Address races have bounded candidates/parallel legs
and join losing work. Proxy/control/tunnel handles do not escape.

`MaxControlStreams` concerns owned CONNECT-UDP control requests; each native QUIC
budget also includes its fixed critical streams, permitted incoming streams,
QPACK state and joined protocol workers. Source replacement does not erase
retired-generation resource charges.

## Native requests and response lifetimes

Requests use `github.com/sardanioss/http.Request`, not a conversion through
standard net/http. Preserve request containers until Do/Open returns; bodies and
replay factories remain borrowed through actual operation release and are closed
natively. Bounds are checked before copying/input acquisition and native admission;
public hard-envelope preflight also occurs before public admission. Server-only
state and unmanaged cancellation handles are refused.
Native `GotConn` trace callbacks on either context are refused because they expose
owning sockets; supported `WroteRequest` and `Got1xxResponse` callbacks remain.

Both the method context and native Request.Context can cancel work. This is not
Surf's method-context replacement contract. Finite `Do` has a separate cleanup
wait context: expiration stops waiting, not actual cleanup. A non-nil receipt
identifies accepted work even when headers or final results are not yet available.

RequestOptions preserve route selection, proxy-only CONNECT headers, native header
order, exact casing/duplicates, TLS-only header mode, client-hint suppression and
redirect policy. ExactHeaders replaces ordinary/preset headers and disables the
jar for that request. Redirect callbacks are metadata/veto-only, not owning
request/response mutation. There is no business retry or credential refresh.

`Client.Open` returns a genuinely retained response Stream. Read uses its
existing reservation; Close bypasses new admission and remains mandatory after
EOF. Repeated Close may observe continuing cleanup. Owner Close seals new work,
cancels owned activity and joins actual release; a timed-out Close preserves this
same reachable cleanup authority. Client and Handle cannot close the source.

## Results, failures and evidence

Result preserves native status/protocol/URL, copied headers/trailers and optional
header-order/casing observations, finite data, encoded/decoded/input byte facts,
input completion, write observations, exchanges, replays and callback causes.
Nil optional observations remain unknown. Finite retained data differs from stream
consumption; partial data is not discarded merely because a failure occurred.

HTTP status, full EOF, input consumption, local release and evidence
acknowledgement are separate. Complete is not business success or absence of
remote effects. Cancellation does not prove that an external operation never
occurred. Native write notifications and exchange counts are not exact wire
attempts. Evidence Retry redelivers evidence; it does not retry HTTP.

Errors use stable Network facility `0x203` / `http_httpcloak`, preserving intended
native errors.Is/As and shared public occurrences through the existing error
bridge. Runtime values refuse serialization; ordinary formatting/slog is redacted.
Deliberate data/native-cause inspection and Settings serialization can be sensitive.

## Evidence and limits

See [source declarations](../../../../../../adapters/httpclient/httpcloak/v1),
[focused public tests](../../../../../../adapters/httpclient/httpcloak/v1/behavior_test.go)
and the [Internal contract](../../../../internal/httpclient/httpcloak/v1/interface.md).
Selected-source corrections and notices remain in third_party; code qualification
does not grant broader redistribution or release clearance. Source-owned local
protocol tests are not production deployment or throughput certification.
