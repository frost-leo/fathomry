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

# Local Nuki compatibility work

**Status:** local correction v2 for the internal and public Nuki capabilities. Native
regressions and loopback behavior do not establish production support or resolve
the separate HTTP/SOCKS publication-permission concerns.

## Source and separate versions

- SDK: github.com/nukilabs/tlsclient v1.8.8.
- Tag and module-proxy origin: a39f4b559907b309f376eb1022172f03d35102e2.
- Local correction: v2, exported as FathomryCompatibilityRevision.
- Go minimum: upstream 1.27; the owner permits project Go 1.27.0.
- Configuration/native/request contracts and Provider import major remain separate.
- UPSTREAM.json records original runtime-file hashes and module provenance.
- Root runtime Go files, profiles, proxy, bandwidth, module metadata and LICENSE
  were copied. Upstream public-site tests, examples and source-generation tools
  are not product fixtures and were not copied or represented as passing tests.
- Original LICENSE is retained byte-for-byte. Project additions do not relicense
  upstream material. The separately used nukilabs/socks module lacks a supplied
  license; its directory records that unresolved publication concern.

## Current corrections

- No-follow preserves the first response rather than setting a nil callback.
- Hook recursion is request-local; concurrent calls do not silently skip hooks.
  Post-hook failures preserve causes and close the response they abandon.
- Decoder initialization errors remain observable; deflate retains its raw closer,
  Close-before-read is safe, and zstd decoder work is closed. Bodyless responses
  are not decoded; missing gzip frames do not become valid empty entities.
- Native terminal Close seals transport entry, owns TCP/QUIC sockets and joins
  H3 warming generations. Shared source controls can wrap physical proxy dials.
- Native receive-header and origin-cache ceilings are explicit.
- Profile factories report nil/panic failures instead of silently selecting a
  default; enabled-family UDP resolution uses a cancellation-aware resolver.
- Proxy CONNECT headers are separate, Basic authentication uses decoded URL
  credentials, and physical sessions remain reachable for cleanup.
- CONNECT H1/H2 headers are bounded during decoding, prefetched tunnel bytes are
  retained, and setup cancellation/timeout callbacks are joined before transfer.
- Explicit TLS session caches and destination SNI are honored. A smaller QUIC
  idle timeout remains effective beneath the source ceiling. Native imposed PSK,
  H3 datagram and path-MTU settings remain explicit in the Provider contract.

This is not a general certificate for all native APIs. No business profile,
profile allowlist, token/Cookie refresh, business retry or Provider rotation is added.

The automatic 32-entry session cache is source-owned and cleared on terminal
Close; explicitly supplied caches remain borrowed and are never cleared.
Source-wide tunnel admission covers both HTTP CONNECT and CONNECT-UDP, separately
from the physical proxy socket count. This bounds multiplexed H2 origin handshakes
and cached tunnels rather than treating one proxy socket as one origin resource.

## Dependency corrections and verification

The separately documented [QUIC](../nuki-quic-go/FATHOMRY.md) and
[QPACK](../nuki-qpack/FATHOMRY.md) corrections preserve stream-scoped cancellation
and bounded dynamic decoding. [SOCKS](../nuki-socks/FATHOMRY.md) retains control
and datagram socket ownership. Root go.mod TODOs and source manifests keep these
revisions separate from official releases and other Providers.
The owner-authorized [HTTP correction](../nuki-http/FATHOMRY.md) retains v1.3.2
while making advertised H2 receive windows authoritative, including omitted and
explicit-zero settings. Its source-notice/redistribution record remains explicit.

The Provider suite exercises these modules using the actual consumer dependency
versions, with offline/restricted-cache controls and a combined existing-Provider
consumer. The #121 correction adds bounded lazy ClientHello installation,
explicit resolver authority and controlled CONNECT-UDP templates with separate
proxy TLS. Tunnel addresses are fixed; setup interruption, read/write deadlines
and terminal capsule/I/O cleanup are joined. A source-wide tunnel bound is
separate from physical sockets. Context-aware native datagram enqueue replaces
uncancelable timeout wrappers. Independent local qualification assumes sufficient
encapsulation MTU and does not qualify arbitrary or production routes.
CONNECT-UDP ingress now combines bounded context-zero DATAGRAM capsules and
QUIC datagrams; unknown capsules/contexts are streamed past. The capsule parser
retains clean EOF versus incomplete framing/reset, and Close joins both receive
workers, entered I/O and stopped deadline callbacks before releasing the tunnel.
The selected, already declared httpsfv v1.1.0 parses Capsule-Protocol as a Boolean
Item with ignorable parameters. Duplicate/malformed negotiation and prohibited
content fields/statuses cannot establish a tunnel. No owning PacketConn or tunnel
is added to the public Adapter, and unreliable queue acceptance is not delivery.
Dead origin entries and cached MASQUE outer clients are synchronously retired
through the owning HTTP/3 ClientConn before their slot/pointer is discarded.
Raw QUIC context termination alone is not a control-worker join. Live multiplexed
connections remain in the pool after isolated stream failures.
Private original failures, experiments and owner/agent reviews must not be
uploaded with this source. Repeat the consuming-graph gates before dependency
upgrades and integration changes.
