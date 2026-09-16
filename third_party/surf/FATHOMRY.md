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

# Local Surf compatibility corrections

**Status:** local replacement for `internal/httpclient/surf/v1`, issue #50.
No production, arbitrary-profile or full upstream-suite qualification is implied.

## Exact source

- Module `github.com/enetx/surf v1.0.206`.
- Immutable origin `7da0502899af06f8318f95e632797cb2ac0c6c20`.
- Local compatibility revision `v1`.
- Original file hashes and module/ZIP identity: [UPSTREAM.json](UPSTREAM.json).
- The original [MIT LICENSE](LICENSE) is retained unchanged. Project-authored
  additions have their own complete project notices and do not relicense upstream.

The consuming graph also replaces enetx/http2 v1.0.26 and http3 v1.0.9 with
separately recorded corrections. Do not test against independently resolved
upstream module defaults and call that the Fathomry dependency combination.

## Corrections and ownership boundary

The changes preserve external native profiles, per-connection custom Hello/shuffle,
H2/H3 configuration, middleware and native status retries. They correct JA custom
roots and causal TLS errors; configuration copying; request/response failure
cleanup; replay factories; encoded and decoded completeness; decoder-pool close
ordering; proxy routing, trust and H2 CONNECT lifetimes; and owned TCP/UDP,
SOCKS-association, dial, callback and writer shutdown.
The corrective controls also cover clean versus joined EOF, separately typed
primary/cleanup failures, callback panic containment, post-handshake cache
ownership, explicit JA cache precedence, case-insensitive CONNECT authorization,
H2 CONNECT response-header bounds and SOCKS4 USERID/fragmented replies.
Explicit proxy resolver failures no longer fall through to a different resolver.

`FathomryControlV1` is an internal integration hook, not a public application
client. It connects existing Fathomry admission and work ownership to actual SDK
resources. The Provider never returns its native client or transports. Native
middleware views expose metadata only. Custom functions/keys/session caches remain
borrowed under their stated lifetime and concurrency contract; arbitrary Go code
is not sandboxed.
Notification-style TLS callbacks cannot report errors to the TLS stack. Their
panic disables further native work, cancels the route and remains in Close;
borrowed callback ownership is retained until notification reporting finishes.
Connection cleanup errors also stop new native work on that route; outstanding
bounded acquisitions still close and report their causes. This prevents an
unbounded lifetime error chain, without hiding cleanup failures or evicting routes.

H3 uses native crypto/tls, not JA/uTLS. Its fallback is standard TLS and is reported
by the actual response protocol. H3 transport auto-decompression is disabled so
Surf's controlled decoding can verify the encoded framing first. Local closure is
not evidence of remote rollback or exactly-once effects.

SOCKS5 association ownership includes the TCP control connection, UDP socket and
QUIC transport. Datagram deadlines delegate through the managed packet interface,
rather than depending on a concrete `*net.UDPConn`. Remote QUIC termination also
retires its association; this is native cleanup, not business Provider eviction.

## Verification and maintenance

Maintained native controls are `TestFathomry*` in this module and the two transport
modules. The Provider's `TestNativeCompatibilityUsesConsumingSelectionsOffline`
uses the actual consuming selection graph, rebases every local replacement and
keeps `GOPROXY=off`, `GOSUMDB=off`, `GOTOOLCHAIN=local`.
It requires that compatibility tests actually execute. Provider tests add real
loopback TLS/H1/H2/H3 and CONNECT/SOCKS peers, independent body/route/lifetime
observations, cancellation and separate evidence. These are not public-site tests.

Before upgrades, rerun the native rejecting controls, protocol, alias, replay,
callback, framing, cleanup and constrained-cache gates. Retire local changes only
after an upstream candidate passes the same actual-consuming-graph checks.
Private experiments, review transcripts and development logs are not shipped.
