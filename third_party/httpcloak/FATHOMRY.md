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

# HTTPcloak local compatibility boundary

**Status:** local #51 compatibility implementation; not an upstream release or a
blanket qualification of every SDK API.

The original MIT [LICENSE](LICENSE) is unchanged. [UPSTREAM.json](UPSTREAM.json)
pins v1.7.2, commit `30379124605d212efe5240aacd60bd4b9ab7c6cf`, module sums,
the original file hashes and the Go 1.26.0 declaration. The inspected Git tag and
module-proxy origin match. First-party additions do not relicense upstream files.

The separately consumed `github.com/sardanioss/http` v1.2.0 retains Go BSD-style
source notices, but its inspected module and
[pinned repository root](https://github.com/sardanioss/http/tree/a5c3cf6aa4c37ce8865b39c938483e4fd0fc92eb)
contain no top-level LICENSE file. This packaging/provenance gap remains explicit;
HTTPcloak's MIT license is not a substitute for that dependency's notices.

## Maintained boundary

The copied runtime packages are fingerprint, transport, DNS, proxy, protocol and
their internal helpers. Bindings, examples, higher-level Client/Session and upstream
test suites are not copied. The Provider consumes `FathomryTransport`, not the
buffered SDK `Transport.Do`, mutable Session setters or the auto-racing dispatcher.

- Clone custom raw ClientHello/PSK bytes and all ClientHelloID pointer state.
  Registry insertion snapshots its input; strict insertion is atomic.
- Construct from a private preset rather than temporary global registration.
- Supply an immutable managed TCP connector, native receive-header bounds and raw
  response bodies. Separate optional native response ordering/casing observations
  from bookkeeping keys. Bounded H1 parsing does not collect response spelling.
- Apply native headers to a fresh container, preserving case-sensitive exact H1
  fields without duplicate framing. Enforce the effective merged header budget.
- Bound known-length H1 writes before bytes cross the socket, detect short or
  surplus input, preserve terminal-read/Close failures, and honor parsed request
  and response Close fields. Cancellation/replay errors retain initiating causes.
- Preserve exact H2 Cookie field pairs using an immutable per-request marker,
  without changing a reusable connection's cookie-splitting settings.
- Consume bounded H1 informational responses through the final response, retaining
  veto callbacks and one aggregate receive-header budget across those blocks.
- Keep managed ECH failure state local rather than consulting or mutating the
  process-global fallback state. Provider credentials are seeded once so native
  redirect filtering cannot be undone by reapplying preset defaults. Removing
  credential-only ordered headers cannot reactivate an inactive legacy map.
- Do not inject default Go/QUIC User-Agent headers into exact/TLS-only requests.
- Join address-race losers, DNS/ECH work and TCP cleanup tasks.
- Join and preserve direct H3 cleanup, including the actually owned UDP socket.
- Use the matching local HTTP/2 and HTTP/3 request-lifetime completion boundaries.
  ALPN errors lose their owning TLS handle only after that handle is reclaimed.

The Provider controls exclusive checkouts, dynamic TCP routes, actual socket
admission, decoded/wire bounds and native callback evidence. No business preset,
Cookie/token lifecycle, rotation, distributed cache or retry policy is installed.
Original mutable/refresh/cache/racing paths outside this managed entry remain
unqualified. Their presence in retained source is not a support claim.

## Upgrade checks

`TestNativeCompatibilityUnitControls` runs every maintained native regression
against consuming dependency selections with offline, read-only module resolution.
Provider loopback tests additionally verify real H1/H2/H3, encoded framing, request
replay, cancellation, callback exit and shared source limits. Private failure,
repair and review logs remain in the owner-local #51 directory, not this repository.

Runtime files also receive Go formatting required by repository checks. All
semantic changes remain identifiable against the original hashes. Re-evaluate
these patches before SDK upgrades or framework releases; do not retire them from
a version number or release note alone.

[Issue #51](https://github.com/frost-leo/fathomry/issues/51).
