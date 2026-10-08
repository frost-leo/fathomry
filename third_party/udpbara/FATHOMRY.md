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

# UDP relay compatibility corrections

**Status:** scoped #51 and #120 corrections of `github.com/sardanioss/udpbara`
v1.1.0. The #120 managed boundary admits the selected SOCKS UDP relay path;
HTTPcloak's separate MASQUE path does not use this package.
The current local compatibility marker is `udpbara-v2`.

The original MIT [LICENSE](LICENSE) is unchanged. [UPSTREAM.json](UPSTREAM.json)
records commit `4c13335a27cb4d7769c34823bd556caac512db13`, sums, original runtime
hashes and the Go 1.25.5 declaration.

Pending SOCKS control sockets are registered before greeting I/O; context
cancellation and tunnel shutdown can reclaim them. `FathomryClose` joins accepted
connect/dial operations and relay/control workers, instead of equating a quick
Close return with completion. Native callback cooperation remains required.
Existing source notices are preserved and runtime formatting is normalized.

Managed construction requires explicit TCP/UDP acquisition, close and resolver
hooks and disables automatic reconnection. The source owns the SOCKS control
socket, relay-facing socket and two loopback sockets per native connection.
Relay-domain and target-dispatch DNS use that source's authority, never ambient
DNS. The wire still carries the original target hostname. If an IP-only relay
reply cannot match a local target-dispatch lookup, native broadcast is confined
to that target's managed tunnel; it is not cross-target delivery or direct routing.
Native 7 MiB socket-buffer requests remain native kernel hints, not claimed RSS.

The maintained native regressions exercise both a stalled greeting and an
established relay. They run offline with the consuming dependency versions.
No Redis coordination, application retries or proxy-rotation policy is added.

See the [HTTPcloak boundary](../httpcloak/FATHOMRY.md) and
[issue #51](https://github.com/frost-leo/fathomry/issues/51) and
[issue #120](https://github.com/frost-leo/fathomry/issues/120).
