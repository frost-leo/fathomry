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

# HTTPcloak HTTP/2 compatibility corrections

**Status:** scoped #51 and #120 replacement for `github.com/sardanioss/net` v1.2.10.

The original BSD-style [LICENSE](LICENSE) and Go source notices are preserved.
[UPSTREAM.json](UPSTREAM.json) records commit
`131e2ee032e4eb76a2e6dbb9a8ea3ab9ccec0f9a`, sums, original runtime hashes and
the Go 1.24.0 declaration.

The copied packages are HTTP/2/HPACK, HTTP helper packages, IDNA and the required
internal HTTP common code. The unconsumed interactive h2i CLI and upstream tests
are not republished. Maintained native
regressions and consuming-version offline checks cover the local changes:

- Treat 304/204 as bodyless rather than inventing a missing representation body.
- In managed mode, wait for the peer's END_STREAM before certifying bodyless EOF.
- Expose an explicit join for request writers, body-close work and native trace
  callbacks. Ordinary canceled Body.Close remains distinct from that join.
- Preserve exact Cookie field pairs through the request-local managed marker,
  without mutating shared transport settings or affecting ordinary requests.
- Use explicitly supplied native SETTINGS values for actual decoder capacity and
  initial stream receive credit, including zero, instead of advertising zero while
  retaining unrelated default credit. Independent local H2 controls observe both
  legal zero/nonzero exchanges and flow-control/HPACK violations at zero.
- Derive connection receive credit from the actual initial WINDOW_UPDATE increment,
  preserving zero's native default meaning. Refuse signed-31-bit overflow and use
  the same effective credit for wire output, local inflow and managed preparation
  metadata. A large stream window cannot mask a smaller connection-level bound.

The current local marker is `v2`. Connection-credit controls use an independent
peer and DATA followed by PING while application reads are held; tests compare
small exact/over-limit credit and a larger legal advertised window, not only a
settings struct or a successful small response.

Only the HTTPcloak managed bridge enables the additional completion contract;
it stops new checkouts before waiting. This is not a general claim that native
Close rolls back remote effects. Runtime formatting follows repository checks.

See the [HTTPcloak boundary](../httpcloak/FATHOMRY.md) and
[issue #51](https://github.com/frost-leo/fathomry/issues/51) and
[issue #120](https://github.com/frost-leo/fathomry/issues/120).
