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

# Local Surf HTTP/3 transport corrections

**Status:** narrowly scoped dependency replacement for Surf issue #50.

- Module: `github.com/enetx/http3 v1.0.9`.
- Origin: `ba6a50293c3c477f83648f3faa9e58adaa807206`.
- Compatibility revision: `v1`.
- Original source hashes: [UPSTREAM.json](UPSTREAM.json).

Native request-writing goroutines retain registered work through body writes,
trace callbacks, trailers and stream closure. A returned response or canceled
body does not independently certify that this work has ended. Declared body
lengths also reject premature EOF rather than accepting a truncated prefix.

The module archive and inspected upstream license endpoint provide no standalone
LICENSE file. The upstream README identifies its quic-go HTTP/3 basis, but that
reference is not a license grant for every fork contribution. Existing notices
are retained; redistribution remains an owner-review prerequisite. First-party
files retain the complete project notice without relicensing upstream material.

Run `TestFathomry*` with the consuming dependency selections. Real loopback H3,
body/trust/encoding and blocked-native-callback controls live in the Surf Provider
suite. Official quic-go peers are separate from this modified client dependency.
No production-site or universal H3 fingerprint claim is made.
