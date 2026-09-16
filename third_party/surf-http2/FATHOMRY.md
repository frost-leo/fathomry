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

# Local Surf HTTP/2 transport corrections

**Status:** narrowly scoped dependency replacement for Surf issue #50.

- Module: `github.com/enetx/http2 v1.0.26`.
- Origin: `7d3b7bd8a0bc8a6c27de826aa5e98faf208a61d7`.
- Compatibility revision: `v1`.
- Original source hashes: [UPSTREAM.json](UPSTREAM.json).

Connection construction no longer writes the shared transport's header settings.
The existing native settings and receive-limit interpretation are retained.
`FathomryWithWork` additionally retains each native request writer through its
actual completion, including trace callbacks and cleanup, even if a response or
canceled wait returns earlier. Surf installs it from its controlled request
context. It is not a new application scheduler or retry controller.

The module archive contains no standalone LICENSE file. Existing Go Authors
BSD-style source notices are preserved; the absence of the referenced license
file and the fork's additional contributions require owner review before
redistribution. This record does not assert or grant distribution permission.
Project-authored files carry the full project notice without replacing upstream
notices.

Run `TestFathomry*` with the consuming module's exact dependency selections.
Surf's native and Provider suites exercise the actual protocol and ownership
paths; helper-only passes do not certify transport behavior.
