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

# Selected Nuki HTTP correction

**Status:** owner-authorized local correction v1 for the selected Nuki consuming
graph, not an upstream release or redistribution clearance.

- Module: github.com/nukilabs/http v1.3.2.
- Origin:983638da7de8b192b1e13b7e22f37badfc1854de.
- Original module sum:h1:UAKRsKT+DkfIewI2R4BH8k2C1b3XAanzBHxqCYWW6E0=.
- UPSTREAM.json records original copied-file hashes before correction.
- Root runtime source and the consumed cookiejar, HTTP2/HPACK, HTTP utility and
  internal packages are retained. httptest/internal testcert are required by the
  selected native module tests. Upstream tests, ignored generators/examples,
  cgi/fcgi/h2c/httputil and unrelated assets were not copied or claimed qualified.

The HTTP2 client formerly advertised profile SETTINGS_INITIAL_WINDOW_SIZE while
enforcing an unrelated configured/default4MiB receive window. Corrected client
construction derives receive credit from the actual emitted settings: nil uses
native generated defaults; a non-nil custom list without that setting means65535;
explicit zero and nonzero values remain exact. Invalid settings cannot install an
owned connection. Independent peer controls distinguish advertised credit, bytes
processed before application reads, valid data and flow-control refusal.
The same settings/configuration split previously turned an advertised HPACK
capacity of zero back into a4096-byte decoder table. The decoder now derives its
maximum from emitted settings too; independent static-zero, valid dynamic and
forbidden dynamic-reference controls retain the compression-error distinction.
The inert FathomryMaxReadFrameSize and FathomryMaxDecoderHeaderTableSize helpers
expose the same resolved receive-frame/table limits used by native construction,
including defaults and clipping. Internal
preparation uses it when declaring both live and per-exchange retained H2 graphs;
it does not reproduce another set of native default formulas or open a socket.
The default outer CONNECT proxy graph is resolved independently of a custom
inner origin profile, rather than charging two copies of the origin's settings.

This is a correction of the already selected SDK, not a version upgrade or another
upstream release. One copied CRLF source file is gofmt-normalized for repository
checks; its original bytes remain identified in UPSTREAM.json. This is not another
product module. Root replacements are not inherited by external consumers and a
root module ZIP omits this nested module. The existing generator must deliberately
publish/select the matching immutable pin/checksum before normal delivery passes.

## Source notices and unresolved permission

The selected module ZIP and exact repository tree contain no LICENSE, COPYING or
NOTICE file; repository license metadata was null when checked on2026-10-08.
Original per-file Go Authors/BSD and other upstream notices remain unchanged.
Their references to a source LICENSE file are not silently rewritten, and no grant
for Nuki modifications is inferred. Project GPL notices apply only to first-party
additions/metadata. Owner-authorized integration and protocol tests do not settle
the separate publication-permission concern; existing SOCKS constraints also
remain explicit. Do not fabricate a license or supported-release clearance.
