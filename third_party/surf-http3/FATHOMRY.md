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

**Status:** narrowly scoped dependency replacement for Surf issues #50 and #119.

- Module: `github.com/enetx/http3 v1.0.9`.
- Origin: `ba6a50293c3c477f83648f3faa9e58adaa807206`.
- Compatibility revision: `v3`.
- Original source hashes: [UPSTREAM.json](UPSTREAM.json).

Native request-writing goroutines retain registered work through body writes,
trace callbacks, trailers and stream closure. A returned response or canceled
body does not independently certify that this work has ended. Declared body
lengths also reject premature EOF rather than accepting a truncated prefix.

Revision v2 additionally bounds cached, pending and retiring native clients via
FathomryAcquireClient, independently of UDP socket count. Response bodies and
request writers retain their actual client until Close/completion; a failed
stream can retire its cached client without killing a retained sibling response.
Removal compares client identity, failed orphaned dials release their slot, and
shutdown cancels all clients before joining them. A rejected-request retry closes
and joins only its previous request writer before acquiring the replay client;
successful early responses remain incremental and one-shot unsafe replay stops.
These are issue119 ownership corrections, not business retry or provider rotation.

Revision v3 implements dynamic response QPACK, including all reference forms,
encoder instructions, blocked sections, informational responses and trailers.
The private decoder reuses the selected static table/Huffman implementation and
adapts bounded table routines with their [MIT provenance](internal/qpack/PROVENANCE.md).
The request encoder remains static; no shared QPACK/QUIC replacement is changed.

Decoder feedback is a bounded connection-owned FIFO. ACK/cancellation ordering and
known received counts are serialized; insert notifications coalesce. Partial write
failure terminates the connection instead of appending to a damaged critical stream.
Client retirement joins accept/parser/control/feedback workers before quota release.
Read cancellation never uses QUIC's completed send-half context. A local decoded
header limit cancels only its section; malformed references/instructions and critical
stream failures use their actual connection error codes. Normal connection shutdown
does not manufacture protocol failure when the QUIC context notification is delayed.

The selected upstream module archive and inspected upstream license endpoint provide no standalone
LICENSE file. The upstream README identifies its quic-go HTTP/3 basis, but that
reference is not a license grant for every fork contribution. Existing notices
are retained; redistribution remains an owner-review prerequisite. First-party
files retain the complete project notice without relicensing upstream material.
Go may include the repository-root license when packaging this corrected nested
module; that mechanical inheritance does not resolve upstream contribution rights.

Run `TestFathomry*` with the consuming dependency selections. Real loopback H3,
body/trust/encoding and blocked-native-callback controls live in the Surf Provider
suite. Official quic-go peers are separate from this modified client dependency.
No production-site or universal H3 fingerprint claim is made.
