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

# Selected QPACK decoder correction

This private package retains the decoder from
[github.com/sardanioss/qpack v0.6.3](https://github.com/sardanioss/qpack/tree/99927365012d19777a0828b99fdfe3d8a8402b3a),
not another HTTP provider's codec. [UPSTREAM.json](UPSTREAM.json) records original
file hashes and exact module sums. The original [MIT notice](LICENSE.md) remains
unchanged. The static request encoder and public field identity remain provided
by the already selected external module.

The existing QUIC module owns this decoder because the selected external API
accepts only complete instruction buffers and waits with a fixed polling timeout.
The corrections retain native field decoding while providing:

- Incremental, bounded encoder input, initial zero current capacity and rejection
  of oversized insertions without changing insertion state.
- Exact advertised table/blocked-section ceilings; cancellation and receive
  deadlines without polling workers. Normal FIN is not cancellation.
- Valid Required Insert Count reconstruction and all four dynamic reference
  forms constrained below that count, with overflow-safe base/index arithmetic.
- Explicit shutdown of waits and removal of retained instruction/table storage.
  Upstream decoder positive and malformed-input controls remain executable.

HTTP/3 owns critical-stream processing, ordered insertion credit and section
acknowledgements, exactly-once stream cancellation on abandonment, and joined
bounded feedback/read/control workers. This package exposes no public Adapter
runtime, socket, stream or owning client.

The declared maximum remains 64 MiB of table capacity and 1024 blocked sections.
Encoded incomplete instructions are bounded by twice the advertised table
capacity plus 32 bytes. Native source accounting additionally includes slice
growth, table-entry overhead, Huffman expansion and parsing overlap; wire table
capacity alone is not a memory envelope. These declarations are not hard RSS.

This is a local correction for issue #120, not an upstream release, a new
replacement module or broader redistribution clearance.
