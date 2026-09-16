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

# Nuki QPACK compatibility correction

**Status:** local revision v1 used by the Nuki QUIC correction, not a general
certification of every peer/encoding.

Source: github.com/nukilabs/qpack v0.7.0 at
8b61eff997c65a046f1134614e6ee9a118158313. UPSTREAM.json records original source
hashes; LICENSE.md is retained unchanged. Native root source/unit tests and module
metadata are included; interop applications/examples are not product fixtures.

DecodeForStreamContext adds per-section cancellation without clearing a shared
dynamic table. Cancellation wakeups are joined. Decoded field bytes are checked
before accumulating the eager field list; encoder literal lengths are bounded
before allocation. Decoder-control write failures retain their causes and short
writes are errors. The Nuki HTTP3 integration supplies a context-aware atomic
writer; a generic caller-provided io.Writer still has its declared cooperative
lifetime.

The existing background-context APIs remain available for native callers.
This is the distinct github.com/nukilabs/qpack namespace; it does not replace or
downgrade github.com/quic-go/qpack selected by other Providers. Native regressions
cover canceled versus surviving sections, dynamic limits, literal allocation,
control-write causality and unchanged static EOF behavior.
