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

# Nuki QUIC compatibility correction

**Status:** local revision v1 for the internal Nuki Provider, not production or
arbitrary QUIC-mode qualification.

Source: github.com/nukilabs/quic-go v1.3.0 at
9486bcdd6e8047e212fcbd653cd258000f8cd401. UPSTREAM.json records original runtime
and unit-test file hashes. LICENSE remains byte-for-byte upstream. Root runtime,
internal, HTTP3, qlog, qlogwriter, quicvarint, metrics and unit-test helpers are
retained. Public interop/example/integration applications and infrastructure
scripts are not product fixtures.
Snapshot whitespace is normalized where required by repository checks; upstream
license notices and original-source hashes are unchanged.

The correction passes each HTTP request's context into QPACK header and trailer
decoding, bounds decoded field accumulation, and closes the decoder on connection
termination. A canceled request does not close its healthy shared connection.
The decoder control writer exists before responses arrive, preventing lost early
acknowledgements. Small instructions use atomic context-aware stream writes, so
cancellation cannot leave a partially queued instruction. Connection-owned,
bounded cancellation delivery is joined on terminal Close; saturation retires the
connection with an explicit protocol error rather than silently losing control.

Early response completion cancels and joins the request upload and trailer writer
without abandoning background stream work. Its upload error remains separately
available after response completion or Close; response EOF is not input delivery.
Native qlog writers preserve encoding/short-write/Close failures, make producer
Close idempotent, and retry unresolved output closure with separate positive
release evidence. Historical encoding failures do not keep closed outputs live.
Raw output/encoding errors are no longer sent to the global logger; their causes
remain available from Close without disclosing arbitrary native error payloads.

The patch changes neither the official github.com/quic-go/quic-go module nor its
shared QPACK selection. It requires the separately versioned local Nuki QPACK
correction. Go 1.27 and the selected uTLS stack remain genuine requirements; no
cryptographic backport or language-version-only downgrade is supplied.

Original failure controls and owner/agent reviews remain private. Maintained
native unit tests, the Provider's independent-peer cancellation/isolation test,
and restricted-cache consumer-version checks are the executable review surface.
