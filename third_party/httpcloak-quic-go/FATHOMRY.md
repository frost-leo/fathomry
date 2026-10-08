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

# HTTPcloak QUIC compatibility corrections

**Status:** scoped local #51/#120 replacement for `github.com/sardanioss/quic-go`
v1.2.29, not the independent official `github.com/quic-go/quic-go` module.

The original MIT [LICENSE](LICENSE) is unchanged. [UPSTREAM.json](UPSTREAM.json)
records commit `305d3f64e530285a25a24cef5b4c1a90a4d717e8`, sums, original runtime
hashes and the Go 1.24.0 declaration. Tag and module origin were checked separately.

The HTTP/3 changes join request-body writers through trace/trailer/stream-close
completion; preserve input, sender and cleanup causes through native error
translation and cancellation; and detect short declared bodies before transparent
decoding. HEAD, 204, 304 and informational responses use their actual body-length
semantics rather than treating representation length as a payload requirement.
Response Close explicitly aborts a still-active request write before joining it;
closing only its input reader cannot unblock QUIC flow control. Informational
callback vetoes remain errors. Removed retry clients are closed by identity without
deleting a replacement connection or abandoning the old connection's cleanup.
Replay factory failures retain the initiating rejection, factory and cleanup
causes; a reader returned alongside an error is closed rather than abandoned.

Response Close may now wait for a non-cooperating input or callback. The Provider
keeps an independently bounded caller wait and retains the source obligation;
a timed-out wait is not a successful native join. Callbacks must not invoke Close
on their own operation.

The copied runtime excludes upstream tests, example applications and unconsumed
generated `internal/mocks` test helpers. Maintained `TestFathomry*` regressions are
not skipped; the full copied module is tested with the consuming selections.
Go formatting of copied runtime source is separate from the scoped HTTP/3 edits.

Issue #120 additionally retains the selected dynamic decoder as a private
[source-recorded correction](internal/fathomryqpack/FATHOMRY.md), without adding
a Go module or replacing the static request encoder. Advertised QPACK capacities,
fragmented encoder instructions, Required Insert Count references, ordered
feedback, blocked-stream cancellation and critical-stream failures now use owned,
bounded paths. Effective receive aborts are observed without treating FIN or
send-half completion as cancellation; read deadlines wake blocked decoding.
Normal native reliable-reset and EOF precedence is retained. Abandoning unread
trailers sends stream cancellation and suppresses later acknowledgements.

The managed client joins critical readers, feedback, stream-abort observation and
datagram receivers as part of FathomryCloseSenders after QUIC shutdown. An
independent critical watcher remains effective during feedback write backpressure.
Context-aware datagram enqueue preserves ordinary SendDatagram while allowing
MASQUE deadline/cancellation to stop waiting without killing sibling streams.
Nil means local enqueue, not delivery; a canceled call returning an error did not
enqueue that packet. Native/source queue and parser storage remain separately
accounted, not reduced to a nominal socket count.

Per-request datagram writes also observe actual local/remote send cancellation,
joining their cancellation callback without confusing receive FIN with abort.
Refused datagram-worker admission closes with H3_EXCESSIVE_LOAD rather than
advertising an unusable receive path. Priority-update writes use cancellable
control-stream admission; an interrupted partial frame terminates the connection
instead of leaving a corrupt shared stream or a blocked operation. Independent
QUIC peers cover active and queued write cancellation alongside healthy controls.
The local compatibility marker is `httpcloak-quic-v2`.

See the [HTTPcloak boundary](../httpcloak/FATHOMRY.md) and
[issue #51](https://github.com/frost-leo/fathomry/issues/51) and
[issue #120](https://github.com/frost-leo/fathomry/issues/120).
