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

# Nuki SOCKS ownership correction

**Status:** local compatibility revision v1, not upstream publication clearance.

The exact source is github.com/nukilabs/socks v1.0.1 at
6f45075855e22f75d2c0e69107356f49bea7bf53. UPSTREAM.json records original hashes.
The acquired module contains no LICENSE or other license file; GitHub reports
null license metadata. The parent Nuki module's MIT license is not assumed to
cover this module. Project notices apply to first-party additions and provenance
documents, not an invented upstream grant. Framework integration authorization
does not establish upstream license permission; distribution clearance remains
an explicit unresolved concern.

Local revision v1 changes UDP association ownership: use the native packet-connection
interface rather than a hard *net.UDPConn cast, close the control connection with
the UDP socket, and join its monitor. Read/write buffers are serialized and malformed
or foreign relay frames are rejected before slicing their payload.
Failed physical Close can be retried. A separate ReleaseConfirmed observation
requires closed control/datagram resources and a joined monitor; historical
monitor failures do not imply that successfully closed resources remain live.

This remains a local replacement for the existing upstream module, not a separate
process or module used to evade dependency coexistence. Maintained loopback
regressions exercise control/datagram quotas and failed/recovered closure under
the Provider's original shared allowance. They do not establish publication
permission or production proxy qualification.
