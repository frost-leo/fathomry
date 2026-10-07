/**
 * fathomry
 * Copyright (C) 2026  Frost Leo
 * SPDX-License-Identifier: GPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package tlsclient

import "math"

// Bandwidth is a read-only source observation from the native tracker. Coverage
// is origin TLS over the dialer's TCP/tunnel stream only: not cleartext HTTP,
// HTTP3, proxy establishment, outer proxy TLS/framing, or per-request attribution.
// Separate counters are atomic observations, not one atomic wire snapshot.
type Bandwidth struct {
	private
	enabled, available bool
	read, written      int64
}

func (value Bandwidth) Enabled() bool             { return value.enabled }
func (value Bandwidth) Scope() string             { return "origin-tls-over-tcp" }
func (value Bandwidth) ReadBytes() (int64, bool)  { return value.read, value.available }
func (value Bandwidth) WriteBytes() (int64, bool) { return value.written, value.available }

// Bandwidth inspects native counters without dispatching network work or exposing
// Reset/TrackConnection authority. Disabled or overflowed observations are unknown.
func (client *Client) Bandwidth() Bandwidth {
	if client == nil || client.owner == nil {
		return Bandwidth{}
	}
	own := client.owner
	own.mu.Lock()
	defer own.mu.Unlock()
	result := Bandwidth{enabled: own.settings.Bandwidth, available: own.settings.Bandwidth && !own.bandwidthUnknown,
		read: own.bandwidthRead, written: own.bandwidthWritten}
	add := func(current *binding) {
		if current.native == nil || current.released {
			return
		}
		tracker := current.native.GetBandwidthTracker()
		var ok bool
		result.read, ok = addBandwidth(result.read, tracker.GetReadBytes())
		result.available = result.available && ok
		result.written, ok = addBandwidth(result.written, tracker.GetWriteBytes())
		result.available = result.available && ok
	}
	for _, current := range own.bindings {
		select {
		case <-current.ready:
			add(current)
		default:
		}
	}
	for current := range own.held {
		add(current)
	}
	if !result.available {
		result.read, result.written = 0, 0
	}
	return result
}

func addBandwidth(total, value int64) (int64, bool) {
	if total < 0 || value < 0 || value > math.MaxInt64-total {
		return 0, false
	}
	return total + value, true
}

// Called only with the owner lock after confirmed native quiescence.
func (own *owner) retainBandwidth(current *binding) {
	if !own.settings.Bandwidth || current.native == nil {
		return
	}
	tracker := current.native.GetBandwidthTracker()
	var ok bool
	own.bandwidthRead, ok = addBandwidth(own.bandwidthRead, tracker.GetReadBytes())
	own.bandwidthUnknown = own.bandwidthUnknown || !ok
	own.bandwidthWritten, ok = addBandwidth(own.bandwidthWritten, tracker.GetWriteBytes())
	own.bandwidthUnknown = own.bandwidthUnknown || !ok
}
