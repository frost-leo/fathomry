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

package http2

// FathomryMaxReadFrameSize returns the receive-frame bound used by construction,
// including native clipping and defaults, without opening a connection.
func (transport *Transport) FathomryMaxReadFrameSize() uint32 {
	return configFromTransport(transport).MaxReadFrameSize
}

// FathomryMaxDecoderHeaderTableSize returns the actual receive-table ceiling,
// preserving the explicit zero SETTINGS correction used by construction.
func (transport *Transport) FathomryMaxDecoderHeaderTableSize() uint32 {
	maximum := configFromTransport(transport).MaxDecoderHeaderTableSize
	if transport.Settings != nil {
		maximum = initialHeaderTableSize
	}
	for _, setting := range transport.Settings {
		if setting.ID == SettingHeaderTableSize {
			maximum = setting.Val
		}
	}
	return maximum
}
