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

import "errors"

// BoundedPinsSnapshot refuses excessive containers before allocating a copy.
func (pinner *Pinner) BoundedPinsSnapshot(maxEntries, maxPins int) (map[string][]string, bool, error) {
	pinner.RLock()
	defer pinner.RUnlock()
	if len(pinner.pins) > maxEntries {
		return nil, pinner.auto, errors.New("tlsclient: pin entry limit")
	}
	for host, values := range pinner.pins {
		if len(host) > 8192 || len(values) > maxPins {
			return nil, pinner.auto, errors.New("tlsclient: pin container limit")
		}
		for _, value := range values {
			if len(value) > 128 {
				return nil, pinner.auto, errors.New("tlsclient: pin value limit")
			}
		}
	}
	result := make(map[string][]string, len(pinner.pins))
	for host, values := range pinner.pins {
		result[host] = append([]string(nil), values...)
	}
	return result, pinner.auto, nil
}
