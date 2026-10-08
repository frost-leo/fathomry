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

package quic

import "testing"

func TestFathomryReceiveBoundsIncludeLargerInitialWindows(t *testing.T) {
	for _, config := range []*Config{
		{},
		{InitialStreamReceiveWindow: 64 << 20, InitialConnectionReceiveWindow: 64 << 20},
		{InitialStreamReceiveWindow: 64 << 20, InitialConnectionReceiveWindow: 64 << 20, MaxStreamReceiveWindow: 1 << 20, MaxConnectionReceiveWindow: 2 << 20},
		{InitialStreamReceiveWindow: 1 << 20, InitialConnectionReceiveWindow: 2 << 20, MaxStreamReceiveWindow: 64 << 20, MaxConnectionReceiveWindow: 64 << 20},
	} {
		before := *config
		selected := populateConfig(config)
		stream, connection := FathomryReceiveBounds(config)
		if stream != max(selected.InitialStreamReceiveWindow, selected.MaxStreamReceiveWindow) ||
			connection != max(selected.InitialConnectionReceiveWindow, selected.MaxConnectionReceiveWindow) {
			t.Errorf("declared bounds %d/%d omit native receive state %d/%d %d/%d", stream, connection,
				selected.InitialStreamReceiveWindow, selected.MaxStreamReceiveWindow,
				selected.InitialConnectionReceiveWindow, selected.MaxConnectionReceiveWindow)
		}
		if config.InitialStreamReceiveWindow != before.InitialStreamReceiveWindow ||
			config.InitialConnectionReceiveWindow != before.InitialConnectionReceiveWindow ||
			config.MaxStreamReceiveWindow != before.MaxStreamReceiveWindow ||
			config.MaxConnectionReceiveWindow != before.MaxConnectionReceiveWindow {
			t.Fatal("accounting mutated caller configuration")
		}
	}
}
