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

package mysql

import "testing"

func TestEvidenceBudgetUsesNativeDefaults(t *testing.T) {
	for _, input := range []OptionsV1{
		{},
		{MaxRows: 1, MaxResultBytes: 1024, MaxPacketBytes: 1024},
		{MaxRows: 65536, MaxResultBytes: 16 << 20, MaxPacketBytes: 4 << 20},
	} {
		value := defaults(input)
		if got := EvidenceBytesV1(input); got != value.evidenceReservation() {
			t.Fatal("exported evidence envelope diverged from prepared source")
		}
	}
	implicit := EvidenceBytesV1(OptionsV1{})
	explicit := EvidenceBytesV1(OptionsV1{MaxRows: 1024, MaxResultBytes: 4 << 20, MaxPacketBytes: 1 << 20})
	if implicit != explicit {
		t.Fatal("default and explicit native envelopes differ")
	}
}
