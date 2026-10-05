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

package kafka

import "testing"

func TestAbsentResultDoesNotBecomePresentEmptyEvidence(t *testing.T) {
	var value Result
	if value.HasData() || value.WritesCopy() != nil || value.ReadsCopy() != nil || value.TopicsCopy() != nil || value.CheckpointsCopy() != nil {
		t.Fatal("absent native data became present empty evidence")
	}
	if _, _, _, observed := value.Page().Watermarks(); observed {
		t.Fatal("zero page manufactured a watermark")
	}
}
