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

package objectstore_test

import (
	"testing"

	"github.com/frost-leo/fathomry/adapters/objectstore/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestPolicyOverlapAndBounds(t *testing.T) {
	base := objectstore.Policy{Budget: objectstore.Budget{WorkBytes: 100, EvidenceBytes: 20}, Runtime: adapters.Options{MaxActive: 2, MaxQueued: 1, MaxWorkBytes: 200, MaxQueuedBytes: 100}, Evidence: adapters.EvidenceOptions{Capacity: 4, MaxBytes: 80}}
	combined, err := base.ForGenerations(2)
	if err != nil || combined.Budget != base.Budget || combined.Runtime.MaxActive != 4 || combined.Evidence.MaxBytes != 160 || base.Runtime.MaxActive != 2 {
		t.Fatal("policy mutation or undercharge")
	}
	for _, count := range []int{-1, 0, 17} {
		if _, err := base.ForGenerations(count); err == nil {
			t.Fatal("invalid multiplier")
		}
	}
	huge := base
	huge.Runtime.MaxWorkBytes = 1 << 40
	if _, err := huge.ForGenerations(2); err == nil {
		t.Fatal("overflowing recommendation")
	}
}
